"""迁移生命周期：在线回填、断点续跑、校验、原子切换、回退、已完成研究不改。"""
import json,unittest
from tests.helpers import make_world,make_study,seed_legacy,tx
from tests.sqlite_gateway import SqliteCountGateway
from src.services import migration,migration_phases as phases
from src.services import counts_ops
from src.services.count_store import (
    NonIntegralCount,SwitchInProgress,MigrationConflict,coerce_int_values,
)

class LifecycleTest(unittest.TestCase):
    def setUp(self):
        self.conn,self.gw=make_world()
        self.study=make_study(self.conn)
        seed_legacy(self.conn,self.study,12)

    # 迁前旧记录仍可读写
    def test_01_legacy_read_write_before_migration(self):
        st=self.gw.get_state(self.conn)
        self.assertEqual(st['phase'],phases.LEGACY)
        bundle=counts_ops.list_rows(self.gw,self.conn,self.study)
        self.assertEqual(len(bundle['rows']),12)
        self.assertIn('旧浮点',bundle['read_basis'])
        row=tx(self.conn,counts_ops.save,self.gw,self.conn,self.study,'K-9001',
               {'group':'A','success':5.0,'failure':2.0})
        self.assertEqual(row['values']['success'],5.0)  # 旧表仍是浮点 JSON
        self.assertEqual(self.gw.int_count(self.conn),0)

    # 回填分批 + 中断后续跑不重复迁入
    def test_02_backfill_resume_is_idempotent(self):
        tx(self.conn,migration.start,self.gw,self.conn)
        r1=tx(self.conn,migration.backfill_batch,self.gw,self.conn,5)
        self.assertEqual(r1['batch'],5);self.assertFalse(r1['reached_end'])
        self.assertEqual(self.gw.int_count(self.conn),5)
        # 模拟中断：另起会话重跑，游标继续，已回填行只对齐不重复
        r2=tx(self.conn,migration.backfill_batch,self.gw,self.conn,5)
        self.assertEqual(r2['batch'],5)
        # 本用例独立世界共 12 行：前两批各 5，末批 2
        r3=tx(self.conn,migration.backfill_batch,self.gw,self.conn,5)
        self.assertEqual(r3['batch'],2);self.assertTrue(r3['reached_end'])
        self.assertEqual(self.gw.int_count(self.conn),12)
        # 再跑一轮：没有新行，计数不变（不重复迁入）
        r4=tx(self.conn,migration.backfill_batch,self.gw,self.conn,100)
        self.assertEqual(r4['batch'],0);self.assertEqual(self.gw.int_count(self.conn),12)
        # 整数列为原生 int
        int_row=self.gw.int_get_by_natural(self.conn,self.study,'K-0001')
        self.assertIsInstance(int_row['values']['success'],int)

    # 非整数旧记录无法迁移，被记入 skipped 且校验拦截切换
    def test_03_non_integral_record_blocks_switch(self):
        bad={'group':'A','success':3.5,'failure':1.0}
        self.conn.execute('INSERT INTO observations(study_id,source_key,values_json) VALUES(?,?,?)',
                          (self.study,'K-BAD',json.dumps(bad)));self.conn.commit()
        tx(self.conn,migration.start,self.gw,self.conn)
        tx(self.conn,migration.run_backfill_until_end,self.gw,self.conn,10)
        state=self.gw.get_state(self.conn)
        self.assertTrue(any(s['source_key']=='K-BAD' for s in state['skipped']))
        v=tx(self.conn,migration.verify,self.gw,self.conn)
        self.assertFalse(v['ok'])
        self.assertTrue(any(d['source_key']=='K-BAD' for d in v['diffs']))
        # 直接 complete 必须先 begin；begin 后 complete 因差异拒绝切换
        tx(self.conn,migration.begin_switch,self.gw,self.conn)
        r=tx(self.conn,migration.complete_switch,self.gw,self.conn)
        self.assertFalse(r['switched'])
        self.assertEqual(self.gw.get_state(self.conn)['phase'],phases.SWITCHING)
        # 更正旧记录后重新回填、校验、切换成功
        good={'group':'A','success':4.0,'failure':1.0}
        bad_id=self.gw.legacy_get_by_natural(self.conn,self.study,'K-BAD')['id']
        tx(self.conn,counts_ops.save,self.gw,self.conn,self.study,'K-BAD',good,bad_id)
        tx(self.conn,migration.backfill_batch,self.gw,self.conn,100)
        v2=tx(self.conn,migration.verify,self.gw,self.conn)
        self.assertTrue(v2['ok'],v2['diffs'])
        r2=tx(self.conn,migration.complete_switch,self.gw,self.conn)
        self.assertTrue(r2['switched'])
        self.assertEqual(self.gw.get_state(self.conn)['phase'],phases.INTEGER)

    # 正常全流程：校验通过 → 原子切换 → 读取依据变为整数表
    def test_04_switch_changes_read_basis(self):
        tx(self.conn,migration.start,self.gw,self.conn)
        tx(self.conn,migration.run_backfill_until_end,self.gw,self.conn,10)
        v=tx(self.conn,migration.verify,self.gw,self.conn)
        self.assertTrue(v['ok'])
        tx(self.conn,migration.begin_switch,self.gw,self.conn)
        ok=tx(self.conn,migration.complete_switch,self.gw,self.conn)
        self.assertTrue(ok['switched'])
        bundle=counts_ops.list_rows(self.gw,self.conn,self.study)
        self.assertEqual(bundle['phase'],phases.INTEGER)
        self.assertIn('整数专用存储',bundle['read_basis'])
        self.assertEqual(len(bundle['rows']),12)

    # 回退后恢复旧表读写，且保留切换期间的新写入
    def test_05_rollback_keeps_writes_made_on_integer(self):
        tx(self.conn,migration.start,self.gw,self.conn)
        tx(self.conn,migration.run_backfill_until_end,self.gw,self.conn,10)
        tx(self.conn,migration.verify,self.gw,self.conn)
        tx(self.conn,migration.begin_switch,self.gw,self.conn)
        tx(self.conn,migration.complete_switch,self.gw,self.conn)
        # 切换后在整数存储上新增 + 更正 + 移除
        tx(self.conn,counts_ops.save,self.gw,self.conn,self.study,'K-NEW',
           {'group':'B','success':7,'failure':3})
        first=self.gw.legacy_get_by_natural(self.conn,self.study,'K-0001')['id']
        tx(self.conn,counts_ops.save,self.gw,self.conn,self.study,'K-0001',
           {'group':'A','success':99,'failure':5},first)
        gone=self.gw.legacy_get_by_natural(self.conn,self.study,'K-0002')['id']
        tx(self.conn,counts_ops.remove,self.gw,self.conn,self.study,gone)
        # 回退
        r=tx(self.conn,migration.rollback,self.gw,self.conn)
        self.assertTrue(r['rolled_back'])
        self.assertEqual(self.gw.get_state(self.conn)['phase'],phases.DUAL)
        # 读取依据恢复旧表，但期间更改全部保留
        bundle=counts_ops.list_rows(self.gw,self.conn,self.study)
        keys={row['source_key'] for row in bundle['rows']}
        self.assertIn('K-NEW',keys);self.assertNotIn('K-0002',keys)
        updated=next(row for row in bundle['rows'] if row['source_key']=='K-0001')
        self.assertEqual(float(updated['values']['success']),99.0)
        # 两存储仍一致（回退不丢数据），可再次校验并切换
        v=tx(self.conn,migration.verify,self.gw,self.conn)
        self.assertTrue(v['ok'],v['diffs'])

    # 已完成研究的快照不受迁移与切换影响
    def test_06_completed_run_snapshot_immutable(self):
        before=counts_ops.list_rows(self.gw,self.conn,self.study)['rows']
        snap1=tx(self.conn,counts_ops.freeze_snapshot,self.gw,self.conn,self.study)['snapshot']
        self.conn.execute("INSERT INTO runs(study_id,name,status,checksum) VALUES(?,?,'completed','abc')",
                          (self.study,'历史研究'));self.conn.commit()
        run_before=self.conn.execute('SELECT * FROM runs WHERE id=1').fetchone()
        # 全量迁移、切换
        tx(self.conn,migration.start,self.gw,self.conn)
        tx(self.conn,migration.run_backfill_until_end,self.gw,self.conn,10)
        tx(self.conn,migration.verify,self.gw,self.conn)
        tx(self.conn,migration.begin_switch,self.gw,self.conn)
        tx(self.conn,migration.complete_switch,self.gw,self.conn)
        # 切换后修改源数据（用自然键定位，行编号即两表共享的 legacy_id）
        target=self.gw.int_get_by_natural(self.conn,self.study,'K-0001')
        tx(self.conn,counts_ops.save,self.gw,self.conn,self.study,'K-0001',
           {'group':'A','success':1,'failure':1},target['legacy_id'])
        run_after=self.conn.execute('SELECT * FROM runs WHERE id=1').fetchone()
        self.assertEqual(run_before['checksum'],run_after['checksum'])
        self.assertEqual(run_after['status'],'completed')
        # 新冻结取整数表版本，行数不变（更正不增删行），读取依据为整数存储
        frozen2=tx(self.conn,counts_ops.freeze_snapshot,self.gw,self.conn,self.study)
        self.assertEqual(self.gw.get_state(self.conn)['phase'],phases.INTEGER)
        self.assertIn('整数专用存储',frozen2['read_basis'])
        self.assertEqual(len(snap1),len(frozen2['snapshot']))
        changed=next(x for x in frozen2['snapshot'] if x['source_key']=='K-0001')
        self.assertEqual(changed['values']['success'],1)

    # 阶段机非法动作被拒绝
    def test_07_illegal_transitions_rejected(self):
        with self.assertRaises(MigrationConflict):
            tx(self.conn,migration.verify,self.gw,self.conn)  # legacy 不能校验
        tx(self.conn,migration.start,self.gw,self.conn)
        with self.assertRaises(MigrationConflict):
            tx(self.conn,migration.start,self.gw,self.conn)  # 重复 start
        with self.assertRaises(MigrationConflict):
            tx(self.conn,migration.rollback,self.gw,self.conn)  # dual 不能回退

    def test_08_coerce_helper(self):
        self.assertEqual(coerce_int_values({'group':'A','success':7.0,'failure':0.0})['success'],7)
        with self.assertRaises(NonIntegralCount):
            coerce_int_values({'group':'A','success':7.5,'failure':0.0})

    # 更正时改自然键：两存储必须同时改名，且旧键不再残留
    def test_09_rename_natural_key_keeps_stores_in_sync(self):
        tx(self.conn,migration.start,self.gw,self.conn)
        tx(self.conn,migration.run_backfill_until_end,self.gw,self.conn,10)
        row=self.gw.legacy_get_by_natural(self.conn,self.study,'K-0001')
        tx(self.conn,counts_ops.save,self.gw,self.conn,self.study,'K-RENAMED',
           {'group':'A','success':12,'failure':3},row['id'])
        self.assertIsNone(self.gw.legacy_get_by_natural(self.conn,self.study,'K-0001'))
        self.assertIsNone(self.gw.int_get_by_natural(self.conn,self.study,'K-0001'))
        renamed=self.gw.int_get_by_natural(self.conn,self.study,'K-RENAMED')
        self.assertIsNotNone(renamed);self.assertEqual(renamed['legacy_id'],row['id'])
        self.assertEqual(renamed['values']['success'],12)
        # 旧键的墓碑不应阻止新键，且全量校验一致
        self.assertEqual(tx(self.conn,migration.verify,self.gw,self.conn)['ok'],True)

    # 切换后外部行编号必须仍是稳定 legacy_id：用列表返回的 row.id 可直接编辑/移除
    def test_10_external_row_id_stable_after_switch(self):
        tx(self.conn,migration.start,self.gw,self.conn)
        tx(self.conn,migration.run_backfill_until_end,self.gw,self.conn,10)
        tx(self.conn,migration.verify,self.gw,self.conn)
        tx(self.conn,migration.begin_switch,self.gw,self.conn)
        tx(self.conn,migration.complete_switch,self.gw,self.conn)
        # 切换前列表的外部 id（旧表主键）
        before={r['source_key']:r['id'] for r in self.gw.legacy_list(self.conn,self.study)}
        # 切换后经统一读取入口取列表，外部 id 必须与切换前一致
        bundle=counts_ops.list_rows(self.gw,self.conn,self.study)
        after={r['source_key']:r['id'] for r in bundle['rows']}
        self.assertEqual(before,after)
        target_key='K-0003';ext_id=after[target_key]
        self.assertEqual(ext_id,self.gw.int_get_by_natural(self.conn,self.study,target_key)['legacy_id'])
        # 前端就是拿这个 row.id 回来做编辑
        tx(self.conn,counts_ops.save,self.gw,self.conn,self.study,target_key,
           {'group':'B','success':42,'failure':7},ext_id)
        edited=next(r for r in counts_ops.list_rows(self.gw,self.conn,self.study)['rows']
                    if r['source_key']==target_key)
        self.assertEqual(edited['values']['success'],42);self.assertEqual(edited['id'],ext_id)
        # 同样用外部 id 移除
        tx(self.conn,counts_ops.remove,self.gw,self.conn,self.study,ext_id)
        self.assertIsNone(self.gw.int_get_by_natural(self.conn,self.study,target_key))
        self.assertTrue(self.gw.tombstone_has(self.conn,self.study,target_key))

if __name__=='__main__':
    unittest.main(verbosity=2)
