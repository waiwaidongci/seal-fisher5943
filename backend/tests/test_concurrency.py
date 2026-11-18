"""并发验证：移除撞回填、切换撞冻结、回退前新写入，以及双写最终一致。

使用 WAL 共享内存 sqlite 的多个连接 + 线程，真实制造写事务交错；
生产 MySQL 的行锁语义更强，这里验证的是同一套网关协议逻辑。
"""
import json,threading,unittest
from tests.helpers import make_world,make_study,retry_locked
from tests.sqlite_gateway import connect,init_schema,SqliteCountGateway
from src.services import migration,migration_phases as phases
from src.services import counts_ops
from src.services.count_store import SwitchInProgress,DuplicateRow,RowMissing

def find_shared_uri_hint(): return None

class ConcurrencyTest(unittest.TestCase):
    def setUp(self):
        # 用文件库以便多连接共享（内存共享缓存对临时连接支持不稳）
        import tempfile,os
        self.dir=tempfile.mkdtemp(prefix='migration-test-')
        self.path=os.path.join(self.dir,'world.db')
        self.conn=connect(self.path);init_schema(self.conn)
        self.gw=SqliteCountGateway()
        self.gw.ensure_state(self.conn);self.conn.commit()
        self.study=make_study(self.conn)
        # 200 条历史浮点记录
        for i in range(1,201):
            vals={'group':'A' if i%2 else 'B','success':float(20+i),'failure':float(i%7)}
            self.conn.execute('INSERT INTO observations(study_id,source_key,values_json) VALUES(?,?,?)',
                              (self.study,f'C-{i:04d}',json.dumps(vals)))
        self.conn.commit()

    def _tx(self,fn,args):
        conn=connect(self.path)
        try:
            out=fn(self.gw,conn,*args)
            self.gw.commit(conn);return out
        except Exception:
            self.gw.rollback(conn);raise
        finally:
            conn.close()

    def _new_conn(self):
        return connect(self.path)

    # 场景一：移除撞回填 —— 移除记录不被回填复活
    def test_01_delete_during_backfill_never_resurrects(self):
        self._tx(migration.start,(self.conn,)) if False else None
        c=self._new_conn()
        migration.start(self.gw,c);c.commit();c.close()

        errors=[]
        stop=threading.Event()
        def backfill_worker():
            try:
                while not stop.is_set():
                    conn=self._new_conn()
                    try:
                        r=migration.backfill_batch(self.gw,conn,batch_size=7)
                        self.gw.commit(conn)
                        if r['reached_end']: break
                    except Exception:
                        self.gw.rollback(conn);raise
                    finally:
                        conn.close()
            except Exception as e:
                errors.append(('backfill',e))

        def delete_worker():
            # 边回填边移除 60 条；每条先解析 legacy id
            try:
                for i in range(1,61):
                    key=f'C-{i:04d}'
                    conn=self._new_conn()
                    try:
                        row=self.gw.legacy_get_by_natural(conn,self.study,key)
                        if row:
                            counts_ops.remove(self.gw,conn,self.study,row['id'])
                            self.gw.commit(conn)
                    except Exception:
                        self.gw.rollback(conn);raise
                    finally:
                        conn.close()
            except Exception as e:
                errors.append(('delete',e))

        t_back=threading.Thread(target=backfill_worker)
        t_del=threading.Thread(target=delete_worker)
        t_back.start();t_del.start();t_del.join();stop.set();t_back.join()
        self.assertEqual(errors,[],errors)

        # 回填全部跑完
        conn=self._new_conn()
        try:
            migration.run_backfill_until_end(self.gw,conn,50);self.gw.commit(conn)
            legacy_keys={r['source_key'] for r in self.gw.legacy_list(conn,self.study)}
            int_keys={r['source_key'] for r in self.gw.int_list(conn,self.study)}
            tombstoned={f'C-{i:04d}' for i in range(1,61)}
            # 被移除的 60 条既不在旧表，也绝不在整数表（未复活）
            self.assertTrue(tombstoned.isdisjoint(int_keys))
            self.assertTrue(tombstoned.isdisjoint(legacy_keys))
            # 其余 140 条两存储齐全且一致
            self.assertEqual(len(int_keys),140)
            diffs=migration.full_diff(self.gw,conn)
            self.assertEqual(diffs,[],diffs)
        finally:
            conn.close()

    # 场景二：切换撞冻结 —— switching 窗口冻结被拒绝，完成后恢复
    def test_02_freeze_blocked_while_switching(self):
        c=self._new_conn()
        migration.start(self.gw,c);c.commit()
        migration.run_backfill_until_end(self.gw,c,50);c.commit()
        migration.verify(self.gw,c);c.commit();c.close()

        begin=threading.Event();allow_complete=threading.Event()
        outcomes=[]
        def switch_worker():
            conn=self._new_conn()
            try:
                migration.begin_switch(self.gw,conn);conn.commit()
                begin.set()
                self.assertTrue(allow_complete.wait(timeout=5))
                r=migration.complete_switch(self.gw,conn);conn.commit()
                outcomes.append(('switch',r['switched']))
            except Exception as e:
                outcomes.append(('switch_error',repr(e)))
            finally:
                conn.close()
        def freeze_worker():
            self.assertTrue(begin.wait(timeout=5))
            conn=self._new_conn()
            try:
                counts_ops.freeze_snapshot(self.gw,conn,self.study)
                outcomes.append(('freeze','allowed'))
            except SwitchInProgress:
                outcomes.append(('freeze','blocked'))
            finally:
                conn.close()
        t_switch=threading.Thread(target=switch_worker)
        t_freeze=threading.Thread(target=freeze_worker)
        t_switch.start();t_freeze.start()
        t_freeze.join();allow_complete.set();t_switch.join()
        self.assertIn(('freeze','blocked'),outcomes)
        self.assertIn(('switch',True),outcomes)
        # 切换完成后冻结恢复，且取整数表完整版本
        conn=self._new_conn()
        try:
            frozen=counts_ops.freeze_snapshot(self.gw,conn,self.study)
            self.assertEqual(len(frozen['snapshot']),200)
            self.assertIn('整数专用存储',frozen['read_basis'])
        finally:
            conn.close()

    # 场景三：回退前新写入 —— 回退后写入保留且两存储一致
    def test_03_writes_before_rollback_are_retained(self):
        c=self._new_conn()
        migration.start(self.gw,c);c.commit()
        migration.run_backfill_until_end(self.gw,c,50);c.commit()
        migration.verify(self.gw,c);c.commit()
        migration.begin_switch(self.gw,c);c.commit()
        migration.complete_switch(self.gw,c);c.commit();c.close()

        # 多线程在 integer 阶段混合新增/更正/移除
        def writer(start,end,tag):
            for i in range(start,end):
                conn=self._new_conn()
                try:
                    key=f'W-{tag}-{i:03d}'
                    counts_ops.save(self.gw,conn,self.study,key,
                                    {'group':'A','success':i,'failure':2});conn.commit()
                finally:
                    conn.close()
        def updater():
            for i in range(100,140):
                conn=self._new_conn()
                try:
                    row=self.gw.int_get_by_natural(conn,self.study,f'C-{i:04d}')
                    if row:
                        counts_ops.save(self.gw,conn,self.study,f'C-{i:04d}',
                                        {'group':'B','success':500,'failure':9},row['legacy_id'])
                        conn.commit()
                finally:
                    conn.close()
        def remover():
            for i in range(140,170):
                conn=self._new_conn()
                try:
                    row=self.gw.int_get_by_natural(conn,self.study,f'C-{i:04d}')
                    if row:
                        counts_ops.remove(self.gw,conn,self.study,row['legacy_id']);conn.commit()
                finally:
                    conn.close()
        threads=[threading.Thread(target=writer,args=(0,40,'X')),
                 threading.Thread(target=writer,args=(0,40,'Y')),
                 threading.Thread(target=updater),threading.Thread(target=remover)]
        for t in threads:t.start()
        for t in threads:t.join()

        conn=self._new_conn()
        try:
            migration.rollback(self.gw,conn);conn.commit()
            self.assertEqual(self.gw.get_state(conn)['phase'],phases.DUAL)
            # 回退后读旧表：新写入在、更新生效、移除生效
            legacy={r['source_key']:r for r in self.gw.legacy_list(conn,self.study)}
            self.assertIn('W-X-007',legacy);self.assertIn('W-Y-039',legacy)
            self.assertEqual(float(legacy['C-0100']['values']['success']),500.0)
            self.assertNotIn('C-0150',legacy)
            # 两存储最终一致，可重新校验通过
            diffs=migration.full_diff(self.gw,conn)
            self.assertEqual(diffs,[],diffs)
            v=migration.verify(self.gw,conn);conn.commit()
            self.assertTrue(v['ok'])
        finally:
            conn.close()

    # 回填与新增/更正并发：自然键冲突与双写最终一致
    def test_04_concurrent_save_and_backfill_converges(self):
        c=self._new_conn()
        migration.start(self.gw,c);c.commit();c.close()
        errors=[];stop=threading.Event()
        def backfiller():
            try:
                while not stop.is_set():
                    conn=self._new_conn()
                    try:
                        r=migration.backfill_batch(self.gw,conn,batch_size=11);conn.commit()
                        if r['reached_end']:break
                    finally:
                        conn.close()
            except Exception as e: errors.append(('bf',e))
        def saver():
            try:
                for i in range(50):
                    conn=self._new_conn()
                    try:
                        counts_ops.save(self.gw,conn,self.study,f'P-{i:03d}',
                                        {'group':'A','success':10+i,'failure':1});conn.commit()
                    finally:
                        conn.close()
            except Exception as e: errors.append(('save',e))
        ts=[threading.Thread(target=backfiller),threading.Thread(target=saver)]
        for t in ts:t.start()
        for t in ts:t.join();stop.set()
        self.assertEqual(errors,[],errors)
        conn=self._new_conn()
        try:
            migration.run_backfill_until_end(self.gw,conn,100);conn.commit()
            diffs=migration.full_diff(self.gw,conn)
            self.assertEqual(diffs,[],diffs)
            # 双写阶段每一条新增都同时存在于两存储
            self.assertEqual(self.gw.legacy_count(conn),self.gw.int_count(conn))
            # 重名仍明确拒绝（独立连接，异常即回滚）；i=0 经 :03d 格式化即 P-000
            dup=connect(self.path)
            try:
                with self.assertRaises(DuplicateRow):
                    counts_ops.save(self.gw,dup,self.study,'P-000',
                                    {'group':'A','success':1,'failure':1})
            finally:
                dup.rollback();dup.close()
        finally:
            conn.close()

    # 并发移除后用被释放的自然键重新录入：墓碑被清除，行可正常存在
    def test_05_reinsert_same_natural_key_clears_tombstone(self):
        c=self._new_conn()
        migration.start(self.gw,c);c.commit()
        migration.run_backfill_until_end(self.gw,c,50);c.commit();c.close()
        conn=self._new_conn()
        try:
            row=self.gw.legacy_get_by_natural(conn,self.study,'C-0005')
            counts_ops.remove(self.gw,conn,self.study,row['id']);conn.commit()
            self.assertTrue(self.gw.tombstone_has(conn,self.study,'C-0005'))
            # 回填再跑也不会复活
            migration.run_backfill_until_end(self.gw,conn,100);conn.commit()
            self.assertIsNone(self.gw.legacy_get_by_natural(conn,self.study,'C-0005'))
            self.assertIsNone(self.gw.int_get_by_natural(conn,self.study,'C-0005'))
            # 重新录入同自然键
            counts_ops.save(self.gw,conn,self.study,'C-0005',
                            {'group':'B','success':33,'failure':4});conn.commit()
            self.assertFalse(self.gw.tombstone_has(conn,self.study,'C-0005'))
            self.assertIsNotNone(self.gw.int_get_by_natural(conn,self.study,'C-0005'))
            # 再来一轮回填仍保留（墓碑已清）
            migration.run_backfill_until_end(self.gw,conn,100);conn.commit()
            self.assertIsNotNone(self.gw.int_get_by_natural(conn,self.study,'C-0005'))
        finally:
            conn.close()

if __name__=='__main__':
    unittest.main(verbosity=2)
