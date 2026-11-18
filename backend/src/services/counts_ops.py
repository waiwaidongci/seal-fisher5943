"""迁移期间的工艺计数写入、移除、读取与研究冻结。

同一份逻辑经 CountGateway 同时运行在生产 MySQL 与验证用 sqlite 上：
- legacy 阶段：旧浮点表唯一读写。
- dual 阶段：读取依据仍是旧表；新增/更正/移除在一个事务内双写两存储，
  移除落墓碑，保证在线回填不会把已移除记录复活。
- switching 阶段：数据写入照常双写，但拒绝发起新的研究冻结。
- integer 阶段：读取依据切换为整数表；旧表继续同步写入以保留回退通道。
"""
from contextlib import contextmanager
from src.services import migration_phases as phases
from src.services.count_store import (
    DuplicateRow,RowMissing,SwitchInProgress,
)
from src.utils.timeutil import now

@contextmanager
def _state_lock(gw,tx):
    gw.lock_state(tx)
    try:
        yield
    except Exception:
        gw.unlock_state(tx,False);raise
    else:
        gw.unlock_state(tx,True)

def resolve_row(gw,tx,phase,study_id,row_id):
    """把外部行编号解析为当前读取依据中的行；两表共享 legacy_id 主键。"""
    if phase==phases.INTEGER:
        row=gw.int_get_by_legacy_id(tx,row_id)
        if not row or row['study_id']!=study_id: raise RowMissing('记录不存在')
        return row
    row=gw.legacy_get(tx,row_id)
    if not row or row['study_id']!=study_id: raise RowMissing('记录不存在')
    return row

def save(gw,tx,study_id,source_key,values,row_id=None):
    """新增或更正一条工艺计数，按阶段保证两存储最终一致。返回读取依据上的行。"""
    with _state_lock(gw,tx):
        phase=gw.get_state(tx)['phase']
        if phase==phases.LEGACY:
            other=gw.legacy_get_by_natural(tx,study_id,source_key)
            if other and other['id']!=row_id: raise DuplicateRow('源记录编号重复')
            if row_id:
                row=gw.legacy_get(tx,row_id)
                if not row or row['study_id']!=study_id: raise RowMissing('记录不存在')
                gw.legacy_update(tx,row,source_key,values);return gw.legacy_get(tx,row_id)
            return gw.legacy_insert(tx,study_id,source_key,values)

        # dual / switching / integer：旧表 + 整数表双写
        other=gw.legacy_get_by_natural(tx,study_id,source_key)
        if other and other['id']!=row_id: raise DuplicateRow('源记录编号重复')
        if row_id:
            resolve_row(gw,tx,phase,study_id,row_id)
            legacy=gw.legacy_get(tx,row_id)
            if not legacy: raise RowMissing('记录不存在')
            old_key=legacy['source_key']
            gw.legacy_update(tx,legacy,source_key,values)
            gw.int_upsert(tx,legacy['id'],study_id,source_key,values)
            # 若更正改回了一个曾被删除过的自然键，墓碑必须失效
            gw.tombstone_drop(tx,study_id,source_key)
            # 改自然键时，旧键视为被释放：立墓碑并清理可能残留的旧键整数行，
            # 防止回填或重录把旧键数据复活。
            if old_key!=source_key:
                gw.int_delete_by_natural(tx,study_id,old_key)
                gw.tombstone_add(tx,study_id,old_key,now())
            return gw.int_get_by_legacy_id(tx,row_id) if phase==phases.INTEGER else gw.legacy_get(tx,row_id)

        legacy=gw.legacy_insert(tx,study_id,source_key,values)
        gw.tombstone_drop(tx,study_id,source_key)
        gw.int_upsert(tx,legacy['id'],study_id,source_key,values)
        return gw.int_get_by_legacy_id(tx,legacy['id']) if phase==phases.INTEGER else legacy

def remove(gw,tx,study_id,row_id):
    """移除一条计数；dual 之后同时清理整数表并立墓碑，回填不得复活。"""
    with _state_lock(gw,tx):
        phase=gw.get_state(tx)['phase']
        row=resolve_row(gw,tx,phase,study_id,row_id)
        source_key=row['source_key']
        if phase==phases.LEGACY:
            gw.legacy_delete(tx,row);return {'deleted':True,'source_key':source_key,'tombstoned':False}
        legacy=gw.legacy_get(tx,row_id)
        if legacy: gw.legacy_delete(tx,legacy)
        gw.int_delete_by_natural(tx,study_id,source_key)
        gw.tombstone_add(tx,study_id,source_key,now())
        return {'deleted':True,'source_key':source_key,'tombstoned':True}

def list_rows(gw,tx,study_id):
    """按当前读取依据返回该批次全部计数，并附读取依据说明。"""
    state=gw.get_state(tx);phase=state['phase']
    if phase==phases.INTEGER:
        rows=gw.int_list(tx,study_id)
    else:
        rows=gw.legacy_list(tx,study_id)
    return {'phase':phase,'read_basis':phases.basis(phase),'rows':rows}

def freeze_snapshot(gw,tx,study_id):
    """研究冻结：锁定迁移状态与该批次研究行，取同一完整版本的快照。

    switching 阶段拒绝冻结，避免在原子切换窗口取到混合存储版本。
    已完成运行的历史快照存于 runs.snapshot，迁移与切换均不触碰。
    """
    with _state_lock(gw,tx):
        phase=gw.get_state(tx)['phase']
        if phase==phases.SWITCHING:
            raise SwitchInProgress('整数存储切换进行中，暂不能发起研究；请稍后重试')
        gw.lock_study_runs(tx,study_id)
        bundle=list_rows(gw,tx,study_id)
        snapshot=[{'source_key':r['source_key'],'values':r['values']} for r in bundle['rows']]
        return {'phase':phase,'read_basis':bundle['read_basis'],'snapshot':snapshot}
