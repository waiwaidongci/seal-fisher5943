"""计数存储网关协议。

迁移业务逻辑（src/services/migration.py 与 counts_ops.py）只依赖本协议，不直接
依赖 SQLAlchemy 或 FastAPI，因此同一份逻辑可在：
  - 生产环境由 SqlAlchemyCountGateway 接到 MySQL；
  - 验证环境由 tests/sqlite_gateway.py 以标准库 sqlite3 真实跑并发与崩溃续跑。
所有方法均在调用方给定的会话/事务内执行，提交边界由服务层统一控制。
"""

class CountError(Exception):
    """计数存储相关业务错误基类。"""

class DuplicateRow(CountError):
    """同一批次下自然键 source_key 冲突。"""

class RowMissing(CountError):
    """目标行不存在或不属于该批次。"""

class NonIntegralCount(CountError):
    """旧浮点记录无法无损转为非负整数，必须先更正再回填/切换。"""

class SwitchInProgress(CountError):
    """切换进行中，研究冻结被拒绝。"""

class MigrationConflict(CountError):
    """迁移动作与当前阶段不符，或已有切换在进行。"""

INT_FIELDS=('success','failure')

def coerce_int_values(values):
    """把合格/失败计数从浮点记录无损归一化为 Python int；text 字段原样保留。"""
    out=dict(values)
    for key in INT_FIELDS:
        v=out[key]
        if isinstance(v,bool):
            raise NonIntegralCount(f'{key} 必须是整数计数')
        fv=float(v)
        if fv != int(fv) or fv < 0:
            raise NonIntegralCount(f'{key}={v!r} 不是非负整数，无法迁入整数存储；请先更正旧记录')
        out[key]=int(fv)
    if not isinstance(out.get('group'),str) or out['group'] not in ('A','B'):
        raise NonIntegralCount('工艺组必须为 A 或 B')
    return out

def values_equal(legacy_values,int_values):
    """旧浮点 values 与整数行在业务口径上是否一致。"""
    if legacy_values.get('group') != int_values.get('group'):
        return False
    for key in INT_FIELDS:
        lv,iv=legacy_values.get(key),int_values.get(key)
        if lv is None or iv is None:
            return False
        try:
            if float(lv) != int(iv) or int(iv) != float(lv):
                return False
        except (TypeError,ValueError):
            return False
    return True

class CountGateway:
    """生产/验证网关必须实现的最小接口（鸭子类型，无强制继承）。"""

    # ---- 迁移状态 ----
    def ensure_state(self,tx): ...
    def get_state(self,tx): ...
    def lock_state(self,tx): ...
    def unlock_state(self,tx,ok): ...
    def save_state(self,tx,state): ...

    # ---- 旧浮点表 observations ----
    def legacy_get_by_natural(self,tx,study_id,source_key): ...
    def legacy_get(self,tx,row_id): ...
    def legacy_list(self,tx,study_id): ...
    def legacy_insert(self,tx,study_id,source_key,values): ...
    def legacy_update(self,tx,row,source_key,values): ...
    def legacy_delete(self,tx,row): ...
    def legacy_scan_batch(self,tx,after_id,limit): ...
    def legacy_count(self,tx): ...

    # ---- 整数表 observations_int ----
    def int_get_by_legacy_id(self,tx,legacy_id): ...
    def int_get_by_natural(self,tx,study_id,source_key): ...
    def int_list(self,tx,study_id): ...
    def int_upsert(self,tx,legacy_id,study_id,source_key,values): ...
    def int_delete_by_natural(self,tx,study_id,source_key): ...
    def int_count(self,tx): ...

    # ---- 墓碑 observation_tombstones ----
    def tombstone_has(self,tx,study_id,source_key): ...
    def tombstone_add(self,tx,study_id,source_key,removed_at): ...
    def tombstone_drop(self,tx,study_id,source_key): ...

    # ---- 研究冻结 ----
    def lock_study_runs(self,tx,study_id): ...
