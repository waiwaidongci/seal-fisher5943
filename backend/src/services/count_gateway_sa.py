"""SQLAlchemy / MySQL 生产网关。

事务由调用方的 Session 管理；行级锁在 MySQL 上使用 SELECT ... FOR UPDATE，
在 SQLite 上退化为普通读取（SQLite 单写者，锁语义由写事务本身保证）。
"""
import json
from sqlalchemy import select,func
from src.models.observation import Observation
from src.models.run import Run
from src.models.migration import IntegerCount,CountTombstone,MigrationState
from src.services.count_store import coerce_int_values
from src.services import migration_phases as phases
from src.utils.timeutil import now

def _is_mysql(db):
    return db.bind.dialect.name in ('mysql','mariadb')

def _for_update(stmt,db):
    return stmt.with_for_update() if _is_mysql(db) else stmt

class SqlAlchemyCountGateway:
    # ---- 迁移状态 ----
    def ensure_state(self,db):
        obj=db.get(MigrationState,1)
        if not obj:
            obj=MigrationState(id=1,phase=phases.LEGACY,last_cursor_id=0,backfilled=0,
                               skipped=[],diffs=[],verified_at=None,updated_at=now(),detail='初始化：旧浮点存储在线')
            db.add(obj);db.flush()
        return self._state_dict(obj)

    def get_state(self,db):
        return self._state_dict(db.get(MigrationState,1) or self.ensure_state(db))

    def lock_state(self,db):
        obj=db.scalar(_for_update(select(MigrationState).where(MigrationState.id==1),db))
        if not obj: obj=self.ensure_state(db)
        return self._state_dict(obj)

    def unlock_state(self,db,ok):
        # SQLAlchemy Session 统一管理事务与 FOR UPDATE 锁，无需保存点配对
        return

    def save_state(self,db,state):
        obj=db.get(MigrationState,1)
        for key in ('phase','last_cursor_id','backfilled','skipped','diffs','verified_at','updated_at','detail'):
            setattr(obj,key,state[key])
        db.flush()

    @staticmethod
    def _state_dict(obj):
        return {'phase':obj.phase,'last_cursor_id':obj.last_cursor_id,'backfilled':obj.backfilled,
                'skipped':list(obj.skipped or []),'diffs':list(obj.diffs or []),
                'verified_at':obj.verified_at,'updated_at':obj.updated_at,'detail':obj.detail or ''}

    # ---- 旧浮点表 ----
    def legacy_get_by_natural(self,db,study_id,source_key):
        obj=db.scalar(select(Observation).where(Observation.study_id==study_id,Observation.source_key==source_key))
        return self._legacy_dict(obj) if obj else None

    def legacy_get(self,db,row_id):
        obj=db.get(Observation,row_id)
        return self._legacy_dict(obj) if obj else None

    def legacy_list(self,db,study_id):
        return [self._legacy_dict(o) for o in db.scalars(
            select(Observation).where(Observation.study_id==study_id).order_by(Observation.id))]

    def legacy_insert(self,db,study_id,source_key,values):
        obj=Observation(study_id=study_id,source_key=source_key,values=values)
        db.add(obj);db.flush();return self._legacy_dict(obj)

    def legacy_update(self,db,row,source_key,values):
        obj=db.get(Observation,row['id'])
        obj.source_key=source_key;obj.values=values;db.flush();return self._legacy_dict(obj)

    def legacy_delete(self,db,row):
        obj=db.get(Observation,row['id'])
        if obj: db.delete(obj);db.flush()

    def legacy_scan_batch(self,db,after_id,limit):
        return [self._legacy_dict(o) for o in db.scalars(
            select(Observation).where(Observation.id>after_id).order_by(Observation.id).limit(limit))]

    def legacy_count(self,db):
        return db.scalar(select(func.count()).select_from(Observation))

    @staticmethod
    def _legacy_dict(obj):
        return {'id':obj.id,'study_id':obj.study_id,'source_key':obj.source_key,'values':dict(obj.values)}

    # ---- 整数表 ----
    def int_get_by_legacy_id(self,db,legacy_id):
        obj=db.scalar(select(IntegerCount).where(IntegerCount.legacy_id==legacy_id))
        return self._int_dict(obj) if obj else None

    def int_get_by_natural(self,db,study_id,source_key):
        obj=db.scalar(select(IntegerCount).where(IntegerCount.study_id==study_id,IntegerCount.source_key==source_key))
        return self._int_dict(obj) if obj else None

    def int_list(self,db,study_id):
        return [self._int_dict(o) for o in db.scalars(
            select(IntegerCount).where(IntegerCount.study_id==study_id).order_by(IntegerCount.legacy_id))]

    def int_upsert(self,db,legacy_id,study_id,source_key,values):
        iv=coerce_int_values(values)
        obj=db.scalar(select(IntegerCount).where(IntegerCount.legacy_id==legacy_id))
        if obj is None:
            obj=IntegerCount(legacy_id=legacy_id,study_id=study_id,source_key=source_key,
                             group_name=iv['group'],success=iv['success'],failure=iv['failure'])
            db.add(obj)
        else:
            obj.study_id=study_id;obj.source_key=source_key
            obj.group_name=iv['group'];obj.success=iv['success'];obj.failure=iv['failure']
        db.flush();return self._int_dict(obj)

    def int_delete_by_natural(self,db,study_id,source_key):
        obj=db.scalar(select(IntegerCount).where(IntegerCount.study_id==study_id,IntegerCount.source_key==source_key))
        if obj: db.delete(obj);db.flush()

    def int_count(self,db):
        return db.scalar(select(func.count()).select_from(IntegerCount))

    @staticmethod
    def _int_dict(obj):
        # 对外行编号统一暴露为 legacy_id（与旧表 observations.id 相同），
        # 保证切换前后前端持有的 row.id 始终可用于编辑/移除；物理主键仅内部保留。
        return {'id':obj.legacy_id,'int_pk':obj.id,'legacy_id':obj.legacy_id,
                'study_id':obj.study_id,'source_key':obj.source_key,
                'values':{'group':obj.group_name,'success':int(obj.success),'failure':int(obj.failure)}}

    # ---- 墓碑 ----
    def tombstone_has(self,db,study_id,source_key):
        return db.scalar(select(func.count()).select_from(CountTombstone).where(
            CountTombstone.study_id==study_id,CountTombstone.source_key==source_key))>0

    def tombstone_add(self,db,study_id,source_key,removed_at):
        if self.tombstone_has(db,study_id,source_key): return
        db.add(CountTombstone(study_id=study_id,source_key=source_key,removed_at=removed_at));db.flush()

    def tombstone_drop(self,db,study_id,source_key):
        for obj in db.scalars(select(CountTombstone).where(
                CountTombstone.study_id==study_id,CountTombstone.source_key==source_key)):
            db.delete(obj)
        db.flush()

    # ---- 研究冻结 ----
    def lock_study_runs(self,db,study_id):
        db.scalars(_for_update(select(Run.id).where(Run.study_id==study_id),db)).all()
