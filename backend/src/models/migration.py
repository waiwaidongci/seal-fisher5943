from sqlalchemy import Column,Integer,String,JSON,Text,ForeignKey,UniqueConstraint,BigInteger
from src.config.database import Base

class IntegerCount(Base):
    """整数专用存储：工艺计数以原生整数列保存，不再走 JSON 浮点记录。"""
    __tablename__='observations_int'
    __table_args__=(UniqueConstraint('study_id','source_key'),)
    id=Column(Integer,primary_key=True)
    # 与旧表 observations.id 保持同一主键，回填时逐行对齐，切换后外部行编号不变
    legacy_id=Column(Integer,Unique=True,nullable=False)
    study_id=Column(Integer,ForeignKey('studies.id'),nullable=False)
    source_key=Column(String(100),nullable=False)
    group_name=Column(String(8),nullable=False)
    success=Column(Integer,nullable=False)
    failure=Column(Integer,nullable=False)

class CountTombstone(Base):
    """删除墓碑：回填期间被移除的自然键不得被后续回填复活；重新录入时清除。"""
    __tablename__='observation_tombstones'
    __table_args__=(UniqueConstraint('study_id','source_key'),)
    id=Column(Integer,primary_key=True)
    study_id=Column(Integer,nullable=False)
    source_key=Column(String(100),nullable=False)
    removed_at=Column(String(40),nullable=False)

class MigrationState(Base):
    """单例行（id=1）：记录浮点→整数存储迁移的阶段、游标与最近一次校验结果。"""
    __tablename__='migration_state'
    id=Column(Integer,primary_key=True)
    phase=Column(String(20),nullable=False,default='legacy')
    last_cursor_id=Column(BigInteger,nullable=False,default=0)
    backfilled=Column(Integer,nullable=False,default=0)
    skipped=Column(JSON,nullable=False,default=list)
    diffs=Column(JSON,nullable=False,default=list)
    verified_at=Column(String(40),nullable=True)
    updated_at=Column(String(40),nullable=False)
    detail=Column(Text,nullable=False,default='')
