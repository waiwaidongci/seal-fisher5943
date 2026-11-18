from fastapi import HTTPException
from sqlalchemy import select,func
from src.models.study import Study
from src.models.observation import Observation
from src.services.domain_validation import validate_values
from src.services.audits import log
from src.services.count_store import CountError,DuplicateRow,RowMissing
from src.services.count_gateway_sa import SqlAlchemyCountGateway
from src.services import counts_ops
from src.utils.serializers import record

# 生产环境单一计数存储网关（无状态，仅持有 SQLAlchemy 映射）
count_gw=SqlAlchemyCountGateway()

def list_studies(db):
    data=[]
    for s in db.scalars(select(Study).order_by(Study.id)):
        x=record(s);x['row_count']=db.scalar(select(func.count()).select_from(Observation).where(Observation.study_id==s.id));data.append(x)
    return data

def create(db,data,user):
    if db.scalar(select(Study).where(Study.name==data.name)): raise HTTPException(409,'批次名称已存在')
    s=Study(**data.model_dump());db.add(s);log(db,user,'study.create',data.name);db.commit();return record(s)

def require(db,id):
    s=db.get(Study,id)
    if not s: raise HTTPException(404,'业务批次不存在')
    return s

def observations(db,id):
    require(db,id)
    bundle=counts_ops.list_rows(count_gw,db,id)
    return bundle['rows']

def migration_status(db):
    from src.services import migration
    return migration.status(count_gw,db)

def save_observation(db,id,data,user,row_id=None):
    require(db,id)
    try: values=validate_values(data.values)
    except (ValueError,TypeError) as e: raise HTTPException(422,str(e))
    try:
        row=counts_ops.save(count_gw,db,id,data.source_key,values,row_id)
    except DuplicateRow as e: raise HTTPException(409,str(e))
    except RowMissing as e: raise HTTPException(404,str(e))
    except CountError as e: raise HTTPException(422,str(e))
    log(db,user,'observation.save',f'{id}/{data.source_key}');db.commit();return row

def delete_observation(db,id,row_id,user):
    require(db,id)
    try:
        result=counts_ops.remove(count_gw,db,id,row_id)
    except RowMissing as e: raise HTTPException(404,str(e))
    except CountError as e: raise HTTPException(422,str(e))
    log(db,user,'observation.delete',f"{id}/{result['source_key']} tombstone={result['tombstoned']}")
    db.commit();return result
