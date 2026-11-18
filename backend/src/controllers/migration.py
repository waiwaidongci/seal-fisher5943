from fastapi import HTTPException
from src.services import studies,migration
from src.services.count_store import MigrationConflict

def status(db):
    return studies.migration_status(db)

def _run(db,fn,*args):
    try:
        result=fn(studies.count_gw,db,*args)
        db.commit();return result
    except MigrationConflict as e:
        db.rollback();raise HTTPException(409,str(e))

def start(db):
    return _run(db,migration.start)

def backfill(db,limit):
    return _run(db,migration.backfill_batch,batch_size=max(1,min(limit,5000)))

def verify(db):
    return _run(db,migration.verify)

def begin_switch(db):
    return _run(db,migration.begin_switch)

def complete_switch(db):
    return _run(db,migration.complete_switch)

def rollback(db):
    return _run(db,migration.rollback)
