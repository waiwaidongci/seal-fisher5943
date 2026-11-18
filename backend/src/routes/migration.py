from fastapi import APIRouter,Depends
from src.config.database import get_db
from src.middlewares.auth import current_user,admin
from src.controllers import migration as migration_ctrl
router=APIRouter(prefix='/migration',tags=['migration'])

@router.get('/status')
def status(db=Depends(get_db),user=Depends(current_user)):
    return migration_ctrl.status(db)

@router.post('/start')
def start(db=Depends(get_db),user=Depends(admin)):
    return migration_ctrl.start(db)

@router.post('/backfill')
def backfill(limit:int=500,db=Depends(get_db),user=Depends(admin)):
    return migration_ctrl.backfill(db,limit)

@router.post('/verify')
def verify(db=Depends(get_db),user=Depends(admin)):
    return migration_ctrl.verify(db)

@router.post('/switch/begin')
def begin_switch(db=Depends(get_db),user=Depends(admin)):
    return migration_ctrl.begin_switch(db)

@router.post('/switch/complete')
def complete_switch(db=Depends(get_db),user=Depends(admin)):
    return migration_ctrl.complete_switch(db)

@router.post('/rollback')
def rollback(db=Depends(get_db),user=Depends(admin)):
    return migration_ctrl.rollback(db)
