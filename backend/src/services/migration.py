"""在线回填、校验、原子切换与回退的编排逻辑。

只依赖 CountGateway 协议，因此可在无外部依赖的 sqlite 网关上做真实并发验证。
关键不变量：
  1. 回填按旧表主键游标推进，墓碑命中即跳过；中断后重跑从游标继续，不重复迁入。
  2. 双写阶段新增/更正/移除与回填在同一行锁/写事务内串行，两存储最终一致，
     已移除记录由墓碑保证不被回填复活。
  3. 切换分 begin_switch（legacy→switching，拒绝新研究冻结）与 complete_switch
     （同事务全量校验通过后 switching→integer）；任何差异都拒绝切换并停在
     switching，可回退到 dual 继续旧表读写。
  4. integer 阶段旧表仍被双写；回退 integer→dual 保留期间全部写入。
"""
from contextlib import contextmanager
from src.services import migration_phases as phases
from src.services.count_store import (
    NonIntegralCount,MigrationConflict,coerce_int_values,values_equal,
)
from src.utils.timeutil import now

@contextmanager
def _locked(gw,tx):
    gw.lock_state(tx)
    try:
        yield
    except Exception:
        gw.unlock_state(tx,False);raise
    else:
        gw.unlock_state(tx,True)

def status(gw,tx):
    state=gw.get_state(tx)
    return {
        'phase':state['phase'],
        'read_basis':phases.basis(state['phase']),
        'last_cursor_id':state['last_cursor_id'],
        'backfilled':state['backfilled'],
        'legacy_rows':gw.legacy_count(tx),
        'integer_rows':gw.int_count(tx),
        'diffs':state['diffs'],
        'skipped':state['skipped'],
        'verified_at':state['verified_at'],
        'updated_at':state['updated_at'],
        'detail':state['detail'],
    }

def start(gw,tx):
    """进入在线回填/双写阶段；迁前旧记录始终可读写。"""
    with _locked(gw,tx):
        state=gw.get_state(tx)
        if state['phase']!=phases.LEGACY:
            raise MigrationConflict(f"当前阶段 {state['phase']} 无需启动回填")
        state.update(phase=phases.DUAL,last_cursor_id=0,backfilled=0,skipped=[],diffs=[],
                     updated_at=now(),detail='在线回填已启动：双写新旧存储，读取依据仍为旧浮点表')
        gw.save_state(tx,state)
        return gw.get_state(tx)

def backfill_batch(gw,tx,batch_size=500):
    """推进一批回填。幂等：已存在的整数行只做对齐更新，绝不重复插入。"""
    with _locked(gw,tx):
        state=gw.get_state(tx)
        if state['phase'] not in (phases.DUAL,phases.SWITCHING):
            raise MigrationConflict('仅在回填/双写阶段可执行回填')
        rows=gw.legacy_scan_batch(tx,state['last_cursor_id'],batch_size)
        done=0
        for row in rows:
            state['last_cursor_id']=row['id']
            # 回填只依据旧表这一事实来源；墓碑负责在“移除”路径挡住复活，
            # 同自然键重新录入时墓碑已在 save() 中被清除。
            try:
                iv=coerce_int_values(row['values'])
            except NonIntegralCount as e:
                mark={'legacy_id':row['id'],'study_id':row['study_id'],
                      'source_key':row['source_key'],'reason':str(e)}
                if mark not in state['skipped']: state['skipped'].append(mark)
                continue
            gw.int_upsert(tx,row['id'],row['study_id'],row['source_key'],iv)
            done+=1;state['backfilled']+=1
        # 增量对齐整数表，且只处理“已被回填游标覆盖”的行（legacy_id<=游标）：
        # 新增记录的 legacy_id 必然大于当前游标，绝不会被清理误伤。
        # 仅当旧表已不存在且仍立墓碑时才删除，保证移除记录不被回填复活。
        study_ids={r['study_id'] for r in rows}
        cursor=state['last_cursor_id']
        for sid in study_ids:
            for int_row in gw.int_list(tx,sid):
                if int_row['legacy_id']>cursor: continue
                if gw.legacy_get_by_natural(tx,sid,int_row['source_key']) is not None: continue
                if gw.tombstone_has(tx,sid,int_row['source_key']):
                    gw.int_delete_by_natural(tx,sid,int_row['source_key'])
        state['updated_at']=now()
        state['detail']=f"本批回填 {done} 行，游标 {state['last_cursor_id']}"
        gw.save_state(tx,state)
        return {'batch':done,'cursor':state['last_cursor_id'],
                'reached_end':len(rows)<batch_size,
                'backfilled':state['backfilled'],'skipped':state['skipped']}

def run_backfill_until_end(gw,tx,batch_size=500,max_batches=100000):
    """测试/运维便捷入口：在连续事务内把旧表回填到末尾（中断后续跑同样安全）。"""
    total=0
    for _ in range(max_batches):
        r=backfill_batch(gw,tx,batch_size)
        total+=r['batch']
        if r['reached_end']: break
    return total

def full_diff(gw,tx):
    """全量比对两存储：自然键集合 + 逐字段计数；返回差异列表（空列表即一致）。"""
    legacy={}
    cursor=0
    while True:
        batch=gw.legacy_scan_batch(tx,cursor,1000)
        if not batch: break
        for row in batch:
            legacy[(row['study_id'],row['source_key'])]=row
            cursor=row['id']
    int_rows={}
    # 网关按批次列出整数行；逐批次覆盖全部研究
    study_ids={sid for sid,_ in legacy}
    for sid in study_ids:
        for row in gw.int_list(tx,sid):
            int_rows[(row['study_id'],row['source_key'])]=row
    diffs=[]
    for key,row in legacy.items():
        if gw.tombstone_has(tx,row['study_id'],row['source_key']): continue
        other=int_rows.get(key)
        if other is None:
            diffs.append({'type':'missing_in_integer','study_id':key[0],'source_key':key[1]})
            continue
        if not values_equal(row['values'],other['values']):
            diffs.append({'type':'value_mismatch','study_id':key[0],'source_key':key[1],
                          'legacy':row['values'],'integer':other['values']})
    for key,row in int_rows.items():
        if key not in legacy:
            diffs.append({'type':'missing_in_legacy','study_id':key[0],'source_key':key[1]})
    return diffs

def verify(gw,tx):
    """全量校验；发现差异即记录并拒绝后续切换。"""
    with _locked(gw,tx):
        state=gw.get_state(tx)
        if state['phase'] not in (phases.DUAL,phases.SWITCHING):
            raise MigrationConflict('仅在回填/切换阶段可执行校验')
        diffs=full_diff(gw,tx)
        state['diffs']=diffs;state['verified_at']=now()
        state['detail']=('校验通过：新旧存储完全一致' if not diffs
                         else f'校验发现 {len(diffs)} 处差异，已拒绝切换')
        gw.save_state(tx,state)
        return {'ok':not diffs,'diffs':diffs,'verified_at':state['verified_at']}

def begin_switch(gw,tx):
    """原子切换第一步：进入 switching，冻结新的研究发起。"""
    with _locked(gw,tx):
        state=gw.get_state(tx)
        if state['phase']==phases.SWITCHING:
            raise MigrationConflict('切换已在进行中')
        if state['phase']!=phases.DUAL:
            raise MigrationConflict('仅在回填/双写阶段可发起切换')
        state['phase']=phases.SWITCHING
        state['updated_at']=now();state['detail']='切换进行中：暂停新研究冻结，准备全量校验'
        gw.save_state(tx,state)
        return gw.get_state(tx)

def complete_switch(gw,tx):
    """原子切换第二步：同一事务内全量校验，通过才切到 integer，否则保持 switching。"""
    with _locked(gw,tx):
        state=gw.get_state(tx)
        if state['phase']!=phases.SWITCHING:
            raise MigrationConflict('没有进行中的切换')
        diffs=full_diff(gw,tx)
        state['diffs']=diffs;state['verified_at']=now()
        if diffs:
            state['detail']=f'切换被拒绝：校验发现 {len(diffs)} 处差异，仍读旧浮点存储'
            gw.save_state(tx,state)
            return {'switched':False,'diffs':diffs}
        state['phase']=phases.INTEGER
        state['detail']='切换完成：读取依据为整数专用存储，旧表继续同步以支持回退'
        state['updated_at']=now()
        gw.save_state(tx,state)
        return {'switched':True,'diffs':[]}

def rollback(gw,tx):
    """回退到 dual：切换失败或切换后均可回退，期间写入全部保留。"""
    with _locked(gw,tx):
        state=gw.get_state(tx)
        if state['phase'] not in (phases.SWITCHING,phases.INTEGER):
            raise MigrationConflict('仅切换中或切换后可回退')
        previous=state['phase']
        state['phase']=phases.DUAL
        state['diffs']=[];state['verified_at']=None
        state['updated_at']=now()
        state['detail']=f'已从 {previous} 回退到双写阶段，恢复旧浮点存储读写；期间写入保留'
        gw.save_state(tx,state)
        return {'rolled_back':True,'from_phase':previous,'state':gw.get_state(tx)}
