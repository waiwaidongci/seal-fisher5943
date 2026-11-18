"""测试辅助：共享内存 sqlite 库、事务包装与并发重试。"""
import time
import sqlite3
from tests.sqlite_gateway import connect,init_schema,SqliteCountGateway

def make_world():
    conn=connect(':memory:')
    init_schema(conn)
    gw=SqliteCountGateway()
    gw.ensure_state(conn);conn.commit()
    return conn,gw

def make_study(conn,name='S'):
    cur=conn.execute('INSERT INTO studies(name) VALUES(?)',(name,))
    conn.commit()
    return cur.lastrowid

def seed_legacy(conn,study_id,n):
    """模拟历史浮点记录：success/failure 以 float 落 JSON。"""
    for i in range(1,n+1):
        vals={'group':'A' if i%2 else 'B','success':float(10+i),'failure':float(i)}
        conn.execute('INSERT INTO observations(study_id,source_key,values_json) VALUES(?,?,?)',
                     (study_id,f'K-{i:04d}',__import__('json').dumps(vals)))
    conn.commit()

def tx(conn,fn,*args):
    """在一个事务里跑服务调用；成功提交，业务异常回滚并抛出。"""
    try:
        out=fn(*args)
        conn.commit()
        return out
    except Exception:
        conn.rollback()
        raise

def retry_locked(fn,attempts=50,delay=0.02):
    """WAL 下写锁冲突时短暂重试，模拟真实服务的等待行为。"""
    last=None
    for _ in range(attempts):
        try:
            return fn()
        except sqlite3.OperationalError as e:
            last=e
            if 'locked' not in str(e).lower(): raise
            time.sleep(delay)
    raise last
