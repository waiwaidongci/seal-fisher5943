"""标准库 sqlite3 版计数存储网关，仅用于验证与教学测试。

与生产 SqlAlchemyCountGateway 实现同一 CountGateway 协议，因此 migration /
counts_ops 的同一份逻辑可在无 FastAPI、无 SQLAlchemy 的环境里真实执行，
并通过多连接 + WAL 线程跑回填/写入/移除/冻结的并发场景。

tx 即一个 sqlite3.Connection（行工厂为 sqlite3.Row）；提交/回滚由调用方控制。
"""
import json
from src.services import migration_phases as phases
from src.services.count_store import coerce_int_values
from src.utils.timeutil import now

class TxConn:
    """连接包装：以保存点模拟嵌套事务，tx_depth 与底层事务状态保持一致。

    最外层 lock 开启 BEGIN IMMEDIATE；内层 lock 开 SAVEPOINT；
    unlock 配对 RELEASE/ROLLBACK TO；最外层 unlock 后由 commit() 落盘。
    """
    def __init__(self,raw):
        self.raw=raw;self.tx_depth=0
    def execute(self,sql,params=()): return self.raw.execute(sql,params)
    def executescript(self,sql): return self.raw.executescript(sql)
    def begin(self):
        if self.tx_depth==0:
            # 兼容锁外 DML 隐式开启的事务：已在事务中则直接纳入计数管理
            if not self.raw.in_transaction:
                self.raw.execute('BEGIN IMMEDIATE')
        self.tx_depth+=1
    def savepoint(self):
        self.raw.execute(f'SAVEPOINT sp{self.tx_depth}');self.tx_depth+=1
    def release_one(self):
        self.tx_depth-=1
        if self.tx_depth>0: self.raw.execute(f'RELEASE SAVEPOINT sp{self.tx_depth}')
    def rollback_one(self):
        self.tx_depth-=1
        if self.tx_depth>0: self.raw.execute(f'ROLLBACK TO SAVEPOINT sp{self.tx_depth}')
    def commit(self):
        if self.tx_depth:
            # 仍有未配对的锁（不应发生），先压平再提交
            self.tx_depth=0
        self.raw.commit()
    def rollback(self):
        self.tx_depth=0;self.raw.rollback()
    def close(self): self.raw.close()
    def in_transaction(self): return self.raw.in_transaction
    def __getattr__(self,name): return getattr(self.raw,name)

SCHEMA="""
CREATE TABLE IF NOT EXISTS studies(id INTEGER PRIMARY KEY AUTOINCREMENT,name TEXT);
CREATE TABLE IF NOT EXISTS observations(
  id INTEGER PRIMARY KEY AUTOINCREMENT,study_id INTEGER NOT NULL,
  source_key TEXT NOT NULL,values_json TEXT NOT NULL,
  UNIQUE(study_id,source_key));
CREATE TABLE IF NOT EXISTS observations_int(
  id INTEGER PRIMARY KEY AUTOINCREMENT,legacy_id INTEGER NOT NULL UNIQUE,
  study_id INTEGER NOT NULL,source_key TEXT NOT NULL,
  group_name TEXT NOT NULL,success INTEGER NOT NULL,failure INTEGER NOT NULL,
  UNIQUE(study_id,source_key));
CREATE TABLE IF NOT EXISTS observation_tombstones(
  id INTEGER PRIMARY KEY AUTOINCREMENT,study_id INTEGER NOT NULL,
  source_key TEXT NOT NULL,removed_at TEXT NOT NULL,
  UNIQUE(study_id,source_key));
CREATE TABLE IF NOT EXISTS migration_state(
  id INTEGER PRIMARY KEY,phase TEXT NOT NULL,last_cursor_id INTEGER NOT NULL,
  backfilled INTEGER NOT NULL,skipped_json TEXT NOT NULL,diffs_json TEXT NOT NULL,
  verified_at TEXT,updated_at TEXT NOT NULL,detail TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS runs(
  id INTEGER PRIMARY KEY AUTOINCREMENT,study_id INTEGER NOT NULL,
  name TEXT,status TEXT DEFAULT 'queued',checksum TEXT);
"""

def connect(path=':memory:'):
    import sqlite3,uuid
    if path==':memory:':
        # 多连接共享的内存库需要 file: URI 与共享缓存
        uri=f'file:{uuid.uuid4().hex}?mode=memory&cache=shared'
        raw=sqlite3.connect(uri,uri=True,timeout=30,isolation_level=None)
    else:
        raw=sqlite3.connect(path,timeout=30,isolation_level=None)
    raw.row_factory=sqlite3.Row
    conn=TxConn(raw)
    conn.execute('PRAGMA journal_mode=WAL')
    conn.execute('PRAGMA foreign_keys=ON')
    conn.execute('PRAGMA busy_timeout=30000')
    return conn

def init_schema(conn):
    conn.executescript(SCHEMA)

class SqliteCountGateway:
    # ---- 迁移状态 ----
    def ensure_state(self,conn):
        row=conn.execute('SELECT * FROM migration_state WHERE id=1').fetchone()
        if not row:
            conn.execute("INSERT INTO migration_state VALUES(1,?,?,?,?,?,?,?,?)",
                         (phases.LEGACY,0,0,'[]','[]',None,now(),'初始化：旧浮点存储在线'))
        return self.get_state(conn)

    def get_state(self,conn):
        row=conn.execute('SELECT * FROM migration_state WHERE id=1').fetchone()
        if not row: return self.ensure_state(conn)
        return self._state(row)

    def lock_state(self,conn):
        # SQLite 单写者：写事务本身即互斥。同一连接的嵌套服务调用通过保存点复用
        # 外层事务（与 SQLAlchemy Session 内多次 SELECT FOR UPDATE 的行为对齐）；
        # 最外层开启 IMMEDIATE 立即取得保留锁。服务层退出时必须配对调用
        # unlock_state（成功 RELEASE，异常 ROLLBACK TO）。
        if conn.tx_depth:
            conn.savepoint()
        else:
            conn.begin()
        row=conn.execute('SELECT * FROM migration_state WHERE id=1').fetchone()
        return self._state(row)

    def unlock_state(self,conn,ok):
        if conn.tx_depth<=0: return
        (conn.release_one if ok else conn.rollback_one)()

    @staticmethod
    def commit(conn):
        conn.commit()

    @staticmethod
    def rollback(conn):
        conn.rollback()

    def save_state(self,conn,state):
        conn.execute("""UPDATE migration_state SET phase=?,last_cursor_id=?,backfilled=?,
                        skipped_json=?,diffs_json=?,verified_at=?,updated_at=?,detail=? WHERE id=1""",
                     (state['phase'],state['last_cursor_id'],state['backfilled'],
                      json.dumps(state['skipped'],ensure_ascii=False),
                      json.dumps(state['diffs'],ensure_ascii=False),
                      state['verified_at'],state['updated_at'],state['detail']))

    @staticmethod
    def _state(row):
        return {'phase':row['phase'],'last_cursor_id':row['last_cursor_id'],
                'backfilled':row['backfilled'],'skipped':json.loads(row['skipped_json']),
                'diffs':json.loads(row['diffs_json']),'verified_at':row['verified_at'],
                'updated_at':row['updated_at'],'detail':row['detail']}

    # ---- 旧浮点表 ----
    def legacy_get_by_natural(self,conn,study_id,source_key):
        row=conn.execute('SELECT * FROM observations WHERE study_id=? AND source_key=?',
                         (study_id,source_key)).fetchone()
        return self._legacy(row) if row else None

    def legacy_get(self,conn,row_id):
        row=conn.execute('SELECT * FROM observations WHERE id=?',(row_id,)).fetchone()
        return self._legacy(row) if row else None

    def legacy_list(self,conn,study_id):
        return [self._legacy(r) for r in conn.execute(
            'SELECT * FROM observations WHERE study_id=? ORDER BY id',(study_id,))]

    def legacy_insert(self,conn,study_id,source_key,values):
        cur=conn.execute('INSERT INTO observations(study_id,source_key,values_json) VALUES(?,?,?)',
                         (study_id,source_key,json.dumps(values,ensure_ascii=False)))
        return self.legacy_get(conn,cur.lastrowid)

    def legacy_update(self,conn,row,source_key,values):
        conn.execute('UPDATE observations SET source_key=?,values_json=? WHERE id=?',
                     (source_key,json.dumps(values,ensure_ascii=False),row['id']))

    def legacy_delete(self,conn,row):
        conn.execute('DELETE FROM observations WHERE id=?',(row['id'],))

    def legacy_scan_batch(self,conn,after_id,limit):
        return [self._legacy(r) for r in conn.execute(
            'SELECT * FROM observations WHERE id>? ORDER BY id LIMIT ?',(after_id,limit))]

    def legacy_count(self,conn):
        return conn.execute('SELECT COUNT(*) c FROM observations').fetchone()['c']

    @staticmethod
    def _legacy(row):
        return {'id':row['id'],'study_id':row['study_id'],
                'source_key':row['source_key'],'values':json.loads(row['values_json'])}

    # ---- 整数表 ----
    def int_get_by_legacy_id(self,conn,legacy_id):
        row=conn.execute('SELECT * FROM observations_int WHERE legacy_id=?',(legacy_id,)).fetchone()
        return self._int(row) if row else None

    def int_get_by_natural(self,conn,study_id,source_key):
        row=conn.execute('SELECT * FROM observations_int WHERE study_id=? AND source_key=?',
                         (study_id,source_key)).fetchone()
        return self._int(row) if row else None

    def int_list(self,conn,study_id):
        return [self._int(r) for r in conn.execute(
            'SELECT * FROM observations_int WHERE study_id=? ORDER BY legacy_id',(study_id,))]

    def int_upsert(self,conn,legacy_id,study_id,source_key,values):
        iv=coerce_int_values(values)
        conn.execute("""INSERT INTO observations_int(legacy_id,study_id,source_key,group_name,success,failure)
                        VALUES(?,?,?,?,?,?)
                        ON CONFLICT(legacy_id) DO UPDATE SET study_id=excluded.study_id,
                          source_key=excluded.source_key,group_name=excluded.group_name,
                          success=excluded.success,failure=excluded.failure""",
                     (legacy_id,study_id,source_key,iv['group'],iv['success'],iv['failure']))

    def int_delete_by_natural(self,conn,study_id,source_key):
        conn.execute('DELETE FROM observations_int WHERE study_id=? AND source_key=?',
                     (study_id,source_key))

    def int_count(self,conn):
        return conn.execute('SELECT COUNT(*) c FROM observations_int').fetchone()['c']

    @staticmethod
    def _int(row):
        # 对外 id 即稳定的 legacy_id；物理主键放在 int_pk，仅内部使用。
        return {'id':row['legacy_id'],'int_pk':row['id'],'legacy_id':row['legacy_id'],
                'study_id':row['study_id'],'source_key':row['source_key'],
                'values':{'group':row['group_name'],'success':int(row['success']),
                          'failure':int(row['failure'])}}

    # ---- 墓碑 ----
    def tombstone_has(self,conn,study_id,source_key):
        return conn.execute('SELECT COUNT(*) c FROM observation_tombstones WHERE study_id=? AND source_key=?',
                            (study_id,source_key)).fetchone()['c']>0

    def tombstone_add(self,conn,study_id,source_key,removed_at):
        conn.execute("""INSERT OR IGNORE INTO observation_tombstones(study_id,source_key,removed_at)
                        VALUES(?,?,?)""",(study_id,source_key,removed_at))

    def tombstone_drop(self,conn,study_id,source_key):
        conn.execute('DELETE FROM observation_tombstones WHERE study_id=? AND source_key=?',
                     (study_id,source_key))

    # ---- 研究冻结 ----
    def lock_study_runs(self,conn,study_id):
        conn.execute('SELECT id FROM runs WHERE study_id=?',(study_id,)).fetchall()
