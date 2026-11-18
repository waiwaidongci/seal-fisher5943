# 验证记录：工艺计数浮点→整数存储迁移

## 运行方式（仅依赖 Python 标准库，无需 FastAPI/SQLAlchemy/MySQL/Redis）

```bash
cd backend
python3 -m unittest tests.test_migration_lifecycle tests.test_concurrency -v
```

迁移核心（`src/services/migration.py`、`counts_ops.py`、`count_store.py`、
`migration_phases.py`）只依赖存储网关协议 `CountGateway`，不导入任何 Web/ORM 库。
`tests/sqlite_gateway.py` 用标准库 `sqlite3`（WAL + 多连接 + 线程）实现同一协议，
因此同一份生产逻辑可在隔离环境真实跑并发与崩溃续跑；生产环境由
`src/services/count_gateway_sa.py` 接到 MySQL，行锁用 `SELECT ... FOR UPDATE`。

## 覆盖的不变量与场景

### 生命周期（tests/test_migration_lifecycle.py，9 例）
1. 迁前 `legacy` 阶段旧浮点记录可正常读写，整数表为空。
2. 回填按旧表主键游标分批推进；中断后另起会话续跑，已迁行只对齐不重复，末尾幂等。
3. 非整数（如 3.5）旧记录无法无损转换，记入 `skipped`；全量校验产生差异并拒绝切换，
   停在 `switching`；更正后重新回填、校验、切换成功。
4. 校验通过 → begin/complete 原子切换 → 读取依据变为整数专用存储。
5. 切换后在整数存储上新增/更正/移除，再回退到 `dual`：读取恢复旧表，期间写入全部保留，
   两存储仍可重新校验通过。
6. 已完成研究的 `runs.snapshot` 与 checksum 不随迁移/切换改变；新冻结取整数表完整版本。
7. 阶段机非法动作（legacy 校验、重复 start、dual 回退等）明确拒绝。
8. `coerce_int_values` 对非整数/负数/布尔值拒绝。
9. 更正时改自然键，两存储同步改名、旧键立墓碑，全量校验一致。

### 并发（tests/test_concurrency.py，5 例，WAL 多连接真实线程交错）
1. **移除撞回填**：回填线程与移除线程并发，被移除的 60 条既不留在旧表也绝不被回填复活；
   其余 140 条两存储齐全，`full_diff` 为空。
2. **切换撞冻结**：`switching` 窗口内研究冻结抛 `SwitchInProgress`；切换完成后冻结恢复，
   且快照取自整数表、行数完整。
3. **回退前新写入**：integer 阶段多线程混合新增/更正/移除，回退后全部保留且两存储一致，
   可重新校验通过。
4. **回填撞新增**：双写阶段新增与回填并发，最终两存储行数相等、无差异；重名自然键仍被拒绝。
5. **删除后同自然键重录**：墓碑阻止复活；重新录入同键时墓碑清除，后续回填保留该行。

## 最近一次执行结果

```
$ python3 -m unittest tests.test_migration_lifecycle tests.test_concurrency
Ran 14 tests in 4.2s
OK
```

并发用例连续多轮（含 5 轮重复）均稳定通过，无偶发失败。

## 生产环境差异说明

- SQLite 单写者以 `BEGIN IMMEDIATE` + 保存点模拟行锁；MySQL 上 `migration_state`
  单行 `FOR UPDATE` 提供更强的跨连接互斥，服务层事务边界与异常处理完全一致。
- 生产切换/回填/回退接口见 `src/routes/migration.py`（仅 admin），
  页面在「存储迁移」与「来源录入」「分析执行」呈现阶段、差异与当前读取依据。
