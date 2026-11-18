# Schema 版本说明

## v1（初始）
由 SQLAlchemy 模型在启动时 `Base.metadata.create_all` 建立；本地小应用不执行破坏性迁移。
修改 schema 前请备份命名卷 db-data。

## v2（工艺计数浮点 → 整数专用存储，在线迁移）
新增三张表（见 `src/models/migration.py`，启动时同样自动 create_all，不影响旧表）：

- `observations_int`：整数专用存储。`success`/`failure` 为原生 `Integer` 列，
  `legacy_id` 与旧 `observations.id` 一一对应并唯一，外部行编号在切换后保持不变；
  保留 `(study_id, source_key)` 自然键唯一约束。
- `observation_tombstones`：移除墓碑。回填期间被移除的自然键不被回填复活；
  同自然键重新录入时清除墓碑。
- `migration_state`：单例行（id=1），记录阶段 `legacy/dual/switching/integer`、
  回填游标 `last_cursor_id`、已回填数、无法回填的非整数记录 `skipped`、
  最近校验差异 `diffs` 与时间戳。

阶段机与不变量见 `src/services/migration_phases.py`、`migration.py`，
无依赖的并发/续跑验证见 `TESTING.md`。旧表 `observations` 在整个迁移期间保留并继续双写，
回退通道始终可用；已完成研究的 `runs.snapshot` 不被迁移触碰。
