# 砚田包装试封比较席

两种封口工艺独立合格/失败计数→固定方案比较→复试决策；双侧Fisher按概率不大于观测表求和。原生 HTML/CSS/JavaScript 多模块工作席，所有计算由 Python 完成。

## 模拟业务
砚田研究协作组，12名分析师与业务人员，日常使用 `seal-fisher-202611-1.csv` 手工抄表。当前三个痛点：观测与口径不可追溯、重算易出错、结果与复核割裂。上游提供独立合成业务观测；本应用接管数据登记、方法执行和输入冻结；下游负责人形成复测/预算方案，观察员获取复核后的CSV。

核心业务实体：SealTrial / TrialCount / ExactRun / TrialDecision。源码 model/schema/service/controller/route 与前端 api/store/pages 完成链路，不依赖浏览器本地假数据。

## 功能与方法
- 试封方案、工艺计数录入/编辑/删除；重名记录与非法数值明确拒绝。
- Fisher精确检验、Wilson单比例区间与率差。两组试件独立，组别预先固定。双侧 p 值是固定边际下概率不大于观察表的所有表之和。
- 每次运行冻结参数、观测和SHA256。Redis任务由独立worker计算，写DB与共享产物卷；输入修改不会改写历史结果。
- 负责人复核不可覆盖；审计记录登录、数据改动、排队、复核与导出；240请求/分钟限流、全局可恢复错误。

## 快速启动
```bash
cp .env.example .env
docker compose up -d --build
docker compose exec -T backend python -m scripts.seed_demo
```
访问 http://localhost:18241 ，API http://localhost:19241/docs 。数据库映射 17741。无需Node构建。可以从中文和带空格目录启动。清理 `docker compose down -v --remove-orphans` 会删除本题演示数据卷。

|角色|账号|密码|权限与典型动作|
|---|---|---|---|
|admin 负责人|admin@demo.local|Research2026!|全部读写、批准复核、下载|
|analyst 分析师|analyst@demo.local|Research2026!|数据修改、创建任务、读取结果及下载；不能复核|
|viewer 观察员|viewer@demo.local|Research2026!|只读；仅可导出已复核结果|

## 一天的工作剧本
1. 09:00 admin登录，查“砚田·薄膜一轮”。
2. 09:10 analyst核对 `seal-fisher-202611-1.csv` 的seed记录。
3. 09:15 来源录入页检查 `工艺组 (A/B)` 等业务字段，必要时编辑。
4. 09:20 分析执行页保留默认参数 `{"alpha": 0.05}`，命名本轮研究。
5. 09:21 冻结输入并排队，worker实算，结果页自动刷新。
6. 09:25 阅读核心数值、方法边界、明细与SHA256；发现失败先修数据重跑。
7. 09:35 admin打开业务复核，写明限制与下一步动作，归档后不可覆盖。
8. 09:40 viewer读取复核意见并下载CSV，在操作留痕页核对下载事件。

seed提供3次试验×2个工艺组，共6条汇总计数，代表600份独立试件。每次运行每组只允许一条预聚合记录；修正计数应编辑原记录，重复组会明确失败。

## 技术与目录
|部分|实现|
|---|---|
|前端|原生ES modules、HTML/CSS、Nginx1.27，无前端框架|
|API|Python3.11、FastAPI0.115、SQLAlchemy2、JWT、PBKDF2密码|
|计算|NumPy2.2、SciPy1.15，固定seed42|
|数据库|MySQL8|
|任务|Redis7持久队列，Python独立worker，心跳healthcheck|

`frontend/src/`分api、stores、types、components/common、hooks、pages、router、utils、constants；`backend/src/`分config、models、schemas、services、controllers、routes、middlewares、utils、constants；`backend/scripts/seed_demo.py`自然键插入、重复不覆盖修改；`backend/migrations/README.md`说明schema版本。

环境变量 `.env.example`：COMPOSE_PROJECT_NAME、FRONTEND_PORT、BACKEND_PORT、DB_PORT、DB_PASSWORD、JWT_SECRET、SEED_DEMO。开发时在本地venv安装backend/requirements.txt，设置DATABASE_URL、REDIS_URL、JWT_SECRET再运行uvicorn；前端开发经Nginx代理 `/api/` 到不带前缀的API。

共享枚举：角色admin/analyst/viewer和队列queued/running/completed/failed；后端`src/constants/enums.py`、认证与worker使用相同值；前端`src/types/enums.js`、状态与角色界面使用相同值。Google Fonts：Noto Serif SC + IBM Plex Mono；断网使用serif/monospace回退。

## 算法输入、失败态与复现
输入为选定批次全部观测和参数，输出核心数值、明细、解释；参数经后端严格白名单和范围验证。两组试件独立，组别预先固定。双侧 p 值是固定边际下概率不大于观察表的所有表之和。。空批次拒绝；不完整字段/非有限数/非法计数拒绝；常量相关列与不足样本使任务failed并保留错误。固定seed42、输入checksum相同可复现。独立oracle：2×2固定边际对scipy fisher_exact；零格与计数非法拒绝。

已知边界：小型本地研究应用，使用合成数据，不含企业SSO与外发邮件；结果由人工复核，不自动执行业务决策；单worker恢复queued/running任务，数据库与Redis间未采用分布式事务。以当前测试目录TESTING.md记录实际验证结果。

## 工艺计数存储迁移（浮点 JSON → 整数专用存储）
工艺计数长期存于 `observations.values` 的浮点 JSON，现迁入整数专用表 `observations_int`，迁移期间录入与研究发起不中断：
- 阶段机 `legacy → dual → switching → integer`，可从 `switching/integer` 一键回退到 `dual`；状态存于单例 `migration_state`。
- `dual` 阶段在线回填（按旧表主键游标、幂等、可中断续跑），新增/更正/移除在同一事务双写两存储；移除落 `observation_tombstones` 墓碑，回填绝不复活已移除记录，同自然键重录自动清碑。
- 原子切换两步：`switch/begin` 进入 switching 并拒绝新研究冻结；`switch/complete` 在同事务全量校验，发现任何差异即拒绝切换并保留旧表读写；回退保留期间全部写入。
- 研究冻结锁定迁移状态与该批次研究行，取同一完整版本快照；已完成研究的历史快照与 SHA256 不被迁移触碰。
- 接口 `/api/migration/{status,start,backfill,verify,switch/begin,switch/complete,rollback}`（仅 admin）；前端「存储迁移」页与来源录入/分析执行页呈现迁移阶段、校验差异与当前读取依据。
- 迁移核心只依赖存储网关协议，`backend/TESTING.md` 记录仅用标准库 sqlite 的 14 项并发与生命周期验证（含移除撞回填、切换撞冻结、回退前新写入）。

MIT License。
