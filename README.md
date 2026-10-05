# 天文瞬变事件警报与后续观测

只使用Python标准库和SQLite的模块化服务，默认端口`8338`。支持全天巡天来源、候选事件去重、坐标与亮度测量合并、优先级计算、观测申请、望远镜排程、撤回、重分类、修正、观测队冲突、角色权限、审计，以及瞬变事件的**连拍接力链（relay chain）**。

## 模块结构

- `app.py`：参数解析、依赖组装和服务生命周期。
- `src/domain.py`：角色、领域异常和数据对象。
- `src/rules.py`：状态机、优先级、测量合并、排程冲突约束、连拍节奏与空闲窗口探测。
- `src/chains.py`：接力链编排（开链、段完成推进、失败重试、撤回/重分类/变暗重算）。
- `src/repository.py`：SQLite持久化、乐观锁、单事务原语、幂等和审计查询。
- `src/service.py`：用例编排、权限校验、版本控制和链路由。
- `src/http_api.py`：JSON接口和统一错误响应。
- `src/audit.py`：操作审计。
- `static/index.html`：最小演示页面。

## 初始化与启动

```bash
python3 app.py --db ./data.db --port 8338
```

## 核心对象

`source`为巡天来源，`candidate`为瞬变候选，`telescope`为望远镜，`observation`为后续观测申请，`chain`为绑定候选/望远镜/观测队的接力链。

## 接口

- `GET /health`
- `GET /api/<kind>`（含`chains`），可用`?status=`过滤
- `GET /api/entities/<id>`
- `POST /api/<kind>`
- `POST /api/entities/<id>/actions`
- `GET /api/audit`

身份通过`X-User-Id`和`X-Role`请求头传入。可选`Idempotency-Key`防止重复创建。

## 连拍接力链

瞬变事件按间隔连拍，每段观测单独建单：

- 对候选执行`start_chain`（`{"telescope_id","team_id","mode"?,"duration_hours"?}`）即开链。第N段的名义窗口 = 最新测量时刻 + 该事件类型节奏表（`CADENCE_HOURS`，GRB/超新星/TDE/变星/未知各自不同）；窗口在名义时刻之后以15分钟步长向后探测，只选望远镜和观测队都空闲的时段（半开区间，背靠背不算冲突），48小时内找不到则该段落为`failed`。
- 段观测`complete`后链在**同一事务**内推进：完成记录照常落库，并尝试占下一段。下一段写入失败不会回滚完成进度，也不会占用任何窗口；段上保留`failure_reason`和`attempts`，对链执行`retry_segment`即可重新占位，恢复后不重复占用。
- 候选`withdraw`（链终止）、`reclassify`（按新类型节奏）或合并测量后变暗严格超过2等时，未完成段全部置为`invalidated`（其观测单撤回），已完成段原样保留，再从最新测量时刻重算剩余段。
- 两人同时提交同一段完成结果时，单事务加`BEGIN IMMEDIATE`串行化并配合乐观版本号：只有一份推进链，另一份收到`409 ConflictError`。
- 已排程的链内观测单被手动`withdraw`时，对应段落回`failed`，可通过`retry_segment`重新占位。

链实体的`data.segments`以当前节奏索引为序，状态为`pending/scheduled/completed/failed/invalidated`；被重算取代的非完成段进入`invalidated_history`，审计日志保留全部轨迹。

## 测试

```bash
python3 -m unittest discover -s tests -v
```

## 局限

轨道和观测计划使用简化的ISO时间窗比较，不包含真实天文历表、可见性预报、望远镜控制系统和观测数据存储。
