# 地震记录包断点续传服务（纯后端）

野外地震仪通过不稳定链路上传**单个记录包**。服务保证：

- 上传前登记该包的**总字节数**与**整包 SHA-256**；
- 每个分块是携带 `start`（起始偏移）、`length`（字节数）、`sha256`（该块 SHA-256）的
  **二进制 PUT**，服务端独立重算块摘要；
- 新块必须**恰好从 `confirmed_offset` 开始**且不得越过 `total_bytes`；
- 已确认范围内**字节完全相同**的重发幂等成功（允许改变分块边界），其余旧偏移一律
  `409 stale_offset` 并回带当前期望偏移；重试永远不会覆盖已确认字节；
- 检查点（`confirmed_offset`）与每块字节都持久化在 PostgreSQL，**API 进程重启后可查询、
  可续传**；
- 仅当 `confirmed_offset == total_bytes` **且** 由持久化字节重算的整包 SHA-256 与登记值
  相符才封存（`sealed`，不可变）；
- 整包摘要不符 → 会话进入**不可续传的失败终态**（`failed`），必须用正确元数据新建会话；
- 封存校验与每次成功压实都在**同一事务**内追加一条不可变的归档审计快照（会话标识、事件
  顺序、发生时间、整包长度与摘要；压实快照另含目标块大小与前后分块数），可通过只读轨迹
  接口按事件顺序回溯一次布局调整对应哪份已校验内容；
- 封存记录支持 **HTTP 单区间读取**（`Range: bytes=a-b` / `a-` / `-n`）：跨持久化分块
  提取片段前重新核验全部归档字节（连续性、块长度、块摘要、整包摘要），压实前后同一区间
  响应一致；区间读取不留下审计记录或数据写入；
- 所有错误都定位到**偏移或摘要**；最终只能观察到摘要与长度一致的封存记录。

技术栈：Python 3.12 · FastAPI · SQLAlchemy 2.0 · PostgreSQL 16 · Docker Compose · pytest。

## 目录

```
app/                 FastAPI 应用（models/service/main/errors/schemas/database/config）
tests/               一次性验收用例（HTTP 黑盒 + PostgreSQL 校验 + 真实重启续传）
Dockerfile           API 镜像
Dockerfile.verify    verify 验收镜像（内置静态 docker CLI 用于重启 API 容器）
docker-compose.yml   db / api / verify 三服务编排
```

## 启动

```bash
# 默认宿主端口 8000；可用 API_PORT 覆盖：
API_PORT=9000 docker compose up --build -d

curl -s http://localhost:9000/health
```

## 一次性验收服务 verify

```bash
docker compose --profile verify run --rm --build verify
# 或显式：
docker compose --profile verify up --build verify
```

`verify` 是**一次性** pytest 容器（结束即退出），通过 compose 网络访问 `api:8000`，
并只读挂载 `/var/run/docker.sock`：用例会真实 `docker restart` API 容器，验证检查点与
分块字节在进程重启后仍可查询并续传。若环境没有 docker socket（例如无特权 CI），重启类
用例自动 skip，其余用例照常运行。

## HTTP 协议

### 1. 登记会话

```
POST /sessions
{"total_bytes": 5000, "whole_sha256": "<64 位小写 hex>"}
→ 201 {id, total_bytes, whole_sha256, confirmed_offset, status, ...}
```

`total_bytes=0` 时：摘要等于 `sha256("")` 直接封存；否则会话直接进入失败终态并返回
`422 whole_digest_mismatch`（错误定位到摘要）。

### 2. 上传二进制分块

```
PUT /sessions/{id}/chunks?start=0&length=4096&sha256=<块摘要>
Content-Type: application/octet-stream
<binary body>
```

服务端实际读取 body 字节并：核对声明长度 == 实际字节数；重算块 SHA-256；校验
`start == confirmed_offset`、`start + length <= total_bytes`。成功返回：

```json
{"id": "...", "status": "active|sealed", "start_offset": 0, "length": 4096,
 "chunk_sha256": "...", "confirmed_offset": 4096, "expected_offset": 4096,
 "total_bytes": 5000, "idempotent_replay": false}
```

- 旧偏移/跳跃/越界拼接：`409 stale_offset`，body 中 `location.offset` 与
  `location.expected_offset` 指明该从哪重传；
- 越过总长：`416 chunk_beyond_total`（含 `end`/`total_bytes`）；
- 声明长度与 body 不符：`422 length_mismatch`（定位偏移，含两个长度）；
- 块摘要不符：`400 chunk_digest_mismatch`（定位偏移、声明摘要、实算摘要）；
- 全部字节到齐但整包摘要不符：`422 whole_digest_mismatch`，会话转 `failed`，
  之后任何续传都得到 `409 session_failed_terminal`；
- 封存后：字节相同的重发仍 `200 idempotent_replay=true`；任何不同内容被
  `400/409` 拒绝，封存内容不可变。

### 3. 查询 / 下载

- `GET /sessions?status=sealed` — 列出会话；
- `GET /sessions/{id}` — 状态、`confirmed_offset`、登记/实算摘要；
- `GET /sessions/{id}/chunks` — 已持久化的分块检查点（偏移、长度、摘要）；
- `GET /sessions/{id}/content` — 仅 `sealed` 可下载；响应头 `ETag`/
  `X-Whole-SHA256` 为登记摘要，且服务端出库时再次校验。携带
  `Range: bytes=a-b`（闭区间）、`bytes=a-`（开放结尾）或 `bytes=-n`（后缀长度）
  时执行**单区间读取**：按封存包总长度解析区间，跨持久化分块提取字节，返回
  `206` 及 `Content-Range`/`Content-Length`/`Accept-Ranges`/原 `ETag`/
  `X-Whole-SHA256`。每次读取都按偏移重查全部分块的连续性、块长度与块摘要并
  重算整包摘要，确认归档完整后才交付片段，因此压实前后的不同分块边界对同一
  区间返回完全一致；格式错误或多区间 → `400 invalid_range`（`details.range`
  回带原请求头）；超出包长或零字节包的任意区间 → `416 range_not_satisfiable`
  并携带 `Content-Range: bytes */<total_bytes>`；区间读取不产生审计记录或数据
  写入；未带 `Range` 时仍以 `200` 返回完整字节与原响应头。

### 4. 压实封存记录

```
POST /sessions/{id}/compact
{"target_chunk_bytes": 4096}
→ 200 {"id", "status": "sealed", "target_chunk_bytes",
       "chunks_before", "chunks_after",
       "total_bytes_before", "total_bytes_after",
       "whole_sha256", "chunks": [{"start_offset","end_offset","length","sha256"}, ...]}
```

封存后在**会话行锁保护的单个事务**中按偏移顺序读出既有字节，重排为长度不超过
`target_chunk_bytes` 的连续分块并重算每块摘要；提交前再次核对总长度与整包
SHA-256，任一不符则完整回滚。整包身份（`id`、总字节数、整包摘要）不变，
压实仅改变分块边界：分块数与总字节数的前后值及新布局随响应返回。

- 同一目标重复压实是确定性、幂等的：得到相同布局与块摘要（已是目标布局时
  不写任何行，`chunks_before == chunks_after`）；但每次**成功**压实（含幂等
  空操作）都会在审计轨迹追加一条快照；
- 仅 `sealed` 会话可压实；`active`/`failed` 返回
  `409 compaction_state_conflict`（`location.expected_offset` 与
  `details.status` 定位会话状态），且无任何副作用；
- `target_chunk_bytes` 非正整数 / 非整数返回 `422 validation_error`；
- 压实期间其他查询只能看到压实前或压实后的完整布局（单事务提交）；
- 创建、上传、分块查询与内容下载契约保持兼容。

### 5. 归档审计轨迹

```
GET /sessions/{id}/audit
→ 200 {"id", "events": [{"sequence", "event", "occurred_at",
       "total_bytes", "whole_sha256",
       "target_chunk_bytes", "chunks_before", "chunks_after"}, ...]}
```

只读接口，按 `sequence` 升序返回快照，不改变上传会话的生命周期：

- 封存校验成功时，在封存 PUT 的**同一事务**写入唯一首条记录
  （`sequence=1`、`event="sealed"`，含会话标识、整包长度与已校验摘要；
  0 字节包创建即封存时同样写入）；
- 每次压实成功追加一条 `event="compacted"` 记录，复用本次压实的核验摘要
  与统计：`target_chunk_bytes`、`chunks_before`/`chunks_after`、整包长度与
  摘要（不重新读取或改写 Chunk 内容）；
- `sealed` 记录的三个压实字段为 `null`；
- 未知会话返回与其他接口一致的 `404 session_not_found`；
- `active`/`failed` 会话、以及升级前已封存（没有审计行）的旧会话返回
  `{"events": []}`；旧会话之后再压实会从 `sequence=1` 开始追加
  `compacted` 记录（封存快照不回填）；
- 封存失败不产生任何记录；压实任一步校验失败或审计写入失败时整个事务回滚，
  布局与审计行同生共灭，不留下孤立记录。

审计记录保存在新表 `audit_events`（依附 `upload_sessions`，外键
`ON DELETE CASCADE`，`(session_id, sequence)` 唯一），随会话一并持久化，
API 重启后可查询；新库由 `create_all` 建表，旧库启动时自动补建。

## 示例（curl）

```bash
head -c 5000 /dev/urandom > pkg.bin
TOTAL=$(stat -c%s pkg.bin)
WHOLE=$(sha256sum pkg.bin | cut -d' ' -f1)

SID=$(curl -s -XPOST localhost:8000/sessions \
  -H 'content-type: application/json' \
  -d "{\"total_bytes\":$TOTAL,\"whole_sha256\":\"$WHOLE\"}" | jq -r .id)

dd if=pkg.bin bs=4096 count=1 2>/dev/null | curl -s -X PUT --data-binary @- \
  "localhost:8000/sessions/$SID/chunks?start=0&length=4096&sha256=$(
    head -c 4096 pkg.bin | sha256sum | cut -d' ' -f1)"
# 断连后用 GET /sessions/$SID 取回 expected_offset，从该偏移继续即可
```

## 持久化与并发

- `upload_sessions(id, total_bytes, whole_sha256, confirmed_offset, status,
  computed_sha256, failure_reason, created_at, updated_at)`；
- `chunks(session_id, start_offset, end_offset, length, sha256, data bytea)`，
  `(session_id, start_offset)` 唯一约束；
- `audit_events(session_id, sequence, event, occurred_at, total_bytes,
  whole_sha256, target_chunk_bytes, chunks_before, chunks_after)`，
  `(session_id, sequence)` 唯一约束，外键随会话级联删除；封存/压实审计行与
  对应状态变更在同一个事务内提交或回滚；
- 每个分块 PUT 在事务内对会话行 `SELECT ... FOR UPDATE` 加锁，同会话并发写入按序
  提交；败者收到带期望偏移的 `409` 后按协议重试即可；
- 封存时按偏移顺序流式喂给 hasher 重算整包摘要（不在内存里拼装整包）。
- 压实同样在会话行锁保护的**单个事务**内完成：按偏移读出旧字节→重排为不超过
  `target_chunk_bytes` 的连续分块并重算块摘要→删除旧行、插入新行→提交前再次核对
  连续性、总长度与整包摘要；任一不符完整回滚。读已提交隔离下，压实进行中的外部
  查询只能看到压实前或压实后的完整布局。
