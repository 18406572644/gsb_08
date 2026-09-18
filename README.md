# 可靠消息聊天室（Node + ws + SQLite）

基于 WebSocket 的可靠消息投递聊天室。不引入 MQ，以 SQLite 为唯一持久化设施，实现：

- **消息不丢失**：先落库、再 ACK、后广播；服务重启后消息完整可补发
- **ACK 确认**：双向确认 —— 发送方收服务端 ACK（含分配的 seq）；接收方对推送做累积 ACK
- **断线补发**：重连后按 `lastSeq` 增量回放缺口，分批拉取
- **幂等去重**：`clientMsgId` 唯一约束防发送重试产生重复；客户端按 `seq` 过滤重复投递
- **消息时序可控**：每房间单调递增 `seq`，由计数器在写事务内分配，保证房间内全序
- **连接管理**：心跳保活、全局/单用户连接数上限、背压断开、优雅退出
- **房间权限**：管理员 / 成员 / 禁言三种状态，管理员可禁言、解禁
- **发送限流**：按用户令牌桶
- **消息审核工作流**：敏感词 + 频率异常检测，三种处置模式，人工队列/复核/申诉/规则版本/操作日志

## 快速开始

```bash
npm install
npm start          # http://localhost:8080
npm test           # 34 个集成测试（13 个可靠投递 + 21 个审核工作流）
```

浏览器打开 `http://localhost:8080`，用不同昵称开两个标签页即可体验（建房、发消息、
禁言管理、切换审核模式、审核台处理队列与申诉）。断网/刷新页面后自动重连并补发离线期间的消息与审核指令。

要求 Node.js ≥ 22.13（使用内置 `node:sqlite`，唯一第三方依赖是 `ws`）。

## 架构

```
src/
├── config.js     配置（端口、连接上限、心跳、重发、限流、审核，均可环境变量覆盖）
├── db.js         SQLite 持久层：schema/迁移、幂等写入、seq 分配、游标、审核状态机、规则版本、申诉、日志
├── detector.js   检测服务（异步）：敏感词匹配、滑动窗口频率异常、超时/故障 fail-open
├── moderator.js  审核编排器：检测→状态机→广播联动、人工决策、申诉裁决、TTL 兜底、断线事件补发
├── hub.js        连接注册中心：房间索引、广播、消息/审核指令双可靠通道、心跳/重发扫描
├── server.js     HTTP + WS 服务：认证、消息路由、权限检查、限流、审核协议、生命周期
└── util.js       token 签名、帧解析等工具
public/index.html 演示客户端（可靠投递协议 + 审核状态展示 + 申诉 + 管理员审核台）
test/              集成测试（node:test）
```

### 数据模型

| 表 | 说明 |
|---|---|
| `users` | 用户（演示级 token 认证） |
| `rooms` | 房间，`last_seq` 为房间消息序号计数器 |
| `members` | 成员关系：`role`（admin/member）+ `muted_until`（禁言截止时间） |
| `messages` | 消息。主键 `(room_id, seq)`；幂等键 `(room_id, sender_id, client_msg_id)`；审核字段 `review_status/flags/reason/rule_version/...` |
| `cursors` | 每用户每房间消息投递游标 `last_ack_seq`（断线补发兜底） |
| `review_cursors` | 每用户每房间**审核指令**水位 `last_event`（与投递游标独立） |
| `review_rules` | 审核规则不可变版本（敏感词 + 频率阈值），旧版本归档不删除 |
| `room_review_policy` | 每房间处置模式 `pre/post/mark` |
| `review_queue` | 人工审核队列（每消息至多一个 open 条目，部分唯一索引） |
| `review_events` | 审核事件只增日志：held/released/blocked/recalled/restored/flagged |
| `appeals` | 用户申诉（每用户每消息一条，驳回冷却后可重新打开） |
| `admin_log` | 管理员操作日志（只增，全链路留痕） |

旧库启动时自动迁移：`messages` 追加审核列，历史消息一律视为 `released`。

## 可靠性设计

### 1. 不丢失：持久化先于广播

发送路径在一个 SQLite 事务内完成「递增 `rooms.last_seq` 分配 seq + 写入 messages」，
**提交后**才向发送方回 ACK、向房间广播。因此：凡是客户端收到 ACK 的消息，必然已落库，
进程崩溃/重启后不丢（WAL + `synchronous=FULL`）。广播失败的连接由补发机制兜底。

### 2. 发送幂等：clientMsgId 唯一约束

客户端为每条消息生成唯一 `clientMsgId`，未收到 ACK 时以**同一 ID** 重发。
服务端命中 `(room_id, sender_id, client_msg_id)` 唯一约束时直接返回原消息的 ACK
（含原 seq），不重复写入、不重复广播。网络重试、双击、超时重发都不会产生重复消息。

### 3. 至少一次投递 + 幂等消费 = 效果上的恰好一次

- 服务端向在线连接推送消息后登记「未 ACK 队列」，超时未收到该连接的累积 ACK 则重发；
  超过最大重发次数判定连接不可用并断开，等客户端重连走补发。
- 客户端按房间维护 `lastSeenSeq`，凡是 `seq <= lastSeenSeq` 的投递一律丢弃 ——
  重发、补发重叠都不会重复上屏。

### 4. 断线补发：sync 协议

客户端持久化每个房间的 `lastSeenSeq`。重连后：

```
client → {type:'join', room, lastSeq: 41}
server → {type:'joined', ...}
server → {type:'msg', seq: 42} ... {type:'msg', seq: 57}   （缺口回放，按序）
server → {type:'sync_done', roomId, lastSeq: 57, hasMore: false}
```

`hasMore=true` 时客户端用新的 `lastSeq` 继续 `sync` 拉取下一批（单批上限
`SYNC_BATCH_SIZE`，默认 500）。`lastSeq` 缺省时使用服务端保存的确认游标
（新设备场景）；历史消息可用 `history` 向前翻页。

### 5. 时序可控

`seq` 由 `rooms.last_seq` 在写事务内递增分配（单写者 + 事务 = 无空洞、无并发交错），
房间内消息严格全序。客户端凭 seq 即可检测空洞并触发补发，无需依赖时钟。

## 消息审核工作流

### 三种处置模式（每房间可配，`review_policy`）

| 模式 | 发送路径 | 检测命中 |
|---|---|---|
| `pre` 先审后发 | 落库→ACK→广播**无内容「审核中」占位帧**（占据 seq） | 停留 pending 入人工队列；无命中/检测降级立即放行，广播同 seq 正文帧 |
| `post` 先发后撤（默认） | 落库→ACK→正常广播（与原链路一致） | 消息转 recalled，广播撤回帧，入人工队列（可恢复） |
| `mark` 仅标记 | 正常广播 | 只打风险标（`msg_flagged`）并入队，可见性不变，管理员可再撤回 |

### 为什么不破坏原有可靠性语义

1. **ACK 机制不变**：ACK 仍在「落库事务提交后」立即回发送方。pre 模式只是暂缓*广播*，
   消息确实已持久化，ACK 不含任何审核承诺；检测慢/挂不影响 ACK 时延。
2. **seq 全序不变**：审核只改消息的 `review_status`，绝不删消息、绝不复用 seq。
   每个 seq 在任何时刻都恰好对应一帧 —— 正文 / pending 占位 / blocked 占位 / recalled 占位，
   占位帧同样推进客户端 `lastSeenSeq`，不产生空洞。
3. **断线补发不变**：消息缺口仍按 seq 升序回放，只是按消息*当前*审核状态转帧；
   审核指令（撤回/恢复/标记）走**第二条可靠通道** `reviewEventId`：
   独立追踪、独立超时重发、独立累积 ACK（`ack.reviewSeq`），互不污染 seq 水位。
   即使消息 seq 已追平，重连时仍按 `review_cursors` 补发晚到的撤回等指令。
4. **发送幂等不变**：`clientMsgId` 重试只回原 ACK，不重复入审、不重复广播；
   编排器另以 in-flight 去重 + DB 层 CAS 状态迁移保证同消息只调度一次检测。
5. **检测故障 fail-open**：检测是异步 RPC（可配超时），超时/异常时 pre 自动放行、
   post/mark 不处置，只写 `detect_degraded` 日志，绝不阻塞聊天主链路。

### 异常链路覆盖

- **检测超时/故障** → fail-open 降级，留痕不阻塞；
- **误判** → 人工恢复 / 用户申诉，申诉通过联动 `recalled|blocked → released`，原 seq 恢复可见；
- **重复审核** → 检测 in-flight 去重 + 队列部分唯一索引 + 状态迁移 CAS，重复提交幂等无副作用；
- **消息已被人工撤回** → 晚到的检测结果只接受当前仍 `released` 的消息，不覆盖人工决策、不补入队；
- **审核结果晚于客户端展示**（post 的撤回、pre 的放行、申诉恢复）→ 实时帧 + 断线事件补发双通道兜底；
- **pre 积压** → `REVIEW_PENDING_TTL_MS` 定时兜底自动放行（fail-open，默认关闭）。

### 审核状态机

```
                pre 落库
 released ───────────────▶ pending ──人工/检测放行──▶ released（广播正文）
    ▲   ▲                     │
    │   └────人工恢复─────────┴──人工拦截──▶ blocked
    │（申诉通过 / review_restore）             │
    │                                        │
    └────────────────────────────────────────┘
 released ──post 检测命中/人工撤回──▶ recalled ──人工恢复/申诉通过──▶ released
```

所有迁移在 SQLite 事务内完成「CAS 改状态（带期望源状态）+ 关队列项 + 写 review_events」，
并发/重复决策命中 0 行即放弃，天然幂等。

## 协议（JSON 文本帧）

### 客户端 → 服务端

| 类型 | 字段 | 说明 |
|---|---|---|
| `ping` | `t` | 应用层心跳，回 `pong` |
| `create_room` | `name` | 建房，创建者为管理员，回 `joined` |
| `join` | `room, lastSeq?` | 加入房间（room 可为 id 或名称）；带进度则立即补发 |
| `leave` | `roomId` | 离开房间 |
| `msg` | `roomId, clientMsgId, content` | 发消息，回 `ack` |
| `ack` | `roomId, seq, reviewSeq?` | 累积确认：消息 seq + 审核指令 reviewEventId 两条独立水位 |
| `sync` | `roomId, lastSeq?` | 请求补发 |
| `history` | `roomId, beforeSeq?, limit?` | 历史翻页（升序返回，按审核状态转帧） |
| `rooms` | — | 我加入的房间列表 |
| `members` | `roomId` | 成员列表（含在线状态） |
| `mute` | `roomId, userId, minutes` | 禁言（仅管理员，1..1440 分钟） |
| `unmute` | `roomId, userId` | 解除禁言（仅管理员） |
| `review_policy` | `roomId, mode?` | 查询模式；带 `mode=pre/post/mark` 为设置（仅管理员） |
| `review_queue` | `roomId?` | 人工审核队列（房间管理员/全局管理员） |
| `review_decide` | `roomId, seq, action, reason?` | 人工决策：`release/block/recall/restore/dismiss` |
| `review_enqueue` | `roomId, seq` | 手动把消息加入审核队列（巡检） |
| `review_recheck` | `roomId, seq` | 按当前最新规则只读重检（误判复核辅助） |
| `appeal_submit` | `roomId, seq, reason` | 发送者对被处置消息申诉 |
| `appeals_list` | `roomId?, status?` | 申诉列表（管理员） |
| `appeal_decide` | `appealId, decision, note?` | 裁决：`approved`（联动恢复）/ `rejected` |
| `rules_list` | `limit?` | 规则版本列表 + 当前生效版本 |
| `rule_publish` | `words[], freqWindowMs, freqMaxCount, note?` | 发布规则新版本（任意房间管理员） |
| `review_log` | `roomId, beforeId?` | 管理员操作日志（分页） |

### 服务端 → 客户端

| 类型 | 说明 |
|---|---|
| `welcome` | 连接建立：`{userId, name, serverTime}` |
| `joined` | 入房成功：`{roomId, name, role, mutedUntil, lastSeq, reviewMode, lastReviewEvent}` |
| `msg` | 房间消息（正文）：`{roomId, seq, clientMsgId, from, fromName, content, ts, reviewFlags?}` |
| `ack` | 发送确认：`{roomId, clientMsgId, seq, ts}` |
| `msg_review` | 审核中/未通过占位帧（占 seq）：`{roomId, seq, status: pending/blocked, flags, reason, reviewEventId?}` |
| `msg_recalled` | 先发后撤指令（reviewEventId 通道）：`{roomId, seq, flags, reason, reviewEventId}` |
| `msg_flagged` | 仅标记风险（可见性不变）：`{roomId, seq, flags, reason, ruleVersion, reviewEventId}` |
| `review_policy` | 房间模式变更/查询结果：`{roomId, mode}` |
| `review_queue` | 队列响应：`{roomId, items[], total}` |
| `review_decided` | 人工决策回执：`{roomId, seq, action, changed, idempotent, status}` |
| `appeal_update` | 申诉状态广播：`{appeal}` |
| `appeals` | 申诉列表响应 |
| `rules` / `review_rule` | 规则版本列表 / 新版本发布广播 |
| `review_recheck` | 只读重检结果（含当前规则命中与原判定规则版本对照） |
| `review_log` | 操作日志响应：`{roomId, entries[], hasMore}` |
| `sync_done` | 一批补发结束：`{roomId, lastSeq, hasMore}` |
| `history` / `rooms` / `members` | 对应查询的响应 |
| `notice` | 房间事件（`muted` / `unmuted`） |
| `error` | `{code, message, ref?}`，code 见下 |
| `server_shutdown` | 服务即将关闭，请准备重连 |

错误码：`BAD_FRAME` `BAD_REQUEST` `UNKNOWN_TYPE` `NOT_MEMBER` `NO_SUCH_ROOM`
`ROOM_EXISTS` `FORBIDDEN` `MUTED` `RATE_LIMITED` `INTERNAL` `NOT_FOUND`
`PENDING`（消息审核中不可申诉）`APPEAL_OPEN`（已有进行中申诉）
`APPEAL_COOLDOWN`（申诉冷却中）`NOT_FLAGGED`（未被处置无需申诉）；
升级阶段拒绝：`401`（认证失败）、`503 SERVER_FULL` / `503 TOO_MANY_DEVICES`。

### 连接建立

```
POST /api/login {"name":"alice"}  →  {userId, name, token}
GET  /ws?token=<token>            →  WebSocket 升级
```

## 关键配置（环境变量）

| 变量 | 默认 | 说明 |
|---|---|---|
| `PORT` / `HOST` | `8080` / `0.0.0.0` | 监听地址 |
| `CHAT_DB_PATH` | `chat.db` | SQLite 路径（`:memory:` 用于测试） |
| `MAX_CONNECTIONS` | `1000` | 全局并发连接上限 |
| `MAX_CONNECTIONS_PER_USER` | `3` | 单用户连接上限（多端） |
| `HEARTBEAT_INTERVAL_MS` / `HEARTBEAT_TIMEOUT_MS` | `30000` / `75000` | 心跳周期 / 判死超时 |
| `ACK_RESEND_AFTER_MS` / `ACK_MAX_RESEND` | `3000` / `5` | 未 ACK 重发阈值 / 最大次数 |
| `MAX_UNACKED_PER_CONN` | `1000` | 单连接未确认积压上限（背压） |
| `RATE_LIMIT_PER_SEC` / `RATE_LIMIT_BURST` | `10` / `20` | 发送限流令牌桶 |
| `REVIEW_ENABLED` | `1` | 审核总开关（`0` 关闭，消息走原链路） |
| `REVIEW_DEFAULT_MODE` | `post` | 房间未配置时的默认模式：`pre`/`post`/`mark` |
| `REVIEW_FREQ_WINDOW_MS` / `REVIEW_FREQ_MAX_COUNT` | `10000` / `8` | 频率异常滑动窗口与阈值 |
| `REVIEW_DETECT_TIMEOUT_MS` | `800` | 检测服务超时（超时 fail-open，不阻塞主链路） |
| `REVIEW_QUEUE_LIMIT` | `100` | 审核队列/申诉单次拉取上限 |
| `REVIEW_APPEAL_COOLDOWN_MS` | `60000` | 同一消息被驳回后再次申诉的冷却 |
| `REVIEW_PENDING_TTL_MS` | `0` | 先审后发暂存超时自动放行（`0` 关闭） |
| `REVIEW_PENDING_SWEEP_MS` | `5000` | TTL 扫描周期 |
| `SYNC_BATCH_SIZE` | `500` | 补发单批条数 |
| `AUTH_SECRET` | — | token HMAC 密钥，**生产必须设置** |

## 已知边界（演示级取舍）

- 认证为演示级（用户名即账号、HMAC token），生产应替换为正式账号体系；
- 单进程架构，多实例部署需引入外部 Pub/Sub 做跨节点广播（DB 层无需改动）；
- 消息无保留期清理，长期使用需自行加定时清理任务。
