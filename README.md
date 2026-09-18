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
- **消息审核（可选，默认关闭）**：敏感词检测、频率异常识别、外部检测后端、管理员人工复核、
  用户申诉、规则版本管理与管理员操作日志；支持**先审后发 / 先发后撤 / 仅标记风险**三种处置模式

## 快速开始

```bash
npm install
npm start          # http://localhost:8080
npm test           # 30 个集成测试（核心 13 + 审核 17）
```

浏览器打开 `http://localhost:8080`，用不同昵称开两个标签页即可体验（建房、发消息、
禁言管理）。断网/刷新页面后自动重连并补发离线期间的消息。

要求 Node.js ≥ 22.13（使用内置 `node:sqlite`，唯一第三方依赖是 `ws`）。

## 架构

```
src/
├── config.js       配置（端口、连接上限、心跳、重发、限流、审核，均可环境变量覆盖）
├── db.js           SQLite 持久层：schema、幂等写入、seq 分配、游标、审核/申诉/规则/日志
├── hub.js          连接注册中心：房间索引、广播、未 ACK 追踪、心跳/重发扫描
├── moderation.js   消息审核工作流：检测调度、三种处置模式、门控放行、人工复核、申诉
├── server.js       HTTP + WS 服务：认证、消息路由、权限检查、限流、审核接线、生命周期
└── util.js         token 签名、帧解析等工具
public/index.html     演示客户端（可靠投递协议 + 审核状态渲染 + 管理员审核面板）
test/chat.test.js         核心可靠投递集成测试（13 个）
test/moderation.test.js   审核工作流集成测试（17 个）
```

### 数据模型

| 表 | 说明 |
|---|---|
| `users` | 用户（演示级 token 认证） |
| `rooms` | 房间，`last_seq` 为房间消息序号计数器 |
| `members` | 成员关系：`role`（admin/member）+ `muted_until`（禁言截止时间） |
| `messages` | 消息。主键 `(room_id, seq)`；唯一键 `(room_id, sender_id, client_msg_id)` 为幂等键；`mod_status` 为审核状态 |
| `cursors` | 每用户每房间已确认游标 `last_ack_seq`，断线补发的服务端兜底依据 |
| `mod_rule_versions` | 审核规则版本（每房间至多一个 `published`），含处置模式/敏感词/频率/超时配置 |
| `mod_reviews` | 审核记录（人工复核队列项）：检测状态、命中项、人工终判，CAS 防重复审核 |
| `mod_appeals` | 用户申诉（同一用户对同一消息仅一条），状态 open/upheld/reversed |
| `mod_audit_log` | 管理员审核操作日志（发规则/回滚/终判/撤回/申诉处理…），留存可查 |

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

## 消息审核与违规处理

审核能力默认关闭（不发布规则即为 `off`），**关闭时行为与未接入审核完全一致**，
原有发送顺序、ACK、断线补发语义不变。

### 1. 三种处置模式

| 模式 | 发送路径 | 违规处置 | 可疑处置 |
|---|---|---|---|
| `pre` 先审后发 | 消息以 `held` 落库占用 seq，**暂不广播** | 保持 held 转人工，驳回则永不可见 | 保持 held 转人工 |
| `post` 先发后撤 | 正常广播 | 自动广播 `recall` 撤回，转人工 | 标 `flagged` 转人工 |
| `flag` 仅标记 | 正常广播 | 附 `mod_update` 风险标记，不阻断/不撤回 | 同左 |
| `off` 关闭 | 完全旁路 | — | — |

### 2. 不破坏可靠投递的关键设计

- **seq 仍在写事务内分配**：`held` 消息也占用 seq，房间内全序不被打乱。
- **可见性门控（水位线）**：`pre` 模式下 held 消息不广播，且其后的消息排队等待
  （服务端维护按 seq 的放行队列，遇到最小的 held 即停止）。held 放行时连带其后已就绪
  的消息**按 seq 顺序**一次性送达，客户端不会看到越过审核消息的乱序内容。
- **ACK 语义不变**：ACK 表示「服务端已收并分配 seq」，不代表对他人可见。放行与撤回
  通过独立广播帧（`msg` / `recall` / `mod_update`）异步联动。
- **补发/历史门控**：`held`/`rejected` 对非发送者不下发；`recalled` 以**不含正文的墓碑帧**
  `msg_recalled` 占位（仍推进游标）。因此断线重连、新设备、历史翻页与实时广播最终一致，
  且永远看不到已撤回/被驳回的正文。
- **检测整体延迟到微任务**：保证 `post`/`flag` 下「原消息先广播、撤回/标记后到」。

### 3. 异常链路覆盖

- **检测服务超时**：超过 `detectTimeoutMs` 未返回即 fail-open 放行（pre 也放行），消息不被卡死；
- **审核结果晚于客户端展示**：超时放行后真正的违规结果才到达时，补做撤回（`source:'late'`）
  或补标记，并转人工；
- **误判**：用户可对被撤回/驳回/标记的**本人消息**申诉，管理员推翻则恢复（recalled 重新广播、
  rejected 走门控放行）；
- **重复审核**：检测结果与超时兜底、两次人工终判都用数据库 compare-and-set 抢占，只有一方生效；
  重复撤回/重复申诉幂等（`ALREADY_DECIDED` / `ALREADY_APPEALED`）；
- **消息已被撤回**：再次撤回直接幂等成功；补发与历史只给墓碑。

### 4. 检测来源

- 敏感词（本地、同步、可配置，规则版本内固化）；
- 频率异常（按用户滑窗，窗口内条数超阈值判可疑→转人工）；
- 可注入外部检测后端：`moderation.setDetector(roomId|'*', async (content, rule) => ({status, hits?, ms?}))`，
  `status ∈ clean|suspect|violation|error`。

### 5. 规则版本与审计

每次发布/回滚都生成**只增的新版本号**（回滚 = 以历史内容再发新版本，保留完整演进链），
每条消息记录判定时的 `rule_version`。管理员的发布、回滚、人工终判、撤回、申诉处理等
全部写入 `mod_audit_log`。

### 审核协议帧（在原协议基础上新增）

客户端 → 服务端（除 `recall`/`appeal_create` 外，`mod_*`/`appeal_*` 均仅管理员）：

| 类型 | 字段 | 说明 |
|---|---|---|
| `recall` | `roomId, seq` | 撤回已发消息（发送者本人或管理员） |
| `mod_queue` | `roomId` | 拉人工复核队列（待处理 + 最近） |
| `mod_decide` | `roomId, seq, decision, reason?` | 人工终判：`approve`/`reject`/`recall` |
| `mod_rules_get` | `roomId` | 当前规则 + 版本列表 |
| `mod_rules_publish` | `roomId, mode, sensitiveWords?, freqMaxCount?, detectTimeoutMs?` | 发布新版本 |
| `mod_rules_rollback` | `roomId, version` | 回滚到历史版本 |
| `appeal_create` | `roomId, seq, reason` | 本人消息申诉 |
| `appeal_list` | `roomId` | 申诉列表（管理员） |
| `appeal_handle` | `roomId, appealId, uphold, reply?` | 维持/推翻申诉（管理员） |
| `mod_audit` | `roomId` | 操作日志（管理员） |

服务端 → 客户端：

| 类型 | 说明 |
|---|---|
| `msg_pending` | 先审后发的「审核中」占位（仅发送者，`{roomId,seq,clientMsgId,ts}`） |
| `msg_recalled` / `recall` | 撤回墓碑（补发/历史）与实时撤回；`{roomId,seq,reason,source,ts}`，无正文 |
| `msg_blocked` | 驳回墓碑（补发时仅发送者本人） |
| `mod_update` | 风险标记更新：`{roomId,seq,mod,reason,ts}`（`mod=flagged`/`visible`） |
| `mod_result` | 发送者收到的人工处置结果：`{roomId,seq,outcome,by,reason}` |
| `mod_queue_update` / `mod_appeal_update` / `mod_rule_changed` | 管理员实时推送 |
| `mod_queue` / `appeal_list` / `mod_rules` / `mod_audit` | 对应查询响应 |
| `appeal_ok` / `appeal_result` | 申诉受理 / 申诉裁定结果 |
| `recall_ok` / `mod_decide_ok` / `mod_rules_published` / `appeal_handle_ok` | 操作确认 |

新增错误码：`HELD` `ALREADY_DECIDED` `NOT_APPEALABLE` `ALREADY_APPEALED`
`ALREADY_HANDLED` `NOT_FOUND`。`msg` 帧在被标记风险时附带 `mod:'flagged'` 与 `modReason`。

## 协议（JSON 文本帧）

### 客户端 → 服务端

| 类型 | 字段 | 说明 |
|---|---|---|
| `ping` | `t` | 应用层心跳，回 `pong` |
| `create_room` | `name` | 建房，创建者为管理员，回 `joined` |
| `join` | `room, lastSeq?` | 加入房间（room 可为 id 或名称）；带进度则立即补发 |
| `leave` | `roomId` | 离开房间 |
| `msg` | `roomId, clientMsgId, content` | 发消息，回 `ack` |
| `ack` | `roomId, seq` | 累积确认：seq 及之前均已收到 |
| `sync` | `roomId, lastSeq?` | 请求补发 |
| `history` | `roomId, beforeSeq?, limit?` | 历史翻页（升序返回） |
| `rooms` | — | 我加入的房间列表 |
| `members` | `roomId` | 成员列表（含在线状态） |
| `mute` | `roomId, userId, minutes` | 禁言（仅管理员，1..1440 分钟） |
| `unmute` | `roomId, userId` | 解除禁言（仅管理员） |

### 服务端 → 客户端

| 类型 | 说明 |
|---|---|
| `welcome` | 连接建立：`{userId, name, serverTime}` |
| `joined` | 入房成功：`{roomId, name, role, mutedUntil, lastSeq}` |
| `msg` | 房间消息：`{roomId, seq, clientMsgId, from, fromName, content, ts}` |
| `ack` | 发送确认：`{roomId, clientMsgId, seq, ts}` |
| `sync_done` | 一批补发结束：`{roomId, lastSeq, hasMore}` |
| `history` / `rooms` / `members` | 对应查询的响应 |
| `notice` | 房间事件（`muted` / `unmuted`） |
| `error` | `{code, message, ref?}`，code 见下 |
| `server_shutdown` | 服务即将关闭，请准备重连 |

错误码：`BAD_FRAME` `BAD_REQUEST` `UNKNOWN_TYPE` `NOT_MEMBER` `NO_SUCH_ROOM`
`ROOM_EXISTS` `FORBIDDEN` `MUTED` `RATE_LIMITED` `INTERNAL`；
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
| `SYNC_BATCH_SIZE` | `500` | 补发单批条数 |
| `MODERATION_ENABLED` | `1` | 审核能力总开关（置 `0` 完全关闭） |
| `MOD_DEFAULT_MODE` | `off` | 未发布规则时的默认处置模式（off/pre/post/flag） |
| `MOD_DETECT_TIMEOUT_MS` | `800` | 检测服务超时阈值，超时 fail-open 放行 |
| `MOD_FREQ_WINDOW_MS` / `MOD_FREQ_MAX_COUNT` | `10000` / `8` | 频率异常统计窗口 / 窗口内条数上限 |
| `MOD_SENSITIVE_WORDS` | 空 | 默认敏感词，逗号分隔（发布房间规则后以规则为准） |
| `AUTH_SECRET` | — | token HMAC 密钥，**生产必须设置** |

## 已知边界（演示级取舍）

- 认证为演示级（用户名即账号、HMAC token），生产应替换为正式账号体系；
- 单进程架构，多实例部署需引入外部 Pub/Sub 做跨节点广播（DB 层无需改动）；
- 消息无保留期清理，长期使用需自行加定时清理任务。
