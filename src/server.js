'use strict';

const http = require('node:http');
const fs = require('node:fs');
const path = require('node:path');
const { WebSocketServer } = require('ws');

const { ChatDB } = require('./db');
const { Hub, Connection } = require('./hub');
const { ModerationService } = require('./moderation');
const defaultConfig = require('./config');
const {
  randomId,
  randomSecret,
  signToken,
  verifyToken,
  isNonEmptyString,
  parseFrame,
  now,
} = require('./util');

/** 业务错误：handler 抛出，统一转成 error 帧回给客户端 */
class ChatError extends Error {
  constructor(code, message) {
    super(message);
    this.code = code;
  }
}
const fail = (code, message) => {
  throw new ChatError(code, message);
};

/** 令牌桶限流（按用户），防刷屏 */
class TokenBucket {
  constructor(ratePerSec, burst) {
    this.rate = ratePerSec;
    this.burst = burst;
    this.buckets = new Map();
  }
  take(key) {
    const t = now();
    let b = this.buckets.get(key);
    if (!b) {
      b = { tokens: this.burst, updated: t };
      this.buckets.set(key, b);
    }
    b.tokens = Math.min(this.burst, b.tokens + ((t - b.updated) / 1000) * this.rate);
    b.updated = t;
    if (b.tokens < 1) return false;
    b.tokens -= 1;
    return true;
  }
}

/** 数据库消息行 -> 下发帧。flagged 消息附 mod 风险标记，其余状态由专门的墓碑帧表达 */
function msgFrame(m) {
  return {
    type: 'msg',
    roomId: m.roomId,
    seq: m.seq,
    clientMsgId: m.clientMsgId,
    from: m.from,
    fromName: m.fromName,
    content: m.content,
    ts: m.ts,
    ...(m.modStatus === 'flagged' ? { mod: 'flagged', modReason: m.modReason || undefined } : {}),
  };
}

/** 被审核拦截/撤回的消息 -> 不含正文的墓碑帧（占据 seq 槽位，客户端据此推进游标） */
function tombstoneFrame(m, kind) {
  return {
    type: kind === 'blocked' ? 'msg_blocked' : 'msg_recalled',
    roomId: m.roomId,
    seq: m.seq,
    reason: m.modReason || null,
    ruleVersion: m.ruleVersion ?? null,
    ts: m.ts,
  };
}

function createChatServer(overrides = {}) {
  const config = { ...defaultConfig, ...overrides };
  const db = new ChatDB(config.dbPath);
  const hub = new Hub(config);
  const limiter = new TokenBucket(config.rateLimitPerSec, config.rateLimitBurst);
  const moderation = new ModerationService({
    db, hub, config,
    setTimer: overrides.setTimer, // 测试可注入假定时器
    buildMsgFrame: msgFrame,
  });
  // 崩溃恢复：重建 pre 门控队列，重跑未完成检测，放行已干净的 held 消息
  moderation.recoverPending();
  const publicDir = path.join(__dirname, '..', 'public');

  // ---------------------------------------------------------------- 消息处理

  /**
   * 成员感知的门控帧映射：
   *  - visible/flagged：正常 msg（flagged 附风险标记）
   *  - recalled：墓碑帧（标准聊天体验，也让离线期间被撤回的消息在重连后达成一致）
   *  - rejected/held：仅对发送者本人下发（驳回/审核中占位），其他成员不可见、不泄露存在性
   * 返回 null 表示该成员不应收到这一行。
   */
  function frameForMember(m, userId) {
    switch (m.modStatus) {
      case 'visible':
      case 'flagged':
        return msgFrame(m);
      case 'recalled':
        return tombstoneFrame(m, 'recalled');
      case 'rejected':
        return m.from === userId ? tombstoneFrame(m, 'blocked') : null;
      case 'held':
        return m.from === userId
          ? { type: 'msg_pending', roomId: m.roomId, seq: m.seq, clientMsgId: m.clientMsgId, ts: m.ts }
          : null;
      default:
        return null;
    }
  }

  /** 断线补发：成员感知门控，按序推 seq > fromSeq 的帧；lastSeq 取本批最后一条原始 seq */
  function replayRoom(conn, roomId, fromSeq) {
    // 取原始行（含各审核状态），逐行按成员映射；这样游标能越过被过滤的行，不会重复补发
    const batch = db.getMessagesAfter(roomId, fromSeq, config.syncBatchSize + 1);
    const hasMore = batch.length > config.syncBatchSize;
    const slice = hasMore ? batch.slice(0, config.syncBatchSize) : batch;
    let lastSeq = fromSeq;
    for (const m of slice) {
      lastSeq = m.seq; // 即使被过滤也推进游标
      const frame = frameForMember(m, conn.userId);
      if (frame) hub.send(conn, frame, { track: true, roomId, seq: m.seq });
    }
    hub.send(conn, { type: 'sync_done', roomId, lastSeq, hasMore });
  }

  function requireMember(conn, roomId) {
    const member = db.getMember(roomId, conn.userId);
    if (!member) fail('NOT_MEMBER', 'not a member of this room');
    return member;
  }

  function requireAdmin(conn, roomId) {
    const member = requireMember(conn, roomId);
    if (member.role !== 'admin') fail('FORBIDDEN', 'admin role required');
    return member;
  }

  const handlers = {
    ping(conn, msg) {
      hub.send(conn, { type: 'pong', t: msg.t });
    },

    create_room(conn, msg) {
      if (!isNonEmptyString(msg.name, 64)) fail('BAD_REQUEST', 'invalid room name');
      if (db.getRoomByName(msg.name)) fail('ROOM_EXISTS', 'room name already taken');
      const room = db.createRoom(randomId('r_'), msg.name, conn.userId);
      hub.joinRoom(conn, room.id);
      hub.send(conn, {
        type: 'joined',
        roomId: room.id,
        name: room.name,
        role: 'admin',
        mutedUntil: 0,
        lastSeq: 0,
      });
    },

    join(conn, msg) {
      if (!isNonEmptyString(msg.room, 128)) fail('BAD_REQUEST', 'invalid room');
      const room = db.getRoom(msg.room) || db.getRoomByName(msg.room);
      if (!room) fail('NO_SUCH_ROOM', 'room not found');
      db.joinRoom(room.id, conn.userId);
      hub.joinRoom(conn, room.id);
      const member = db.getMember(room.id, conn.userId);
      hub.send(conn, {
        type: 'joined',
        roomId: room.id,
        name: room.name,
        role: member.role,
        mutedUntil: member.muted_until,
        lastSeq: room.last_seq,
      });
      // 补发：优先用客户端上报的进度，否则用服务端游标（新设备则从游标开始）
      const fromSeq = Number.isInteger(msg.lastSeq) ? msg.lastSeq : db.getCursor(room.id, conn.userId);
      if (fromSeq < room.last_seq) replayRoom(conn, room.id, fromSeq);
    },

    leave(conn, msg) {
      hub.leaveRoom(conn, msg.roomId);
      hub.send(conn, { type: 'left', roomId: msg.roomId });
    },

    msg(conn, msg) {
      if (!isNonEmptyString(msg.roomId, 128)) fail('BAD_REQUEST', 'invalid roomId');
      if (!isNonEmptyString(msg.clientMsgId, 64)) fail('BAD_REQUEST', 'invalid clientMsgId');
      if (!isNonEmptyString(msg.content, config.maxContentLength)) {
        fail('BAD_REQUEST', `content must be 1..${config.maxContentLength} chars`);
      }
      const member = requireMember(conn, msg.roomId);
      if (member.muted_until > now()) {
        fail('MUTED', `you are muted until ${new Date(member.muted_until).toISOString()}`);
      }
      if (!limiter.take(conn.userId)) fail('RATE_LIMITED', 'sending too fast, slow down');

      // 先落库（同事务分配 seq）—— pre 模式下以 held 初始状态落库，占用 seq 但暂不广播
      const init = moderation.initialStatus(msg.roomId);
      const { message, duplicate } = db.insertMessage({
        roomId: msg.roomId,
        clientMsgId: msg.clientMsgId,
        senderId: conn.userId,
        content: msg.content,
        modStatus: init.modStatus,
        ruleVersion: init.ruleVersion,
      });
      hub.send(conn, {
        type: 'ack',
        roomId: msg.roomId,
        clientMsgId: msg.clientMsgId,
        seq: message.seq,
        ts: message.ts,
      });
      if (!duplicate) {
        // 重复提交（客户端重试）只回 ACK，不再广播 —— 发送幂等
        // 审核工作流决定是否立即广播：pre 命中暂存时 gate=true（held 不广播，后续放行/驳回异步联动）
        const verdict = moderation.onNewMessage(message, conn);
        if (!verdict.gate) {
          hub.broadcast(msg.roomId, msgFrame(message), { track: true, seq: message.seq });
        } else {
          // 门控（pre，或前方有 held 积压）：给发送者本人一个「审核中」占位（多端同步），
          // 其他成员在放行前不可见
          hub.sendToUser(conn.userId, {
            type: 'msg_pending', roomId: msg.roomId, seq: message.seq,
            clientMsgId: msg.clientMsgId, ts: message.ts,
          });
        }
      }
    },

    // 客户端累积 ACK：清除未确认队列 + 持久化游标（断线补发的兜底依据）
    ack(conn, msg) {
      if (!isNonEmptyString(msg.roomId, 128) || !Number.isInteger(msg.seq)) return;
      if (!conn.rooms.has(msg.roomId)) return; // 只处理本连接已加入的房间
      conn.ack(msg.roomId, msg.seq);
      db.saveCursor(msg.roomId, conn.userId, msg.seq);
    },

    sync(conn, msg) {
      if (!isNonEmptyString(msg.roomId, 128)) fail('BAD_REQUEST', 'invalid roomId');
      requireMember(conn, msg.roomId);
      const fromSeq = Number.isInteger(msg.lastSeq) ? msg.lastSeq : db.getCursor(msg.roomId, conn.userId);
      replayRoom(conn, msg.roomId, fromSeq);
    },

    history(conn, msg) {
      if (!isNonEmptyString(msg.roomId, 128)) fail('BAD_REQUEST', 'invalid roomId');
      requireMember(conn, msg.roomId);
      const limit = Math.min(Math.max(1, msg.limit || 50), config.historyMaxLimit);
      const before = Number.isInteger(msg.beforeSeq) ? msg.beforeSeq : Number.MAX_SAFE_INTEGER;
      // 门控：held/rejected 不入历史；recalled 以墓碑形式保留（占位且不泄露正文）
      const rows = db.getVisibleMessagesBefore(msg.roomId, before, limit);
      const messages = rows.map((m) =>
        m.modStatus === 'recalled' ? tombstoneFrame(m, 'recalled') : msgFrame(m)
      );
      hub.send(conn, { type: 'history', roomId: msg.roomId, messages, hasMore: rows.length === limit });
    },

    rooms(conn) {
      hub.send(conn, { type: 'rooms', rooms: db.listRoomsForUser(conn.userId) });
    },

    members(conn, msg) {
      if (!isNonEmptyString(msg.roomId, 128)) fail('BAD_REQUEST', 'invalid roomId');
      requireMember(conn, msg.roomId);
      const online = new Set(hub.onlineUserIds(msg.roomId));
      const members = db.listMembers(msg.roomId).map((m) => ({ ...m, online: online.has(m.userId) }));
      hub.send(conn, { type: 'members', roomId: msg.roomId, members });
    },

    mute(conn, msg) {
      requireAdmin(conn, msg.roomId);
      const target = db.getMember(msg.roomId, msg.userId);
      if (!target) fail('NOT_MEMBER', 'target is not a member');
      if (target.role === 'admin') fail('FORBIDDEN', 'cannot mute an admin');
      const minutes = Number(msg.minutes);
      if (!Number.isFinite(minutes) || minutes < 1 || minutes > 1440) {
        fail('BAD_REQUEST', 'minutes must be 1..1440');
      }
      const until = now() + Math.round(minutes * 60_000);
      db.setMuted(msg.roomId, msg.userId, until);
      hub.broadcast(msg.roomId, {
        type: 'notice',
        roomId: msg.roomId,
        event: 'muted',
        userId: msg.userId,
        until,
        by: conn.userId,
      });
    },

    unmute(conn, msg) {
      requireAdmin(conn, msg.roomId);
      const target = db.getMember(msg.roomId, msg.userId);
      if (!target) fail('NOT_MEMBER', 'target is not a member');
      db.setMuted(msg.roomId, msg.userId, 0);
      hub.broadcast(msg.roomId, {
        type: 'notice',
        roomId: msg.roomId,
        event: 'unmuted',
        userId: msg.userId,
        by: conn.userId,
      });
    },

    // ------------------------------------------------ 消息审核

    /** 撤回一条已发出消息：发送者本人或管理员 */
    recall(conn, msg) {
      if (!isNonEmptyString(msg.roomId, 128) || !Number.isInteger(msg.seq)) {
        fail('BAD_REQUEST', 'invalid roomId/seq');
      }
      requireMember(conn, msg.roomId);
      const member = db.getMember(msg.roomId, conn.userId);
      const r = moderation.recall(msg.roomId, msg.seq, conn, { isAdmin: member.role === 'admin' });
      if (!r.ok) fail(r.code, r.message);
      hub.send(conn, { type: 'recall_ok', roomId: msg.roomId, seq: msg.seq });
    },

    /** 拉取人工复核队列（待处理 + 最近记录），仅管理员 */
    mod_queue(conn, msg) {
      requireAdmin(conn, msg.roomId);
      hub.send(conn, { type: 'mod_queue', roomId: msg.roomId, ...moderation.queue(msg.roomId) });
    },

    /** 管理员人工终判：approve / reject / recall */
    mod_decide(conn, msg) {
      requireAdmin(conn, msg.roomId);
      if (!Number.isInteger(msg.seq)) fail('BAD_REQUEST', 'invalid seq');
      const reason = msg.reason == null ? null : String(msg.reason).slice(0, 500);
      const r = moderation.decide(msg.roomId, msg.seq, conn.userId, msg.decision, reason);
      if (!r.ok) fail(r.code, r.message);
      hub.send(conn, { type: 'mod_decide_ok', roomId: msg.roomId, seq: msg.seq, decision: msg.decision });
    },

    /** 查询规则版本列表与当前生效版本，仅管理员 */
    mod_rules_get(conn, msg) {
      requireAdmin(conn, msg.roomId);
      const rule = moderation.getRule(msg.roomId);
      hub.send(conn, {
        type: 'mod_rules', roomId: msg.roomId,
        current: rule, versions: moderation.listRules(msg.roomId),
      });
    },

    /** 发布新规则版本（部分字段更新），仅管理员 */
    mod_rules_publish(conn, msg) {
      requireAdmin(conn, msg.roomId);
      const patch = {
        mode: msg.mode,
        sensitiveWords: msg.sensitiveWords,
        freqWindowMs: msg.freqWindowMs,
        freqMaxCount: msg.freqMaxCount,
        detectTimeoutMs: msg.detectTimeoutMs,
      };
      const { rule } = moderation.publishRule(msg.roomId, conn.userId, patch);
      hub.send(conn, { type: 'mod_rules_published', roomId: msg.roomId, rule: moderation._publicRule(rule) });
    },

    /** 回滚到历史规则版本（生成新版本，保留演进链），仅管理员 */
    mod_rules_rollback(conn, msg) {
      requireAdmin(conn, msg.roomId);
      if (!Number.isInteger(msg.version)) fail('BAD_REQUEST', 'invalid version');
      const r = moderation.rollbackRule(msg.roomId, conn.userId, msg.version);
      if (!r) fail('NOT_FOUND', 'rule version not found');
      hub.send(conn, { type: 'mod_rules_published', roomId: msg.roomId, rolledBackFrom: msg.version, rule: moderation._publicRule(r.rule) });
    },

    // ------------------------------------------------ 用户申诉

    /** 用户对被处置（撤回/驳回/标记）的本人消息发起申诉 */
    appeal_create(conn, msg) {
      if (!isNonEmptyString(msg.roomId, 128) || !Number.isInteger(msg.seq)) {
        fail('BAD_REQUEST', 'invalid roomId/seq');
      }
      if (!isNonEmptyString(msg.reason, 500)) fail('BAD_REQUEST', 'appeal reason required');
      requireMember(conn, msg.roomId);
      const r = moderation.appealCreate(msg.roomId, msg.seq, conn.userId, msg.reason);
      if (!r.ok) fail(r.code, r.message);
      hub.send(conn, { type: 'appeal_ok', roomId: msg.roomId, seq: msg.seq, appealId: r.id });
    },

    /** 申诉列表，仅管理员 */
    appeal_list(conn, msg) {
      requireAdmin(conn, msg.roomId);
      hub.send(conn, { type: 'appeal_list', roomId: msg.roomId, appeals: moderation.appeals(msg.roomId) });
    },

    /** 处理申诉：uphold=true 维持，false 推翻（恢复消息），仅管理员 */
    appeal_handle(conn, msg) {
      requireAdmin(conn, msg.roomId);
      if (!Number.isInteger(msg.appealId)) fail('BAD_REQUEST', 'invalid appealId');
      const reply = msg.reply == null ? null : String(msg.reply).slice(0, 500);
      const r = moderation.appealHandle(msg.roomId, msg.appealId, conn.userId, !!msg.uphold, reply);
      if (!r.ok) fail(r.code, r.message);
      hub.send(conn, { type: 'appeal_handle_ok', roomId: msg.roomId, appealId: msg.appealId, status: r.status });
    },

    /** 管理员操作日志（审计留存），仅管理员 */
    mod_audit(conn, msg) {
      requireAdmin(conn, msg.roomId);
      hub.send(conn, { type: 'mod_audit', roomId: msg.roomId, entries: moderation.auditLog(msg.roomId) });
    },
  };

  function onFrame(conn, raw) {
    const msg = parseFrame(raw);
    if (!msg) {
      hub.send(conn, { type: 'error', code: 'BAD_FRAME', message: 'invalid JSON frame' });
      return;
    }
    const handler = handlers[msg.type];
    if (!handler) {
      hub.send(conn, { type: 'error', code: 'UNKNOWN_TYPE', message: `unknown type: ${msg.type}` });
      return;
    }
    try {
      handler(conn, msg);
    } catch (err) {
      if (err instanceof ChatError) {
        hub.send(conn, {
          type: 'error',
          code: err.code,
          message: err.message,
          ref: msg.clientMsgId || msg.roomId || undefined,
        });
      } else {
        console.error('[handler error]', msg.type, err);
        hub.send(conn, { type: 'error', code: 'INTERNAL', message: 'internal error' });
      }
    }
  }

  // ---------------------------------------------------------------- HTTP 层

  const MIME = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript', '.css': 'text/css' };

  function readBody(req, limit = 4096) {
    return new Promise((resolve, reject) => {
      let size = 0;
      const chunks = [];
      req.on('data', (c) => {
        size += c.length;
        if (size > limit) {
          reject(new Error('body too large'));
          req.destroy();
          return;
        }
        chunks.push(c);
      });
      req.on('end', () => resolve(Buffer.concat(chunks).toString()));
      req.on('error', reject);
    });
  }

  const httpServer = http.createServer(async (req, res) => {
    const url = new URL(req.url, 'http://localhost');
    const json = (code, obj) => {
      res.writeHead(code, { 'content-type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify(obj));
    };

    if (req.method === 'POST' && url.pathname === '/api/login') {
      // 演示级登录：按用户名创建/复用账号，返回签名 token
      try {
        const body = JSON.parse(await readBody(req));
        if (!isNonEmptyString(body.name, 32)) return json(400, { error: 'invalid name' });
        let user = db.getUserByName(body.name);
        if (!user) user = db.createUser(randomId('u_'), body.name, randomSecret());
        const token = signToken(user.id, user.token_random, config.authSecret);
        return json(200, { userId: user.id, name: user.name, token });
      } catch {
        return json(400, { error: 'bad request' });
      }
    }

    if (req.method === 'GET' && url.pathname === '/healthz') {
      return json(200, { ok: true, ...hub.stats() });
    }

    if (req.method === 'GET') {
      const rel = url.pathname === '/' ? 'index.html' : url.pathname.slice(1);
      const file = path.resolve(publicDir, rel);
      if (!file.startsWith(publicDir) || !MIME[path.extname(file)]) {
        res.writeHead(404).end('not found');
        return;
      }
      fs.readFile(file, (err, data) => {
        if (err) {
          res.writeHead(404).end('not found');
          return;
        }
        res.writeHead(200, { 'content-type': MIME[path.extname(file)] });
        res.end(data);
      });
      return;
    }

    res.writeHead(404).end('not found');
  });

  // ---------------------------------------------------------------- WS 层

  const wss = new WebSocketServer({ noServer: true });

  httpServer.on('upgrade', (req, socket, head) => {
    const reject = (code, text) => {
      socket.write(`HTTP/1.1 ${code} ${text}\r\nConnection: close\r\n\r\n`);
      socket.destroy();
    };
    const url = new URL(req.url, 'http://localhost');
    if (url.pathname !== '/ws') return reject(404, 'Not Found');

    const userId = verifyToken(url.searchParams.get('token'), config.authSecret);
    const user = userId && db.getUserById(userId);
    if (!user) return reject(401, 'Unauthorized');

    const denied = hub.checkAdmission(user.id);
    if (denied) return reject(503, denied);

    wss.handleUpgrade(req, socket, head, (ws) => {
      const conn = new Connection(ws, user);
      hub.add(conn);

      ws.on('pong', () => {
        conn.lastPong = now();
      });
      ws.on('message', (raw) => onFrame(conn, raw));
      ws.on('close', () => hub.remove(conn));
      ws.on('error', () => {}); // 错误后必随 close，统一在 close 清理

      hub.send(conn, { type: 'welcome', userId: user.id, name: user.name, serverTime: now() });
    });
  });

  // ---------------------------------------------------------------- 定时任务

  const timers = [
    setInterval(() => hub.heartbeatSweep(), config.heartbeatIntervalMs),
    setInterval(() => hub.resendSweep(), config.ackResendIntervalMs),
  ];
  for (const t of timers) t.unref();

  // ---------------------------------------------------------------- 生命周期

  function start() {
    return new Promise((resolve) => {
      httpServer.listen(config.port, config.host, () => {
        const addr = httpServer.address();
        console.log(`[chat] listening on http://${addr.address}:${addr.port}  (db: ${config.dbPath})`);
        resolve(addr);
      });
    });
  }

  function stop() {
    moderation.shutdown(); // 先关停审核检测（清在途定时器），再关连接与 DB
    for (const t of timers) clearInterval(t);
    for (const conn of [...hub.all]) {
      hub.send(conn, { type: 'server_shutdown' });
      conn.ws.terminate();
    }
    wss.close();
    httpServer.close();
    db.close();
  }

  return { config, db, hub, moderation, httpServer, wss, start, stop };
}

// 直接运行：node src/server.js
if (require.main === module) {
  const server = createChatServer();
  server.start();
  const shutdown = () => {
    console.log('\n[chat] shutting down...');
    server.stop();
    process.exit(0);
  };
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}

module.exports = { createChatServer };
