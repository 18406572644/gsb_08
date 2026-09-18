'use strict';

const http = require('node:http');
const fs = require('node:fs');
const path = require('node:path');
const { WebSocketServer } = require('ws');

const { ChatDB } = require('./db');
const { Hub, Connection } = require('./hub');
const { Detector } = require('./detector');
const { Moderator } = require('./moderator');
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

/** 数据库消息行 -> 下发帧（released 正文；带审核风险标时附上审核字段） */
function msgFrame(m) {
  const f = {
    type: 'msg',
    roomId: m.roomId,
    seq: m.seq,
    clientMsgId: m.clientMsgId,
    from: m.from,
    fromName: m.fromName,
    content: m.content,
    ts: m.ts,
  };
  const flags = typeof m.reviewFlags === 'string' ? safeParse(m.reviewFlags) : m.reviewFlags;
  if (flags && flags.length) {
    f.reviewFlags = flags;
    f.reviewReason = m.reviewReason ?? null;
    f.reviewRuleVersion = m.reviewRuleVersion ?? null;
  }
  return f;
}

function safeParse(s) {
  try { return JSON.parse(s); } catch { return []; }
}

function createChatServer(overrides = {}) {
  const config = { ...defaultConfig, ...overrides };
  const db = new ChatDB(config.dbPath);
  const hub = new Hub(config);
  const limiter = new TokenBucket(config.rateLimitPerSec, config.rateLimitBurst);
  const publicDir = path.join(__dirname, '..', 'public');

  // 检测服务可注入（测试模拟超时/故障/词表/频率阈值）；编排器可注入以便挂载自定义 detector
  const detector = overrides.detector instanceof Detector
    ? overrides.detector
    : new Detector({
        words: overrides.detector?.words || [],
        freqWindowMs: overrides.detector?.freqWindowMs ?? config.reviewFreqWindowMs,
        freqMaxCount: overrides.detector?.freqMaxCount ?? config.reviewFreqMaxCount,
        timeoutMs: overrides.detector?.timeoutMs ?? config.reviewDetectTimeoutMs,
        delayMs: overrides.detector?.delayMs || 0,
        faulty: overrides.detector?.faulty || false,
      });
  const moderator = overrides.moderator || new Moderator({ db, hub, detector, config });

  // ---------------------------------------------------------------- 消息处理

  /**
   * 断线期间审核指令增量补发（独立于消息缺口）：即使消息 seq 已追平，撤回/恢复/标记
   * 指令仍可能晚于客户端上一次在线时间，必须按 review 事件水位单独补齐。
   */
  function replayReviewEvents(conn, roomId, seenSeq, coveredSeq = seenSeq) {
    const afterEvent = db.getReviewCursor(roomId, conn.userId);
    const ev = moderator.replayEvents(conn, roomId, afterEvent, seenSeq, coveredSeq);
    if (ev.lastId > afterEvent) db.saveReviewCursor(roomId, conn.userId, ev.lastId);
  }

  /**
   * 断线补发：roomId 中 seq > fromSeq 的消息按当前审核状态转帧后按序推送（seq 连续，
   * 任何状态都有对应帧）；随后补发缺口期间的审核指令（撤回/恢复/标记，幂等应用）。
   */
  function replayRoom(conn, roomId, fromSeq) {
    const batch = db.getMessagesAfter(roomId, fromSeq, config.syncBatchSize + 1);
    const hasMore = batch.length > config.syncBatchSize;
    const slice = hasMore ? batch.slice(0, config.syncBatchSize) : batch;
    for (const m of slice) {
      const frame = moderator.frameForReplay(m, conn.userId);
      // 补发的正式 msg 帧走 seq 通道；占位帧（pending/blocked/recalled）也占 seq 位
      hub.send(conn, frame, { track: true, roomId, seq: m.seq });
    }
    const lastSeq = slice.length ? slice[slice.length - 1].seq : fromSeq;

    // 审核指令增量补发：只纠偏客户端已按旧状态上屏的消息（seq <= fromSeq），幂等应用
    replayReviewEvents(conn, roomId, fromSeq, lastSeq);

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

  /** 全局审核视图（跨房间队列/申诉）要求在任意房间担任管理员（演示级权限） */
  function isGlobalAdmin(userId) {
    return db.isAdminAnywhere(userId);
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
        reviewMode: config.reviewEnabled ? moderator.modeFor(room.id) : null,
        lastReviewEvent: db.getReviewCursor(room.id, conn.userId),
      });
      // 补发：优先用客户端上报的进度，否则用服务端游标（新设备则从游标开始）
      const fromSeq = Number.isInteger(msg.lastSeq) ? msg.lastSeq : db.getCursor(room.id, conn.userId);
      if (fromSeq < room.last_seq) replayRoom(conn, room.id, fromSeq);
      else replayReviewEvents(conn, room.id, fromSeq); // 消息已追平，仍需补发晚到的审核指令
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

      // 先落库（同事务分配 seq），再 ACK，再广播 —— 崩溃也不丢已确认消息
      const { message, duplicate } = db.insertMessage({
        roomId: msg.roomId,
        clientMsgId: msg.clientMsgId,
        senderId: conn.userId,
        content: msg.content,
      });
      // ACK 语义不变：落库即确认（pre 模式下消息确实已持久化，只是广播被暂缓）
      hub.send(conn, {
        type: 'ack',
        roomId: msg.roomId,
        clientMsgId: msg.clientMsgId,
        seq: message.seq,
        ts: message.ts,
      });
      if (!duplicate) {
        // 重复提交（客户端重试）只回 ACK，不再广播 —— 发送幂等
        if (config.reviewEnabled) {
          // ingest：pre 返回 null（占位帧已由编排器广播）；post/mark 返回正文帧立即广播
          const toBroadcast = moderator.ingest(message);
          if (toBroadcast) {
            hub.broadcast(msg.roomId, msgFrame(toBroadcast), { track: true, seq: message.seq });
          }
        } else {
          hub.broadcast(msg.roomId, msgFrame(message), { track: true, seq: message.seq });
        }
      }
    },

    // 客户端累积 ACK：清除未确认队列 + 持久化游标（断线补发的兜底依据）
    ack(conn, msg) {
      if (!isNonEmptyString(msg.roomId, 128) || !Number.isInteger(msg.seq)) return;
      if (!conn.rooms.has(msg.roomId)) return; // 只处理本连接已加入的房间
      conn.ack(msg.roomId, msg.seq);
      db.saveCursor(msg.roomId, conn.userId, msg.seq);
      // 审核指令通道的独立累积 ACK（reviewEventId），与 seq 水位互不影响
      if (Number.isInteger(msg.reviewSeq)) {
        conn.ackReview(msg.roomId, msg.reviewSeq);
        db.saveReviewCursor(msg.roomId, conn.userId, msg.reviewSeq);
      }
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
      const messages = db.getMessagesBefore(msg.roomId, before, limit)
        .map((m) => moderator.frameForReplay(m, conn.userId));
      hub.send(conn, { type: 'history', roomId: msg.roomId, messages, hasMore: messages.length === limit });
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

    // ================================================================ 消息审核

    /** 查询/设置房间处置模式（pre 先审后发 / post 先发后撤 / mark 仅标记） */
    review_policy(conn, msg) {
      if (!isNonEmptyString(msg.roomId, 128)) fail('BAD_REQUEST', 'invalid roomId');
      requireMember(conn, msg.roomId);
      if (msg.mode) {
        requireAdmin(conn, msg.roomId);
        moderator.setMode(msg.roomId, msg.mode, { id: conn.userId, name: conn.name });
        // 广播即回执（请求方也在房间内），不再单发，避免重复帧
        return;
      }
      hub.send(conn, {
        type: 'review_policy',
        roomId: msg.roomId,
        mode: config.reviewEnabled ? moderator.modeFor(msg.roomId) : null,
        enabled: !!config.reviewEnabled,
      });
    },

    /** 人工审核队列（可按房间过滤） */
    review_queue(conn, msg) {
      const roomId = isNonEmptyString(msg.roomId, 128) ? msg.roomId : '';
      if (roomId) requireAdmin(conn, roomId);
      else if (!isGlobalAdmin(conn.userId)) fail('FORBIDDEN', 'admin role required');
      const limit = Math.min(Math.max(1, msg.limit || config.reviewQueueFetchLimit), config.reviewQueueFetchLimit);
      const items = moderator.db.listQueue({ roomId, limit });
      hub.send(conn, {
        type: 'review_queue',
        roomId,
        items,
        total: moderator.db.pendingCount(roomId),
      });
    },

    /** 人工决策：release / block / recall / restore / dismiss */
    review_decide(conn, msg) {
      if (!isNonEmptyString(msg.roomId, 128) || !Number.isInteger(msg.seq)) fail('BAD_REQUEST', 'invalid target');
      requireAdmin(conn, msg.roomId);
      const result = moderator.decide({
        roomId: msg.roomId,
        seq: msg.seq,
        action: msg.action,
        admin: { id: conn.userId, name: conn.name },
        reason: isNonEmptyString(msg.reason, 500) ? msg.reason : null,
      });
      if (result.error) fail(result.error === 'NO_SUCH_MESSAGE' ? 'NOT_FOUND' : 'BAD_REQUEST', result.error);
      hub.send(conn, {
        type: 'review_decided',
        roomId: msg.roomId,
        seq: msg.seq,
        action: msg.action,
        changed: !!result.changed,
        idempotent: !!result.idempotent,
        status: result.status,
      });
    },

    /** 管理员手动把消息加入审核队列（巡检 / mark 模式加队） */
    review_enqueue(conn, msg) {
      if (!isNonEmptyString(msg.roomId, 128) || !Number.isInteger(msg.seq)) fail('BAD_REQUEST', 'invalid target');
      requireAdmin(conn, msg.roomId);
      const added = moderator.enqueueManual(msg.roomId, msg.seq, { id: conn.userId, name: conn.name });
      hub.send(conn, { type: 'review_enqueued', roomId: msg.roomId, seq: msg.seq, added });
    },

    /** 按最新规则重新检测（误判复核辅助，只读，不改状态） */
    async review_recheck(conn, msg) {
      if (!isNonEmptyString(msg.roomId, 128) || !Number.isInteger(msg.seq)) fail('BAD_REQUEST', 'invalid target');
      requireAdmin(conn, msg.roomId);
      const result = await moderator.recheck(msg.roomId, msg.seq);
      if (!result) fail('NOT_FOUND', 'message not found');
      hub.send(conn, { type: 'review_recheck', ...result });
    },

    // —— 规则版本 ——

    rules_list(conn, msg) {
      const limit = Math.min(Math.max(1, msg.limit || 50), 200);
      hub.send(conn, { type: 'rules', rules: moderator.listRuleVersions(limit), active: moderator.activeRule() });
    },

    rule_publish(conn, msg) {
      if (!isGlobalAdmin(conn.userId)) fail('FORBIDDEN', 'admin role required');
      const words = Array.isArray(msg.words)
        ? [...new Set(msg.words.map((w) => String(w).trim()).filter(Boolean))].slice(0, 500)
        : fail('BAD_REQUEST', 'words must be an array');
      const freqWindowMs = Number(msg.freqWindowMs) || config.reviewFreqWindowMs;
      const freqMaxCount = Number(msg.freqMaxCount) || config.reviewFreqMaxCount;
      if (freqWindowMs < 500 || freqWindowMs > 3_600_000) fail('BAD_REQUEST', 'freqWindowMs out of range');
      if (freqMaxCount < 1 || freqMaxCount > 10_000) fail('BAD_REQUEST', 'freqMaxCount out of range');
      moderator.publishRule(
        { words, freqWindowMs, freqMaxCount, note: isNonEmptyString(msg.note, 200) ? msg.note : '' },
        { id: conn.userId, name: conn.name }
      );
      // publishRule 已全局广播 review_rule（含发布者），不再单发
    },

    // —— 用户申诉 ——

    appeal_submit(conn, msg) {
      if (!isNonEmptyString(msg.roomId, 128) || !Number.isInteger(msg.seq)) fail('BAD_REQUEST', 'invalid target');
      requireMember(conn, msg.roomId);
      const result = moderator.submitAppeal({
        roomId: msg.roomId,
        seq: msg.seq,
        userId: conn.userId,
        reason: msg.reason || '',
      });
      if (result.error) fail(result.error, result.message || result.error);
      // 申诉结果由编排器广播到房间（提交者也在房间内），不在此单发，避免重复帧
    },

    appeals_list(conn, msg) {
      const roomId = isNonEmptyString(msg.roomId, 128) ? msg.roomId : '';
      if (roomId) requireAdmin(conn, roomId);
      else if (!isGlobalAdmin(conn.userId)) fail('FORBIDDEN', 'admin role required');
      const status = ['open', 'approved', 'rejected'].includes(msg.status) ? msg.status : '';
      const appeals = moderator.db.listAppeals({ roomId, status, limit: config.reviewQueueFetchLimit });
      hub.send(conn, { type: 'appeals', roomId, appeals });
    },

    appeal_decide(conn, msg) {
      if (!isNonEmptyString(msg.appealId, 64)) fail('BAD_REQUEST', 'invalid appealId');
      const appeal = moderator.db.getAppealById(msg.appealId);
      if (!appeal) fail('NOT_FOUND', 'appeal not found');
      requireAdmin(conn, appeal.roomId);
      const result = moderator.decideAppeal({
        appealId: msg.appealId,
        decision: msg.decision,
        admin: { id: conn.userId, name: conn.name },
        note: isNonEmptyString(msg.note, 500) ? msg.note : '',
      });
      if (result.error) fail(result.error === 'NO_SUCH_APPEAL' ? 'NOT_FOUND' : 'BAD_REQUEST', result.error);
      // 裁决结果由编排器广播到房间，不在此单发
    },

    /** 管理员操作日志（按房间分页） */
    review_log(conn, msg) {
      if (!isNonEmptyString(msg.roomId, 128)) fail('BAD_REQUEST', 'invalid roomId');
      requireAdmin(conn, msg.roomId);
      const beforeId = Number.isInteger(msg.beforeId) ? msg.beforeId : Number.MAX_SAFE_INTEGER;
      const entries = moderator.db.listAdminLog({ roomId: msg.roomId, beforeId, limit: 100 });
      hub.send(conn, { type: 'review_log', roomId: msg.roomId, entries, hasMore: entries.length === 100 });
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
      const r = handler(conn, msg);
      // 支持 async 处理器（如 review_recheck）：把异步抛出的业务错误同样转成 error 帧
      if (r && typeof r.catch === 'function') {
        r.catch((err) => sendError(conn, msg, err));
      }
    } catch (err) {
      sendError(conn, msg, err);
    }
  }

  function sendError(conn, msg, err) {
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
  // 先审后发兜底：暂存超时自动放行（fail-open，默认关闭：reviewPendingTtlMs=0）
  if (config.reviewPendingTtlMs > 0) {
    timers.push(setInterval(() => moderator.sweepPending(), config.reviewPendingSweepMs));
  }
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
    for (const t of timers) clearInterval(t);
    for (const conn of [...hub.all]) {
      hub.send(conn, { type: 'server_shutdown' });
      conn.ws.terminate();
    }
    wss.close();
    httpServer.close();
    db.close();
  }

  return { config, db, hub, detector, moderator, httpServer, wss, start, stop };
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
