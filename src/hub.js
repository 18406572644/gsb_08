'use strict';

const { now } = require('./util');

let nextConnId = 1;

/**
 * 单条连接的运行时状态。
 * unacked: Map<roomId, Map<seq, {frame, lastSent, tries}>> —— 已推送但未被客户端
 * 累积 ACK 确认的消息，超时重发；这是「至少一次投递」的服务端正，配合客户端
 * 按 seq 去重（幂等消费）达到效果上的恰好一次。
 *
 * unackedReview: 审核指令的第二条可靠通道，键为 reviewEventId（每房间单调）。
 * 撤回/拦截/放行/标记等指令与消息流分开追踪、分开累积 ACK，互不干扰：
 * 指令晚于消息 ACK 到达也能独立重发，且不会污染 seq 水位。
 */
class Connection {
  constructor(ws, user) {
    this.id = nextConnId++;
    this.ws = ws;
    this.userId = user.id;
    this.name = user.name;
    this.connectedAt = now();
    this.lastPong = now(); // 最近一次收到 pong 的时间，心跳判活依据
    this.rooms = new Set(); // 本连接已加入的房间
    this.unacked = new Map();
    this.unackedCount = 0;
    this.ackSeqs = new Map(); // roomId -> 该连接累积 ACK 过的最大 seq
    this.unackedReview = new Map();
    this.unackedReviewCount = 0;
    this.ackReviewSeqs = new Map(); // roomId -> 已 ACK 的最大 reviewEventId
  }

  trackUnacked(roomId, seq, frame) {
    // seq 已被累积 ACK 覆盖（如先审后放的正式帧晚于 ACK 到达）：不再追踪，
    // 否则该条目永远等不到 ACK，最终被重发扫描误判为死连接
    if ((this.ackSeqs.get(roomId) || 0) >= seq) return;
    let room = this.unacked.get(roomId);
    if (!room) {
      room = new Map();
      this.unacked.set(roomId, room);
    }
    // 同一 seq 可能先推送「审核中占位帧」、放行后再推正式 msg 帧：替换帧但不重复计数
    if (!room.has(seq)) this.unackedCount++;
    room.set(seq, { frame, lastSent: now(), tries: 0 });
  }

  /** 累积 ACK：清除 roomId 下所有 seq <= ackSeq 的未确认项，返回新确认的数量 */
  ack(roomId, ackSeq) {
    this.ackSeqs.set(roomId, Math.max(this.ackSeqs.get(roomId) || 0, ackSeq));
    const room = this.unacked.get(roomId);
    if (!room) return 0;
    let cleared = 0;
    for (const seq of room.keys()) {
      if (seq <= ackSeq) {
        room.delete(seq);
        cleared++;
      }
    }
    if (room.size === 0) this.unacked.delete(roomId);
    this.unackedCount -= cleared;
    return cleared;
  }

  trackReviewUnacked(roomId, reviewId, frame) {
    if ((this.ackReviewSeqs.get(roomId) || 0) >= reviewId) return;
    let room = this.unackedReview.get(roomId);
    if (!room) {
      room = new Map();
      this.unackedReview.set(roomId, room);
    }
    if (!room.has(reviewId)) this.unackedReviewCount++;
    room.set(reviewId, { frame, lastSent: now(), tries: 0 });
  }

  /** 审核指令累积 ACK（reviewEventId 通道，与 seq 通道互不影响） */
  ackReview(roomId, reviewId) {
    this.ackReviewSeqs.set(roomId, Math.max(this.ackReviewSeqs.get(roomId) || 0, reviewId));
    const room = this.unackedReview.get(roomId);
    if (!room) return 0;
    let cleared = 0;
    for (const id of room.keys()) {
      if (id <= reviewId) {
        room.delete(id);
        cleared++;
      }
    }
    if (room.size === 0) this.unackedReview.delete(roomId);
    this.unackedReviewCount -= cleared;
    return cleared;
  }

  /**
   * 替换 seq 通道中已追踪帧的内容（不改变计数/追踪关系）。
   * 用于「占位帧→放行正文」「正文→撤回」：避免重发扫描把旧状态帧重发给客户端，
   * 把已推进的审核状态打回。
   */
  replaceTrackedSeqFrame(roomId, seq, frame) {
    const room = this.unacked.get(roomId);
    const entry = room?.get(seq);
    if (entry) {
      entry.frame = frame;
      entry.lastSent = now();
    }
  }

  /** 摘出所有超时未确认、需要重发的条目（消息流 + 审核指令两条通道） */
  *pendingResends(staleMs) {
    const t = now();
    for (const room of this.unacked.values()) {
      for (const entry of room.values()) {
        if (t - entry.lastSent >= staleMs) yield entry;
      }
    }
    for (const room of this.unackedReview.values()) {
      for (const entry of room.values()) {
        if (t - entry.lastSent >= staleMs) yield entry;
      }
    }
  }
}

/**
 * 连接注册中心：全局/按用户/按房间的连接索引，广播，心跳与重发扫描。
 */
class Hub {
  constructor(config) {
    this.config = config;
    this.all = new Set(); // 全部连接
    this.byUser = new Map(); // userId -> Set<Connection>
    this.byRoom = new Map(); // roomId -> Set<Connection>
  }

  /** 准入控制：全局上限 + 单用户上限。返回 null 表示可接入，否则返回拒绝原因码。 */
  checkAdmission(userId) {
    if (this.all.size >= this.config.maxConnections) return 'SERVER_FULL';
    const mine = this.byUser.get(userId);
    if (mine && mine.size >= this.config.maxConnectionsPerUser) return 'TOO_MANY_DEVICES';
    return null;
  }

  add(conn) {
    this.all.add(conn);
    let set = this.byUser.get(conn.userId);
    if (!set) {
      set = new Set();
      this.byUser.set(conn.userId, set);
    }
    set.add(conn);
  }

  remove(conn) {
    this.all.delete(conn);
    const mine = this.byUser.get(conn.userId);
    if (mine) {
      mine.delete(conn);
      if (mine.size === 0) this.byUser.delete(conn.userId);
    }
    for (const roomId of conn.rooms) this._leaveRoomSet(roomId, conn);
    conn.rooms.clear();
    conn.unacked.clear();
    conn.unackedCount = 0;
    conn.ackSeqs.clear();
    conn.unackedReview.clear();
    conn.unackedReviewCount = 0;
    conn.ackReviewSeqs.clear();
  }

  joinRoom(conn, roomId) {
    let set = this.byRoom.get(roomId);
    if (!set) {
      set = new Set();
      this.byRoom.set(roomId, set);
    }
    set.add(conn);
    conn.rooms.add(roomId);
  }

  leaveRoom(conn, roomId) {
    this._leaveRoomSet(roomId, conn);
    conn.rooms.delete(roomId);
    const room = conn.unacked.get(roomId);
    if (room) {
      conn.unackedCount -= room.size;
      conn.unacked.delete(roomId);
    }
    conn.ackSeqs.delete(roomId);
    const rv = conn.unackedReview.get(roomId);
    if (rv) {
      conn.unackedReviewCount -= rv.size;
      conn.unackedReview.delete(roomId);
    }
    conn.ackReviewSeqs.delete(roomId);
  }

  _leaveRoomSet(roomId, conn) {
    const set = this.byRoom.get(roomId);
    if (set) {
      set.delete(conn);
      if (set.size === 0) this.byRoom.delete(roomId);
    }
  }

  /** 房间内在线用户 ID 列表（去重） */
  onlineUserIds(roomId) {
    const set = this.byRoom.get(roomId);
    if (!set) return [];
    return [...new Set([...set].map((c) => c.userId))];
  }

  /**
   * 发送单帧到指定连接。track=true 时登记未 ACK 追踪（用于 msg 类帧）。
   * 背压：未确认积压超过上限时断开连接（客户端重连后走 sync 补发）。
   */
  send(conn, frame, { track = false, trackReview = false, roomId = null, seq = null, reviewId = null } = {}) {
    if (conn.ws.readyState !== 1 /* OPEN */) return false;
    if (track && conn.unackedCount >= this.config.maxUnackedPerConn) {
      conn.ws.close(1013, 'backpressure: too many unacked messages');
      return false;
    }
    if (trackReview && conn.unackedReviewCount >= this.config.maxUnackedPerConn) {
      conn.ws.close(1013, 'backpressure: too many unacked review directives');
      return false;
    }
    const str = typeof frame === 'string' ? frame : JSON.stringify(frame);
    try {
      conn.ws.send(str);
    } catch {
      return false;
    }
    if (track && roomId != null && seq != null) conn.trackUnacked(roomId, seq, str);
    if (trackReview && roomId != null && reviewId != null) {
      conn.trackReviewUnacked(roomId, reviewId, str);
    }
    return true;
  }

  /** 广播到房间所有连接（含发送者的其他设备）。frame 只序列化一次。 */
  broadcast(roomId, frame, { track = false, seq = null } = {}) {
    const set = this.byRoom.get(roomId);
    if (!set) return 0;
    const str = JSON.stringify(frame);
    let delivered = 0;
    for (const conn of set) {
      if (this.send(conn, str, { track, roomId, seq })) delivered++;
    }
    return delivered;
  }

  /**
   * 广播审核指令（reviewEventId 通道，独立追踪/重发/ACK，不影响 seq 水位）。
   * replaceSeq=true 时同步替换该连接 seq 通道中同 seq 的缓存帧：审核状态推进后，
   * seq 通道若发生重发，发出的也是最新状态帧，不会把客户端打回 pending/已撤回前。
   */
  broadcastReview(roomId, frame, reviewId, { replaceSeq = null } = {}) {
    const set = this.byRoom.get(roomId);
    if (!set) return 0;
    const str = JSON.stringify(frame);
    let delivered = 0;
    for (const conn of set) {
      if (this.send(conn, str, { trackReview: true, roomId, reviewId })) delivered++;
      if (replaceSeq != null) conn.replaceTrackedSeqFrame?.(roomId, replaceSeq, str);
    }
    return delivered;
  }

  /** 心跳扫描：超时未 pong 的连接直接 terminate（触发 close 走正常清理） */
  heartbeatSweep() {
    const t = now();
    for (const conn of this.all) {
      if (t - conn.lastPong > this.config.heartbeatTimeoutMs) {
        conn.ws.terminate();
        continue;
      }
      try {
        conn.ws.ping();
      } catch { /* 连接已损坏，等待 close 事件清理 */ }
    }
  }

  /** 重发扫描：超时未 ACK 的消息重发；超过最大重发次数判定连接不可用，断开让客户端重连补发 */
  resendSweep() {
    const { ackResendAfterMs, ackMaxResend } = this.config;
    for (const conn of this.all) {
      for (const entry of conn.pendingResends(ackResendAfterMs)) {
        entry.tries++;
        if (entry.tries > ackMaxResend) {
          conn.ws.close(1011, 'ack timeout');
          break;
        }
        if (conn.ws.readyState === 1) {
          try {
            conn.ws.send(entry.frame);
            entry.lastSent = now();
          } catch { /* 下一轮再处理 */ }
        }
      }
    }
  }

  stats() {
    return {
      connections: this.all.size,
      users: this.byUser.size,
      rooms: this.byRoom.size,
    };
  }
}

module.exports = { Hub, Connection };
