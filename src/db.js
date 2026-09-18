'use strict';

const { DatabaseSync } = require('node:sqlite');
const { now } = require('./util');

/**
 * 持久层：SQLite（WAL 模式）。
 *
 * 可靠性设计要点：
 * 1. 消息与房间序号 seq 在同一事务中「先落库、后广播」——进程崩溃也不丢已确认消息。
 * 2. messages 上 (room_id, sender_id, client_msg_id) 唯一约束——客户端重试/网络重复
 *    提交同一条消息时不会产生重复记录，实现发送幂等。
 * 3. seq 为每房间单调递增序号，由 rooms.last_seq 计数器在事务内分配——保证房间内
 *    消息全序（时序可控），客户端可凭 seq 检测空洞并触发补发。
 */

const SCHEMA = `
PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;
PRAGMA busy_timeout = 5000;
PRAGMA synchronous = FULL;

CREATE TABLE IF NOT EXISTS users (
  id           TEXT PRIMARY KEY,
  name         TEXT NOT NULL UNIQUE,
  token_random TEXT NOT NULL,
  created_at   INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS rooms (
  id         TEXT PRIMARY KEY,
  name       TEXT NOT NULL UNIQUE,
  created_by TEXT NOT NULL REFERENCES users(id),
  created_at INTEGER NOT NULL,
  last_seq   INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS members (
  room_id     TEXT NOT NULL REFERENCES rooms(id),
  user_id     TEXT NOT NULL REFERENCES users(id),
  role        TEXT NOT NULL DEFAULT 'member' CHECK (role IN ('admin','member')),
  muted_until INTEGER NOT NULL DEFAULT 0,
  joined_at   INTEGER NOT NULL,
  PRIMARY KEY (room_id, user_id)
);

CREATE TABLE IF NOT EXISTS messages (
  room_id       TEXT NOT NULL REFERENCES rooms(id),
  seq           INTEGER NOT NULL,
  client_msg_id TEXT NOT NULL,
  sender_id     TEXT NOT NULL REFERENCES users(id),
  content       TEXT NOT NULL,
  ts            INTEGER NOT NULL,
  -- 审核状态：visible(默认/通过) held(先审后发，暂存不可见) flagged(仅标记风险)
  --           recalled(先发后撤，已撤回) rejected(先审后发驳回)
  mod_status    TEXT NOT NULL DEFAULT 'visible' CHECK (mod_status IN ('visible','held','flagged','recalled','rejected')),
  mod_reason    TEXT,                       -- 命中原因/处置说明（敏感词、频率、人工说明）
  rule_version  INTEGER,                    -- 判定时使用的规则版本号
  PRIMARY KEY (room_id, seq),
  UNIQUE (room_id, sender_id, client_msg_id)  -- 幂等键
);

-- 服务端保存的每用户每房间已确认游标（断线补发的兜底依据）
CREATE TABLE IF NOT EXISTS cursors (
  room_id      TEXT NOT NULL REFERENCES rooms(id),
  user_id      TEXT NOT NULL REFERENCES users(id),
  last_ack_seq INTEGER NOT NULL DEFAULT 0,
  updated_at   INTEGER NOT NULL,
  PRIMARY KEY (room_id, user_id)
);

-- ============================== 消息审核 ==============================

-- 审核规则版本：每房间一行一个版本，published 版本用于检测，历史版本可追溯/回滚
CREATE TABLE IF NOT EXISTS mod_rule_versions (
  id          INTEGER PRIMARY KEY AUTOINCREMENT,
  room_id     TEXT NOT NULL REFERENCES rooms(id),
  version     INTEGER NOT NULL,
  config      TEXT NOT NULL,               -- JSON：{mode, sensitiveWords, freqWindowMs, freqMaxCount, detectTimeoutMs}
  published   INTEGER NOT NULL DEFAULT 0, -- 1=当前生效版本（每房间至多一个）
  created_by  TEXT NOT NULL REFERENCES users(id),
  created_at  INTEGER NOT NULL,
  published_at INTEGER,
  UNIQUE (room_id, version)
);

-- 审核记录（队列项）：一条消息一条，detect/decision 均为终态写一次（compare-and-set 防重复审核）
CREATE TABLE IF NOT EXISTS mod_reviews (
  room_id        TEXT NOT NULL REFERENCES rooms(id),
  seq            INTEGER NOT NULL,
  rule_version   INTEGER,
  detect_status  TEXT NOT NULL DEFAULT 'pending' CHECK (detect_status IN ('pending','clean','suspect','violation','error','timeout')),
  hits           TEXT,                    -- JSON 数组：命中的敏感词/规则
  detect_ms      INTEGER,
  reviewed_by    TEXT,                    -- 人工复核管理员 userId（系统自动判定为空）
  decision       TEXT,                    -- approve / reject / recall（人工终判）
  decide_reason  TEXT,
  created_at     INTEGER NOT NULL,
  decided_at     INTEGER,
  PRIMARY KEY (room_id, seq)
);

-- 用户申诉：针对被处置（held 驳回 / recalled）的消息
CREATE TABLE IF NOT EXISTS mod_appeals (
  id          INTEGER PRIMARY KEY AUTOINCREMENT,
  room_id     TEXT NOT NULL REFERENCES rooms(id),
  seq         INTEGER NOT NULL,
  user_id     TEXT NOT NULL REFERENCES users(id),
  reason      TEXT NOT NULL,
  status      TEXT NOT NULL DEFAULT 'open' CHECK (status IN ('open','upheld','reversed')),
  admin_id    TEXT,
  reply       TEXT,
  created_at  INTEGER NOT NULL,
  handled_at  INTEGER,
  UNIQUE (room_id, seq, user_id)          -- 同一用户对同一消息只能申诉一次
);

-- 管理员操作日志（审核相关所有敏感动作留存）
CREATE TABLE IF NOT EXISTS mod_audit_log (
  id          INTEGER PRIMARY KEY AUTOINCREMENT,
  room_id     TEXT,
  actor_id    TEXT NOT NULL,
  action      TEXT NOT NULL,              -- publish_rule/rollback_rule/decision/recall/force_approve/appeal_handle/...
  detail      TEXT,                       -- JSON
  created_at  INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_mod_audit_room ON mod_audit_log (room_id, id);
`;

/** 旧库平滑升级：messages 缺少审核列时补上（IF NOT EXISTS 不负责列级迁移） */
const MIGRATIONS = [
  `ALTER TABLE messages ADD COLUMN mod_status TEXT NOT NULL DEFAULT 'visible'`,
  `ALTER TABLE messages ADD COLUMN mod_reason TEXT`,
  `ALTER TABLE messages ADD COLUMN rule_version INTEGER`,
];

const MSG_SELECT = `
  SELECT m.room_id AS roomId, m.seq, m.client_msg_id AS clientMsgId,
         m.sender_id AS "from", u.name AS fromName, m.content, m.ts,
         m.mod_status AS modStatus, m.mod_reason AS modReason, m.rule_version AS ruleVersion
    FROM messages m JOIN users u ON u.id = m.sender_id
`;

/** 审核队列项：审核记录 + 消息正文/发送者 + 最近一条申诉 */
const REVIEW_SELECT = `
  SELECT r.room_id AS roomId, r.seq, r.rule_version AS ruleVersion,
         r.detect_status AS detectStatus, r.hits, r.detect_ms AS detectMs,
         r.reviewed_by AS reviewedBy, r.decision, r.decide_reason AS decideReason,
         r.created_at AS createdAt, r.decided_at AS decidedAt,
         m.client_msg_id AS clientMsgId, m.sender_id AS "from", u.name AS fromName,
         m.content, m.ts, m.mod_status AS modStatus, m.mod_reason AS modReason
    FROM mod_reviews r
    JOIN messages m ON m.room_id = r.room_id AND m.seq = r.seq
    JOIN users u ON u.id = m.sender_id
`;

class ChatDB {
  constructor(dbPath) {
    this.db = new DatabaseSync(dbPath);
    this.db.exec(SCHEMA);
    this._migrate();
    this._prepare();
  }

  /** 列级迁移：旧库的 messages 没有审核列，ALTER 已存在则忽略报错 */
  _migrate() {
    const cols = new Set(this.db.prepare('PRAGMA table_info(messages)').all().map((c) => c.name));
    for (const sql of MIGRATIONS) {
      const name = /ADD COLUMN (\w+)/.exec(sql)[1];
      if (!cols.has(name)) this.db.exec(sql);
    }
  }

  _prepare() {
    const d = this.db;
    this.stmt = {
      insertUser: d.prepare('INSERT INTO users (id, name, token_random, created_at) VALUES (?, ?, ?, ?)'),
      userByName: d.prepare('SELECT * FROM users WHERE name = ?'),
      userById: d.prepare('SELECT * FROM users WHERE id = ?'),

      insertRoom: d.prepare('INSERT INTO rooms (id, name, created_by, created_at) VALUES (?, ?, ?, ?)'),
      roomById: d.prepare('SELECT * FROM rooms WHERE id = ?'),
      roomByName: d.prepare('SELECT * FROM rooms WHERE name = ?'),
      roomsForUser: d.prepare(
        `SELECT r.id, r.name, r.last_seq AS lastSeq, m.role, m.muted_until AS mutedUntil
           FROM rooms r JOIN members m ON m.room_id = r.id
          WHERE m.user_id = ? ORDER BY r.created_at`
      ),

      upsertMember: d.prepare(
        `INSERT INTO members (room_id, user_id, role, muted_until, joined_at)
         VALUES (?, ?, ?, 0, ?)
         ON CONFLICT (room_id, user_id) DO NOTHING`
      ),
      member: d.prepare('SELECT * FROM members WHERE room_id = ? AND user_id = ?'),
      setMuted: d.prepare('UPDATE members SET muted_until = ? WHERE room_id = ? AND user_id = ?'),
      membersOfRoom: d.prepare(
        `SELECT m.user_id AS userId, u.name, m.role, m.muted_until AS mutedUntil
           FROM members m JOIN users u ON u.id = m.user_id WHERE m.room_id = ?`
      ),

      // —— 消息写入（事务内使用）——
      msgByClientId: d.prepare(
        `${MSG_SELECT} WHERE m.room_id = ? AND m.sender_id = ? AND m.client_msg_id = ?`
      ),
      bumpSeq: d.prepare('UPDATE rooms SET last_seq = last_seq + 1 WHERE id = ? RETURNING last_seq'),
      insertMsg: d.prepare(
        `INSERT INTO messages (room_id, seq, client_msg_id, sender_id, content, ts, mod_status, rule_version)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
      ),

      // —— 消息读取 ——
      msgsAfter: d.prepare(`${MSG_SELECT} WHERE m.room_id = ? AND m.seq > ? ORDER BY m.seq LIMIT ?`),
      msgBySeq: d.prepare(`${MSG_SELECT} WHERE m.room_id = ? AND m.seq = ?`),
      // 门控可见读取：历史只返回对普通成员可见的消息（visible/flagged/recalled），
      // held（先审后发暂存）、rejected（驳回）不下发
      visibleBefore: d.prepare(
        `${MSG_SELECT} WHERE m.room_id = ? AND m.seq < ?
         AND m.mod_status IN ('visible','flagged','recalled')
         ORDER BY m.seq DESC LIMIT ?`
      ),
      // 崩溃恢复：所有仍 held 的消息（可能正卡在检测/人工环节）
      heldMessages: d.prepare(`${MSG_SELECT} WHERE m.mod_status = 'held' ORDER BY m.room_id, m.seq`),

      // —— 游标 ——
      upsertCursor: d.prepare(
        `INSERT INTO cursors (room_id, user_id, last_ack_seq, updated_at) VALUES (?, ?, ?, ?)
         ON CONFLICT (room_id, user_id)
         DO UPDATE SET last_ack_seq = MAX(last_ack_seq, excluded.last_ack_seq), updated_at = excluded.updated_at`
      ),
      cursor: d.prepare('SELECT last_ack_seq AS lastAckSeq FROM cursors WHERE room_id = ? AND user_id = ?'),

      // —— 消息审核状态 ——
      setMsgMod: d.prepare(
        `UPDATE messages SET mod_status = ?, mod_reason = ?, rule_version = ?
          WHERE room_id = ? AND seq = ?`
      ),
      msgMod: d.prepare(
        `SELECT mod_status AS modStatus, mod_reason AS modReason, rule_version AS ruleVersion
           FROM messages WHERE room_id = ? AND seq = ?`
      ),
      // 门控：房间内仍处于 held（暂存不可见）的最小 seq；无则 NULL
      minHeldSeq: d.prepare(
        `SELECT MIN(seq) AS s FROM messages WHERE room_id = ? AND mod_status = 'held'`
      ),

      // —— 规则版本 ——
      maxRuleVersion: d.prepare('SELECT COALESCE(MAX(version),0) AS v FROM mod_rule_versions WHERE room_id = ?'),
      insertRuleVersion: d.prepare(
        `INSERT INTO mod_rule_versions (room_id, version, config, published, created_by, created_at, published_at)
         VALUES (?, ?, ?, ?, ?, ?, ?)`
      ),
      ruleById: d.prepare('SELECT * FROM mod_rule_versions WHERE room_id = ? AND id = ?'),
      ruleByVersion: d.prepare('SELECT * FROM mod_rule_versions WHERE room_id = ? AND version = ?'),
      publishedRule: d.prepare('SELECT * FROM mod_rule_versions WHERE room_id = ? AND published = 1'),
      listRuleVersions: d.prepare(
        'SELECT * FROM mod_rule_versions WHERE room_id = ? ORDER BY version DESC LIMIT ?'
      ),
      unpublishRules: d.prepare('UPDATE mod_rule_versions SET published = 0 WHERE room_id = ?'),
      publishRule: d.prepare(
        'UPDATE mod_rule_versions SET published = 1, published_at = ? WHERE room_id = ? AND version = ?'
      ),

      // —— 审核记录 ——
      insertReview: d.prepare(
        `INSERT INTO mod_reviews (room_id, seq, rule_version, detect_status, hits, detect_ms, created_at)
         VALUES (?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT (room_id, seq) DO NOTHING`
      ),
      review: d.prepare('SELECT * FROM mod_reviews WHERE room_id = ? AND seq = ?'),
      // compare-and-set：仅当 detect_status 仍是期望值时更新，返回 changes 判定是否抢占成功
      casDetect: d.prepare(
        `UPDATE mod_reviews SET detect_status = ?, hits = ?, detect_ms = ?
          WHERE room_id = ? AND seq = ? AND detect_status = ?`
      ),
      // 晚到结果升级：超时/出错/clean 已 fail-open 后，真正的违规/可疑结果到达时抢占升级
      // （仅限尚无人工终判），用于「审核结果晚于客户端展示」时补做撤回/标记
      casDetectEscalate: d.prepare(
        `UPDATE mod_reviews SET detect_status = ?, hits = ?, detect_ms = ?
          WHERE room_id = ? AND seq = ? AND decision IS NULL
            AND detect_status IN ('timeout','error','clean')`
      ),
      casDecision: d.prepare(
        `UPDATE mod_reviews SET reviewed_by = ?, decision = ?, decide_reason = ?, decided_at = ?
          WHERE room_id = ? AND seq = ? AND decision IS NULL`
      ),
      // 人工复核队列：机器判违规/可疑、且尚无人工终判（decision IS NULL）
      pendingReviews: d.prepare(
        `${REVIEW_SELECT} WHERE r.room_id = ? AND r.decision IS NULL
           AND r.detect_status IN ('violation','suspect') ORDER BY r.seq ASC LIMIT ?`
      ),
      recentReviews: d.prepare(
        `${REVIEW_SELECT} WHERE r.room_id = ? ORDER BY r.seq DESC LIMIT ?`
      ),
      reviewExists: d.prepare('SELECT 1 FROM mod_reviews WHERE room_id = ? AND seq = ?'),

      // —— 申诉 ——
      insertAppeal: d.prepare(
        `INSERT INTO mod_appeals (room_id, seq, user_id, reason, created_at) VALUES (?, ?, ?, ?, ?)`
      ),
      appealById: d.prepare('SELECT * FROM mod_appeals WHERE id = ?'),
      listAppeals: d.prepare(
        `SELECT a.*, u.name AS user_name FROM mod_appeals a JOIN users u ON u.id = a.user_id
          WHERE a.room_id = ? ORDER BY a.id DESC LIMIT ?`
      ),
      casAppeal: d.prepare(
        `UPDATE mod_appeals SET status = ?, admin_id = ?, reply = ?, handled_at = ?
          WHERE id = ? AND status = 'open'`
      ),

      // —— 审计日志 ——
      insertAudit: d.prepare(
        'INSERT INTO mod_audit_log (room_id, actor_id, action, detail, created_at) VALUES (?, ?, ?, ?, ?)'
      ),
      auditByRoom: d.prepare(
        'SELECT * FROM mod_audit_log WHERE room_id = ? ORDER BY id DESC LIMIT ?'
      ),
      auditRecent: d.prepare('SELECT * FROM mod_audit_log ORDER BY id DESC LIMIT ?'),
    };
  }

  /** 在 IMMEDIATE 事务中执行 fn，失败回滚。node:sqlite 为同步驱动，单进程内无并发交错。 */
  _tx(fn) {
    this.db.exec('BEGIN IMMEDIATE');
    try {
      const r = fn();
      this.db.exec('COMMIT');
      return r;
    } catch (err) {
      try { this.db.exec('ROLLBACK'); } catch { /* 已回滚 */ }
      throw err;
    }
  }

  // ---------- 用户 ----------

  createUser(id, name, tokenRandom) {
    this.stmt.insertUser.run(id, name, tokenRandom, now());
    return this.stmt.userById.get(id);
  }

  getUserByName(name) { return this.stmt.userByName.get(name); }
  getUserById(id) { return this.stmt.userById.get(id); }

  // ---------- 房间与成员 ----------

  createRoom(id, name, creatorId) {
    return this._tx(() => {
      this.stmt.insertRoom.run(id, name, creatorId, now());
      // 创建者即管理员
      this.stmt.upsertMember.run(id, creatorId, 'admin', now());
      return this.stmt.roomById.get(id);
    });
  }

  getRoom(id) { return this.stmt.roomById.get(id); }
  getRoomByName(name) { return this.stmt.roomByName.get(name); }
  listRoomsForUser(userId) { return this.stmt.roomsForUser.all(userId); }
  listMembers(roomId) { return this.stmt.membersOfRoom.all(roomId); }

  joinRoom(roomId, userId) {
    this.stmt.upsertMember.run(roomId, userId, 'member', now());
    return this.stmt.member.get(roomId, userId);
  }

  getMember(roomId, userId) { return this.stmt.member.get(roomId, userId); }

  /** 设置禁言截止时间（0 表示解除禁言） */
  setMuted(roomId, userId, mutedUntil) {
    this.stmt.setMuted.run(mutedUntil, roomId, userId);
    return this.stmt.member.get(roomId, userId);
  }

  // ---------- 消息 ----------

  /** 断线补发：取 seq > afterSeq 的原始行（升序，最多 limit 条），由调用方按成员做可见性门控 */
  getMessagesAfter(roomId, afterSeq, limit) {
    return this.stmt.msgsAfter.all(roomId, afterSeq, limit);
  }

  /** 门控历史翻页：只取可见消息，返回时按升序 */
  getVisibleMessagesBefore(roomId, beforeSeq, limit) {
    return this.stmt.visibleBefore.all(roomId, beforeSeq, limit).reverse();
  }

  /** 全部仍 held 的消息（崩溃恢复用），按房间、seq 升序 */
  listHeldMessages() { return this.stmt.heldMessages.all(); }

  /** 取单条消息（完整含审核字段），用于撤回/申诉等场景 */
  getMessage(roomId, seq) {
    return this.stmt.msgBySeq.get(roomId, seq);
  }

  // ---------- 游标 ----------

  saveCursor(roomId, userId, lastAckSeq) {
    this.stmt.upsertCursor.run(roomId, userId, lastAckSeq, now());
  }

  getCursor(roomId, userId) {
    const row = this.stmt.cursor.get(roomId, userId);
    return row ? row.lastAckSeq : 0;
  }

  // ---------- 消息审核状态 ----------

  /** 幂等写入消息（支持初始审核状态）。语义同原 insertMessage。 */
  insertMessage({ roomId, clientMsgId, senderId, content, modStatus = 'visible', ruleVersion = null }) {
    return this._tx(() => {
      const existing = this.stmt.msgByClientId.get(roomId, senderId, clientMsgId);
      if (existing) return { message: existing, duplicate: true };

      const { last_seq: seq } = this.stmt.bumpSeq.get(roomId);
      const ts = now();
      this.stmt.insertMsg.run(roomId, seq, clientMsgId, senderId, content, ts, modStatus, ruleVersion);
      const message = this.stmt.msgByClientId.get(roomId, senderId, clientMsgId);
      return { message, duplicate: false };
    });
  }

  /** 更新消息审核状态（可见性翻转），返回更新后的状态行 */
  setMessageModeration(roomId, seq, modStatus, modReason = null, ruleVersion = null) {
    this.stmt.setMsgMod.run(modStatus, modReason, ruleVersion, roomId, seq);
    return this.stmt.msgMod.get(roomId, seq);
  }

  getMessageModeration(roomId, seq) {
    return this.stmt.msgMod.get(roomId, seq);
  }

  /** 房间内仍 held 的最小 seq（可见性门控水位线），无 held 消息返回 null */
  minHeldSeq(roomId) {
    const row = this.stmt.minHeldSeq.get(roomId);
    return row && row.s != null ? row.s : null;
  }

  // ---------- 规则版本 ----------

  /** 创建草稿版本（version 自动递增），不发布 */
  createRuleDraft(roomId, config, createdBy) {
    return this._tx(() => {
      const { v } = this.stmt.maxRuleVersion.get(roomId);
      const version = v + 1;
      this.stmt.insertRuleVersion.run(roomId, version, JSON.stringify(config), 0, createdBy, now(), null);
      return this.stmt.ruleByVersion.get(roomId, version);
    });
  }

  /** 发布指定版本：同房间旧版本全部下线，目标版本上线（房间内至多一个 published） */
  publishRuleVersion(roomId, version, publishedAt = now()) {
    return this._tx(() => {
      const row = this.stmt.ruleByVersion.get(roomId, version);
      if (!row) return null;
      this.stmt.unpublishRules.run(roomId);
      this.stmt.publishRule.run(publishedAt, roomId, version);
      return this.stmt.ruleByVersion.get(roomId, version);
    });
  }

  getPublishedRule(roomId) { return this.stmt.publishedRule.get(roomId); }
  getRuleByVersion(roomId, version) { return this.stmt.ruleByVersion.get(roomId, version); }
  listRuleVersions(roomId, limit = 50) { return this.stmt.listRuleVersions.all(roomId, limit); }

  // ---------- 审核记录 ----------

  /** 登记审核记录（重复登记不覆盖），返回是否新建 */
  ensureReview(roomId, seq, ruleVersion, detectStatus, hits, detectMs = null, createdAt = now()) {
    const info = this.stmt.insertReview.run(
      roomId, seq, ruleVersion, detectStatus, hits ? JSON.stringify(hits) : null, detectMs, createdAt
    );
    return info.changes > 0;
  }

  getReview(roomId, seq) {
    const r = this.stmt.review.get(roomId, seq);
    return r ? this._hydrateReview(r) : null;
  }

  _hydrateReview(r) {
    return { ...r, hits: r.hits ? JSON.parse(r.hits) : null };
  }

  /**
   * compare-and-set 检测结果：仅当当前 detect_status === expect 时写入。
   * 返回 true 表示抢占成功（超时兜底与真实检测结果之间用它去重，防重复审核）。
   */
  casDetectResult(roomId, seq, expect, status, hits, detectMs) {
    const info = this.stmt.casDetect.run(
      status, hits ? JSON.stringify(hits) : null, detectMs, roomId, seq, expect
    );
    return info.changes > 0;
  }

  /** 晚到结果升级（超时/error/clean fail-open 之后真正结果才到）。返回是否升级成功。 */
  casDetectEscalate(roomId, seq, status, hits, detectMs) {
    const info = this.stmt.casDetectEscalate.run(
      status, hits ? JSON.stringify(hits) : null, detectMs, roomId, seq
    );
    return info.changes > 0;
  }

  /** compare-and-set 人工终判：仅当尚无 decision 时写入（防同一队列项被重复处理） */
  casDecision(roomId, seq, reviewedBy, decision, reason, decidedAt = now()) {
    const info = this.stmt.casDecision.run(reviewedBy, decision, reason, decidedAt, roomId, seq);
    return info.changes > 0;
  }

  listPendingReviews(roomId, limit = 100) {
    return this.stmt.pendingReviews.all(roomId, limit).map((r) => this._hydrateReview(r));
  }

  listRecentReviews(roomId, limit = 100) {
    return this.stmt.recentReviews.all(roomId, limit).map((r) => this._hydrateReview(r));
  }

  hasReview(roomId, seq) { return !!this.stmt.reviewExists.get(roomId, seq); }

  // ---------- 申诉 ----------

  createAppeal(roomId, seq, userId, reason, createdAt = now()) {
    try {
      const info = this.stmt.insertAppeal.run(roomId, seq, userId, reason, createdAt);
      return { ok: true, id: Number(info.lastInsertRowid) };
    } catch (err) {
      // UNIQUE(room_id, seq, user_id)：重复申诉
      if (String(err.message).includes('UNIQUE')) return { ok: false, code: 'ALREADY_APPEALED' };
      throw err;
    }
  }

  getAppeal(id) { return this.stmt.appealById.get(Number(id)); }
  listAppeals(roomId, limit = 100) { return this.stmt.listAppeals.all(roomId, limit); }

  /** 处理申诉：仅 open 可被处理（compare-and-set 防并发重复处理） */
  resolveAppeal(id, status, adminId, reply, handledAt = now()) {
    const info = this.stmt.casAppeal.run(status, adminId, reply, handledAt, Number(id));
    return info.changes > 0;
  }

  // ---------- 审计日志 ----------

  addAudit(actorId, action, detail = null, roomId = null, createdAt = now()) {
    const info = this.stmt.insertAudit.run(roomId, actorId, action, detail ? JSON.stringify(detail) : null, createdAt);
    return Number(info.lastInsertRowid);
  }

  listAudit(roomId, limit = 100) {
    const rows = roomId ? this.stmt.auditByRoom.all(roomId, limit) : this.stmt.auditRecent.all(limit);
    return rows.map((r) => ({ ...r, detail: r.detail ? JSON.parse(r.detail) : null }));
  }

  close() {
    try { this.db.exec('PRAGMA wal_checkpoint(TRUNCATE)'); } catch { /* 内存库无 WAL */ }
    this.db.close();
  }
}

module.exports = { ChatDB };
