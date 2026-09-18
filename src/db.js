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
 *
 * 审核扩展（review_*）：
 * - messages.review_status 四态：pending（先审后发暂存）/ released（正常可见）/
 *   recalled（先发后撤）/ blocked（审核不通过）。审核只改状态、永不删消息，seq 不空洞。
 * - 先审后发用「无内容占位帧」占据 seq（客户端 lastSeenSeq 照常推进），放行后同一 seq
 *   补发正文；断线补发按消息当前状态转帧，无需单独的每用户放行水位。
 * - review_cursors 记录每用户每房间审核指令水位（reviewEventId），与投递 ACK（cursors）
 *   是两条独立的可靠通道，互不污染。
 * - review_rules 不可变版本表，命中时把版本号写回消息，供误判追溯与规则回滚对照。
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
  -- 审核状态机：pending -> released/blocked；released -> recalled -> released（申诉恢复）
  review_status        TEXT NOT NULL DEFAULT 'released'
                         CHECK (review_status IN ('pending','released','recalled','blocked')),
  review_flags         TEXT NOT NULL DEFAULT '[]',   -- 命中项 JSON，如 ["word:违禁词","freq"]
  review_reason        TEXT,                          -- 人类可读说明
  review_rule_version  INTEGER,                       -- 判定时生效的规则版本
  review_held_at       INTEGER NOT NULL DEFAULT 0,    -- 进入 pending 的时间（队列排序/TTL）
  review_decided_by    TEXT,                          -- 最后人工/系统决策者
  review_decided_at    INTEGER NOT NULL DEFAULT 0,
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

-- 先审后发模式下消息以「审核中」占位帧占位（seq 连续、不空洞），
-- 断线补发按消息当前状态转换帧即可，无需单独的放行水位。

-- 审核规则版本（不可变；新版本生效时旧版本 active=0，历史消息保留命中时的版本号）
CREATE TABLE IF NOT EXISTS review_rules (
  version         INTEGER PRIMARY KEY AUTOINCREMENT,
  words           TEXT NOT NULL DEFAULT '[]', -- 敏感词 JSON 数组
  freq_window_ms  INTEGER NOT NULL,
  freq_max_count  INTEGER NOT NULL,
  note            TEXT NOT NULL DEFAULT '',
  created_by      TEXT,
  created_at      INTEGER NOT NULL,
  active          INTEGER NOT NULL DEFAULT 1
);

-- 每房间处置策略（缺省由全局配置/服务端默认决定：pre 先审后发 / post 先发后撤 / mark 仅标记）
CREATE TABLE IF NOT EXISTS room_review_policy (
  room_id     TEXT PRIMARY KEY REFERENCES rooms(id),
  mode        TEXT NOT NULL CHECK (mode IN ('pre','post','mark')),
  updated_by  TEXT,
  updated_at  INTEGER NOT NULL
);

-- 用户申诉（每用户每消息至多一条，驳回后冷却可重新提交 -> 复用同一行重新打开）
CREATE TABLE IF NOT EXISTS appeals (
  id            TEXT PRIMARY KEY,
  room_id       TEXT NOT NULL REFERENCES rooms(id),
  seq           INTEGER NOT NULL,
  user_id       TEXT NOT NULL REFERENCES users(id),
  reason        TEXT NOT NULL DEFAULT '',
  status        TEXT NOT NULL DEFAULT 'open' CHECK (status IN ('open','approved','rejected','closed')),
  created_at    INTEGER NOT NULL,
  decided_by    TEXT,
  decided_at    INTEGER NOT NULL DEFAULT 0,
  decision_note TEXT NOT NULL DEFAULT '',
  UNIQUE (room_id, seq, user_id)
);

-- 管理员操作日志（只增不改，审核全链路留痕）
CREATE TABLE IF NOT EXISTS admin_log (
  id          INTEGER PRIMARY KEY AUTOINCREMENT,
  ts          INTEGER NOT NULL,
  actor_id    TEXT NOT NULL,
  actor_name  TEXT NOT NULL DEFAULT '',
  room_id     TEXT,
  action      TEXT NOT NULL,
  target_type TEXT NOT NULL DEFAULT '', -- message / appeal / rule / policy
  target_id   TEXT NOT NULL DEFAULT '', -- 如 "roomId:seq" / appealId / version
  detail      TEXT NOT NULL DEFAULT '{}'
);

-- 人工审核队列：每条消息至多一个 open 条目（pre 暂存 / post 撤回 / mark 标记 / 人工加队）
CREATE TABLE IF NOT EXISTS review_queue (
  id         TEXT PRIMARY KEY,
  room_id    TEXT NOT NULL REFERENCES rooms(id),
  seq        INTEGER NOT NULL,
  source     TEXT NOT NULL DEFAULT 'auto' CHECK (source IN ('auto','manual')),
  status     TEXT NOT NULL DEFAULT 'open' CHECK (status IN ('open','closed')),
  created_at INTEGER NOT NULL,
  closed_at  INTEGER NOT NULL DEFAULT 0,
  closed_by  TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_review_queue_open
  ON review_queue (room_id, seq) WHERE status = 'open';

-- 审核事件日志（只增）：held/released/blocked/recalled/restored/flagged。
-- 在线时实时广播；断线期间靠它按客户端水位增量补指令（审核结果晚于展示也能最终一致）
CREATE TABLE IF NOT EXISTS review_events (
  id           INTEGER PRIMARY KEY AUTOINCREMENT,
  room_id      TEXT NOT NULL REFERENCES rooms(id),
  seq          INTEGER NOT NULL,
  kind         TEXT NOT NULL CHECK (kind IN ('held','released','blocked','recalled','restored','flagged')),
  flags        TEXT NOT NULL DEFAULT '[]',
  reason       TEXT,
  actor        TEXT NOT NULL DEFAULT '',          -- detector / system:ttl / userId
  rule_version INTEGER,
  ts           INTEGER NOT NULL
);
-- 注意：依赖 messages 审核列的索引不放在这里 —— 旧库迁移时列尚未追加，
-- 统一在 _migrate() 之后创建（见 REVIEW_INDEXES）。
`;

// 迁移完成后再执行：依赖新审核列的索引 + 后加的表/索引（CREATE IF NOT EXISTS 幂等）
const POST_MIGRATION_DDL = `
CREATE INDEX IF NOT EXISTS idx_admin_log_room ON admin_log (room_id, id);
CREATE INDEX IF NOT EXISTS idx_review_events_room ON review_events (room_id, id);
CREATE INDEX IF NOT EXISTS idx_messages_review ON messages (review_status, review_held_at);
CREATE INDEX IF NOT EXISTS idx_appeals_status ON appeals (room_id, status, created_at);

CREATE TABLE IF NOT EXISTS review_cursors (
  room_id    TEXT NOT NULL REFERENCES rooms(id),
  user_id    TEXT NOT NULL REFERENCES users(id),
  last_event INTEGER NOT NULL DEFAULT 0,
  updated_at INTEGER NOT NULL,
  PRIMARY KEY (room_id, user_id)
);
`;

const MSG_SELECT = `
  SELECT m.room_id AS roomId, m.seq, m.client_msg_id AS clientMsgId,
         m.sender_id AS "from", u.name AS fromName, m.content, m.ts,
         m.review_status AS reviewStatus, m.review_flags AS reviewFlags,
         m.review_reason AS reviewReason, m.review_rule_version AS reviewRuleVersion,
         m.review_held_at AS reviewHeldAt, m.review_decided_by AS reviewDecidedBy,
         m.review_decided_at AS reviewDecidedAt
    FROM messages m JOIN users u ON u.id = m.sender_id
`;

class ChatDB {
  constructor(dbPath) {
    this.db = new DatabaseSync(dbPath);
    this.db.exec(SCHEMA);
    this._migrate();
    this._prepare();
  }

  /** 旧库（无审核列）平滑升级：messages 追加审核列，历史消息一律视为 released */
  _migrate() {
    const cols = new Set(this.db.prepare('PRAGMA table_info(messages)').all().map((c) => c.name));
    const add = (name, ddl) => { if (!cols.has(name)) this.db.exec(`ALTER TABLE messages ADD COLUMN ${ddl}`); };
    add('review_status', "review_status TEXT NOT NULL DEFAULT 'released'");
    add('review_flags', "review_flags TEXT NOT NULL DEFAULT '[]'");
    add('review_reason', 'review_reason TEXT');
    add('review_rule_version', 'review_rule_version INTEGER');
    add('review_held_at', 'review_held_at INTEGER NOT NULL DEFAULT 0');
    add('review_decided_by', 'review_decided_by TEXT');
    add('review_decided_at', 'review_decided_at INTEGER NOT NULL DEFAULT 0');
    // 审核列就位后再建依赖它们的索引与后加的表
    this.db.exec(POST_MIGRATION_DDL);
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
      anyAdminRoom: d.prepare('SELECT 1 FROM members WHERE user_id = ? AND role = ? LIMIT 1'),

      // —— 消息写入（事务内使用）——
      msgByClientId: d.prepare(
        `${MSG_SELECT} WHERE m.room_id = ? AND m.sender_id = ? AND m.client_msg_id = ?`
      ),
      bumpSeq: d.prepare('UPDATE rooms SET last_seq = last_seq + 1 WHERE id = ? RETURNING last_seq'),
      insertMsg: d.prepare(
        `INSERT INTO messages (room_id, seq, client_msg_id, sender_id, content, ts)
         VALUES (?, ?, ?, ?, ?, ?)`
      ),

      // —— 消息读取 ——
      msgsAfter: d.prepare(`${MSG_SELECT} WHERE m.room_id = ? AND m.seq > ? ORDER BY m.seq LIMIT ?`),
      msgsBefore: d.prepare(
        `${MSG_SELECT} WHERE m.room_id = ? AND m.seq < ? ORDER BY m.seq DESC LIMIT ?`
      ),
      msgBySeq: d.prepare(`${MSG_SELECT} WHERE m.room_id = ? AND m.seq = ?`),

      // —— 游标 ——
      upsertCursor: d.prepare(
        `INSERT INTO cursors (room_id, user_id, last_ack_seq, updated_at) VALUES (?, ?, ?, ?)
         ON CONFLICT (room_id, user_id)
         DO UPDATE SET last_ack_seq = MAX(last_ack_seq, excluded.last_ack_seq), updated_at = excluded.updated_at`
      ),
      cursor: d.prepare('SELECT last_ack_seq AS lastAckSeq FROM cursors WHERE room_id = ? AND user_id = ?'),

      // —— review event watermark ——
      holdMsg: d.prepare(
        `UPDATE messages SET review_status = 'pending', review_flags = ?, review_reason = ?,
               review_rule_version = ?, review_held_at = ?, review_decided_by = NULL,
               review_decided_at = 0
         WHERE room_id = ? AND seq = ? AND review_status = 'released'`
      ),
      // 晚到的自动检测结果只允许印在 released（post/mark 模式）；终态/暂存态忽略，不覆盖人工决策
      stampDetection: d.prepare(
        `UPDATE messages SET review_flags = ?, review_reason = ?, review_rule_version = ?
         WHERE room_id = ? AND seq = ? AND review_status = 'released'`
      ),
      // pre 模式检测命中后把命中项补印到仍在 pending 的消息上
      updateHold: d.prepare(
        `UPDATE messages SET review_flags = ?, review_reason = ?, review_rule_version = ?
         WHERE room_id = ? AND seq = ? AND review_status = 'pending'`
      ),
      setReviewReason: d.prepare(
        'UPDATE messages SET review_reason = ? WHERE room_id = ? AND seq = ?'
      ),
      releaseMsg: d.prepare(
        `UPDATE messages SET review_status = 'released', review_decided_by = ?,
               review_decided_at = ?, review_held_at = 0
         WHERE room_id = ? AND seq = ? AND review_status = 'pending'`
      ),
      blockMsg: d.prepare(
        `UPDATE messages SET review_status = 'blocked', review_decided_by = ?,
               review_decided_at = ?, review_held_at = 0
         WHERE room_id = ? AND seq = ? AND review_status = 'pending'`
      ),
      recallMsg: d.prepare(
        `UPDATE messages SET review_status = 'recalled', review_decided_by = ?,
               review_decided_at = ?
         WHERE room_id = ? AND seq = ? AND review_status = 'released'`
      ),
      restoreMsg: d.prepare(
        `UPDATE messages SET review_status = 'released', review_decided_by = ?,
               review_decided_at = ?
         WHERE room_id = ? AND seq = ? AND review_status IN ('recalled','blocked')`
      ),
      pendingList: d.prepare(
        `${MSG_SELECT} WHERE m.review_status = 'pending'
         ORDER BY m.review_held_at, m.seq LIMIT ?`
      ),
      pendingListRoom: d.prepare(
        `${MSG_SELECT} WHERE m.review_status = 'pending' AND m.room_id = ?
         ORDER BY m.review_held_at, m.seq LIMIT ?`
      ),
      pendingOlder: d.prepare(
        `${MSG_SELECT} WHERE m.review_status = 'pending' AND m.review_held_at > 0
               AND m.review_held_at < ? ORDER BY m.review_held_at LIMIT ?`
      ),
      pendingCountAll: d.prepare('SELECT COUNT(*) AS n FROM messages WHERE review_status = ?'),
      pendingCountRoom: d.prepare(
        'SELECT COUNT(*) AS n FROM messages WHERE review_status = ? AND room_id = ?'
      ),

      // —— 规则版本 ——
      deactivateRules: d.prepare('UPDATE review_rules SET active = 0 WHERE active = 1'),
      insertRule: d.prepare(
        `INSERT INTO review_rules (words, freq_window_ms, freq_max_count, note, created_by, created_at, active)
         VALUES (?, ?, ?, ?, ?, ?, 1)`
      ),
      ruleByVersion: d.prepare('SELECT * FROM review_rules WHERE version = ?'),
      activeRule: d.prepare('SELECT * FROM review_rules WHERE active = 1 ORDER BY version DESC LIMIT 1'),
      listRules: d.prepare('SELECT * FROM review_rules ORDER BY version DESC LIMIT ?'),

      // —— 房间策略 ——
      getPolicy: d.prepare('SELECT * FROM room_review_policy WHERE room_id = ?'),
      upsertPolicy: d.prepare(
        `INSERT INTO room_review_policy (room_id, mode, updated_by, updated_at) VALUES (?, ?, ?, ?)
         ON CONFLICT (room_id) DO UPDATE SET mode = excluded.mode,
               updated_by = excluded.updated_by, updated_at = excluded.updated_at`
      ),

      // —— 申诉 ——
      appealByMsg: d.prepare('SELECT * FROM appeals WHERE room_id = ? AND seq = ? AND user_id = ?'),
      appealById: d.prepare('SELECT * FROM appeals WHERE id = ?'),
      insertAppeal: d.prepare(
        `INSERT INTO appeals (id, room_id, seq, user_id, reason, status, created_at)
         VALUES (?, ?, ?, ?, ?, 'open', ?)`
      ),
      reopenAppeal: d.prepare(
        `UPDATE appeals SET reason = ?, status = 'open', created_at = ?,
               decided_by = NULL, decided_at = 0, decision_note = ''
         WHERE id = ?`
      ),
      listAppeals: d.prepare(
        `SELECT * FROM appeals WHERE (? = '' OR room_id = ?)
         AND (? = '' OR status = ?) ORDER BY created_at DESC LIMIT ?`
      ),
      decideAppeal: d.prepare(
        `UPDATE appeals SET status = ?, decided_by = ?, decided_at = ?, decision_note = ?
         WHERE id = ? AND status = 'open'`
      ),

      // —— 操作日志 ——
      insertLog: d.prepare(
        `INSERT INTO admin_log (ts, actor_id, actor_name, room_id, action, target_type, target_id, detail)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
      ),
      logById: d.prepare('SELECT * FROM admin_log WHERE id = ?'),
      listLog: d.prepare(
        `SELECT * FROM admin_log WHERE (? = '' OR room_id = ?) AND id < ? ORDER BY id DESC LIMIT ?`
      ),

      // —— 审核队列 ——
      enqueueReview: d.prepare(
        `INSERT INTO review_queue (id, room_id, seq, source, status, created_at)
         VALUES (?, ?, ?, ?, 'open', ?)
         ON CONFLICT (room_id, seq) WHERE status = 'open' DO NOTHING`
      ),
      closeQueueByMsg: d.prepare(
        `UPDATE review_queue SET status = 'closed', closed_at = ?, closed_by = ?
         WHERE room_id = ? AND seq = ? AND status = 'open'`
      ),
      queueSelect: `
        SELECT q.id AS id, q.room_id AS roomId, q.seq AS seq, q.source AS source,
               q.created_at AS createdAt,
               m.sender_id AS "from", u.name AS fromName, m.content AS content,
               m.client_msg_id AS clientMsgId, m.ts AS ts,
               m.review_status AS reviewStatus, m.review_flags AS reviewFlags,
               m.review_reason AS reviewReason, m.review_rule_version AS reviewRuleVersion
          FROM review_queue q
          JOIN messages m ON m.room_id = q.room_id AND m.seq = q.seq
          JOIN users u ON u.id = m.sender_id
      `,

      // —— 审核事件 ——
      insertReviewEvent: d.prepare(
        `INSERT INTO review_events (room_id, seq, kind, flags, reason, actor, rule_version, ts)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
      ),
      // 断线补发：只回放「客户端已见水位之内」的事件（更高 seq 的当前状态已由消息快照覆盖）
      reviewEventsAfterLe: d.prepare(
        `SELECT id, room_id AS roomId, seq, kind, flags, reason, actor,
                rule_version AS ruleVersion, ts
           FROM review_events WHERE room_id = ? AND id > ? AND seq <= ? ORDER BY id LIMIT ?`
      ),
      reviewEventById: d.prepare(
        `SELECT id, room_id AS roomId, seq, kind, flags, reason, actor,
                rule_version AS ruleVersion, ts
           FROM review_events WHERE id = ?`
      ),
      upsertReviewCursor: d.prepare(
        `INSERT INTO review_cursors (room_id, user_id, last_event, updated_at) VALUES (?, ?, ?, ?)
         ON CONFLICT (room_id, user_id)
         DO UPDATE SET last_event = MAX(last_event, excluded.last_event), updated_at = excluded.updated_at`
      ),
      reviewCursor: d.prepare(
        'SELECT last_event AS lastEvent FROM review_cursors WHERE room_id = ? AND user_id = ?'
      ),
    };
    // 队列查询（开放/指定房间，附带消息快照）
    this.stmt.queueOpenAll = d.prepare(
      `${this.stmt.queueSelect} WHERE q.status = 'open' ORDER BY q.created_at, q.seq LIMIT ?`
    );
    this.stmt.queueOpenRoom = d.prepare(
      `${this.stmt.queueSelect} WHERE q.status = 'open' AND q.room_id = ? ORDER BY q.created_at, q.seq LIMIT ?`
    );
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

  /** 是否在任意房间担任管理员（全局审核队列/规则发布的权限依据） */
  isAdminAnywhere(userId) {
    return !!this.stmt.anyAdminRoom.get(userId, 'admin');
  }

  /** 设置禁言截止时间（0 表示解除禁言） */
  setMuted(roomId, userId, mutedUntil) {
    this.stmt.setMuted.run(mutedUntil, roomId, userId);
    return this.stmt.member.get(roomId, userId);
  }

  // ---------- 消息 ----------

  /**
   * 幂等写入消息。
   * 返回 { message, duplicate }：
   *  - duplicate=false：新消息，已分配 seq 并落库（调用方负责广播）；
   *  - duplicate=true ：同 clientMsgId 的消息已存在，直接返回原消息（调用方只回 ACK，不再广播）。
   */
  insertMessage({ roomId, clientMsgId, senderId, content }) {
    return this._tx(() => {
      const existing = this.stmt.msgByClientId.get(roomId, senderId, clientMsgId);
      if (existing) return { message: existing, duplicate: true };

      const { last_seq: seq } = this.stmt.bumpSeq.get(roomId);
      const ts = now();
      this.stmt.insertMsg.run(roomId, seq, clientMsgId, senderId, content, ts);
      const message = this.stmt.msgByClientId.get(roomId, senderId, clientMsgId);
      return { message, duplicate: false };
    });
  }

  /** 断线补发：取 seq > afterSeq 的消息（升序，最多 limit 条） */
  getMessagesAfter(roomId, afterSeq, limit) {
    return this.stmt.msgsAfter.all(roomId, afterSeq, limit);
  }

  /** 历史翻页：取 seq < beforeSeq 的消息，返回时按升序排列 */
  getMessagesBefore(roomId, beforeSeq, limit) {
    return this.stmt.msgsBefore.all(roomId, beforeSeq, limit).reverse();
  }

  getMessage(roomId, seq) {
    return this.stmt.msgBySeq.get(roomId, seq);
  }

  // ---------- 审核状态机 ----------
  // 所有迁移都带「期望当前态」条件，CAS 语义保证重复审核/并发决策幂等：
  // 命中 0 行说明状态已被其他决策推进，调用方应放弃并返回最新状态。

  /** released -> pending（先审后发暂存 / 队列留存）。返回 true 表示本次确实发生迁移 */
  holdMessage(roomId, seq, { flags = [], reason = null, ruleVersion = null, heldAt = now() } = {}) {
    const r = this.stmt.holdMsg.run(
      JSON.stringify(flags), reason, ruleVersion, heldAt, roomId, seq
    );
    return r.changes > 0;
  }

  /** 自动检测结果落印（不迁状态）；只允许印在 released，终态/暂存态忽略，避免晚到结果覆盖人工决策 */
  stampDetection(roomId, seq, { flags = [], reason = null, ruleVersion = null }) {
    const r = this.stmt.stampDetection.run(
      JSON.stringify(flags), reason, ruleVersion, roomId, seq
    );
    return r.changes > 0;
  }

  /** pre 模式检测命中：命中项补印到仍 pending 的消息（CAS：已被人工/TTL 决策则放弃） */
  updateHold(roomId, seq, { flags = [], reason = null, ruleVersion = null }) {
    const r = this.stmt.updateHold.run(
      JSON.stringify(flags), reason, ruleVersion, roomId, seq
    );
    return r.changes > 0;
  }

  /**
   * post（先发后撤）自动命中：stamp 风险 + released->recalled + 入人工队列 + 事件，同事务。
   * 消息已不处于 released（人工先撤回等竞态）时 changed=false，不覆盖人工决策。
   */
  flagAndRecall(roomId, seq, detection, { actor = 'detector', queueId = null } = {}) {
    return this._tx(() => {
      const before = this.getMessage(roomId, seq);
      if (!before || before.reviewStatus !== 'released') {
        return { changed: false, message: before, event: null };
      }
      this.stampDetection(roomId, seq, detection);
      const r = this.stmt.recallMsg.run(actor, now(), roomId, seq);
      if (r.changes === 0) return { changed: false, message: before, event: null };
      if (queueId) this.enqueueReview(queueId, roomId, seq, 'auto');
      const message = this.getMessage(roomId, seq);
      const event = this.addReviewEvent(roomId, seq, 'recalled', {
        flags: detection.flags, reason: detection.reason, actor, ruleVersion: detection.ruleVersion,
      });
      return { changed: true, message, event };
    });
  }

  /**
   * 审核决策原子应用：CAS 改状态 + 关闭 open 队列项 + 写 review_events，同事务。
   * transition: 'release' | 'block' | 'recall' | 'restore'，各自隐含期望源状态。
   * 返回 { changed, message, event }；changed=false 表示源状态不符（重复/竞态决策）。
   */
  applyDecision(roomId, seq, transition, { actor = '', flags = null, reason = null, ruleVersion = null } = {}) {
    const map = {
      release: { sql: this.stmt.releaseMsg, kind: 'released', from: 'pending' },
      block: { sql: this.stmt.blockMsg, kind: 'blocked', from: 'pending' },
      recall: { sql: this.stmt.recallMsg, kind: 'recalled', from: 'released' },
      restore: { sql: this.stmt.restoreMsg, kind: 'restored', from: ['recalled', 'blocked'] },
    };
    const spec = map[transition];
    if (!spec) throw new Error(`unknown transition: ${transition}`);
    return this._tx(() => {
      const before = this.getMessage(roomId, seq);
      const fromOk = Array.isArray(spec.from)
        ? spec.from.includes(before?.reviewStatus)
        : before?.reviewStatus === spec.from;
      if (!before || !fromOk) {
        return { changed: false, message: before, event: null };
      }
      const t = now();
      const r = spec.sql.run(actor, t, roomId, seq);
      if (r.changes === 0) return { changed: false, message: before, event: null };
      // 人工决策给出的原因写回消息（历史/补发/队列都能看到）
      if (reason != null) this.stmt.setReviewReason.run(String(reason).slice(0, 500), roomId, seq);
      // 任何终态决策（放行/拦截/撤回/恢复）都关闭该消息的 open 队列项；无 open 项时为 no-op
      this.stmt.closeQueueByMsg.run(t, actor, roomId, seq);
      const message = this.getMessage(roomId, seq);
      const event = this.addReviewEvent(roomId, seq, spec.kind, {
        flags: flags ?? JSON.parse(before.reviewFlags || '[]'),
        reason: reason ?? before.reviewReason,
        actor,
        ruleVersion: ruleVersion ?? before.reviewRuleVersion,
      });
      return { changed: true, message, event };
    });
  }

  /** held 事件由编排器在入暂存时写入（与入队同事务） */
  holdWithEvent(roomId, seq, detection, { queueId = null, source = 'auto' } = {}) {
    return this._tx(() => {
      const held = this.holdMessage(roomId, seq, detection);
      if (!held) return { changed: false, message: this.getMessage(roomId, seq), event: null };
      if (queueId) this.enqueueReview(queueId, roomId, seq, source);
      const message = this.getMessage(roomId, seq);
      const event = this.addReviewEvent(roomId, seq, 'held', {
        flags: detection.flags, reason: detection.reason, actor: 'detector',
        ruleVersion: detection.ruleVersion,
      });
      return { changed: true, message, event };
    });
  }

  /** flagged 事件（post/mark 模式检测命中但不暂存），幂等：不重复入队 */
  flagWithQueue(roomId, seq, detection, { queueId = null, enqueue = false } = {}) {
    return this._tx(() => {
      this.stampDetection(roomId, seq, detection);
      if (enqueue && queueId) this.enqueueReview(queueId, roomId, seq, 'auto');
      const message = this.getMessage(roomId, seq);
      const event = this.addReviewEvent(roomId, seq, 'flagged', {
        flags: detection.flags, reason: detection.reason, actor: 'detector',
        ruleVersion: detection.ruleVersion,
      });
      return { message, event };
    });
  }

  listPending(roomId, limit = 100) {
    return roomId
      ? this.stmt.pendingListRoom.all(roomId, limit)
      : this.stmt.pendingList.all(limit);
  }

  /** TTL 自动放行：取出 pending 且暂存时间早于 beforeTs 的消息（不要求房间，全局扫描） */
  listPendingOlderThan(beforeTs, limit = 200) {
    return this.stmt.pendingOlder.all(beforeTs, limit);
  }

  pendingCount(roomId = '') {
    return roomId
      ? this.stmt.pendingCountRoom.get('pending', roomId).n
      : this.stmt.pendingCountAll.get('pending').n;
  }

  // ---------- review event watermark ----------

  getReviewCursor(roomId, userId) {
    const row = this.stmt.reviewCursor.get(roomId, userId);
    return row ? row.lastEvent : 0;
  }

  saveReviewCursor(roomId, userId, eventId) {
    this.stmt.upsertReviewCursor.run(roomId, userId, eventId, now());
  }

  // ---------- 规则版本 ----------

  static parseRule(row) {
    if (!row) return null;
    return {
      version: row.version,
      words: JSON.parse(row.words),
      freqWindowMs: row.freq_window_ms,
      freqMaxCount: row.freq_max_count,
      note: row.note,
      createdBy: row.created_by,
      createdAt: row.created_at,
      active: !!row.active,
    };
  }

  /** 发布新版本：同事务内旧版本置 active=0（历史消息的 review_rule_version 不受影响） */
  createRuleVersion({ words, freqWindowMs, freqMaxCount, note = '', createdBy = null }) {
    return this._tx(() => {
      this.stmt.deactivateRules.run();
      const r = this.stmt.insertRule.run(
        JSON.stringify(words), freqWindowMs, freqMaxCount, note, createdBy, now()
      );
      return ChatDB.parseRule(this.stmt.ruleByVersion.get(Number(r.lastInsertRowid)));
    });
  }

  getRuleVersion(version) {
    return ChatDB.parseRule(this.stmt.ruleByVersion.get(version));
  }

  getActiveRule() {
    return ChatDB.parseRule(this.stmt.activeRule.get());
  }

  listRuleVersions(limit = 50) {
    return this.stmt.listRules.all(limit).map(ChatDB.parseRule);
  }

  // ---------- 房间处置策略 ----------

  getRoomPolicy(roomId) {
    const row = this.stmt.getPolicy.get(roomId);
    return row ? { roomId: row.room_id, mode: row.mode, updatedBy: row.updated_by, updatedAt: row.updated_at } : null;
  }

  setRoomPolicy(roomId, mode, updatedBy) {
    this.stmt.upsertPolicy.run(roomId, mode, updatedBy, now());
    return this.getRoomPolicy(roomId);
  }

  // ---------- 申诉 ----------

  getAppeal(roomId, seq, userId) {
    const row = this.stmt.appealByMsg.get(roomId, seq, userId);
    return row ? this._appealOut(row) : null;
  }

  getAppealById(id) {
    const row = this.stmt.appealById.get(id);
    return row ? this._appealOut(row) : null;
  }

  _appealOut(row) {
    return {
      id: row.id, roomId: row.room_id, seq: row.seq, userId: row.user_id,
      reason: row.reason, status: row.status, createdAt: row.created_at,
      decidedBy: row.decided_by, decidedAt: row.decided_at, decisionNote: row.decision_note,
    };
  }

  /**
   * 提交申诉：首次插入；已存在且处于终态（rejected/closed）时重新打开（幂等键不变）。
   * pending 中的消息、处于 open 的申诉返回 null（由调用方拒绝）。
   */
  submitAppeal({ id, roomId, seq, userId, reason, cooldownMs = 0 }) {
    return this._tx(() => {
      const existing = this.stmt.appealByMsg.get(roomId, seq, userId);
      const t = now();
      if (!existing) {
        this.stmt.insertAppeal.run(id, roomId, seq, userId, reason, t);
        return this._appealOut(this.stmt.appealById.get(id));
      }
      if (existing.status === 'open') return null;
      if (cooldownMs > 0 && t - Math.max(existing.created_at, existing.decided_at) < cooldownMs) {
        return { cooldown: true };
      }
      this.stmt.reopenAppeal.run(reason, t, existing.id);
      return this._appealOut(this.stmt.appealById.get(existing.id));
    });
  }

  listAppeals({ roomId = '', status = '', limit = 100 } = {}) {
    return this.stmt.listAppeals.all(roomId, roomId, status, status, limit).map((r) => this._appealOut(r));
  }

  decideAppeal(id, status, adminId, note = '') {
    const r = this.stmt.decideAppeal.run(status, adminId, now(), note, id);
    return r.changes > 0 ? this.getAppealById(id) : null;
  }

  // ---------- 管理员操作日志 ----------

  addAdminLog({ actorId, actorName = '', roomId = null, action, targetType = '', targetId = '', detail = {} }) {
    const r = this.stmt.insertLog.run(
      now(), actorId, actorName, roomId, action, targetType, targetId, JSON.stringify(detail)
    );
    const row = this.stmt.logById.get(Number(r.lastInsertRowid));
    return {
      id: row.id, ts: row.ts, actorId: row.actor_id, actorName: row.actor_name,
      roomId: row.room_id, action: row.action, targetType: row.target_type,
      targetId: row.target_id, detail: JSON.parse(row.detail),
    };
  }

  listAdminLog({ roomId = '', beforeId = Number.MAX_SAFE_INTEGER, limit = 100 } = {}) {
    return this.stmt.listLog.all(roomId, roomId, beforeId, limit).map((row) => ({
      id: row.id, ts: row.ts, actorId: row.actor_id, actorName: row.actor_name,
      roomId: row.room_id, action: row.action, targetType: row.target_type,
      targetId: row.target_id, detail: JSON.parse(row.detail),
    }));
  }

  // ---------- 审核队列 ----------

  _queueOut(row) {
    return {
      id: row.id, roomId: row.roomId, seq: row.seq, source: row.source, createdAt: row.createdAt,
      from: row.from, fromName: row.fromName, content: row.content, clientMsgId: row.clientMsgId, ts: row.ts,
      reviewStatus: row.reviewStatus,
      reviewFlags: JSON.parse(row.reviewFlags || '[]'),
      reviewReason: row.reviewReason, reviewRuleVersion: row.reviewRuleVersion,
    };
  }

  /** 入队（幂等：同消息已有 open 条目则忽略），返回是否新入队 */
  enqueueReview(id, roomId, seq, source = 'auto', createdAt = now()) {
    const r = this.stmt.enqueueReview.run(id, roomId, seq, source, createdAt);
    return r.changes > 0;
  }

  closeQueueForMessage(roomId, seq, closedBy = null) {
    this.stmt.closeQueueByMsg.run(now(), closedBy, roomId, seq);
  }

  listQueue({ roomId = '', limit = 100 } = {}) {
    const rows = roomId
      ? this.stmt.queueOpenRoom.all(roomId, limit)
      : this.stmt.queueOpenAll.all(limit);
    return rows.map((r) => this._queueOut(r));
  }

  // ---------- 审核事件（状态迁移 + 事件原子写入） ----------

  addReviewEvent(roomId, seq, kind, { flags = [], reason = null, actor = '', ruleVersion = null } = {}) {
    const r = this.stmt.insertReviewEvent.run(
      roomId, seq, kind, JSON.stringify(flags), reason, actor, ruleVersion, now()
    );
    const row = this.stmt.reviewEventById.get(Number(r.lastInsertRowid));
    return {
      id: row.id, roomId: row.roomId, seq: row.seq, kind: row.kind,
      flags: JSON.parse(row.flags), reason: row.reason, actor: row.actor,
      ruleVersion: row.ruleVersion, ts: row.ts,
    };
  }

  listReviewEventsAfterLe(roomId, afterId, maxSeq, limit = 500) {
    return this.stmt.reviewEventsAfterLe.all(roomId, afterId, maxSeq, limit).map((row) => ({
      id: row.id, roomId: row.roomId, seq: row.seq, kind: row.kind,
      flags: JSON.parse(row.flags), reason: row.reason, actor: row.actor,
      ruleVersion: row.ruleVersion, ts: row.ts,
    }));
  }

  // ---------- 游标 ----------

  saveCursor(roomId, userId, lastAckSeq) {
    this.stmt.upsertCursor.run(roomId, userId, lastAckSeq, now());
  }

  getCursor(roomId, userId) {
    const row = this.stmt.cursor.get(roomId, userId);
    return row ? row.lastAckSeq : 0;
  }

  close() {
    try { this.db.exec('PRAGMA wal_checkpoint(TRUNCATE)'); } catch { /* 内存库无 WAL */ }
    this.db.close();
  }
}

module.exports = { ChatDB };
