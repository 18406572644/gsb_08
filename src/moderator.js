'use strict';

const { now, randomId } = require('./util');

/**
 * 审核编排器：检测 → 状态机 → 广播联动 的唯一入口。
 *
 * 三种处置模式（每房间策略，缺省取 config.reviewDefaultMode）：
 * - pre  先审后发：落库即置 pending 并广播无内容占位帧；检测命中则留队列等人工，
 *                  无命中/检测超时（fail-open）立即放行并广播正文。
 * - post 先发后撤：消息先按原链路广播；检测命中则 recalled 并广播撤回，入人工队列。
 * - mark 仅标记  ：正常广播，命中只打风险标 + 入队列，不改变可见性。
 *
 * 顺序/ACK/补发不被破坏的关键约定：
 * 1. ACK 仍在落库后立即回给发送方（pre 模式下也如此 —— 消息确实已持久化）；
 * 2. seq 仍在 insert 事务内分配，永不复用、永不空洞；每个 seq 在任何时刻都对应
 *    一帧（正文 / pending 占位 / blocked 占位 / recalled 占位），客户端 lastSeenSeq 单调；
 * 3. 所有检测走异步，主链路不 await；检测超时/异常 fail-open，只留痕不阻塞；
 * 4. 状态迁移全部 CAS（带期望源状态）+ 同事务写 review_events/关队列，重复决策与
 *    竞态决策（消息已被撤回/人工先判）安全幂等；
 * 5. 断线补发：消息按当前状态转换为对应帧；对客户端水位之前的 seq，另按
 *    review_events 增量补发撤回/恢复/标记指令，客户端幂等应用。
 */

const VALID_MODES = new Set(['pre', 'post', 'mark']);

class Moderator {
  constructor({ db, hub, detector, config, log = console }) {
    this.db = db;
    this.hub = hub;
    this.detector = detector;
    this.config = config;
    this.log = log;
    this.inflight = new Set(); // `${roomId}:${seq}`，防止同消息重复调度检测
    this.ensureRule();
  }

  // ---------------------------------------------------------------- 规则版本

  /** 首次启动播种 v1：沿用检测服务当前词表/窗口（可能由配置/测试注入），之后以库里的生效版本为准 */
  ensureRule() {
    let rule = this.db.getActiveRule();
    if (!rule) {
      const seed = this.detector.getRules ? this.detector.getRules() : null;
      rule = this.db.createRuleVersion({
        words: seed?.words || [],
        freqWindowMs: seed?.freqWindowMs || this.config.reviewFreqWindowMs,
        freqMaxCount: seed?.freqMaxCount || this.config.reviewFreqMaxCount,
        note: '系统默认规则（初始版本）',
        createdBy: 'system',
      });
      this.detector.setRules(rule);
    } else {
      this.detector.setRules(rule);
    }
    return rule;
  }

  activeRule() {
    return this.db.getActiveRule() || this.ensureRule();
  }

  listRuleVersions(limit = 50) {
    return this.db.listRuleVersions(limit);
  }

  /** 发布新版本：旧版本同事务失活，检测服务热更新；历史消息保留命中时的版本号 */
  publishRule({ words, freqWindowMs, freqMaxCount, note = '' }, admin) {
    const rule = this.db.createRuleVersion({
      words, freqWindowMs, freqMaxCount, note, createdBy: admin?.id || 'system',
    });
    this.detector.setRules(rule);
    const entry = this.db.addAdminLog({
      actorId: admin?.id || 'system', actorName: admin?.name || 'system',
      action: 'rule_publish', targetType: 'rule', targetId: String(rule.version),
      detail: { version: rule.version, words, freqWindowMs, freqMaxCount, note },
    });
    this._broadcastAll({ type: 'review_rule', rule, logId: entry.id });
    return rule;
  }

  // ---------------------------------------------------------------- 处置策略

  modeFor(roomId) {
    const p = this.db.getRoomPolicy(roomId);
    const mode = p?.mode || this.config.reviewDefaultMode;
    return VALID_MODES.has(mode) ? mode : 'post';
  }

  setMode(roomId, mode, admin) {
    if (!VALID_MODES.has(mode)) {
      const e = new Error('invalid mode'); e.code = 'BAD_REQUEST'; throw e;
    }
    const policy = this.db.setRoomPolicy(roomId, mode, admin?.id || null);
    this.db.addAdminLog({
      actorId: admin?.id || 'system', actorName: admin?.name || '', roomId,
      action: 'policy_set', targetType: 'policy', targetId: roomId, detail: { mode },
    });
    this.hub.broadcast(roomId, { type: 'review_policy', roomId, mode: policy.mode });
    return policy;
  }

  // ---------------------------------------------------------------- 主入口

  /**
   * 消息落库后调用（同步部分 + 异步检测分离）。
   * pre 模式：同步暂存并广播占位帧，异步出结果后放行/留队列；
   * post/mark：什么都不阻塞，异步出结果后撤回/标记。
   * 返回该消息应立即广播的帧：pre -> null（暂不广播正文），其他 -> 正常正文帧由调用方广播。
   */
  ingest(message) {
    const mode = this.modeFor(message.roomId);
    const rule = this.activeRule();

    if (mode === 'pre') {
      // 同步：released -> pending + 入队 + held 事件（CAS，重复调度/竞态安全）
      const res = this.db.holdWithEvent(
        message.roomId, message.seq,
        { flags: [], reason: null, ruleVersion: rule.version },
        { queueId: randomId('rq_') }
      );
      if (res.changed) {
        // 占位帧占据 seq（seq 通道，保证序号连续）；后续放行/拦截指令走独立的 review 通道
        this.hub.broadcast(
          message.roomId, this._frameFor(res.message, { includeContent: false }),
          { track: true, seq: message.seq }
        );
      }
      this._scheduleDetection(message, 'pre', res.message || message);
      return null;
    }

    this._scheduleDetection(message, mode, message);
    return message;
  }

  _scheduleDetection(message, mode, current) {
    const key = `${message.roomId}:${message.seq}`;
    if (this.inflight.has(key)) return; // 重复审核：同消息检测只允许一个在途
    this.inflight.add(key);
    // 异步、不 await：检测慢/故障都不影响 ACK 与（post/mark 的）广播
    this.detector
      .detect({ roomId: message.roomId, senderId: message.from, content: message.content })
      .then((d) => this._onDetection(message, mode, d))
      .catch((err) => this._onDetection(message, mode, { hit: false, flags: [], degraded: true, reason: err?.message }))
      .finally(() => this.inflight.delete(key));
  }

  _onDetection(message, mode, d) {
    const { roomId, seq } = message;
    const rule = this.activeRule();
    const detection = { flags: d.flags || [], reason: d.reason, ruleVersion: rule.version };
    const m = this.db.getMessage(roomId, seq);

    // 异常链路：消息已被人工撤回/拦截/放行，或已删除 —— 晚到的检测结果一律丢弃
    if (!m) return;

    if (mode === 'pre') {
      if (m.reviewStatus !== 'pending') return; // 人工/TTL 已先行决策
      if (d.degraded) {
        this.db.addAdminLog({
          actorId: 'system:detector', actorName: 'detector', roomId,
          action: 'detect_degraded', targetType: 'message', targetId: `${roomId}:${seq}`,
          detail: { reason: d.reason, fallback: 'release' },
        });
        this._release(roomId, seq, { actor: 'system:detector', flags: ['detector:degraded'], reason: d.reason });
        return;
      }
      if (!d.hit) {
        this._release(roomId, seq, { actor: 'detector' });
        return;
      }
      // 命中：留在 pending + 更新命中信息，等待人工（队列已在 hold 时入好）
      this.db.updateHold(roomId, seq, detection);
      // 仅刷新占位帧的命中说明（非状态迁移，不走可靠通道；丢失后补发会带上最新 flags）
      this.hub.broadcast(roomId, {
        type: 'msg_review', roomId, seq, status: 'pending',
        flags: detection.flags, reason: detection.reason, ruleVersion: rule.version, ts: now(),
      });
      return;
    }

    // post / mark：此时消息必须仍 released，否则说明已被人工撤回（晚到结果不覆盖人工决策）
    if (m.reviewStatus !== 'released') return;

    if (d.degraded) {
      this.db.addAdminLog({
        actorId: 'system:detector', actorName: 'detector', roomId,
        action: 'detect_degraded', targetType: 'message', targetId: `${roomId}:${seq}`,
        detail: { reason: d.reason, fallback: 'pass' },
      });
      return;
    }
    if (!d.hit) return;

    if (mode === 'post') {
      // 先发后撤：recall + 入人工队列（管理员可恢复）
      const res = this.db.flagAndRecall(roomId, seq, detection, {
        actor: 'detector', queueId: randomId('rq_'),
      });
      if (res.changed) this.hub.broadcastReview(roomId, this._decisionFrame(res), res.event.id, { replaceSeq: seq });
      return;
    }

    // mark：仅标记风险，可见性不变，入人工队列
    const res = this.db.flagWithQueue(roomId, seq, detection, {
      queueId: randomId('rq_'), enqueue: true,
    });
    const flagFrame = {
      type: 'msg_flagged', roomId, seq,
      flags: detection.flags, reason: detection.reason, ruleVersion: rule.version,
      ts: now(), reviewEventId: res.event.id,
    };
    // 标记不改变 seq 帧（仍是正文），不替换 seq 通道缓存
    this.hub.broadcastReview(roomId, flagFrame, res.event.id);
    return res;
  }

  // ---------------------------------------------------------------- 人工决策

  /**
   * 管理员队列决策。action: release / block / recall / restore / dismiss
   * - pending  → release | block
   * - released → recall | dismiss（mark 队列放行）
   * - recalled/blocked → restore
   * CAS 失败（重复提交、他人已判）返回 {changed:false, status}，不重复广播、不重复记日志。
   */
  decide({ roomId, seq, action, admin, reason = null }) {
    const m = this.db.getMessage(roomId, seq);
    if (!m) return { changed: false, status: null, error: 'NO_SUCH_MESSAGE' };

    const allowed = {
      release: ['pending'],
      block: ['pending'],
      recall: ['released'],
      restore: ['recalled', 'blocked'],
      // dismiss 只用于「保留消息但移出队列」（mark 模式的 released 消息）；
      // pending 必须给出明确的 release/block 结论，避免留下无人处理的暂存
      dismiss: ['released'],
    }[action];
    if (!allowed) return { changed: false, status: m.reviewStatus, error: 'BAD_ACTION' };
    if (!allowed.includes(m.reviewStatus)) {
      // 幂等：目标态与现状一致即视为成功（例如对已撤回消息再撤一次），但不广播不记日志
      return { changed: false, status: m.reviewStatus, idempotent: true };
    }

    if (action === 'dismiss') {
      this.db.closeQueueForMessage(roomId, seq, admin.id);
      this.db.addAdminLog({
        actorId: admin.id, actorName: admin.name, roomId,
        action: 'review_dismiss', targetType: 'message', targetId: `${roomId}:${seq}`,
        detail: { seq, reason },
      });
      return { changed: true, status: m.reviewStatus, dismissed: true };
    }

    const transition = { release: 'release', block: 'block', recall: 'recall', restore: 'restore' }[action];
    const res = this.db.applyDecision(roomId, seq, transition, { actor: admin.id, reason });
    if (!res.changed) return { changed: false, status: m.reviewStatus, idempotent: true };

    this.db.addAdminLog({
      actorId: admin.id, actorName: admin.name, roomId,
      action: `review_${action}`, targetType: 'message', targetId: `${roomId}:${seq}`,
      detail: { seq, from: m.reviewStatus, reason, flags: JSON.parse(m.reviewFlags || '[]') },
    });
    this.hub.broadcastReview(
      roomId, this._decisionFrame(res, { reason: reason ?? res.event.reason }), res.event.id,
      { replaceSeq: seq }
    );
    return { changed: true, status: res.message.reviewStatus, message: res.message };
  }

  /** 管理员手动把已发布消息加入审核队列（mark 模式/巡检场景） */
  enqueueManual(roomId, seq, admin) {
    const m = this.db.getMessage(roomId, seq);
    if (!m) return false;
    const added = this.db.enqueueReview(randomId('rq_'), roomId, seq, 'manual');
    if (added) {
      this.db.addAdminLog({
        actorId: admin.id, actorName: admin.name, roomId,
        action: 'review_enqueue', targetType: 'message', targetId: `${roomId}:${seq}`, detail: { seq },
      });
    }
    return added;
  }

  /** 用当前最新规则重新检测（误判复核辅助）；只返回结果，不自动改状态 */
  async recheck(roomId, seq) {
    const m = this.db.getMessage(roomId, seq);
    if (!m) return null;
    const rule = this.activeRule();
    const d = await this.detector.detect({ roomId, senderId: m.from, content: m.content });
    return {
      roomId, seq, status: m.reviewStatus,
      hit: d.hit, flags: d.flags, reason: d.reason, degraded: !!d.degraded,
      ruleVersion: rule.version,
      storedFlags: JSON.parse(m.reviewFlags || '[]'),
      storedRuleVersion: m.reviewRuleVersion,
    };
  }

  // ---------------------------------------------------------------- 申诉

  /** 发送者对 recalled/blocked 消息申诉；pending 不可申诉 */
  submitAppeal({ roomId, seq, userId, reason }) {
    const m = this.db.getMessage(roomId, seq);
    if (!m) return { error: 'NO_SUCH_MESSAGE' };
    if (m.from !== userId) return { error: 'FORBIDDEN', message: '只能对自己发送的消息申诉' };
    if (m.reviewStatus === 'pending') return { error: 'PENDING', message: '消息仍在审核中，暂不可申诉' };
    if (m.reviewStatus === 'released' && !JSON.parse(m.reviewFlags || '[]').length) {
      return { error: 'NOT_FLAGGED', message: '消息未被处置，无需申诉' };
    }
    const res = this.db.submitAppeal({
      id: randomId('ap_'), roomId, seq, userId,
      reason: String(reason || '').slice(0, 500),
      cooldownMs: this.config.reviewAppealCooldownMs,
    });
    if (!res) return { error: 'APPEAL_OPEN', message: '已有进行中的申诉' };
    if (res.cooldown) return { error: 'APPEAL_COOLDOWN', message: '申诉过于频繁，请稍后再试' };
    this.db.addAdminLog({
      actorId: userId, actorName: '', roomId,
      action: 'appeal_submit', targetType: 'appeal', targetId: res.id,
      detail: { seq, reason: res.reason },
    });
    this.hub.broadcast(roomId, { type: 'appeal_update', appeal: res });
    return { appeal: res };
  }

  /** 管理员裁决申诉：approved 联动恢复消息（误判纠正），rejected 维持处置 */
  decideAppeal({ appealId, decision, admin, note = '' }) {
    const appeal = this.db.getAppealById(appealId);
    if (!appeal) return { error: 'NO_SUCH_APPEAL' };
    if (!['approved', 'rejected'].includes(decision)) return { error: 'BAD_REQUEST' };

    const updated = this.db.decideAppeal(
      appealId, decision === 'approved' ? 'approved' : 'rejected', admin.id, note
    );
    if (!updated) return { error: 'NOT_OPEN', idempotent: true };

    let message = null;
    if (decision === 'approved') {
      const m = this.db.getMessage(appeal.roomId, appeal.seq);
      if (m && (m.reviewStatus === 'recalled' || m.reviewStatus === 'blocked')) {
        const res = this.db.applyDecision(appeal.roomId, appeal.seq, 'restore', {
          actor: admin.id, reason: `申诉通过：${note || '误判恢复'}`,
        });
        if (res.changed) {
          message = res.message;
          this.hub.broadcastReview(appeal.roomId, this._decisionFrame(res), res.event.id,
            { replaceSeq: appeal.seq });
        }
      }
    }

    this.db.addAdminLog({
      actorId: admin.id, actorName: admin.name, roomId: appeal.roomId,
      action: 'appeal_decide', targetType: 'appeal', targetId: appealId,
      detail: { decision, note, seq: appeal.seq },
    });
    this.hub.broadcast(appeal.roomId, { type: 'appeal_update', appeal: updated });
    return { appeal: updated, message };
  }

  // ---------------------------------------------------------------- TTL 自动放行

  /** 先审后发兜底：暂存超时未判的消息自动放行（fail-open），可由定时器周期调用 */
  sweepPending() {
    if (!this.config.reviewPendingTtlMs) return 0;
    const cutoff = now() - this.config.reviewPendingTtlMs;
    const stale = this.db.listPendingOlderThan(cutoff, 200);
    for (const m of stale) {
      this._release(m.roomId, m.seq, {
        actor: 'system:ttl', reason: `审核超时 ${this.config.reviewPendingTtlMs}ms 自动放行`,
      });
    }
    return stale.length;
  }

  /** 构造决策帧并盖上 reviewEventId（客户端据此幂等应用与 ACK） */
  _decisionFrame(res, opts = {}) {
    const frame = this._frameFor(res.message, opts);
    frame.reviewEventId = res.event.id;
    return frame;
  }

  _release(roomId, seq, opts) {
    const res = this.db.applyDecision(roomId, seq, 'release', opts);
    if (res.changed) {
      this.hub.broadcastReview(roomId, this._decisionFrame(res, opts), res.event.id, { replaceSeq: seq });
    }
    return res;
  }

  // ---------------------------------------------------------------- 补发支持

  /** 历史/补发消息行 → 下发帧：任何状态的 seq 都有对应帧，保证序号连续语义 */
  _frameFor(m, { includeContent = true, reason } = {}) {
    const flags = JSON.parse(m.reviewFlags || '[]');
    const base = {
      roomId: m.roomId, seq: m.seq, from: m.from, fromName: m.fromName,
      clientMsgId: m.clientMsgId, ts: m.ts,
      flags, ruleVersion: m.reviewRuleVersion,
      reason: reason ?? m.reviewReason,
    };
    switch (m.reviewStatus) {
      case 'pending':
      case 'blocked':
        return { ...base, type: 'msg_review', status: m.reviewStatus };
      case 'recalled':
        return { ...base, type: 'msg_recalled' };
      default: {
        // released（含申诉恢复）：pre 放行/恢复需要客户端替换占位帧
        const type = includeContent === false ? 'msg_review' : 'msg';
        return {
          type, status: 'released',
          roomId: m.roomId, seq: m.seq, clientMsgId: m.clientMsgId,
          from: m.from, fromName: m.fromName, content: m.content, ts: m.ts,
          reviewFlags: flags, reviewReason: m.reviewReason, reviewRuleVersion: m.reviewRuleVersion,
        };
      }
    }
  }

  /** 补发场景按观看者转换（发送者可看到 blocked 原因，其他成员看到通用占位） */
  frameForReplay(m, viewerId) {
    const frame = this._frameFor(m);
    if (m.reviewStatus === 'blocked' && m.from !== viewerId) {
      return { ...frame, reason: '该消息未通过审核' };
    }
    return frame;
  }

  /**
   * 断线期间的审核指令增量补发：
   * - seq <= seenSeq：客户端已按旧状态上屏，需要撤回/恢复/标记指令纠偏 → 发送；
   * - seenSeq < seq <= coveredSeq：本批消息快照已按当前状态补发，事件不再重发（水位照推进）。
   * 另过滤与当前状态不一致的过时事件。客户端对重复指令幂等。
   * 返回 lastId（可安全推进到的事件水位）。
   */
  replayEvents(conn, roomId, afterEventId, seenSeq, coveredSeq = seenSeq, limit = 500) {
    const events = this.db.listReviewEventsAfterLe(roomId, afterEventId, coveredSeq, limit);
    let lastId = afterEventId;
    for (const ev of events) {
      const m = this.db.getMessage(roomId, ev.seq);
      if (!m) continue;
      if (!this._eventConsistent(ev.kind, m.reviewStatus, ev.flags)) continue;
      if (ev.seq <= seenSeq) {
        const frame = this._eventFrame(ev, m);
        if (frame) this.hub.send(conn, frame, { trackReview: true, roomId, reviewId: ev.id });
      }
      lastId = ev.id;
    }
    return { lastId, count: events.length, hasMore: events.length === limit };
  }

  _eventConsistent(kind, status, eventFlags) {
    switch (kind) {
      case 'held': return status === 'pending';
      case 'released':
      case 'restored':
      case 'flagged':
        return status === 'released';
      case 'blocked': return status === 'blocked';
      case 'recalled': return status === 'recalled';
      default: return false;
    }
  }

  _eventFrame(ev, m) {
    const common = {
      roomId: ev.roomId, seq: ev.seq, flags: ev.flags, reason: ev.reason,
      ruleVersion: ev.ruleVersion, reviewEventId: ev.id, ts: ev.ts,
    };
    switch (ev.kind) {
      case 'held':
      case 'blocked':
        return { ...common, type: 'msg_review', status: ev.kind === 'held' ? 'pending' : 'blocked' };
      case 'recalled':
        return { ...common, type: 'msg_recalled' };
      case 'released':
      case 'restored':
        return { ...this._frameFor(m), reviewEventId: ev.id };
      case 'flagged':
        return { ...common, type: 'msg_flagged' };
      default:
        return null;
    }
  }

  _broadcastAll(frame) {
    const str = JSON.stringify(frame);
    for (const conn of this.hub.all) {
      try { conn.ws.readyState === 1 && conn.ws.send(str); } catch { /* 忽略单连接失败 */ }
    }
  }
}

module.exports = { Moderator, VALID_MODES };
