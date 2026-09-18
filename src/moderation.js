'use strict';

/**
 * 消息审核与违规处理工作流。
 *
 * 三种处置模式（+ off 关闭）：
 *  - pre  先审后发：消息先落库为 held 不广播，通过/超时后放行（广播），命中违规转人工，
 *                   人工驳回则拒绝（占位通知），不通过不广播。
 *  - post 先发后撤：消息照常广播，检测确认为违规后广播 recall 撤回，可疑转人工。
 *  - flag 仅标记风险：消息照常广播，命中/可疑仅附 mod 标记，不阻断、不撤回。
 *  - off  关闭：不检测、不加标记，行为与未接入审核完全一致。
 *
 * 与可靠投递的兼容性（关键）：
 *  1. 每条消息仍在写事务内分配房间内单调 seq —— seq 全序不被破坏；
 *  2. pre 模式下 held 消息不广播，但 seq 已占用；通过门控（minHeldSeq 水位线）保证
 *     后续消息不会越过 held 先显示，客户端看到的仍是连续、按序的可见序列；
 *  3. ACK 语义不变：ACK 表示「服务端已收并分配 seq」，不代表对他人可见；pre 放行与
 *     post 撤回都通过广播 recall / mod_update 独立通知；
 *  4. held 消息不进断线补发（门控），放行后由补发或实时广播送达；recalled 消息同样
 *     从补发中过滤 —— 新成员/重连者永远看不到已撤回内容；
 *  5. 检测超时 fail-open（放行），避免审核服务故障卡死消息流；检测与超时兜底用
 *     compare-and-set 抢占，重复检测/重复回调只有一方生效（幂等审核）。
 */

const { now } = require('./util');

/** 默认规则（房间从未发布规则时使用；mode=off 即完全旁路） */
function defaultRule(config) {
  return {
    mode: config.modDefaultMode,
    sensitiveWords: [...config.modSensitiveWords],
    freqWindowMs: config.modFreqWindowMs,
    freqMaxCount: config.modFreqMaxCount,
    detectTimeoutMs: config.modDetectTimeoutMs,
  };
}

class ModerationService {
  /**
   * @param {object} deps
   * @param {import('./db').ChatDB} deps.db
   * @param {import('./hub').Hub} deps.hub
   * @param {object} deps.config
   * @param {(fn: Function, ms: number) => NodeJS.Timeout} [deps.setTimer] 注入定时器（测试）
   */
  constructor({ db, hub, config, setTimer = (fn, ms) => setTimeout(fn, ms), buildMsgFrame = null }) {
    this.db = db;
    this.hub = hub;
    this.config = config;
    this.setTimer = setTimer;
    this.buildMsgFrame = buildMsgFrame; // server 注入，保持 msg 帧字段单一来源
    // 频率异常检测的滑窗计数：userId -> [{seq, ts}]（仅保留窗口内）
    this.freq = new Map();
    // 进行中的检测回调（key = roomId:seq -> 标记位），防止重复审核
    this.inflight = new Set();
    // pre 模式放行队列：roomId -> [seq,...] 按序等待门控放行
    this.releaseQueue = new Map();
    // 检测后端注入点（测试用）：map roomId|'*' -> async (text, rule) => {status,hits,ms}
    this.detectors = new Map();
    // 在途超时定时器（停止时清理，避免 DB 关闭后回调触碰已终结语句）
    this.timers = new Set();
    this.stopped = false;
  }

  /** 生命周期清理：清掉所有检测超时定时器，之后的晚到回调一律忽略 */
  shutdown() {
    this.stopped = true;
    for (const t of this.timers) clearTimeout(t);
    this.timers.clear();
    this.inflight.clear();
  }

  /**
   * 崩溃恢复：进程重启后重建 pre 门控放行队列，并处理检测未完成的 held 消息。
   *  - detect 仍 pending / 无审核记录：检测在途时崩溃 -> 重新检测；
   *  - 已判违规/可疑但无人工终判：仍在等人工 -> 保持 held，重新通知管理员队列；
   *  - 已 clean/timeout/error 却仍 held（放行广播在途崩溃）-> fail-open 放行。
   */
  recoverPending() {
    const held = this.db.listHeldMessages();
    const byRoom = new Map();
    for (const m of held) {
      if (!byRoom.has(m.roomId)) byRoom.set(m.roomId, []);
      byRoom.get(m.roomId).push(m);
    }
    for (const [roomId, list] of byRoom) {
      const rule = this.getRule(roomId);
      // 重建放行水位线队列（按 seq 保序）
      for (const m of list) this.trackHeld(roomId, m.seq);
      for (const m of list) {
        const review = this.db.getReview(roomId, m.seq);
        if (!review || review.detectStatus === 'pending') {
          this.db.ensureReview(roomId, m.seq, rule.version, 'pending', null);
          this._runDetection(m, rule);
        } else if (review.detectStatus === 'violation' || review.detectStatus === 'suspect') {
          if (!review.decision) {
            this._notifyAdmins(roomId, {
              type: 'mod_queue_update', roomId, item: this._reviewView(review),
            });
          }
        } else {
          // clean / timeout / error：补放行
          this.db.setMessageModeration(roomId, m.seq, 'visible', null, rule.version);
        }
      }
      this._drainHeld(roomId);
    }
    return held.length;
  }

  // ---------------------------------------------------------------- 规则

  /** 取房间当前生效规则；无已发布版本则回落到全局默认规则 */
  getRule(roomId) {
    const row = this.db.getPublishedRule(roomId);
    if (row) {
      const cfg = JSON.parse(row.config);
      return { ...defaultRule(this.config), ...cfg, version: row.version };
    }
    return { ...defaultRule(this.config), version: 0 };
  }

  enabled(roomId) {
    return this.config.moderationEnabled && this.getRule(roomId).mode !== 'off';
  }

  /** 发布新版本规则（草稿事务内创建 + 发布），并通知房间内管理员 */
  publishRule(roomId, actorId, patch) {
    const rule = { ...this.getRule(roomId), ...this._sanitizeRulePatch(patch) };
    const version = this.db.createRuleDraft(roomId, rule, actorId);
    const published = this.db.publishRuleVersion(roomId, version.version);
    this.db.addAudit(actorId, 'publish_rule', { version: published.version, config: rule }, roomId);
    this._notifyAdmins(roomId, {
      type: 'mod_rule_changed',
      roomId,
      version: published.version,
      rule: this._publicRule(rule),
      by: actorId,
    });
    return { row: published, rule };
  }

  /** 回滚到历史版本（以历史版本内容再发一个新版本，版本号只增，保留完整演进链） */
  rollbackRule(roomId, actorId, targetVersion) {
    const target = this.db.getRuleByVersion(roomId, targetVersion);
    if (!target) return null;
    const cfg = JSON.parse(target.config);
    const draft = this.db.createRuleDraft(roomId, cfg, actorId);
    const published = this.db.publishRuleVersion(roomId, draft.version);
    this.db.addAudit(
      actorId, 'rollback_rule',
      { fromVersion: targetVersion, newVersion: published.version, config: cfg },
      roomId
    );
    this._notifyAdmins(roomId, {
      type: 'mod_rule_changed', roomId, version: published.version,
      rule: this._publicRule(cfg), rolledBackFrom: targetVersion, by: actorId,
    });
    return { row: published, rule: cfg };
  }

  listRules(roomId) {
    return this.db.listRuleVersions(roomId, this.config.modQueueLimit).map((r) => ({
      version: r.version,
      config: JSON.parse(r.config),
      published: !!r.published,
      createdBy: r.created_by,
      createdAt: r.created_at,
      publishedAt: r.published_at,
    }));
  }

  _sanitizeRulePatch(patch = {}) {
    const out = {};
    if (['pre', 'post', 'flag', 'off'].includes(patch.mode)) out.mode = patch.mode;
    if (Array.isArray(patch.sensitiveWords)) {
      out.sensitiveWords = [...new Set(patch.sensitiveWords
        .filter((w) => typeof w === 'string' && w.length > 0 && w.length <= 64)
        .map((w) => w.trim()))].slice(0, 500);
    }
    for (const [k, min, max, dflt] of [
      ['freqWindowMs', 1_000, 3_600_000],
      ['freqMaxCount', 1, 10_000],
      ['detectTimeoutMs', 10, 30_000],
    ]) {
      if (Number.isFinite(patch[k]) && patch[k] >= min && patch[k] <= max) out[k] = Math.round(patch[k]);
    }
    return out;
  }

  _publicRule(rule) {
    const { version, ...rest } = rule;
    return rest;
  }

  // ---------------------------------------------------------------- 发送入口

  /**
   * 消息落库后、广播前调用。返回处置指令，由 server 执行广播/ACK：
   *   { gate: true }   pre 命中暂存：先不要广播（消息已 held），server 正常 ACK
   *   { gate: false }  其余：立即按 msgFrame 广播（visible/flagged）
   * 无论哪种结果，检测都异步进行，结果通过 recall/mod_update 联动。
   */
  onNewMessage(message, senderConn) {
    const roomId = message.roomId;
    const rule = this.getRule(roomId);
    if (!this.config.moderationEnabled || rule.mode === 'off') {
      return { gate: false, mode: 'off' };
    }

    // 建立审核记录并启动异步检测（无论 held 还是已发布）
    this.db.ensureReview(roomId, message.seq, rule.version, 'pending', null);

    // 实际落库状态决定是否门控：held（pre 模式，或前方有 held 积压）则暂不广播
    if (message.modStatus === 'held') {
      this.db.setMessageModeration(roomId, message.seq, 'held', null, rule.version);
      this.trackHeld(roomId, message.seq);
      this._runDetection(message, rule);
      return { gate: true, mode: rule.mode };
    }

    // 已发布（post/flag 且无积压）：立即广播，检测异步进行
    this._runDetection(message, rule);
    return { gate: false, mode: rule.mode };
  }

  /**
   * 计算消息落库时的初始 mod_status（供 db.insertMessage 使用）。
   * 除 pre 模式外，只要房间前方还压着 held 消息（运行期模式切换/积压），新消息也以 held
   * 落库 —— 此刻它事实上必须先审后发，否则断线补发会绕过内存门控越过暂存消息。
   * DB 状态即真相，补发/重启都不会乱序。
   */
  initialStatus(roomId) {
    const rule = this.getRule(roomId);
    if (!this.config.moderationEnabled || rule.mode === 'off') {
      return { modStatus: 'visible', ruleVersion: null };
    }
    const backlog = this.db.minHeldSeq(roomId);
    if (rule.mode === 'pre' || backlog != null) {
      return { modStatus: 'held', ruleVersion: rule.version };
    }
    return { modStatus: 'visible', ruleVersion: null };
  }

  // ---------------------------------------------------------------- 检测

  /**
   * 执行检测：敏感词（本地同步）+ 频率异常（本地同步）+ 可注入的外部检测后端（异步）。
   * 超时由定时器兜底（fail-open 放行）；检测结果与超时兜底通过 DB compare-and-set 抢占，
   * 保证只处理一次（幂等审核）。若超时放行后真正的违规/可疑结果才到达，则升级补做
   * 撤回/标记 —— 覆盖「审核结果晚于客户端展示」。
   */
  _runDetection(message, rule) {
    if (this.stopped) return;
    const { roomId, seq } = message;
    const key = `${roomId}:${seq}`;
    if (this.inflight.has(key)) return; // 重复触发防护（重复审核）
    this.inflight.add(key);
    // 整个检测延迟到下一个微任务：保证 post/flag 模式下「原消息先广播」，
    // 检测结果（撤回/标记）永远晚于原消息送达，不会出现 recall 早于 msg 的乱序。
    Promise.resolve()
      .then(() => { if (!this.stopped) return this._detectAsync(message, rule, key); return null; })
      .catch(() => this.inflight.delete(key));
  }

  _detectAsync(message, rule, key) {
    const { roomId, seq } = message;
    const startedAt = now();
    let timedOut = false;

    const failOpen = () => {
      this.timers.delete(timerRef);
      if (this.stopped) return;
      const won = this.db.casDetectResult(
        roomId, seq, 'pending', 'timeout', [{ type: 'detect_timeout' }], now() - startedAt
      );
      if (won) this._applyDetection(message, rule, { status: 'timeout', hits: [{ type: 'detect_timeout' }] });
    };

    // 本地即时命中敏感词即确定性违规，无需等待外部后端
    const local = this._localDetect(message, rule);
    if (local.status === 'violation') {
      this.inflight.delete(key);
      const won = this.db.casDetectResult(roomId, seq, 'pending', 'violation', local.hits, 0);
      if (won && !this.stopped) this._applyDetection(message, rule, { status: 'violation', hits: local.hits });
      return;
    }

    // 超时兜底
    const timeoutMs = rule.detectTimeoutMs || this.config.modDetectTimeoutMs;
    const timerRef = this.setTimer(() => {
      timedOut = true;
      failOpen();
    }, timeoutMs);
    this.timers.add(timerRef);
    if (timerRef && typeof timerRef.unref === 'function') timerRef.unref();

    const finish = () => this.timers.delete(timerRef);
    const backend = this._pickDetector(roomId);
    Promise.resolve()
      .then(() => (backend ? backend(message.content, rule) : null))
      .then((remote) => {
        clearTimeout(timerRef); finish();
        if (this.stopped) return;
        // 合并本地（频率可疑）与外部结果
        let status = 'clean';
        let hits = [];
        if (remote && remote.status === 'violation') {
          status = 'violation';
          hits = [...(local.hits || []), ...(remote.hits || [{ type: 'remote_violation' }])];
        } else if (local.status === 'suspect') {
          status = 'suspect';
          hits = local.hits;
        } else if (remote && remote.status === 'suspect') {
          status = 'suspect';
          hits = remote.hits || [{ type: 'remote_suspect' }];
        } else if (remote && remote.status === 'error') {
          status = 'error';
          hits = remote.hits || [{ type: 'detect_error' }];
        }

        if (timedOut) {
          // 已 fail-open 放行：仅当晚到结果更严重（违规/可疑）时升级补处置
          if (status === 'violation' || status === 'suspect') {
            const escalated = this.db.casDetectEscalate(roomId, seq, status, hits, now() - startedAt);
            if (escalated) this._applyLateDetection(message, rule, { status, hits });
          }
          return;
        }
        const won = this.db.casDetectResult(roomId, seq, 'pending', status, hits, now() - startedAt);
        if (won) {
          this._applyDetection(message, rule, { status, hits });
        } else if (status === 'violation' || status === 'suspect') {
          // 兜底：状态已被别的路径写走（理论上仅超时），尝试升级
          const escalated = this.db.casDetectEscalate(roomId, seq, status, hits, now() - startedAt);
          if (escalated) this._applyLateDetection(message, rule, { status, hits });
        }
      })
      .catch(() => {
        clearTimeout(timerRef); finish();
        if (this.stopped || timedOut) return; // 已停止 / 超时已 fail-open，异常忽略
        const won = this.db.casDetectResult(
          roomId, seq, 'pending', 'error', [{ type: 'detect_exception' }], now() - startedAt
        );
        if (won) this._applyDetection(message, rule, { status: 'error', hits: [{ type: 'detect_exception' }] });
      })
      .finally(() => this.inflight.delete(key));
  }

  _pickDetector(roomId) {
    return this.detectors.get(roomId) || this.detectors.get('*') || null;
  }

  /** 注入（或清除，传 null）外部检测后端：async (content, rule) => {status, hits?, ms?} */
  setDetector(roomIdOrStar, fn) {
    if (fn) this.detectors.set(roomIdOrStar, fn);
    else this.detectors.delete(roomIdOrStar);
  }

  /** 本地检测：敏感词 + 频率异常。违规 > 可疑。 */
  _localDetect(message, rule) {
    const hits = [];
    const text = String(message.content || '');
    for (const w of rule.sensitiveWords || []) {
      if (w && text.includes(w)) hits.push({ type: 'sensitive_word', word: w });
    }

    // 频率滑窗（按发送者）
    const freqHit = this._noteFrequency(message.from, message.seq, rule);
    if (freqHit) hits.push(freqHit);

    const hasSensitive = hits.some((h) => h.type === 'sensitive_word');
    const hasFreq = hits.some((h) => h.type === 'frequency');
    if (hasSensitive) return { status: 'violation', hits };
    if (hasFreq) return { status: 'suspect', hits };
    return { status: 'clean', hits: [] };
  }

  /** 记录发送并判断窗口内是否超频；超频返回命中描述，否则 null */
  _noteFrequency(userId, seq, rule) {
    const t = now();
    const win = rule.freqWindowMs || this.config.modFreqWindowMs;
    const max = rule.freqMaxCount || this.config.modFreqMaxCount;
    let arr = this.freq.get(userId);
    if (!arr) { arr = []; this.freq.set(userId, arr); }
    arr.push({ seq, ts: t });
    while (arr.length && t - arr[0].ts > win) arr.shift();
    if (arr.length > max) return { type: 'frequency', windowMs: win, count: arr.length, max };
    return null;
  }

  // ---------------------------------------------------------------- 检测结果联动

  /**
   * 检测终态 -> 消息状态与广播策略联动。处置以消息「当前是否已发布」为准：
   *  - 仍 held（pre，或被前方积压门控）：违规/可疑保持 held 转人工；clean/超时/错误放行
   *    （置 visible 并按 seq 门控广播），与模式无关 —— 没发出去的消息谈不上撤回；
   *  - 已发布（post/flag 且无积压）：post 违规撤回、可疑标记；flag 违规/可疑均仅标记。
   */
  _applyDetection(message, rule, { status, hits }) {
    const { roomId, seq } = message;
    const cur = this.db.getMessageModeration(roomId, seq);
    const isHeld = cur && cur.modStatus === 'held';

    if (isHeld) {
      if (status === 'violation' || status === 'suspect') {
        this.db.setMessageModeration(roomId, seq, 'held', this._reason(hits), rule.version);
        this._enqueueManual(message, rule, hits);
        return;
      }
      // clean / timeout / error —— 放行，门控按 seq 连带释放其后已就绪消息
      this.db.setMessageModeration(roomId, seq, 'visible', null, rule.version);
      this._drainHeld(roomId);
      return;
    }

    // —— 已发布消息：按模式联动 ——
    if (status === 'violation') {
      if (rule.mode === 'post') {
        this.db.setMessageModeration(roomId, seq, 'recalled', this._reason(hits), rule.version);
        this._broadcastRecall(message, hits, 'auto');
        this._enqueueManual(message, rule, hits, { autoRecalled: true });
        return;
      }
      // flag（理论上也含切模式后的边界）：仅标记
      this.db.setMessageModeration(roomId, seq, 'flagged', this._reason(hits), rule.version);
      this._broadcastModUpdate(message, 'flagged', this._reason(hits));
      this._enqueueManual(message, rule, hits);
      return;
    }

    if (status === 'suspect') {
      this.db.setMessageModeration(roomId, seq, 'flagged', this._reason(hits), rule.version);
      this._broadcastModUpdate(message, 'flagged', this._reason(hits));
      this._enqueueManual(message, rule, hits);
      return;
    }
    // 已发布消息 clean / timeout / error：保持可见，无动作
  }

  /**
   * 晚到结果处置：消息已因超时/错误 fail-open 对客户端展示后，真正的检测结果才到。
   * 此时无法再「先审」，只能按已发出处理 —— 违规补撤回（recall），可疑补标记（flag），
   * 并转人工复核。
   */
  _applyLateDetection(message, rule, { status, hits }) {
    const { roomId, seq } = message;
    if (status === 'violation') {
      this.db.setMessageModeration(roomId, seq, 'recalled', this._reason(hits), rule.version);
      this._broadcastRecall(message, hits, 'late');
      this.db.addAudit('system', 'late_recall', { seq, hits, prior: 'fail-open' }, roomId);
    } else if (status === 'suspect') {
      this.db.setMessageModeration(roomId, seq, 'flagged', this._reason(hits), rule.version);
      this._broadcastModUpdate(message, 'flagged', this._reason(hits));
    }
    this._enqueueManual(this.db.getMessage(roomId, seq), rule, hits, { late: true });
  }

  /** pre 模式门控放行：从当前最小 held 水位线开始，把连续已 visible/flagged 的消息补发广播 */
  _drainHeld(roomId) {
    // 采用 per-room 的 held 等待队列（releaseQueue）记录所有被门控挡住的消息，放行时按
    // seq 顺序弹出；遇到第一个仍 held 的停止（保序门控），recalled/rejected 直接跳过。
    const queue = this.releaseQueue.get(roomId);
    if (!queue) return;
    while (queue.length) {
      const seq = queue[0];
      const mod = this.db.getMessageModeration(roomId, seq);
      if (!mod || mod.modStatus === 'held') break; // 水位线：前面还有暂存消息，不能越过
      queue.shift();
      if (mod.modStatus === 'visible' || mod.modStatus === 'flagged') {
        const m = this.db.getMessage(roomId, seq);
        if (m) this.hub.broadcast(roomId, this._msgFrame(m), { track: true, seq });
      }
      // rejected / recalled：不广播（驳回/撤回已由各自流程通知）
    }
    if (queue.length === 0) this.releaseQueue.delete(roomId);
  }

  // ---------------------------------------------------------------- 人工复核

  /** 进入人工队列并通知管理员 */
  _enqueueManual(message, rule, hits, extra = {}) {
    // detect_status 已是 suspect/violation/pending；确保有审核记录
    this.db.ensureReview(message.roomId, message.seq, rule.version, 'pending', hits);
    this.db.addAudit('system', 'flag_for_review', {
      seq: message.seq, hits, mode: rule.mode, ...extra,
    }, message.roomId);
    const review = this.db.getReview(message.roomId, message.seq);
    this._notifyAdmins(message.roomId, {
      type: 'mod_queue_update', roomId: message.roomId,
      item: this._reviewView(review),
    });
  }

  /**
   * 管理员人工终判。decision:
   *  - approve 放行：held->visible（pre 广播放行）/ recalled->visible（恢复，post 误撤）/ flagged->visible
   *  - reject  驳回：held->rejected（不广播），通知发送者占位；flagged->rejected 并撤回
   *  - recall  撤回：任意可见态 -> recalled，广播 recall
   * 幂等：同一队列项 CAS 只能终判一次。
   */
  decide(roomId, seq, adminId, decision, reason) {
    if (!['approve', 'reject', 'recall'].includes(decision)) {
      return { ok: false, code: 'BAD_REQUEST', message: 'invalid decision' };
    }
    const message = this.db.getMessage(roomId, seq);
    if (!message) return { ok: false, code: 'NOT_FOUND', message: 'message not found' };

    // CAS 抢占人工终判（防重复审核/双重操作）
    const won = this.db.casDecision(roomId, seq, adminId, decision, reason || null);
    const review = this.db.getReview(roomId, seq);
    if (!won && review && review.decision && review.decision !== decision) {
      return { ok: false, code: 'ALREADY_DECIDED', message: `already decided: ${review.decision}` };
    }

    const before = message.modStatus;
    if (decision === 'approve') {
      if (before === 'held') {
        this.db.setMessageModeration(roomId, seq, 'visible', null, review?.ruleVersion ?? null);
        // 进入门控放行流程：先确保它在 release 队列中
        this._trackRelease(roomId, seq);
        this._drainHeld(roomId);
      } else if (before === 'recalled') {
        // 误撤恢复：重新可见并广播
        this.db.setMessageModeration(roomId, seq, 'visible', null, review?.ruleVersion ?? null);
        const m = this.db.getMessage(roomId, seq);
        this.hub.broadcast(roomId, this._msgFrame(m), { track: true, seq });
      } else if (before === 'flagged') {
        this.db.setMessageModeration(roomId, seq, 'visible', null, review?.ruleVersion ?? null);
        this._broadcastModUpdate(message, 'visible', null);
      } else {
        // already visible —— 幂等成功，无额外动作
      }
      this._notifySender(roomId, message.from, {
        type: 'mod_result', roomId, seq, outcome: 'approved', by: adminId, reason: reason || null,
      });
    } else if (decision === 'reject') {
      if (before === 'held') {
        this.db.setMessageModeration(roomId, seq, 'rejected', reason || 'rejected by moderator', review?.ruleVersion ?? null);
        this._releaseQueueDelete(roomId, seq);
        this._drainHeld(roomId); // 驳回也释放水位线（它本身不再阻挡后续）
        this._notifySender(roomId, message.from, {
          type: 'mod_result', roomId, seq, outcome: 'rejected', by: adminId, reason: reason || 'rejected by moderator',
        });
      } else {
        // 已发出的消息执行驳回 = 撤回
        this.db.setMessageModeration(roomId, seq, 'recalled', reason || 'rejected by moderator', review?.ruleVersion ?? null);
        this._broadcastRecall(message, [{ type: 'manual_reject', reason }], 'manual');
      }
    } else {
      // recall
      if (before === 'recalled') {
        return { ok: true, idempotent: true }; // 已撤回，幂等
      }
      this.db.setMessageModeration(roomId, seq, 'recalled', reason || 'recalled by moderator', review?.ruleVersion ?? null);
      if (before === 'held') {
        this._releaseQueueDelete(roomId, seq);
        this._drainHeld(roomId);
        this._notifySender(roomId, message.from, {
          type: 'mod_result', roomId, seq, outcome: 'recalled', by: adminId, reason: reason || null,
        });
      } else {
        this._broadcastRecall(message, [{ type: 'manual_recall', reason }], 'manual');
      }
    }

    this.db.addAudit(adminId, 'decision', {
      seq, decision, reason: reason || null, before, after: this.db.getMessageModeration(roomId, seq).modStatus,
    }, roomId);
    this._notifyAdmins(roomId, {
      type: 'mod_queue_update', roomId, item: this._reviewView(this.db.getReview(roomId, seq)),
    });
    return { ok: true, decision, before, after: this.db.getMessageModeration(roomId, seq).modStatus };
  }

  /**
   * 用户/管理员直接撤回一条已发出的消息（非审核流程的常规撤回）。
   * 仅发送者本人或房间管理员可撤。返回错误码或 {ok:true}。
   */
  recall(roomId, seq, actorConn, { isAdmin }) {
    const message = this.db.getMessage(roomId, seq);
    if (!message) return { ok: false, code: 'NOT_FOUND', message: 'message not found' };
    if (message.modStatus === 'recalled') return { ok: true, idempotent: true };
    const isOwner = message.from === actorConn.userId;
    if (!isOwner && !isAdmin) return { ok: false, code: 'FORBIDDEN', message: 'only owner or admin can recall' };
    if (message.modStatus === 'held') {
      return { ok: false, code: 'HELD', message: 'message is pending review and not delivered' };
    }
    this.db.setMessageModeration(roomId, seq, 'recalled', 'recalled by ' + (isOwner ? 'sender' : 'admin'), message.ruleVersion ?? null);
    this._broadcastRecall(message, [{ type: isOwner ? 'self_recall' : 'admin_recall' }], isOwner ? 'self' : 'manual');
    this.db.addAudit(actorConn.userId, 'recall', { seq, owner: message.from, isAdmin }, roomId);
    return { ok: true };
  }

  // ---------------------------------------------------------------- 申诉

  appealCreate(roomId, seq, userId, reason) {
    const message = this.db.getMessage(roomId, seq);
    if (!message) return { ok: false, code: 'NOT_FOUND', message: 'message not found' };
    if (message.from !== userId) return { ok: false, code: 'FORBIDDEN', message: 'not your message' };
    if (!['recalled', 'rejected', 'flagged'].includes(message.modStatus)) {
      return { ok: false, code: 'NOT_APPEALABLE', message: 'message is not under a penalty' };
    }
    const r = this.db.createAppeal(roomId, seq, userId, reason);
    if (!r.ok) return { ok: false, code: r.code, message: 'already appealed' };
    this.db.addAudit(userId, 'appeal_create', { seq, appealId: r.id, reason }, roomId);
    this._notifyAdmins(roomId, { type: 'mod_appeal_update', roomId, appeal: this._appealView(this.db.getAppeal(r.id)) });
    return { ok: true, id: r.id };
  }

  appealHandle(roomId, appealId, adminId, uphold, reply) {
    const appeal = this.db.getAppeal(appealId);
    if (!appeal || appeal.room_id !== roomId) {
      return { ok: false, code: 'NOT_FOUND', message: 'appeal not found' };
    }
    const status = uphold ? 'upheld' : 'reversed';
    const won = this.db.resolveAppeal(appealId, status, adminId, reply || null);
    if (!won) return { ok: false, code: 'ALREADY_HANDLED', message: 'appeal already handled' };

    // 申诉成功（推翻处置）：恢复消息
    const message = this.db.getMessage(roomId, appeal.seq);
    if (!uphold && message) {
      if (message.modStatus === 'recalled' || message.modStatus === 'rejected') {
        this.db.setMessageModeration(roomId, appeal.seq, 'visible', null, message.ruleVersion ?? null);
        if (message.modStatus === 'recalled') {
          const m = this.db.getMessage(roomId, appeal.seq);
          this.hub.broadcast(roomId, this._msgFrame(m), { track: true, seq: appeal.seq });
        } else {
          // rejected 原本未发出，走门控放行
          this._trackRelease(roomId, appeal.seq);
          this._drainHeld(roomId);
        }
      } else if (message.modStatus === 'flagged') {
        this.db.setMessageModeration(roomId, appeal.seq, 'visible', null, message.ruleVersion ?? null);
        this._broadcastModUpdate(message, 'visible', null);
      }
    }

    this.db.addAudit(adminId, 'appeal_handle', {
      appealId, uphold, reply: reply || null, seq: appeal.seq,
    }, roomId);
    const view = this._appealView(this.db.getAppeal(appealId));
    this._notifyAdmins(roomId, { type: 'mod_appeal_update', roomId, appeal: view });
    this._notifySender(roomId, appeal.user_id, {
      type: 'appeal_result', roomId, appealId, seq: appeal.seq,
      upheld: !!uphold, reply: reply || null, by: adminId,
    });
    return { ok: true, status };
  }

  // ---------------------------------------------------------------- 队列/日志查询

  queue(roomId) {
    return {
      pending: this.db.listPendingReviews(roomId, this.config.modQueueLimit).map((r) => this._reviewView(r)),
      recent: this.db.listRecentReviews(roomId, this.config.modQueueLimit).map((r) => this._reviewView(r)),
    };
  }

  appeals(roomId) {
    return this.db.listAppeals(roomId, this.config.modQueueLimit).map((a) => this._appealView(a));
  }

  auditLog(roomId) {
    return this.db.listAudit(roomId, this.config.modQueueLimit);
  }

  // ---------------------------------------------------------------- 内部工具

  /** pre 模式新消息进入等待放行队列（按 seq 保序） */
  _trackRelease(roomId, seq) {
    let q = this.releaseQueue.get(roomId);
    if (!q) { q = []; this.releaseQueue.set(roomId, q); }
    if (!q.includes(seq)) {
      q.push(seq);
      q.sort((a, b) => a - b);
    }
  }

  _releaseQueueDelete(roomId, seq) {
    const q = this.releaseQueue.get(roomId);
    if (q) {
      const i = q.indexOf(seq);
      if (i >= 0) q.splice(i, 1);
      if (!q.length) this.releaseQueue.delete(roomId);
    }
  }

  /** 发送路径登记：pre held 消息纳入放行队列 */
  trackHeld(roomId, seq) { this._trackRelease(roomId, seq); }

  _reason(hits) {
    if (!hits || !hits.length) return null;
    return hits.map((h) => h.word ? `敏感词:${h.word}` : h.type).join(';');
  }

  _broadcastRecall(message, hits, source) {
    this.hub.broadcast(message.roomId, {
      type: 'recall',
      roomId: message.roomId,
      seq: message.seq,
      reason: this._reason(hits),
      source, // auto / manual / self
      ts: now(),
    });
  }

  _broadcastModUpdate(message, status, reason) {
    this.hub.broadcast(message.roomId, {
      type: 'mod_update',
      roomId: message.roomId,
      seq: message.seq,
      mod: status,
      reason: reason || null,
      ts: now(),
    });
  }

  _notifyAdmins(roomId, frame) {
    for (const conn of this.hub.roomConns(roomId)) {
      const member = this.db.getMember(roomId, conn.userId);
      if (member && member.role === 'admin') this.hub.send(conn, frame);
    }
  }

  _notifySender(roomId, userId, frame) {
    this.hub.sendToUser(userId, frame);
  }

  _msgFrame(m) {
    if (this.buildMsgFrame) return this.buildMsgFrame(m);
    return {
      type: 'msg',
      roomId: m.roomId,
      seq: m.seq,
      clientMsgId: m.clientMsgId,
      from: m.from,
      fromName: m.fromName,
      content: m.content,
      ts: m.ts,
      mod: m.modStatus === 'visible' ? undefined : m.modStatus,
      modReason: m.modReason || undefined,
    };
  }

  _reviewView(r) {
    if (!r) return null;
    return {
      roomId: r.roomId, seq: r.seq,
      clientMsgId: r.clientMsgId, from: r.from, fromName: r.fromName,
      content: r.content, ts: r.ts,
      detectStatus: r.detectStatus, hits: r.hits || [], detectMs: r.detectMs,
      modStatus: r.modStatus, modReason: r.modReason,
      ruleVersion: r.ruleVersion,
      reviewedBy: r.reviewedBy, decision: r.decision, decideReason: r.decideReason,
      createdAt: r.createdAt, decidedAt: r.decidedAt,
    };
  }

  _appealView(a) {
    return {
      id: a.id, roomId: a.room_id, seq: a.seq, userId: a.user_id,
      userName: a.user_name, reason: a.reason, status: a.status,
      adminId: a.admin_id, reply: a.reply, createdAt: a.created_at, handledAt: a.handled_at,
    };
  }
}

module.exports = { ModerationService, defaultRule };
