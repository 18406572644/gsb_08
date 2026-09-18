'use strict';

const { now } = require('./util');

/**
 * 内容检测服务。
 *
 * 两条检测通道：
 * 1. 敏感词：对当前规则版本的词表做大小写不敏感子串匹配；
 * 2. 频率异常：按 (roomId, senderId) 维护滑动窗口，窗口内消息数超过阈值即命中。
 *
 * 接口刻意做成**异步**（模拟远程检测 RPC：可注入延迟/故障），编排层以超时调用它：
 * 超时或服务异常时返回 degraded=true 且不带任何命中 —— fail-open，检测服务故障
 * 绝不阻塞消息主链路（ACK/广播照常），只在审核记录里留下降级痕迹。
 */
class Detector {
  constructor({
    words = [],
    freqWindowMs = 10_000,
    freqMaxCount = 8,
    timeoutMs = 800,
    delayMs = 0, // 模拟检测耗时（测试用）
    faulty = false, // 模拟检测服务故障（reject）
  } = {}) {
    this.timeoutMs = timeoutMs;
    this.delayMs = delayMs;
    this.faulty = faulty;
    this.windows = new Map(); // `${roomId}:${userId}` -> 时间戳数组
    this.setRules({ words, freqWindowMs, freqMaxCount });
  }

  /** 规则版本切换时热更新（窗口参数随版本走） */
  setRules({ words = [], freqWindowMs = 10_000, freqMaxCount = 8 } = {}) {
    this.words = [...new Set(words.map((w) => String(w).trim().toLowerCase()).filter(Boolean))];
    this.freqWindowMs = freqWindowMs;
    this.freqMaxCount = freqMaxCount;
  }

  getRules() {
    return { words: this.words, freqWindowMs: this.freqWindowMs, freqMaxCount: this.freqMaxCount };
  }

  /**
   * 异步检测。永不 reject：
   * 正常 -> { hit, flags, reason, degraded:false }
   * 超时/故障 -> { hit:false, flags:[], degraded:true, reason }
   */
  async detect({ roomId, senderId, content }, timeoutMs = this.timeoutMs) {
    try {
      return await this._withTimeout(this._run({ roomId, senderId, content }), timeoutMs);
    } catch (err) {
      return {
        hit: false,
        flags: [],
        reason: `检测服务${err?.code === 'TIMEOUT' ? '超时' : '异常'}，已降级放行`,
        degraded: true,
      };
    }
  }

  _withTimeout(p, ms) {
    if (!Number.isFinite(ms) || ms <= 0) return p;
    let timer;
    const timeout = new Promise((_, reject) => {
      timer = setTimeout(() => {
        const e = new Error('detector timeout');
        e.code = 'TIMEOUT';
        reject(e);
      }, ms);
      timer.unref?.();
    });
    return Promise.race([p, timeout]).finally(() => clearTimeout(timer));
  }

  _run({ roomId, senderId, content }) {
    return new Promise((resolve, reject) => {
      const finish = () => {
        if (this.faulty) {
          reject(new Error('detector faulty'));
          return;
        }
        resolve(this._classify(roomId, senderId, content));
      };
      if (this.delayMs > 0) setTimeout(finish, this.delayMs);
      else finish();
    });
  }

  _classify(roomId, senderId, content) {
    const flags = [];
    const text = String(content).toLowerCase();

    // 1) 敏感词
    const hitWords = this.words.filter((w) => text.includes(w));
    for (const w of hitWords) flags.push(`word:${w}`);

    // 2) 频率异常（滑动窗口：计入本次后超阈值即命中）
    const freqHit = this._bumpFreq(roomId, senderId);
    if (freqHit) flags.push('freq');

    const parts = [];
    if (hitWords.length) parts.push(`命中敏感词：${hitWords.join('、')}`);
    if (freqHit) parts.push(`频率异常：${this.freqWindowMs / 1000}s 内超过 ${this.freqMaxCount} 条`);

    return {
      hit: flags.length > 0,
      flags,
      reason: parts.length ? parts.join('；') : null,
      degraded: false,
    };
  }

  _bumpFreq(roomId, senderId) {
    const key = `${roomId}:${senderId}`;
    const t = now();
    let arr = this.windows.get(key);
    if (!arr) {
      arr = [];
      this.windows.set(key, arr);
    }
    while (arr.length && t - arr[0] >= this.freqWindowMs) arr.shift();
    arr.push(t);
    return arr.length > this.freqMaxCount;
  }

  /** 测试/运维：清空频率窗口 */
  resetFreq() {
    this.windows.clear();
  }
}

module.exports = { Detector };
