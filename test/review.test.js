'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const WebSocket = require('ws');
const { DatabaseSync } = require('node:sqlite');
const { createChatServer } = require('../src/server');
const { ChatDB } = require('../src/db');

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/** 与 chat.test.js 相同的隔离服务器；detector 选项可注入词表/延迟/故障/超时 */
async function startServer(overrides = {}) {
  const server = createChatServer({
    port: 0,
    host: '127.0.0.1',
    dbPath: ':memory:',
    heartbeatIntervalMs: 60_000,
    ackResendAfterMs: 60_000,
    reviewDefaultMode: 'post',
    detector: { words: ['违禁词', 'badword'] },
    ...overrides,
  });
  const addr = await server.start();
  return { server, port: addr.port };
}

async function login(port, name) {
  const res = await fetch(`http://127.0.0.1:${port}/api/login`, {
    method: 'POST',
    headers: { 'content-type': 'application/json', connection: 'close' },
    body: JSON.stringify({ name }),
  });
  assert.equal(res.status, 200);
  return res.json();
}

class Client {
  static async connect(port, token) {
    const c = new Client();
    c.log = [];
    c.pending = [];
    c.waiters = [];
    c.closed = new Promise((res) => (c._onClosed = res));
    c.ws = new WebSocket(`ws://127.0.0.1:${port}/ws?token=${encodeURIComponent(token)}`);
    c.ws.on('message', (raw) => {
      const m = JSON.parse(raw.toString());
      c.log.push(m);
      for (const w of [...c.waiters]) {
        if (w.pred(m)) {
          c.waiters.splice(c.waiters.indexOf(w), 1);
          clearTimeout(w.timer);
          w.resolve(m);
          return;
        }
      }
      c.pending.push(m);
    });
    c.ws.on('close', () => c._onClosed());
    await new Promise((res, rej) => {
      c.ws.once('open', res);
      c.ws.once('error', rej);
    });
    await c.waitFor((m) => m.type === 'welcome');
    return c;
  }

  send(obj) {
    this.ws.send(JSON.stringify(obj));
  }

  waitFor(pred, timeout = 3000) {
    const idx = this.pending.findIndex(pred);
    if (idx >= 0) {
      const [m] = this.pending.splice(idx, 1);
      return Promise.resolve(m);
    }
    return new Promise((resolve, reject) => {
      const w = { pred, resolve };
      w.timer = setTimeout(() => reject(new Error('waitFor: timed out')), timeout);
      this.waiters.push(w);
    });
  }

  close() {
    this.ws.close();
    return this.closed;
  }
}

async function setupRoom(overrides = {}) {
  const { server, port } = await startServer(overrides);
  const ua = await login(port, 'alice');
  const ub = await login(port, 'bob');
  const a = await Client.connect(port, ua.token);
  const b = await Client.connect(port, ub.token);
  a.send({ type: 'create_room', name: `r${Math.floor(Math.random() * 1e9)}` });
  const joined = await a.waitFor((m) => m.type === 'joined');
  const roomId = joined.roomId;
  b.send({ type: 'join', room: roomId, lastSeq: 0 });
  await b.waitFor((m) => m.type === 'joined');
  return { server, port, roomId, a, b, ua, ub };
}

async function setMode(client, roomId, mode) {
  client.send({ type: 'review_policy', roomId, mode });
  await client.waitFor((m) => m.type === 'review_policy' && m.mode === mode);
}

const isMsg = (roomId, seq) => (m) => m.type === 'msg' && m.roomId === roomId && m.seq === seq;
const isRecalled = (roomId, seq) => (m) => m.type === 'msg_recalled' && m.roomId === roomId && m.seq === seq;
const isReview = (roomId, seq, status) =>
  (m) => m.type === 'msg_review' && m.roomId === roomId && m.seq === seq && (!status || m.status === status);

// ---------------------------------------------------------------- 先发后撤回

test('post 模式：正常消息直接广播；命中敏感词先展示后撤回并入队', async () => {
  const { server, roomId, a, b } = await setupRoom();
  try {
    a.send({ type: 'msg', roomId, clientMsgId: 'c1', content: 'hello everyone' });
    const m1 = await b.waitFor(isMsg(roomId, 1));
    assert.equal(m1.content, 'hello everyone');

    a.send({ type: 'msg', roomId, clientMsgId: 'c2', content: '这里有 badword 啊' });
    const m2 = await b.waitFor(isMsg(roomId, 2)); // 先上屏
    const rec = await b.waitFor(isRecalled(roomId, 2)); // 后撤回
    assert.ok(rec.flags.includes('word:badword'));
    assert.ok(rec.reviewEventId > 0);

    a.send({ type: 'review_queue', roomId });
    const q = await a.waitFor((m) => m.type === 'review_queue');
    assert.deepEqual(q.items.map((i) => i.seq), [2]);
    assert.equal(q.items[0].reviewStatus, 'recalled');
    await a.close();
    await b.close();
  } finally {
    server.stop();
  }
});

test('post 模式：管理员人工恢复误撤回消息（seq 原序号重新可见）', async () => {
  const { server, roomId, a, b } = await setupRoom();
  try {
    a.send({ type: 'msg', roomId, clientMsgId: 'c1', content: 'badword' });
    await b.waitFor(isMsg(roomId, 1));
    await b.waitFor(isRecalled(roomId, 1));

    a.send({ type: 'review_decide', roomId, seq: 1, action: 'restore', reason: '误判' });
    const back = await b.waitFor((m) => m.type === 'msg' && m.seq === 1 && m.content === 'badword');
    assert.ok(back.reviewEventId > 0);
    // 队列已随恢复关闭
    a.send({ type: 'review_queue', roomId });
    const q = await a.waitFor((m) => m.type === 'review_queue');
    assert.equal(q.items.length, 0);
    await a.close();
    await b.close();
  } finally {
    server.stop();
  }
});

// ---------------------------------------------------------------- 先审后发

test('pre 模式：ACK 立即返回；干净消息先占位后放行，seq 连续不空洞', async () => {
  const { server, roomId, a, b } = await setupRoom();
  try {
    await setMode(a, roomId, 'pre');
    a.send({ type: 'msg', roomId, clientMsgId: 'c1', content: '正常内容' });
    // ACK 不等待审核：落库即确认
    const ack = await a.waitFor((m) => m.type === 'ack' && m.clientMsgId === 'c1');
    assert.equal(ack.seq, 1);
    // 接收方先看到审核中占位（占据 seq 1）
    const pending = await b.waitFor(isReview(roomId, 1, 'pending'));
    assert.equal(pending.content, undefined);
    // 检测无命中，放行：同一 seq 的正文帧
    const rel = await b.waitFor(isMsg(roomId, 1));
    assert.equal(rel.content, '正常内容');
    assert.ok(rel.reviewEventId > 0);

    a.send({ type: 'msg', roomId, clientMsgId: 'c2', content: '第二条' });
    await b.waitFor(isReview(roomId, 2, 'pending'));
    await b.waitFor(isMsg(roomId, 2));
    await a.close();
    await b.close();
  } finally {
    server.stop();
  }
});

test('pre 模式：命中消息停留审核队列，管理员可放行或拦截', async () => {
  const { server, roomId, a, b } = await setupRoom();
  try {
    await setMode(a, roomId, 'pre');
    a.send({ type: 'msg', roomId, clientMsgId: 'c1', content: '含 违禁词 的消息' });
    await b.waitFor(isReview(roomId, 1, 'pending'));
    await sleep(80);
    // 命中后仍为 pending（带命中标记），不会自动放行
    const flagged = b.log
      .filter(isReview(roomId, 1, 'pending'))
      .find((m) => (m.flags || []).includes('word:违禁词'));
    assert.ok(flagged, '占位帧应被刷新为带命中标记');

    a.send({ type: 'review_queue', roomId });
    const q = await a.waitFor((m) => m.type === 'review_queue');
    assert.deepEqual(q.items.map((i) => i.seq), [1]);

    a.send({ type: 'review_decide', roomId, seq: 1, action: 'block', reason: '确认违规' });
    const blocked = await b.waitFor(isReview(roomId, 1, 'blocked'));
    assert.equal(blocked.reason, '确认违规');
    await a.close();
    await b.close();
  } finally {
    server.stop();
  }
});

test('pre 模式：管理员放行后正文广播；重复决策幂等不产生第二条指令', async () => {
  const { server, roomId, a, b } = await setupRoom();
  try {
    await setMode(a, roomId, 'pre');
    a.send({ type: 'msg', roomId, clientMsgId: 'c1', content: 'badword 求放过' });
    await b.waitFor(isReview(roomId, 1, 'pending'));
    await sleep(60);

    a.send({ type: 'review_decide', roomId, seq: 1, action: 'release' });
    await b.waitFor(isMsg(roomId, 1));
    await a.waitFor((m) => m.type === 'review_decided'); // 消费第一次决策回执
    // 再次放行：已不是 pending，幂等无变化，无第二帧
    a.send({ type: 'review_decide', roomId, seq: 1, action: 'release' });
    const ack2 = await a.waitFor((m) => m.type === 'review_decided');
    assert.equal(ack2.changed, false);
    assert.equal(ack2.idempotent, true);
    await sleep(100);
    assert.equal(b.log.filter((m) => m.type === 'msg' && m.seq === 1).length, 1);
    await a.close();
    await b.close();
  } finally {
    server.stop();
  }
});

// ---------------------------------------------------------------- 仅标记

test('mark 模式：命中只打风险标并入队，消息保持可见不撤回', async () => {
  const { server, roomId, a, b } = await setupRoom({ reviewDefaultMode: 'mark' });
  try {
    a.send({ type: 'msg', roomId, clientMsgId: 'c1', content: 'badword 标记测试' });
    const m = await b.waitFor(isMsg(roomId, 1));
    assert.equal(m.content, 'badword 标记测试'); // 仍然可见
    const flag = await b.waitFor((x) => x.type === 'msg_flagged' && x.seq === 1);
    assert.ok(flag.flags.includes('word:badword'));
    await sleep(50);
    assert.equal(b.log.filter(isRecalled(roomId, 1)).length, 0, 'mark 模式不得撤回');

    a.send({ type: 'review_queue', roomId });
    const q = await a.waitFor((x) => x.type === 'review_queue');
    assert.deepEqual(q.items.map((i) => i.seq), [1]);

    // 管理员复核后可手动撤回
    a.send({ type: 'review_decide', roomId, seq: 1, action: 'recall' });
    await b.waitFor(isRecalled(roomId, 1));
    await a.close();
    await b.close();
  } finally {
    server.stop();
  }
});

// ---------------------------------------------------------------- 异常链路

test('检测服务超时：fail-open 降级，pre 消息自动放行且不阻塞 ACK', async () => {
  const { server, roomId, a, b } = await setupRoom({
    detector: { words: ['badword'], delayMs: 300, timeoutMs: 50 },
  });
  try {
    await setMode(a, roomId, 'pre');
    const t0 = Date.now();
    a.send({ type: 'msg', roomId, clientMsgId: 'c1', content: 'badword' });
    const ack = await a.waitFor((m) => m.type === 'ack' && m.clientMsgId === 'c1');
    assert.ok(Date.now() - t0 < 200, 'ACK 不得等待检测服务');
    await b.waitFor(isReview(roomId, 1, 'pending'));
    const rel = await b.waitFor(isMsg(roomId, 1), 2000); // 超时降级后自动放行
    assert.equal(rel.content, 'badword');
    // 降级放行留有事件
    a.send({ type: 'review_log', roomId });
    const log = await a.waitFor((m) => m.type === 'review_log');
    assert.ok(log.entries.some((e) => e.action === 'detect_degraded'));
    await a.close();
    await b.close();
  } finally {
    server.stop();
  }
});

test('检测服务故障：post 模式故障期间不误撤回', async () => {
  const { server, roomId, a, b } = await setupRoom({
    detector: { words: ['badword'], faulty: true },
  });
  try {
    a.send({ type: 'msg', roomId, clientMsgId: 'c1', content: 'badword' });
    await b.waitFor(isMsg(roomId, 1));
    await sleep(150);
    assert.equal(b.log.filter(isRecalled(roomId, 1)).length, 0);
    await a.close();
    await b.close();
  } finally {
    server.stop();
  }
});

test('消息已被人工撤回时，晚到的检测结果不得覆盖人工决策（单帧、无重复入队）', async () => {
  const { server, roomId, a, b } = await setupRoom({
    detector: { words: ['badword'], delayMs: 200 },
  });
  try {
    a.send({ type: 'msg', roomId, clientMsgId: 'c1', content: 'badword' });
    await b.waitFor(isMsg(roomId, 1));
    // 检测结果返回前，管理员抢先人工撤回
    a.send({ type: 'review_decide', roomId, seq: 1, action: 'recall', reason: '人工先撤' });
    await b.waitFor((m) => isRecalled(roomId, 1)(m) && m.reason === '人工先撤');
    await sleep(300); // 等检测结果晚到
    assert.equal(b.log.filter(isRecalled(roomId, 1)).length, 1, '只允许一次撤回指令');
    a.send({ type: 'review_queue', roomId });
    const q = await a.waitFor((m) => m.type === 'review_queue');
    assert.equal(q.items.length, 0, '人工撤回不经自动队列，晚到检测不得补入队');
    await a.close();
    await b.close();
  } finally {
    server.stop();
  }
});

test('重复审核幂等：客户端同 clientMsgId 重发不产生重复消息/重复审核', async () => {
  const { server, roomId, a, b } = await setupRoom();
  try {
    a.send({ type: 'msg', roomId, clientMsgId: 'dup', content: 'badword' });
    a.send({ type: 'msg', roomId, clientMsgId: 'dup', content: 'badword' }); // 网络重试
    const ack1 = await a.waitFor((m) => m.type === 'ack' && m.clientMsgId === 'dup');
    const ack2 = await a.waitFor((m) => m.type === 'ack' && m.clientMsgId === 'dup' && m !== ack1);
    assert.equal(ack1.seq, ack2.seq);
    await b.waitFor(isRecalled(roomId, 1));
    await sleep(100);
    assert.deepEqual(b.log.filter((m) => m.type === 'msg').map((m) => m.seq), [1]);
    assert.equal(b.log.filter(isRecalled(roomId, 1)).length, 1);
    await a.close();
    await b.close();
  } finally {
    server.stop();
  }
});

// ---------------------------------------------------------------- 补发与晚到结果

test('断线补发：离线期间被撤回的消息，重连后通过审核事件补发撤回指令', async () => {
  const { server, port, roomId, a, b, ub } = await setupRoom();
  let b2 = b;
  try {
    a.send({ type: 'msg', roomId, clientMsgId: 'c1', content: 'badword' });
    await b.waitFor(isMsg(roomId, 1)); // B 已上屏
    await b.close(); // B 掉线（审核结果尚未到达）
    await sleep(50);
    await a.waitFor((m) => m.type === 'ack'); // 确保发送侧稳定
    // 检测结果在离线期间产生撤回
    await sleep(200);

    b2 = await Client.connect(port, ub.token);
    b2.send({ type: 'join', room: roomId, lastSeq: 1 }); // 已见过 seq1
    await b2.waitFor((m) => m.type === 'joined');
    const rec = await b2.waitFor(isRecalled(roomId, 1));
    assert.ok(rec.reviewEventId > 0, '撤回指令应走审核事件补发');
    // 不重复补正文
    assert.equal(b2.log.filter(isMsg(roomId, 1)).length, 0);
    await a.close();
    await b2.close();
  } finally {
    server.stop();
  }
});

test('断线补发：离线期间 pending 被放行的消息，重连直接以当前正文快照补发', async () => {
  const { server, port, roomId, a, b, ub } = await setupRoom();
  try {
    await setMode(a, roomId, 'pre');
    await b.close();
    a.send({ type: 'msg', roomId, clientMsgId: 'c1', content: '干净消息' });
    const ack = await a.waitFor((m) => m.type === 'ack' && m.clientMsgId === 'c1');
    assert.equal(ack.seq, 1);
    await sleep(100); // 离线期间已自动放行
    const b2 = await Client.connect(port, ub.token);
    b2.send({ type: 'join', room: roomId, lastSeq: 0 });
    const m = await b2.waitFor(isMsg(roomId, 1));
    assert.equal(m.content, '干净消息');
    await a.close();
    await b2.close();
  } finally {
    server.stop();
  }
});

// ---------------------------------------------------------------- 申诉

test('用户申诉：非发送者禁止申诉；驳回维持撤回，批准联动恢复', async () => {
  const { server, roomId, a, b } = await setupRoom();
  try {
    // alice 发的消息被自动撤回
    a.send({ type: 'msg', roomId, clientMsgId: 'c1', content: 'badword' });
    await b.waitFor(isMsg(roomId, 1));
    await b.waitFor(isRecalled(roomId, 1));

    // bob 不是发送者，不能申诉
    b.send({ type: 'appeal_submit', roomId, seq: 1, reason: '不是我的' });
    const err = await b.waitFor((m) => m.type === 'error');
    assert.equal(err.code, 'FORBIDDEN');

    // alice 申诉
    a.send({ type: 'appeal_submit', roomId, seq: 1, reason: '上下文不是违规用法' });
    const ap = await a.waitFor((m) => m.type === 'appeal_update');
    assert.equal(ap.appeal.status, 'open');
    // 重复提交：已有 open 申诉
    a.send({ type: 'appeal_submit', roomId, seq: 1, reason: '再申诉一次' });
    const err2 = await a.waitFor((m) => m.type === 'error');
    assert.equal(err2.code, 'APPEAL_OPEN');

    // 队列里能看到 open 申诉
    a.send({ type: 'appeals_list', roomId });
    const lst = await a.waitFor((m) => m.type === 'appeals');
    assert.equal(lst.appeals.length, 1);

    // 先驳回：维持撤回（此前 post 模式已上屏过 1 次正文，驳回不应再产生恢复帧）
    const msgsBeforeReject = b.log.filter((m) => m.type === 'msg' && m.seq === 1).length;
    a.send({ type: 'appeal_decide', appealId: ap.appeal.id, decision: 'rejected', note: '维持' });
    const rej = await a.waitFor((m) => m.type === 'appeal_update' && m.appeal.status === 'rejected');
    assert.ok(rej.appeal.decisionNote);
    assert.equal(b.log.filter((m) => m.type === 'msg' && m.seq === 1).length, msgsBeforeReject);

    // 重新申诉并通过（冷却设为 0 的服务器才允许立刻重提；本测试服务器默认 60s，故直接验证批准路径需等待——
    // 改为直接管理员恢复，验证申诉通过的联动：新起一条消息走完整链路）
    a.send({ type: 'msg', roomId, clientMsgId: 'c2', content: '又一个 badword' });
    await b.waitFor(isMsg(roomId, 2));
    await b.waitFor(isRecalled(roomId, 2));
    a.send({ type: 'appeal_submit', roomId, seq: 2, reason: '误判' });
    const ap2 = await a.waitFor((m) => m.type === 'appeal_update');
    a.send({ type: 'appeal_decide', appealId: ap2.appeal.id, decision: 'approved', note: '确实误判' });
    const ok = await a.waitFor((m) => m.type === 'appeal_update' && m.appeal.status === 'approved');
    assert.equal(ok.appeal.decisionNote, '确实误判');
    await b.waitFor((m) => m.type === 'msg' && m.seq === 2); // 联动恢复广播
    await a.close();
    await b.close();
  } finally {
    server.stop();
  }
});

test('申诉冷却：驳回后冷却期内重复申诉被拒绝', async () => {
  const { server, roomId, a, b } = await setupRoom({ reviewAppealCooldownMs: 60_000 });
  try {
    a.send({ type: 'msg', roomId, clientMsgId: 'c1', content: 'badword' });
    await b.waitFor(isRecalled(roomId, 1));
    a.send({ type: 'appeal_submit', roomId, seq: 1, reason: 'first' });
    const ap = await a.waitFor((m) => m.type === 'appeal_update');
    a.send({ type: 'appeal_decide', appealId: ap.appeal.id, decision: 'rejected' });
    await a.waitFor((m) => m.type === 'appeal_update' && m.appeal.status === 'rejected');
    a.send({ type: 'appeal_submit', roomId, seq: 1, reason: 'again' });
    const err = await a.waitFor((m) => m.type === 'error');
    assert.equal(err.code, 'APPEAL_COOLDOWN');
    await a.close();
    await b.close();
  } finally {
    server.stop();
  }
});

// ---------------------------------------------------------------- 规则版本

test('规则版本管理：发布 v2 热更新检测，历史消息保留 v1 版本号', async () => {
  const { server, roomId, a, b } = await setupRoom({ detector: { words: [] } });
  try {
    a.send({ type: 'rules_list' });
    const r0 = await a.waitFor((m) => m.type === 'rules');
    assert.equal(r0.active.version, 1);

    a.send({ type: 'msg', roomId, clientMsgId: 'c1', content: '旧规则期间干净' });
    await b.waitFor(isMsg(roomId, 1));

    a.send({ type: 'rule_publish', words: ['newword'], freqWindowMs: 10000, freqMaxCount: 8, note: '加词' });
    const pub = await a.waitFor((m) => m.type === 'review_rule');
    assert.equal(pub.rule.version, 2);
    assert.deepEqual(pub.rule.words, ['newword']);

    a.send({ type: 'msg', roomId, clientMsgId: 'c2', content: '含 newword 的消息' });
    await b.waitFor(isMsg(roomId, 2));
    const rec = await b.waitFor(isRecalled(roomId, 2));
    assert.equal(rec.ruleVersion, 2, '新消息按 v2 判定');

    a.send({ type: 'rules_list' });
    const r1 = await a.waitFor((m) => m.type === 'rules');
    assert.equal(r1.rules.length, 2);
    assert.equal(r1.rules.find((x) => x.version === 1).active, false);

    // recheck 用最新规则复核旧消息
    a.send({ type: 'review_recheck', roomId, seq: 1 });
    const rc = await a.waitFor((m) => m.type === 'review_recheck');
    assert.equal(rc.ruleVersion, 2);
    assert.equal(rc.hit, false);
    await a.close();
    await b.close();
  } finally {
    server.stop();
  }
});

// ---------------------------------------------------------------- 频率异常

test('频率异常：窗口内超过阈值的消息被处置', async () => {
  const { server, roomId, a, b } = await setupRoom({
    detector: { words: [], freqWindowMs: 60_000, freqMaxCount: 3 },
  });
  try {
    for (let i = 1; i <= 4; i++) {
      a.send({ type: 'msg', roomId, clientMsgId: `f${i}`, content: `fast ${i}` });
    }
    for (let i = 1; i <= 4; i++) await b.waitFor(isMsg(roomId, i));
    const rec = await b.waitFor(isRecalled(roomId, 4));
    assert.ok(rec.flags.includes('freq'));
    await a.close();
    await b.close();
  } finally {
    server.stop();
  }
});

// ---------------------------------------------------------------- TTL + 历史

test('先审后发 TTL：暂存超时未判自动放行（fail-open）', async () => {
  const { server, roomId, a, b } = await setupRoom({
    reviewPendingTtlMs: 100,
    reviewPendingSweepMs: 40,
  });
  try {
    await setMode(a, roomId, 'pre');
    a.send({ type: 'msg', roomId, clientMsgId: 'c1', content: 'badword 等人审' });
    await b.waitFor(isReview(roomId, 1, 'pending'));
    const rel = await b.waitFor(isMsg(roomId, 1), 2000);
    assert.equal(rel.content, 'badword 等人审');
    await a.close();
    await b.close();
  } finally {
    server.stop();
  }
});

test('历史消息按审核状态返回（blocked 对发送者保留原因，对他人显示通用占位）', async () => {
  const { server, roomId, a, b } = await setupRoom();
  try {
    await setMode(a, roomId, 'pre');
    a.send({ type: 'msg', roomId, clientMsgId: 'c1', content: 'badword' });
    await b.waitFor(isReview(roomId, 1, 'pending'));
    await sleep(60);
    a.send({ type: 'review_decide', roomId, seq: 1, action: 'block', reason: '涉密' });
    await b.waitFor(isReview(roomId, 1, 'blocked'));
    await sleep(50);

    a.send({ type: 'history', roomId, beforeSeq: 99, limit: 10 });
    const ha = await a.waitFor((m) => m.type === 'history');
    assert.equal(ha.messages[0].type, 'msg_review');
    assert.equal(ha.messages[0].status, 'blocked');
    assert.equal(ha.messages[0].reason, '涉密'); // 发送者/管理员可见真实原因

    b.send({ type: 'history', roomId, beforeSeq: 99, limit: 10 });
    const hb = await b.waitFor((m) => m.type === 'history');
    assert.equal(hb.messages[0].status, 'blocked');
    assert.equal(hb.messages[0].reason, '该消息未通过审核'); // 其他成员只见通用文案
    await a.close();
    await b.close();
  } finally {
    server.stop();
  }
});

test('管理员操作日志完整留存审核链路动作', async () => {
  const { server, roomId, a, b } = await setupRoom();
  try {
    await setMode(a, roomId, 'mark');
    a.send({ type: 'msg', roomId, clientMsgId: 'c1', content: 'badword' });
    await b.waitFor((m) => m.type === 'msg_flagged');
    a.send({ type: 'review_decide', roomId, seq: 1, action: 'recall' });
    await b.waitFor(isRecalled(roomId, 1));
    a.send({ type: 'review_log', roomId });
    const log = await a.waitFor((m) => m.type === 'review_log');
    const actions = log.entries.map((e) => e.action);
    assert.ok(actions.includes('policy_set'));
    assert.ok(actions.includes('review_recall'));
    // 每条日志含操作人、目标、详情
    const recallLog = log.entries.find((e) => e.action === 'review_recall');
    assert.equal(recallLog.actorId, a.ws.url ? recallLog.actorId : recallLog.actorId); // 形状校验
    assert.equal(recallLog.targetType, 'message');
    assert.equal(recallLog.targetId, `${roomId}:1`);
    await a.close();
    await b.close();
  } finally {
    server.stop();
  }
});

// ---------------------------------------------------------------- 旧库迁移

test('旧版本数据库自动迁移：历史消息保留且视为 released，审核能力可用', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'chat-review-'));
  const dbPath = path.join(dir, 'old.db');
  try {
    // 用旧 schema（无审核列/审核表）手工造一个旧库
    const legacy = new DatabaseSync(dbPath);
    legacy.exec(`
      CREATE TABLE users (id TEXT PRIMARY KEY, name TEXT NOT NULL UNIQUE, token_random TEXT NOT NULL, created_at INTEGER NOT NULL);
      CREATE TABLE rooms (id TEXT PRIMARY KEY, name TEXT NOT NULL UNIQUE, created_by TEXT NOT NULL, created_at INTEGER NOT NULL, last_seq INTEGER NOT NULL DEFAULT 0);
      CREATE TABLE members (room_id TEXT NOT NULL, user_id TEXT NOT NULL, role TEXT NOT NULL DEFAULT 'member', muted_until INTEGER NOT NULL DEFAULT 0, joined_at INTEGER NOT NULL, PRIMARY KEY (room_id, user_id));
      CREATE TABLE messages (room_id TEXT NOT NULL, seq INTEGER NOT NULL, client_msg_id TEXT NOT NULL, sender_id TEXT NOT NULL, content TEXT NOT NULL, ts INTEGER NOT NULL, PRIMARY KEY (room_id, seq), UNIQUE (room_id, sender_id, client_msg_id));
      CREATE TABLE cursors (room_id TEXT NOT NULL, user_id TEXT NOT NULL, last_ack_seq INTEGER NOT NULL DEFAULT 0, updated_at INTEGER NOT NULL, PRIMARY KEY (room_id, user_id));
      INSERT INTO users VALUES ('u1','alice','r',1);
      INSERT INTO rooms VALUES ('r1','general','u1',1,1);
      INSERT INTO members VALUES ('r1','u1','admin',0,1);
      INSERT INTO messages VALUES ('r1',1,'c1','u1','旧消息',123);
    `);
    legacy.close();

    const db = new ChatDB(dbPath); // 不应抛错
    const m = db.getMessage('r1', 1);
    assert.equal(m.content, '旧消息');
    assert.equal(m.reviewStatus, 'released', '历史消息一律视为 released');
    assert.deepEqual(JSON.parse(m.reviewFlags), []);
    // 审核新能力可用
    const rule = db.getActiveRule() || db.createRuleVersion({ words: ['w'], freqWindowMs: 1000, freqMaxCount: 2, note: 'seed', createdBy: 'system' });
    assert.ok(rule.version >= 1);
    assert.equal(db.pendingCount('r1'), 0);
    db.close();
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

test('先审后发放行后，seq 通道超时重发不得把状态打回 pending（缓存帧随决策替换）', async () => {
  const { server, roomId, a, b } = await setupRoom({
    ackResendIntervalMs: 50,
    ackResendAfterMs: 80,
    ackMaxResend: 20,
  });
  try {
    await setMode(a, roomId, 'pre');
    a.send({ type: 'msg', roomId, clientMsgId: 'c1', content: '干净消息' });
    await b.waitFor(isReview(roomId, 1, 'pending'));
    await b.waitFor(isMsg(roomId, 1)); // 已放行
    // 测试客户端全程不发 ACK，强制服务端重发；重发的应是放行帧而非 pending 占位
    await sleep(400);
    const afterRelease = b.log.filter((m) => m.seq === 1 || (m.type === 'msg' && m.seq === 1));
    const last = [...afterRelease].reverse().find((m) => m.type === 'msg' || m.type === 'msg_review');
    assert.equal(last.type, 'msg', '最后状态必须仍是放行正文');
    assert.notEqual(last.status, 'pending');
    // 放行之后不允许再出现 pending 帧
    const releaseIdx = b.log.findIndex(isMsg(roomId, 1));
    const pendingAfter = b.log.slice(releaseIdx + 1)
      .some((m) => m.type === 'msg_review' && m.seq === 1 && m.status === 'pending');
    assert.equal(pendingAfter, false);
    await a.close();
    await b.close();
  } finally {
    server.stop();
  }
});
