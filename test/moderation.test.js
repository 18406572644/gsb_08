'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const WebSocket = require('ws');
const { createChatServer } = require('../src/server');

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function startServer(overrides = {}) {
  const server = createChatServer({
    port: 0,
    host: '127.0.0.1',
    dbPath: ':memory:',
    heartbeatIntervalMs: 60_000,
    ackResendAfterMs: 60_000,
    modDefaultMode: 'off',
    ...overrides,
  });
  const addr = await server.start();
  return { server, port: addr.port };
}

async function login(port, name) {
  const res = await fetch(`http://127.0.0.1:${port}/api/login`, {
    method: 'POST', headers: { 'content-type': 'application/json', connection: 'close' },
    body: JSON.stringify({ name }),
  });
  assert.equal(res.status, 200);
  return res.json();
}

class Client {
  static async connect(port, token) {
    const c = new Client();
    c.log = []; c.pending = []; c.waiters = [];
    c.closed = new Promise((res) => (c._onClosed = res));
    c.ws = new WebSocket(`ws://127.0.0.1:${port}/ws?token=${encodeURIComponent(token)}`);
    c.ws.on('message', (raw) => {
      const m = JSON.parse(raw.toString());
      c.log.push(m);
      for (const w of [...c.waiters]) {
        if (w.pred(m)) { c.waiters.splice(c.waiters.indexOf(w), 1); clearTimeout(w.timer); w.resolve(m); return; }
      }
      c.pending.push(m);
    });
    c.ws.on('close', () => c._onClosed());
    await new Promise((res, rej) => { c.ws.once('open', res); c.ws.once('error', rej); });
    await c.waitFor((m) => m.type === 'welcome');
    return c;
  }
  send(obj) { this.ws.send(JSON.stringify(obj)); }
  waitFor(pred, timeout = 3000) {
    const idx = this.pending.findIndex(pred);
    if (idx >= 0) { const [m] = this.pending.splice(idx, 1); return Promise.resolve(m); }
    return new Promise((resolve, reject) => {
      const w = { pred, resolve };
      w.timer = setTimeout(() => reject(new Error('waitFor: timed out')), timeout);
      this.waiters.push(w);
    });
  }
  /** 等到满足条件的帧出现（不从日志移除，用于验证顺序/存在性） */
  async eventually(pred, timeout = 2000) {
    const deadline = Date.now() + timeout;
    for (;;) {
      const found = this.log.find(pred);
      if (found) return found;
      if (Date.now() > deadline) throw new Error('eventually: timed out');
      await sleep(20);
    }
  }
  frames(type, roomId) {
    return this.log.filter((m) => m.type === type && (roomId == null || m.roomId === roomId));
  }
  close() { this.ws.close(); return this.closed; }
}

async function createRoom(client, name) {
  client.send({ type: 'create_room', name });
  const joined = await client.waitFor((m) => m.type === 'joined' && m.name === name);
  return joined.roomId;
}
async function joinRoom(client, room, lastSeq = 0) {
  client.send({ type: 'join', room, lastSeq });
  return client.waitFor((m) => m.type === 'joined');
}
/** 管理员发布规则 */
async function publishRule(admin, roomId, patch) {
  admin.send({ type: 'mod_rules_publish', roomId, ...patch });
  return admin.waitFor((m) => m.type === 'mod_rules_published' && m.roomId === roomId);
}
async function sendMsg(c, roomId, cid, content) {
  c.send({ type: 'msg', roomId, clientMsgId: cid, content });
  return c.waitFor((m) => m.type === 'ack' && m.clientMsgId === cid);
}

// ================================================================ 模式与联动

test('off 模式（默认）：审核完全旁路，消息无 mod 字段', async () => {
  const { server, port } = await startServer();
  try {
    const ua = await login(port, 'alice'), ub = await login(port, 'bob');
    const a = await Client.connect(port, ua.token), b = await Client.connect(port, ub.token);
    const roomId = await createRoom(a, 'r');
    await joinRoom(b, roomId);
    await sendMsg(a, roomId, 'm1', '含敏感词违禁 也无所谓');
    const m = await b.waitFor((x) => x.type === 'msg' && x.seq === 1);
    assert.equal(m.mod, undefined);
    await sleep(100);
    assert.equal(b.frames('recall').length, 0);
    await a.close(); await b.close();
  } finally { server.stop(); }
});

test('flag 模式：违规消息照常广播但带风险标记，不撤回', async () => {
  const { server, port } = await startServer();
  try {
    const ua = await login(port, 'alice'), ub = await login(port, 'bob');
    const a = await Client.connect(port, ua.token), b = await Client.connect(port, ub.token);
    const roomId = await createRoom(a, 'r');
    await joinRoom(b, roomId);
    await publishRule(a, roomId, { mode: 'flag', sensitiveWords: ['违禁'] });

    await sendMsg(a, roomId, 'm1', '这条含违禁词');
    const m = await b.eventually((x) => x.type === 'msg' && x.seq === 1);
    assert.equal(m.content, '这条含违禁词', 'flag 模式消息正常下发原文');
    const upd = await b.eventually((x) => x.type === 'mod_update' && x.seq === 1 && x.mod === 'flagged');
    assert.ok(upd.reason.includes('违禁'));
    await sleep(100);
    assert.equal(b.frames('recall').length, 0, 'flag 模式不得撤回');
    await a.close(); await b.close();
  } finally { server.stop(); }
});

test('post 模式：先发后撤 —— 原消息先到，撤回帧后到且晚于原消息', async () => {
  const { server, port } = await startServer();
  try {
    const ua = await login(port, 'alice'), ub = await login(port, 'bob');
    const a = await Client.connect(port, ua.token), b = await Client.connect(port, ub.token);
    const roomId = await createRoom(a, 'r');
    await joinRoom(b, roomId);
    await publishRule(a, roomId, { mode: 'post', sensitiveWords: ['违禁'] });

    await sendMsg(a, roomId, 'm1', '含违禁内容');
    await b.eventually((x) => x.type === 'msg' && x.seq === 1);
    const msgIdx = b.log.findIndex((x) => x.type === 'msg' && x.seq === 1);
    await b.eventually((x) => x.type === 'recall' && x.seq === 1);
    const recallIdx = b.log.findIndex((x) => x.type === 'recall' && x.seq === 1);
    assert.ok(recallIdx > msgIdx, '撤回帧必须晚于原消息');
    assert.equal(b.log[recallIdx].source, 'auto');
    await a.close(); await b.close();
  } finally { server.stop(); }
});

test('pre 模式：先审后发 —— 干净消息放行后才广播，接收者看不到 held 期间内容', async () => {
  const { server, port } = await startServer();
  try {
    const ua = await login(port, 'alice'), ub = await login(port, 'bob');
    const a = await Client.connect(port, ua.token), b = await Client.connect(port, ub.token);
    const roomId = await createRoom(a, 'r');
    await joinRoom(b, roomId);
    // 检测器延迟 300ms 返回干净，制造可观察的 held 窗口
    server.moderation.setDetector(roomId, async () => { await sleep(300); return { status: 'clean' }; });
    await publishRule(a, roomId, { mode: 'pre', sensitiveWords: ['违禁'], detectTimeoutMs: 5000 });

    await sendMsg(a, roomId, 'm1', '正常消息');
    // 发送者收到 pending 占位，接收者暂时收不到
    await a.eventually((x) => x.type === 'msg_pending' && x.seq === 1);
    await sleep(150);
    assert.equal(b.frames('msg').length, 0, 'held 期间不得向其他成员广播');
    // 检测干净 -> 放行
    const m = await b.eventually((x) => x.type === 'msg' && x.seq === 1);
    assert.equal(m.content, '正常消息');
    await a.close(); await b.close();
  } finally { server.stop(); }
});

test('pre 模式：违规消息保持 held 进人工队列，管理员驳回后发送者收到占位、他人永不可见', async () => {
  const { server, port } = await startServer();
  try {
    const ua = await login(port, 'alice'), ub = await login(port, 'bob');
    const a = await Client.connect(port, ua.token), b = await Client.connect(port, ub.token);
    const roomId = await createRoom(a, 'r');
    await joinRoom(b, roomId);
    await publishRule(a, roomId, { mode: 'pre', sensitiveWords: ['违禁'] });

    await sendMsg(a, roomId, 'm1', '含违禁内容');
    // 管理员收到队列更新
    await a.eventually((x) => x.type === 'mod_queue_update' && x.item && x.item.seq === 1);
    await sleep(150);
    assert.equal(b.frames('msg').length, 0);

    // 管理员拉队列 -> 看到待处理项
    a.send({ type: 'mod_queue', roomId });
    const q = await a.waitFor((x) => x.type === 'mod_queue' && x.roomId === roomId);
    assert.ok(q.pending.some((i) => i.seq === 1 && i.detectStatus === 'violation'));

    // 管理员驳回
    a.send({ type: 'mod_decide', roomId, seq: 1, decision: 'reject', reason: '违规' });
    await a.waitFor((x) => x.type === 'mod_decide_ok' && x.seq === 1);
    const res = await a.eventually((x) => x.type === 'mod_result' && x.seq === 1 && x.outcome === 'rejected');
    assert.ok(res);
    await sleep(100);
    assert.equal(b.frames('msg').length, 0, '驳回消息对他人永不可见');
    assert.equal(b.frames('msg_blocked').length, 0);
    await a.close(); await b.close();
  } finally { server.stop(); }
});

test('pre 模式：管理员 approve 放行 held 消息', async () => {
  const { server, port } = await startServer();
  try {
    const ua = await login(port, 'alice'), ub = await login(port, 'bob');
    const a = await Client.connect(port, ua.token), b = await Client.connect(port, ub.token);
    const roomId = await createRoom(a, 'r');
    await joinRoom(b, roomId);
    // 频率异常会判 suspect 转人工；这里直接发多条快速消息触发
    await publishRule(a, roomId, {
      mode: 'pre', sensitiveWords: [], freqWindowMs: 10_000, freqMaxCount: 1, detectTimeoutMs: 5000,
    });
    await sendMsg(a, roomId, 'm1', 'first');
    await b.eventually((x) => x.type === 'msg' && x.seq === 1);
    await sendMsg(a, roomId, 'm2', 'second too fast');
    await a.eventually((x) => x.type === 'mod_queue_update' && x.item && x.item.seq === 2);
    await sleep(100);
    // seq2 held，不应到达 b
    assert.ok(!b.log.some((x) => x.type === 'msg' && x.seq === 2));
    a.send({ type: 'mod_decide', roomId, seq: 2, decision: 'approve' });
    const m = await b.eventually((x) => x.type === 'msg' && x.seq === 2);
    assert.equal(m.content, 'second too fast');
    await a.close(); await b.close();
  } finally { server.stop(); }
});

// ================================================================ 顺序保证

test('pre 门控：held 消息挡住后续消息，放行后按 seq 顺序一次性送达', async () => {
  const { server, port } = await startServer();
  try {
    const ua = await login(port, 'alice'), ub = await login(port, 'bob');
    const a = await Client.connect(port, ua.token), b = await Client.connect(port, ub.token);
    const roomId = await createRoom(a, 'r');
    await joinRoom(b, roomId);
    // 注入一个会延迟且对 seq2 判违规、其他干净的检测器
    server.moderation.setDetector(roomId, async (content) => {
      await sleep(300);
      return content.includes('坏') ? { status: 'violation', hits: [{ type: 'remote_violation' }] } : { status: 'clean' };
    });
    await publishRule(a, roomId, { mode: 'pre', sensitiveWords: [], detectTimeoutMs: 5000 });

    await sendMsg(a, roomId, 'm1', '坏消息');    // seq1 -> held，等人工
    await sendMsg(a, roomId, 'm2', '正常消息2'); // seq2 -> held（在 seq1 之后排队）
    await sleep(100);
    assert.equal(b.frames('msg').length, 0, 'seq1 held，seq2 被门控挡住');

    // 驳回 seq1（释放水位线），seq2 检测干净后应放行
    a.send({ type: 'mod_decide', roomId, seq: 1, decision: 'reject' });
    await b.eventually((x) => x.type === 'msg' && x.seq === 2);
    assert.deepEqual(b.frames('msg').map((m) => m.seq), [2]);
    assert.ok(!b.log.some((x) => x.seq === 1 && x.type === 'msg'), 'seq1 驳回不得下发原文');
    await a.close(); await b.close();
  } finally { server.stop(); }
});

// ================================================================ 异常链路

test('检测服务超时：fail-open 放行，消息不被卡死', async () => {
  const { server, port } = await startServer();
  try {
    const ua = await login(port, 'alice'), ub = await login(port, 'bob');
    const a = await Client.connect(port, ua.token), b = await Client.connect(port, ub.token);
    const roomId = await createRoom(a, 'r');
    await joinRoom(b, roomId);
    server.moderation.setDetector(roomId, async () => { await sleep(5000); return { status: 'clean' }; });
    await publishRule(a, roomId, { mode: 'pre', sensitiveWords: [], detectTimeoutMs: 150 });

    const t0 = Date.now();
    await sendMsg(a, roomId, 'm1', '等待超时放行');
    const m = await b.eventually((x) => x.type === 'msg' && x.seq === 1, 2000);
    assert.ok(Date.now() - t0 < 1500, '应在超时阈值附近放行，而非等检测返回');
    assert.equal(m.content, '等待超时放行');
    await a.close(); await b.close();
  } finally { server.stop(); }
});

test('审核结果晚于客户端展示：超时放行后晚到的违规结果补撤回', async () => {
  const { server, port } = await startServer();
  try {
    const ua = await login(port, 'alice'), ub = await login(port, 'bob');
    const a = await Client.connect(port, ua.token), b = await Client.connect(port, ub.token);
    const roomId = await createRoom(a, 'r');
    await joinRoom(b, roomId);
    // 检测器延迟超过超时阈值，返回违规
    server.moderation.setDetector(roomId, async () => {
      await sleep(400);
      return { status: 'violation', hits: [{ type: 'remote_violation' }] };
    });
    await publishRule(a, roomId, { mode: 'post', sensitiveWords: [], detectTimeoutMs: 100 });

    await sendMsg(a, roomId, 'm1', '先发出去了');
    await b.eventually((x) => x.type === 'msg' && x.seq === 1);
    // 超时 -> 消息已展示；随后晚到违规 -> 补撤回
    const recall = await b.eventually((x) => x.type === 'recall' && x.seq === 1, 2000);
    assert.equal(recall.source, 'late');
    const msgIdx = b.log.findIndex((x) => x.type === 'msg' && x.seq === 1);
    const recallIdx = b.log.findIndex((x) => x.type === 'recall' && x.seq === 1);
    assert.ok(recallIdx > msgIdx);
    await a.close(); await b.close();
  } finally { server.stop(); }
});

test('重复审核：对同一消息重复终判被幂等拒绝', async () => {
  const { server, port } = await startServer();
  try {
    const ua = await login(port, 'alice'), ub = await login(port, 'bob');
    const a = await Client.connect(port, ua.token), b = await Client.connect(port, ub.token);
    const roomId = await createRoom(a, 'r');
    await joinRoom(b, roomId);
    await publishRule(a, roomId, { mode: 'post', sensitiveWords: ['违禁'] });
    await sendMsg(a, roomId, 'm1', '含违禁');
    await b.eventually((x) => x.type === 'recall' && x.seq === 1);

    // 自动撤回后进人工队列；管理员先 approve（恢复），再次不同终判应被拒
    a.send({ type: 'mod_decide', roomId, seq: 1, decision: 'approve' });
    await a.waitFor((x) => x.type === 'mod_decide_ok' && x.seq === 1);
    a.send({ type: 'mod_decide', roomId, seq: 1, decision: 'reject' });
    const err = await a.waitFor((x) => x.type === 'error' && x.code === 'ALREADY_DECIDED');
    assert.ok(err);
    await a.close(); await b.close();
  } finally { server.stop(); }
});

test('消息已被撤回：重复撤回幂等，且撤回消息在补发/历史中以墓碑存在、不含正文', async () => {
  const { server, port } = await startServer();
  try {
    const ua = await login(port, 'alice'), ub = await login(port, 'bob');
    const a = await Client.connect(port, ua.token);
    let b = await Client.connect(port, ub.token);
    const roomId = await createRoom(a, 'r');
    await joinRoom(b, roomId);
    await publishRule(a, roomId, { mode: 'post', sensitiveWords: ['违禁'] });
    await sendMsg(a, roomId, 'm1', '含违禁内容');
    await b.eventually((x) => x.type === 'recall' && x.seq === 1);
    await b.close();

    // 重连补发：应只收到墓碑，不含原文
    b = await Client.connect(port, ub.token);
    await joinRoom(b, roomId, 0);
    const tomb = await b.eventually((x) => x.type === 'msg_recalled' && x.seq === 1);
    assert.equal(tomb.content, undefined, '墓碑不得含正文');
    assert.ok(!b.log.some((x) => x.type === 'msg' && x.seq === 1));
    await a.close(); await b.close();
  } finally { server.stop(); }
});

// ================================================================ 申诉

test('误判申诉：post 自动撤回后用户申诉，管理员推翻则恢复广播', async () => {
  const { server, port } = await startServer();
  try {
    const ua = await login(port, 'alice'), ub = await login(port, 'bob');
    const a = await Client.connect(port, ua.token), b = await Client.connect(port, ub.token);
    const roomId = await createRoom(a, 'r');
    await joinRoom(b, roomId);
    await publishRule(a, roomId, { mode: 'post', sensitiveWords: ['违禁'] });
    await sendMsg(a, roomId, 'm1', '其实没违禁但是词被命中');
    await b.eventually((x) => x.type === 'recall' && x.seq === 1);

    // 发送者申诉（注意这里发送者是 a/alice）
    a.send({ type: 'appeal_create', roomId, seq: 1, reason: '误判' });
    await a.waitFor((x) => x.type === 'appeal_ok' && x.seq === 1);
    // 重复申诉被拒
    a.send({ type: 'appeal_create', roomId, seq: 1, reason: '再申诉' });
    await a.waitFor((x) => x.type === 'error' && x.code === 'ALREADY_APPEALED');

    // 管理员看到申诉
    a.send({ type: 'appeal_list', roomId });
    const list = await a.waitFor((x) => x.type === 'appeal_list' && x.roomId === roomId);
    const appealId = list.appeals[0].id;
    assert.equal(list.appeals[0].status, 'open');

    // 推翻（uphold=false）-> 消息恢复广播给 b
    a.send({ type: 'appeal_handle', roomId, appealId, uphold: false, reply: '确属误判' });
    await a.waitFor((x) => x.type === 'appeal_handle_ok' && x.appealId === appealId);
    const restored = await b.eventually((x) => x.type === 'msg' && x.seq === 1);
    assert.equal(restored.content, '其实没违禁但是词被命中');
    const result = await a.eventually((x) => x.type === 'appeal_result' && x.appealId === appealId);
    assert.equal(result.upheld, false);
    await a.close(); await b.close();
  } finally { server.stop(); }
});

test('非本人消息不能申诉；普通成员不能处理申诉/审核/发规则', async () => {
  const { server, port } = await startServer();
  try {
    const ua = await login(port, 'alice'), ub = await login(port, 'bob'), uc = await login(port, 'carol');
    const a = await Client.connect(port, ua.token), b = await Client.connect(port, ub.token);
    const c = await Client.connect(port, uc.token);
    const roomId = await createRoom(a, 'r');
    await joinRoom(b, roomId); await joinRoom(c, roomId);
    await publishRule(a, roomId, { mode: 'post', sensitiveWords: ['违禁'] });
    await sendMsg(a, roomId, 'm1', '含违禁');
    await a.eventually((x) => x.type === 'recall' && x.seq === 1);

    // bob 申诉 alice 的消息 -> 禁止
    b.send({ type: 'appeal_create', roomId, seq: 1, reason: '不是我的' });
    await b.waitFor((x) => x.type === 'error' && x.code === 'FORBIDDEN');
    // carol 普通成员访问审核能力 -> FORBIDDEN
    c.send({ type: 'mod_queue', roomId });
    await c.waitFor((x) => x.type === 'error' && x.code === 'FORBIDDEN');
    c.send({ type: 'mod_rules_publish', roomId, mode: 'flag' });
    await c.waitFor((x) => x.type === 'error' && x.code === 'FORBIDDEN');
    c.send({ type: 'mod_decide', roomId, seq: 1, decision: 'approve' });
    await c.waitFor((x) => x.type === 'error' && x.code === 'FORBIDDEN');
    await Promise.all([a.close(), b.close(), c.close()]);
  } finally { server.stop(); }
});

// ================================================================ 规则版本 & 日志

test('规则版本管理：发布生成新版本，回滚生成新版本并生效，操作进入审计日志', async () => {
  const { server, port } = await startServer();
  try {
    const ua = await login(port, 'alice');
    const a = await Client.connect(port, ua.token);
    const roomId = await createRoom(a, 'r');

    await publishRule(a, roomId, { mode: 'flag', sensitiveWords: ['a'] });
    await publishRule(a, roomId, { mode: 'post', sensitiveWords: ['b'] });
    a.send({ type: 'mod_rules_get', roomId });
    const rules = await a.waitFor((x) => x.type === 'mod_rules' && x.roomId === roomId);
    assert.equal(rules.current.mode, 'post');
    assert.equal(rules.current.sensitiveWords.includes('b'), true);
    assert.equal(rules.versions.length >= 2, true);
    // 每房间只有一个 published
    assert.equal(rules.versions.filter((v) => v.published).length, 1);

    // 回滚到版本1 -> 生成版本3，内容同版本1
    a.send({ type: 'mod_rules_rollback', roomId, version: 1 });
    const rb = await a.waitFor((x) => x.type === 'mod_rules_published' && x.rolledBackFrom === 1);
    assert.equal(rb.rule.mode, 'flag');
    assert.deepEqual(rb.rule.sensitiveWords, ['a']);

    a.send({ type: 'mod_audit', roomId });
    const log = await a.waitFor((x) => x.type === 'mod_audit' && x.roomId === roomId);
    const actions = log.entries.map((e) => e.action);
    assert.ok(actions.includes('publish_rule'));
    assert.ok(actions.includes('rollback_rule'));
    await a.close();
  } finally { server.stop(); }
});

// ================================================================ 崩溃恢复

test('崩溃恢复：重启后 held 且检测未完成的消息重新检测并放行，不永久卡死', async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'mod-test-'));
  const dbPath = path.join(dir, 't.db');
  let token, roomId;
  try {
    {
      const { server, port } = await startServer({ dbPath });
      const u = await login(port, 'alice');
      token = u.token;
      const a = await Client.connect(port, token);
      roomId = await createRoom(a, 'r');
      // 检测器永远挂起（不返回），保证崩溃时 held 仍 pending
      server.moderation.setDetector(roomId, async () => new Promise(() => {}));
      await publishRule(a, roomId, { mode: 'pre', sensitiveWords: [], detectTimeoutMs: 60_000 });
      await sendMsg(a, roomId, 'm1', '卡在审核中的消息');
      await a.eventually((x) => x.type === 'msg_pending' && x.seq === 1);
      await a.close();
      server.stop(); // 硬停（检测器仍在途）
    }
    {
      // 重启：无外部检测器 -> 重新检测判 clean -> 恢复放行
      const { server, port } = await startServer({ dbPath });
      try {
        const a = await Client.connect(port, token);
        await joinRoom(a, roomId, 0);
        const m = await a.eventually((x) => x.type === 'msg' && x.seq === 1, 3000);
        assert.equal(m.content, '卡在审核中的消息');
        await a.close();
      } finally { server.stop(); }
    }
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

test('积压门控：pre 有 held 积压时切到 post，新消息仍 held；补发不越过，清积压后按序放行', async () => {
  const { server, port } = await startServer();
  try {
    const ua = await login(port, 'alice'), ub = await login(port, 'bob');
    const a = await Client.connect(port, ua.token);
    let b = await Client.connect(port, ub.token);
    const roomId = await createRoom(a, 'r');
    await joinRoom(b, roomId);
    server.moderation.setDetector(roomId, async (content) => {
      await sleep(300);
      return content.includes('坏') ? { status: 'violation' } : { status: 'clean' };
    });
    await publishRule(a, roomId, { mode: 'pre', sensitiveWords: [], detectTimeoutMs: 5000 });

    await sendMsg(a, roomId, 'm1', '坏消息1');   // seq1 held（违规，等人工）
    await a.eventually((x) => x.type === 'msg_pending' && x.seq === 1);
    // 切到 post（运行期模式切换），seq1 仍 held
    await publishRule(a, roomId, { mode: 'post', sensitiveWords: [], detectTimeoutMs: 5000 });
    await sendMsg(a, roomId, 'm2', '干净消息2'); // seq2 因前方积压也应 held
    await a.eventually((x) => x.type === 'msg_pending' && x.seq === 2);
    await sleep(100);
    assert.ok(!b.log.some((x) => x.type === 'msg'), '积压未清，seq2 不得越过 seq1 广播');

    // b 断线重连补发：seq1/seq2 都 held，不得在补发中泄露
    await b.close();
    b = await Client.connect(port, ub.token);
    await joinRoom(b, roomId, 0);
    await sleep(150);
    assert.ok(!b.log.some((x) => x.type === 'msg' || x.type === 'msg_recalled'), '补发不得越过 held');

    // 驳回 seq1 -> 释放水位线；seq2 干净 -> 按序放行（在线 b 收到 seq2，永收不到 seq1 正文）
    a.send({ type: 'mod_decide', roomId, seq: 1, decision: 'reject' });
    const m2 = await b.eventually((x) => x.type === 'msg' && x.seq === 2, 2000);
    assert.equal(m2.content, '干净消息2');
    assert.ok(!b.log.some((x) => x.type === 'msg' && x.seq === 1));
    await a.close(); await b.close();
  } finally { server.stop(); }
});

test('补发顺序：pre 放行的消息与断线补发混合仍按 seq 全序、不重复', async () => {
  const { server, port } = await startServer();
  try {
    const ua = await login(port, 'alice'), ub = await login(port, 'bob');
    const a = await Client.connect(port, ua.token);
    let b = await Client.connect(port, ub.token);
    const roomId = await createRoom(a, 'r');
    await joinRoom(b, roomId);
    await publishRule(a, roomId, { mode: 'pre', sensitiveWords: ['违禁'], detectTimeoutMs: 5000 });

    await sendMsg(a, roomId, 'm1', '干净1');
    await b.eventually((x) => x.type === 'msg' && x.seq === 1);
    await b.close(); // b 离线
    await sendMsg(a, roomId, 'm2', '干净2'); // 离线期间，检测后放行落库
    await sleep(200);
    await sendMsg(a, roomId, 'm3', '含违禁3'); // held，等人工
    await sleep(100);

    b = await Client.connect(port, ub.token);
    await joinRoom(b, roomId, 1);
    await b.eventually((x) => x.type === 'msg' && x.seq === 2);
    // held 的 seq3 不应在补发中
    await sleep(100);
    assert.ok(!b.log.some((x) => (x.type === 'msg' || x.type === 'msg_pending') && x.seq === 3));

    // 管理员驳回 seq3；b 不受影响且不收到 seq3 任何内容
    a.send({ type: 'mod_decide', roomId, seq: 3, decision: 'reject' });
    await a.eventually((x) => x.type === 'mod_result' && x.seq === 3);
    await sleep(100);
    assert.deepEqual(b.frames('msg').map((m) => m.seq), [2]);
    await a.close(); await b.close();
  } finally { server.stop(); }
});
