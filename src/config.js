'use strict';

/**
 * 全局配置。全部支持环境变量覆盖，便于测试与部署。
 */
module.exports = {
  // 服务监听
  port: Number(process.env.PORT || 8080),
  host: process.env.HOST || '0.0.0.0',

  // SQLite 文件路径，':memory:' 仅用于测试
  dbPath: process.env.CHAT_DB_PATH || 'chat.db',

  // 连接管理
  maxConnections: Number(process.env.MAX_CONNECTIONS || 1000), // 全局最大并发连接
  maxConnectionsPerUser: Number(process.env.MAX_CONNECTIONS_PER_USER || 3), // 单用户最大连接（多端）
  heartbeatIntervalMs: Number(process.env.HEARTBEAT_INTERVAL_MS || 30_000), // ping 周期
  heartbeatTimeoutMs: Number(process.env.HEARTBEAT_TIMEOUT_MS || 75_000), // 超过该时长无 pong 判定死亡

  // 可靠投递
  ackResendIntervalMs: Number(process.env.ACK_RESEND_INTERVAL_MS || 2_000), // 未 ACK 重发扫描周期
  ackResendAfterMs: Number(process.env.ACK_RESEND_AFTER_MS || 3_000), // 发送后多久未收到 ACK 触发重发
  ackMaxResend: Number(process.env.ACK_MAX_RESEND || 5), // 单条消息最大重发次数，超限断开连接
  maxUnackedPerConn: Number(process.env.MAX_UNACKED_PER_CONN || 1_000), // 单连接未 ACK 积压上限（背压）

  // 消息
  maxContentLength: Number(process.env.MAX_CONTENT_LENGTH || 4_000), // 单条消息最大字符数
  syncBatchSize: Number(process.env.SYNC_BATCH_SIZE || 500), // 断线补发单批最大条数
  historyMaxLimit: Number(process.env.HISTORY_MAX_LIMIT || 100), // 历史消息单次拉取上限

  // 发送限流（令牌桶，按用户）
  rateLimitPerSec: Number(process.env.RATE_LIMIT_PER_SEC || 10),
  rateLimitBurst: Number(process.env.RATE_LIMIT_BURST || 20),

  // —— 消息审核 ——
  reviewEnabled: (process.env.REVIEW_ENABLED ?? '1') !== '0', // 总开关
  // 房间未单独配置策略时的默认处置模式：pre 先审后发 / post 先发后撤 / mark 仅标记
  reviewDefaultMode: process.env.REVIEW_DEFAULT_MODE || 'post',
  // 频率异常检测：每用户每房间滑动窗口
  reviewFreqWindowMs: Number(process.env.REVIEW_FREQ_WINDOW_MS || 10_000),
  reviewFreqMaxCount: Number(process.env.REVIEW_FREQ_MAX_COUNT || 8),
  // 检测服务调用
  reviewDetectTimeoutMs: Number(process.env.REVIEW_DETECT_TIMEOUT_MS || 800), // 超时后降级（fail-open，不阻塞主链路）
  reviewQueueFetchLimit: Number(process.env.REVIEW_QUEUE_LIMIT || 100), // 审核队列单次拉取上限
  reviewAppealCooldownMs: Number(process.env.REVIEW_APPEAL_COOLDOWN_MS || 60_000), // 同消息重复申诉冷却
  // 先审后发：待审消息超过该时长自动放行（fail-open，0=关闭自动放行）
  reviewPendingTtlMs: Number(process.env.REVIEW_PENDING_TTL_MS || 0),
  reviewPendingSweepMs: Number(process.env.REVIEW_PENDING_SWEEP_MS || 5_000),

  // 演示用鉴权：token 签名密钥（生产环境务必替换）
  authSecret: process.env.AUTH_SECRET || 'dev-secret-change-me',
};
