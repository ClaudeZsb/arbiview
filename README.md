# ArbiView

Binance 与 Bybit 跨所永续合约资金费率套利看板。仓库是一个 monorepo：

- `app/`：Next.js 前端，只负责展示、交互和调用 API。
- `services/api/`：Rust/Axum 后端，负责行情聚合、CMC 前 200 筛选、机会计算、账户查询和双腿开平仓。

## 本地运行

需要 Node.js 20+ 与 Rust stable。

```bash
cp .env.example .env.local
npm install
npm run dev
```

- 前端：<http://localhost:3000>
- 后端健康检查：<http://localhost:8080/health>

`npm run dev` 会同时启动前后端。

## 交易模式

默认 `ARBIVIEW_TRADING_MODE=paper`，所有开平仓都在后端内存中模拟，不发送真实订单。

启用真实交易需要在 `.env.local` 配置：

```env
ARBIVIEW_TRADING_MODE=live
BINANCE_API_KEY=
BINANCE_API_SECRET=
BYBIT_API_KEY=
BYBIT_API_SECRET=
```

建议使用仅允许合约交易、禁止提现并绑定服务器 IP 的 API Key。后端不会通过 API 返回密钥。

## API

- `GET /api/opportunities`
- `GET /api/account/summary`
- `GET /api/positions`
- `POST /api/trades/open`
- `POST /api/positions/:id/close`

开仓请求：

```json
{
  "opportunityId": "BTC-Binance-Bybit",
  "notionalUsdt": 1000,
  "leverage": 1
}
```

真实双腿交易不是原子操作。第二腿失败时后端会尝试用 `reduceOnly` 补偿平掉第一腿；补偿也失败时必须人工处理。

真实交易保护：

- 下单前校验两所可用保证金，并预留 5% 安全空间。
- 自动识别 Binance/Bybit 单向或双向持仓模式。
- 每笔订单携带唯一客户端订单 ID；响应不确定时按该 ID 查询，禁止盲目重试。
- Binance 使用成交结果响应，Bybit 主动轮询订单，只有确认 `Filled` 才视为成功。
- `MAX_SLIPPAGE_BPS` 控制最大不利滑点，默认 50 bps（0.5%）。
- 第二腿失败时自动 `reduceOnly` 补偿；无法确认或补偿失败会返回 `NAKED_EXPOSURE`。
- 账户接口返回 `unhedgedLegs`，前端会用红色警报展示未配对仓位。
- 两腿使用并发市价单提交，减少单腿方向暴露时间。
- 每腿实际名义价值距离目标超过 `POSITION_TOLERANCE_USDT`（默认 10 USDT）时，优先对不足腿补单。
- 补单后两腿差额仍超出容差时，对较大腿执行 `reduceOnly` 减仓，与较小腿对齐。
## Telegram Bot

Rust 后端可像 Freqtrade 一样以 Telegram long polling 方式运行 Bot。Bot 直接调用后端现有的行情与交易服务，支持：

- `/opportunities` 查询当前资费套利机会并从 Inline Keyboard 选择开仓；
- `/positions` 查询持仓，并执行加仓、减仓、调整杠杆和完全平仓；
- `/account` 查询各交易所余额和账户盈亏；
- `/open TOKEN 金额 [杠杆]`、`/reduce TOKEN 金额`、`/leverage TOKEN 杠杆`、`/close TOKEN` 精确管理仓位。

所有会修改仓位的操作都需要二次确认。Bot 首先校验 `chat_id`，配置 `TELEGRAM_AUTHORIZED_USERS` 后还会校验发出命令的用户 ID；群组 Topic 可通过 `TELEGRAM_TOPIC_ID` 限定。

在 `services/api/.env.local` 或后端服务的环境文件中配置：

```dotenv
TELEGRAM_ENABLED=true
TELEGRAM_BOT_TOKEN=123456:replace-me
TELEGRAM_CHAT_ID=123456789
TELEGRAM_TOPIC_ID=
TELEGRAM_AUTHORIZED_USERS=123456789
```

不要让两个进程使用同一个 Bot Token 同时执行 long polling，否则它们会争抢 updates。配置修改后重启 `arbiview-api` 即可。
