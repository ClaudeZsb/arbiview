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
