"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
  ArrowDownUp, ArrowRight, ChevronDown, Clock3, Info, RefreshCw,
  Search, ShieldCheck, SlidersHorizontal, Sparkles, TrendingUp,
  WalletCards, X, Zap
} from "lucide-react";

const formatter = new Intl.NumberFormat("en-US", { maximumFractionDigits: 8 });

function price(value) {
  if (value >= 1000) return `$${formatter.format(value)}`;
  if (value >= 1) return `$${value.toLocaleString("en-US", { maximumFractionDigits: 4 })}`;
  return `$${value.toLocaleString("en-US", { maximumSignificantDigits: 6 })}`;
}

function money(value) {
  return `$${Number(value || 0).toLocaleString("en-US", {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2
  })}`;
}

function pct(value, digits = 3) {
  return `${value >= 0 ? "+" : ""}${(value * 100).toFixed(digits)}%`;
}

function duration(hours) {
  if (!Number.isFinite(hours)) return "—";
  if (hours < 1) return `${Math.max(1, Math.round(hours * 60))} 分钟`;
  if (hours < 48) return `${hours.toFixed(1)} 小时`;
  return `${(hours / 24).toFixed(1)} 天`;
}

function time(ts) {
  return new Intl.DateTimeFormat("zh-CN", {
    hour: "2-digit", minute: "2-digit", hour12: false
  }).format(new Date(ts));
}

function ExchangeLogo({ name }) {
  return <span className={`exchange-logo ${name.toLowerCase()}`}>{name === "Binance" ? "◈" : "◆"}</span>;
}

function Leg({ type, leg }) {
  return (
    <div className="leg">
      <div className="leg-head">
        <span className={`side ${type}`}>{type === "long" ? "LONG" : "SHORT"}</span>
        <span className="exchange"><ExchangeLogo name={leg.exchange} />{leg.exchange}</span>
      </div>
      <div className="leg-price">{price(type === "long" ? leg.ask : leg.bid)}</div>
      <div className="leg-meta">
        <span>资金费率 <b className={leg.rate < 0 ? "positive" : ""}>{pct(leg.rate, 4)}</b> / {leg.intervalHours}h</span>
        <span>下次结算 <b>{time(leg.nextFundingTime)}</b></span>
      </div>
    </div>
  );
}

function OpportunityCard({ item, index, onTrade }) {
  return (
    <article className="opportunity-card">
      <div className="rank">#{String(index + 1).padStart(2, "0")}</div>
      <div className="asset">
        <div className="asset-icon">{item.token.symbol.slice(0, 2)}</div>
        <div>
          <div className="asset-symbol">{item.token.symbol}<span>/USDT</span></div>
          <div className="asset-name">{item.token.name} · CMC #{item.token.rank}</div>
        </div>
      </div>
      <div className="legs">
        <Leg type="long" leg={item.long} />
        <div className="direction"><ArrowRight size={16} /></div>
        <Leg type="short" leg={item.short} />
      </div>
      <div className="metric apy">
        <span className="metric-label">资金费率 APY</span>
        <strong>{pct(item.apy, 1)}</strong>
        <small>{pct(item.fundingPerHour, 4)} / 小时</small>
      </div>
      <div className="metric spread">
        <span className="metric-label">开仓价差</span>
        <strong className={item.spread >= 0 ? "positive" : "negative"}>{pct(item.spread)}</strong>
        <small>{item.spread >= 0 ? "顺价开仓" : "逆价开仓"}</small>
      </div>
      <div className="metric breakeven">
        <span className="metric-label">预计回本</span>
        <strong>{duration(item.breakEvenHours)}</strong>
        <small>含双边开平仓费</small>
      </div>
      <button className="trade-button" onClick={() => onTrade(item)}><Zap size={13} />开仓</button>
    </article>
  );
}

function SpreadOpportunityCard({ item, index, onTrade }) {
  const fundingCostPerHour = Math.max(0, -item.fundingPerHour);
  const fundingImpact8h = item.fundingPerHour * 8;
  const edgeAfterFees = item.spread - item.fees;
  const costLimitHours = fundingCostPerHour > 0
    ? Math.max(0, edgeAfterFees) / fundingCostPerHour
    : null;
  return (
    <article className="spread-card">
      <div className="rank">#{String(index + 1).padStart(2, "0")}</div>
      <div className="asset">
        <div className="asset-icon">{item.token.symbol.slice(0, 2)}</div>
        <div><div className="asset-symbol">{item.token.symbol}<span>/USDT</span></div><div className="asset-name">CMC #{item.token.rank}</div></div>
      </div>
      <div className="spread-route">
        <div><i className="side-dot long" /><span>LONG · {item.long.exchange}</span><b>{price(item.long.ask)}</b></div>
        <ArrowRight size={15} />
        <div><i className="side-dot short" /><span>SHORT · {item.short.exchange}</span><b>{price(item.short.bid)}</b></div>
      </div>
      <div className="spread-stat"><span>可执行价差</span><strong>{pct(item.spread)}</strong><small>扣往返费后 {pct(edgeAfterFees)}</small></div>
      <div className="spread-stat"><span>Funding 影响</span><strong className={fundingImpact8h >= 0 ? "positive" : "negative"}>{fundingImpact8h >= 0 ? "预计收入 " : "预计成本 "}{pct(Math.abs(fundingImpact8h))} / 8h</strong><small>净值 {pct(item.fundingPerHour, 4)} / 小时</small></div>
      <div className="spread-stat"><span>成本承受时间</span><strong>{costLimitHours === null ? "无 Funding 成本" : duration(costLimitHours)}</strong><small>{costLimitHours === null ? "当前 Funding 反而增厚收益" : "Funding 消耗净价差的估算时间"}</small></div>
      <button className="trade-button" onClick={() => onTrade(item)}><Zap size={13} />开仓</button>
    </article>
  );
}

function TradeModal({ item, mode, onClose, onSubmit, busy }) {
  const [notional, setNotional] = useState(1000);
  const [leverage, setLeverage] = useState(1);
  const isLive = mode === "live";
  const modeKnown = mode === "live" || mode === "paper";
  return (
    <div className="modal-backdrop" onMouseDown={onClose}>
      <div className="trade-modal" onMouseDown={(e) => e.stopPropagation()}>
        <button className="modal-close" onClick={onClose}><X size={18} /></button>
        <div className="section-kicker"><Zap size={14} /> OPEN HEDGED POSITION</div>
        <div className="trade-title">
          <h3>建立 {item.token.symbol} 双腿仓位</h3>
          <span className={`execution-mode ${mode || "unknown"}`}>
            {isLive ? "LIVE · 真实下单" : mode === "paper" ? "PAPER · 模拟交易" : "模式读取中"}
          </span>
        </div>
        <div className="trade-route">
          <div><span>LONG · {item.long.exchange}</span><b>{price(item.long.ask)}</b></div>
          <ArrowRight />
          <div><span>SHORT · {item.short.exchange}</span><b>{price(item.short.bid)}</b></div>
        </div>
        <label>单腿名义价值（USDT）<input type="number" min="10" value={notional} onChange={(e) => setNotional(Number(e.target.value))} /></label>
        <label>杠杆<select value={leverage} onChange={(e) => setLeverage(Number(e.target.value))}>
          {[1, 2, 3, 5, 10, 20].map((x) => <option key={x} value={x}>{x}×</option>)}
        </select></label>
        <div className={`trade-warning ${isLive ? "live" : "paper"}`}><ShieldCheck size={16} />
          {isLive
            ? "真实交易模式：两腿将并发提交市价单；不足目标时优先补仓，差额超过 10 USDT 时自动减仓对齐。"
            : mode === "paper"
              ? "模拟交易模式：只在后端记录模拟仓位，不会向交易所发送订单。"
              : "尚未读取后端交易模式，暂时禁止提交。"}
        </div>
        <button className={`confirm-trade ${isLive ? "live" : ""}`} disabled={busy || !modeKnown} onClick={() => onSubmit({ opportunityId: item.id, notionalUsdt: notional, leverage })}>
          {busy ? "正在建立双腿…" : isLive ? "确认真实下单" : "确认模拟开仓"}
        </button>
      </div>
    </div>
  );
}

function AdjustModal({ position, type, onClose, onSubmit, busy }) {
  const [notional, setNotional] = useState(20);
  const [leverage, setLeverage] = useState(position.leverage || 1);
  const isIncrease = type === "increase";
  const isLeverage = type === "leverage";
  return (
    <div className="modal-backdrop" onMouseDown={(e) => e.target === e.currentTarget && onClose()}>
      <div className="trade-modal">
        <button className="modal-close" onClick={onClose}><X /></button>
        <div className="section-kicker"><Zap size={15} /> ADJUST HEDGED POSITION</div>
        <div className="trade-title"><h3>{isLeverage ? "调整" : isIncrease ? "增加" : "减少"} {position.token} {isLeverage ? "杠杆" : "双腿仓位"}</h3></div>
        {isLeverage
          ? <label>目标杠杆<select value={leverage} onChange={(e) => setLeverage(Number(e.target.value))}>{[1, 2, 3, 5, 10, 20].map((value) => <option key={value} value={value}>{value}×</option>)}</select></label>
          : <label>每腿调整金额（USDT）<input type="number" min="10" step="10" value={notional} onChange={(e) => setNotional(Number(e.target.value))} /></label>}
        <div className={`trade-warning ${isIncrease || isLeverage ? "live" : "paper"}`}><ShieldCheck size={16} />
          {isLeverage
            ? "将同时修改 Binance 与 Bybit 对应合约的杠杆，不会改变当前仓位数量。提高杠杆会降低可用保证金缓冲。"
            : isIncrease
            ? "两条腿将按该金额并发加仓，成交后根据交易所实际仓位补齐并对冲。"
            : "两条腿将按该金额并发减仓，成交后只减少较大腿完成金额对齐；全量退出请使用双腿平仓。"}
        </div>
        <button className={`confirm-trade ${isIncrease || isLeverage ? "live" : ""}`} disabled={busy || (!isLeverage && notional < 10)} onClick={() => onSubmit(isLeverage ? leverage : notional)}>
          {busy ? "正在调整双腿…" : `确认${isLeverage ? "调整杠杆" : isIncrease ? "加仓" : "减仓"}`}
        </button>
      </div>
    </div>
  );
}

function AccountBoard({ account, positions, onClose, onAdjust, busyId }) {
  return (
    <section className="account-board" id="account">
      <div className="section-head">
        <div>
          <div className="section-kicker"><WalletCards size={15} /> ACCOUNT & POSITIONS</div>
          <h2>账户与持仓 <span className={`mode-badge ${account?.mode}`}>{account?.mode || "offline"}</span></h2>
        </div>
        <div className="configured">{account?.configuredExchanges?.length ? `已连接 ${account.configuredExchanges.join(" · ")}` : "未配置交易所账户"}</div>
      </div>
      <div className="account-stats">
        <div><span>账户权益</span><strong>{money(account?.equityUsdt)}</strong></div>
        <div><span>可用余额</span><strong>{money(account?.availableUsdt)}</strong></div>
        <div><span>未实现盈亏</span><strong className={(account?.unrealizedPnl || 0) >= 0 ? "positive" : "negative"}>{money(account?.unrealizedPnl)}</strong></div>
        <div><span>活跃套利仓位</span><strong>{account?.activePositions || 0}</strong></div>
      </div>
      <div className="exchange-balances">
        {(account?.exchanges || []).map((item) => (
          <article key={item.exchange}>
            <div className="exchange-balance-head"><ExchangeLogo name={item.exchange} /><b>{item.exchange}</b><span>{item.exchange === "Bybit" ? "统一交易账户" : "USDT-M 永续账户"}</span></div>
            <div><span>可用金额</span><strong>{money(item.availableUsdt)}</strong></div>
            <div><span>账户权益</span><b>{money(item.equityUsdt)}</b></div>
            <div><span>未实现盈亏</span><b className={item.unrealizedPnl >= 0 ? "positive" : "negative"}>{money(item.unrealizedPnl)}</b></div>
          </article>
        ))}
      </div>
      <p className="balance-note">Bybit 可用金额为 Unified Account 的 USD 口径，会从保证金余额中扣除初始保证金、订单占用与抵押品折扣，因此可能低于账户权益。</p>
      {(account?.unhedgedLegs || []).length > 0 && (
        <div className="risk-alert">
          <b>检测到未对冲仓位</b>
          <span>以下单腿没有在另一交易所找到反向仓位，请立即核查：</span>
          {account.unhedgedLegs.map((leg) => (
            <code key={`${leg.exchange}-${leg.symbol}-${leg.side}`}>
              {leg.exchange} · {leg.symbol} · {leg.side.toUpperCase()} · {leg.quantity}
            </code>
          ))}
        </div>
      )}
      <div className="position-list">
        {positions.length === 0 && <div className="state compact">暂无持仓，可从下方机会列表建立模拟双腿仓位</div>}
        {positions.map((p) => (
          <details className="position-item" key={p.id}>
            <summary className="position-row">
              <div className="position-token"><ChevronDown size={14} /><div><b>{p.token}/USDT</b><span>{p.openedAt > 0 ? new Date(p.openedAt).toLocaleString("zh-CN") : "交易所实时仓位"}</span></div></div>
              <div><span>LONG · {p.long.exchange}</span><b>{p.long.quantity} @ {price(p.long.entryPrice)}</b></div>
              <div><span>SHORT · {p.short.exchange}</span><b>{p.short.quantity} @ {price(p.short.entryPrice)}</b></div>
              <div><span>名义价值 / 杠杆</span><b>{price(p.notionalUsdt)} · {p.long.leverage === p.short.leverage ? `${p.long.leverage}×` : `${p.long.leverage}× / ${p.short.leverage}×`}</b></div>
              <div><span>未实现盈亏</span><b className={p.unrealizedPnl >= 0 ? "positive" : "negative"}>{price(p.unrealizedPnl)}</b></div>
              <div className="position-actions">
                <button onClick={(event) => { event.preventDefault(); onAdjust(p, "increase"); }}>加仓</button>
                <button onClick={(event) => { event.preventDefault(); onAdjust(p, "reduce"); }}>减仓</button>
                <button onClick={(event) => { event.preventDefault(); onAdjust(p, "leverage"); }}>杠杆</button>
                <button disabled={busyId === p.id} onClick={(event) => { event.preventDefault(); onClose(p.id); }}>{busyId === p.id ? "平仓中…" : "平仓"}</button>
              </div>
            </summary>
            <div className="position-details">
              <div className="position-detail-head">
                <span>方向 / 交易所</span>
                <span>当前价格</span>
                <span>开仓均价</span>
                <span>仓位数量</span>
                <span>Funding Rate</span>
                <span>未实现收益</span>
                <span>累计 Funding</span>
              </div>
              {[p.long, p.short].map((leg) => (
                <div className="position-leg-detail" key={`${leg.exchange}-${leg.side}`}>
                  <div className="leg-detail-title"><i className={`side-dot ${leg.side}`} /><b>{leg.side.toUpperCase()} · {leg.exchange}</b></div>
                  <b>{price(leg.markPrice)}</b>
                  <b>{price(leg.entryPrice)}</b>
                  <b>{formatter.format(leg.quantity)}</b>
                  <b className={leg.fundingRate >= 0 ? "positive" : "negative"}>{pct(leg.fundingRate)}</b>
                  <b className={leg.unrealizedPnl >= 0 ? "positive" : "negative"}>{money(leg.unrealizedPnl)}</b>
                  <b className={leg.fundingEarned >= 0 ? "positive" : "negative"}>{money(leg.fundingEarned)}</b>
                </div>
              ))}
              <p>Funding 为交易所账户近 7 日该合约资金费净额；正数表示收到，负数表示支付。</p>
            </div>
          </details>
        ))}
      </div>
    </section>
  );
}

export default function Dashboard() {
  const [data, setData] = useState(null);
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(true);
  const [sort, setSort] = useState("apy");
  const [query, setQuery] = useState("");
  const [minApy, setMinApy] = useState(0);
  const [account, setAccount] = useState(null);
  const [positions, setPositions] = useState([]);
  const [tradeItem, setTradeItem] = useState(null);
  const [tradeBusy, setTradeBusy] = useState(false);
  const [adjustment, setAdjustment] = useState(null);
  const [closingId, setClosingId] = useState("");
  const [notice, setNotice] = useState("");
  const accountBackoffUntil = useRef(0);
  const marketBackoffUntil = useRef(0);

  const load = useCallback(async () => {
    if (Date.now() < marketBackoffUntil.current) return;
    setLoading(true);
    setError("");
    try {
      const response = await fetch("/backend/opportunities", { cache: "no-store" });
      const json = await response.json();
      if (!response.ok) throw new Error(json.detail || json.error);
      setData(json);
      marketBackoffUntil.current = 0;
    } catch (e) {
      marketBackoffUntil.current = Date.now() + 60_000;
      setError(e.message || "加载失败");
    } finally {
      setLoading(false);
    }
  }, []);

  const loadAccount = useCallback(async () => {
    if (Date.now() < accountBackoffUntil.current) return;
    try {
      const [summaryResponse, positionsResponse] = await Promise.all([
        fetch("/backend/account/summary", { cache: "no-store" }),
        fetch("/backend/positions", { cache: "no-store" })
      ]);
      const summary = await summaryResponse.json();
      const active = await positionsResponse.json();
      if (!summaryResponse.ok) throw new Error(summary.error);
      if (!positionsResponse.ok) throw new Error(active.error);
      setAccount(summary);
      setPositions(active);
      accountBackoffUntil.current = 0;
    } catch (e) {
      accountBackoffUntil.current = Date.now() + 60_000;
      setNotice(`账户服务：${e.message}`);
    }
  }, []);

  const loadAccountSummary = useCallback(async () => {
    if (Date.now() < accountBackoffUntil.current) return;
    try {
      const response = await fetch("/backend/account/summary", { cache: "no-store" });
      const summary = await response.json();
      if (!response.ok) throw new Error(summary.error);
      setAccount(summary);
      accountBackoffUntil.current = 0;
    } catch (e) {
      accountBackoffUntil.current = Date.now() + 60_000;
      setNotice(`账户服务：${e.message}`);
    }
  }, []);

  const loadFullPositions = useCallback(async () => {
    if (Date.now() < accountBackoffUntil.current) return;
    try {
      const response = await fetch("/backend/positions", { cache: "no-store" });
      const active = await response.json();
      if (!response.ok) throw new Error(active.error);
      setPositions(active);
      accountBackoffUntil.current = 0;
    } catch (e) {
      accountBackoffUntil.current = Date.now() + 60_000;
      setNotice(`持仓服务：${e.message}`);
    }
  }, []);

  useEffect(() => {
    load();
    loadAccount();
    const opportunityTimer = window.setInterval(load, 20_000);
    const accountTimer = window.setInterval(loadAccountSummary, 20_000);
    const fundingTimer = window.setInterval(loadFullPositions, 60 * 60_000);
    return () => {
      window.clearInterval(opportunityTimer);
      window.clearInterval(accountTimer);
      window.clearInterval(fundingTimer);
    };
  }, [load, loadAccount, loadAccountSummary, loadFullPositions]);

  const positionSubscriptionKey = useMemo(
    () => account?.mode === "live" ? positions
      .flatMap((position) => [position.long, position.short])
      .map((leg) => `${leg.exchange}:${leg.symbol}`)
      .sort()
      .join("|") : "",
    [account?.mode, positions]
  );

  useEffect(() => {
    const subscriptions = positionSubscriptionKey
      .split("|")
      .filter(Boolean)
      .map((item) => {
        const [exchange, symbol] = item.split(":");
        return { exchange, symbol };
      });
    if (subscriptions.length === 0) return undefined;

    let stopped = false;
    const sockets = [];
    const reconnectTimers = [];
    const pingTimers = [];
    const updateQuote = (exchange, symbol, markPrice, fundingRate) => {
      if (!Number.isFinite(markPrice)) return;
      setPositions((current) => current.map((position) => {
        const updateLeg = (leg) => {
          if (leg.exchange !== exchange || leg.symbol !== symbol) return leg;
          const unrealizedPnl = leg.side === "long"
            ? (markPrice - leg.entryPrice) * leg.quantity
            : (leg.entryPrice - markPrice) * leg.quantity;
          return {
            ...leg,
            markPrice,
            fundingRate: Number.isFinite(fundingRate) ? fundingRate : leg.fundingRate,
            unrealizedPnl
          };
        };
        const long = updateLeg(position.long);
        const short = updateLeg(position.short);
        return { ...position, long, short, unrealizedPnl: long.unrealizedPnl + short.unrealizedPnl };
      }));
    };
    const reconnect = (connect) => {
      if (!stopped) reconnectTimers.push(window.setTimeout(connect, 3_000));
    };

    const binanceSymbols = subscriptions.filter((item) => item.exchange === "Binance");
    const connectBinance = () => {
      if (stopped || binanceSymbols.length === 0) return;
      const streams = binanceSymbols
        .map((item) => `${item.symbol.toLowerCase()}@markPrice@1s`)
        .join("/");
      const socket = new WebSocket(`wss://fstream.binance.com/stream?streams=${streams}`);
      sockets.push(socket);
      socket.onmessage = (event) => {
        const message = JSON.parse(event.data);
        const quote = message.data || message;
        updateQuote("Binance", quote.s, Number(quote.p), Number(quote.r));
      };
      socket.onclose = () => reconnect(connectBinance);
      socket.onerror = () => socket.close();
    };

    const bybitSymbols = subscriptions.filter((item) => item.exchange === "Bybit");
    const connectBybit = () => {
      if (stopped || bybitSymbols.length === 0) return;
      const socket = new WebSocket("wss://stream.bybit.com/v5/public/linear");
      sockets.push(socket);
      let pingTimer;
      socket.onopen = () => {
        socket.send(JSON.stringify({
          op: "subscribe",
          args: bybitSymbols.map((item) => `tickers.${item.symbol}`)
        }));
        pingTimer = window.setInterval(() => {
          if (socket.readyState === WebSocket.OPEN) socket.send(JSON.stringify({ op: "ping" }));
        }, 20_000);
        pingTimers.push(pingTimer);
      };
      socket.onmessage = (event) => {
        const message = JSON.parse(event.data);
        if (!message.topic?.startsWith("tickers.")) return;
        const quote = Array.isArray(message.data) ? message.data[0] : message.data;
        const symbol = quote?.symbol || message.topic.slice("tickers.".length);
        updateQuote("Bybit", symbol, Number(quote?.markPrice), Number(quote?.fundingRate));
      };
      socket.onclose = () => {
        if (pingTimer) window.clearInterval(pingTimer);
        reconnect(connectBybit);
      };
      socket.onerror = () => socket.close();
    };

    connectBinance();
    connectBybit();
    return () => {
      stopped = true;
      reconnectTimers.forEach(window.clearTimeout);
      pingTimers.forEach(window.clearInterval);
      sockets.forEach((socket) => socket.close());
    };
  }, [positionSubscriptionKey]);

  useEffect(() => {
    if (!notice) return undefined;
    const timer = window.setTimeout(() => setNotice(""), 10_000);
    return () => window.clearTimeout(timer);
  }, [notice]);

  async function openTrade(request) {
    setTradeBusy(true);
    try {
      const response = await fetch("/backend/trades/open", {
        method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(request)
      });
      const json = await response.json();
      if (!response.ok) throw new Error(json.error);
      const orderSummary = json.execution?.orders
        ?.map((order) => `${order.exchange} ${order.status} #${order.orderId}`)
        .join(" · ");
      const balanceSummary = json.execution
        ? `Long ${money(json.execution.longNotionalUsdt)} / Short ${money(json.execution.shortNotionalUsdt)} · 第一阶段补单 ${json.execution.supplementOrders?.length || 0}（L ${json.execution.phaseOneLongAttempts || 0} / S ${json.execution.phaseOneShortAttempts || 0} 次）· 第二阶段减仓 ${json.execution.rebalanceOrders?.length || 0}（${json.execution.phaseTwoAttempts || 0} 次）`
        : "";
      setNotice([json.message, balanceSummary, orderSummary].filter(Boolean).join(" · "));
      setTradeItem(null);
      await loadAccount();
    } catch (e) { setNotice(`开仓失败：${e.message}`); }
    finally { setTradeBusy(false); }
  }

  async function closePosition(id) {
    setClosingId(id);
    try {
      const response = await fetch(`/backend/positions/${id}/close`, { method: "POST" });
      const json = await response.json();
      if (!response.ok) throw new Error(json.error);
      const orderSummary = json.execution?.orders
        ?.map((order) => `${order.exchange} ${order.status} #${order.orderId}`)
        .join(" · ");
      setNotice(orderSummary ? `${json.message} · ${orderSummary}` : json.message);
      await loadAccount();
    } catch (e) { setNotice(`平仓失败：${e.message}`); }
    finally { setClosingId(""); }
  }

  async function adjustPosition(notionalUsdt) {
    if (!adjustment) return;
    setTradeBusy(true);
    try {
      let response;
      if (adjustment.type === "increase") {
        const opportunity = data?.opportunities?.find((item) =>
          item.token.symbol === adjustment.position.token &&
          item.long.exchange === adjustment.position.long.exchange &&
          item.short.exchange === adjustment.position.short.exchange
        );
        if (!opportunity) throw new Error("当前机会列表中没有与该持仓方向一致的机会，无法安全加仓");
        response = await fetch("/backend/trades/open", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ opportunityId: opportunity.id, notionalUsdt, leverage: adjustment.position.leverage })
        });
      } else if (adjustment.type === "reduce") {
        response = await fetch(`/backend/positions/${adjustment.position.id}/reduce`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ notionalUsdt })
        });
      } else {
        response = await fetch(`/backend/positions/${adjustment.position.id}/leverage`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ leverage: notionalUsdt })
        });
      }
      const json = await response.json();
      if (!response.ok) throw new Error(json.error);
      setNotice(json.message);
      setAdjustment(null);
      await loadAccount();
    } catch (e) {
      const action = adjustment.type === "increase" ? "加仓" : adjustment.type === "reduce" ? "减仓" : "调整杠杆";
      setNotice(`${action}失败：${e.message}`);
    } finally {
      setTradeBusy(false);
    }
  }

  const rows = useMemo(() => {
    const list = (data?.opportunities || []).filter((x) =>
      x.token.symbol.toLowerCase().includes(query.toLowerCase()) &&
      x.apy * 100 >= minApy
    );
    return [...list].sort((a, b) =>
      sort === "apy" ? b.apy - a.apy : a.breakEvenHours - b.breakEvenHours
    );
  }, [data, query, minApy, sort]);
  const spreadRows = useMemo(
    () => (data?.spreadOpportunities || [])
      .filter((item) => item.token.symbol.toLowerCase().includes(query.toLowerCase()))
      .sort((a, b) => b.spread - a.spread),
    [data, query]
  );

  const best = data?.opportunities?.[0];

  return (
    <main>
      <header className="topbar">
        <a className="brand" href="#"><span className="brand-mark">A</span>ARBIVIEW</a>
        <nav>
          <a href="#account">账户持仓</a>
          <a className="active" href="#opportunities">套利机会</a>
          <a href="#spread-arbitrage">价差套利</a>
          <a href="#methodology">计算说明</a>
        </nav>
        <div className="live-status"><i /> LIVE <span>机会 20s · 持仓 WebSocket</span></div>
      </header>

      <section className="hero">
        <div className="hero-copy">
          <div className="eyebrow"><Sparkles size={14} /> PERPETUAL FUNDING ARBITRAGE</div>
          <h1>捕捉跨所<br /><em>资金费率差</em></h1>
          <p>聚合 Binance 与 Bybit 永续合约，筛选 CoinMarketCap 市值前 200 的实时中性套利机会。</p>
        </div>
        <div className="hero-stats">
          <div><span>覆盖资产</span><strong>{data?.universeSize || "200"}</strong><small>CMC TOP 200</small></div>
          <div><span>共同合约</span><strong>{data?.matchedPairs ?? "—"}</strong><small>BINANCE × BYBIT</small></div>
          <div className="highlight"><span>最高资金 APY</span><strong>{best ? pct(best.apy, 1) : "—"}</strong><small>{best ? `${best.token.symbol} · ${best.long.exchange} → ${best.short.exchange}` : "正在扫描"}</small></div>
        </div>
      </section>

      {notice && <div className="notice"><span>{notice}</span><button onClick={() => setNotice("")}><X size={14} /></button></div>}

      <AccountBoard account={account} positions={positions} onClose={closePosition} onAdjust={(position, type) => setAdjustment({ position, type })} busyId={closingId} />

      <section className="workspace" id="opportunities">
        <div className="section-head">
          <div>
            <div className="section-kicker"><TrendingUp size={15} /> LIVE OPPORTUNITIES</div>
            <h2>当前套利机会 <span>{rows.length}</span></h2>
          </div>
          <div className="updated">
            <Clock3 size={15} />
            {data ? `更新于 ${new Date(data.updatedAt).toLocaleTimeString("zh-CN", { hour12: false })}` : "正在连接行情"}
            <button onClick={load} disabled={loading} aria-label="刷新"><RefreshCw size={15} className={loading ? "spin" : ""} /></button>
          </div>
        </div>

        <div className="toolbar">
          <label className="search"><Search size={16} /><input value={query} onChange={(e) => setQuery(e.target.value)} placeholder="搜索 BTC、ETH…" /></label>
          <label className="filter"><SlidersHorizontal size={15} />最低 APY
            <select value={minApy} onChange={(e) => setMinApy(Number(e.target.value))}>
              <option value="0">不限</option><option value="10">10%</option><option value="25">25%</option><option value="50">50%</option>
            </select><ChevronDown size={14} />
          </label>
          <div className="sorter">
            <span><ArrowDownUp size={14} />排序</span>
            <button className={sort === "apy" ? "selected" : ""} onClick={() => setSort("apy")}>资金 APY</button>
            <button className={sort === "breakeven" ? "selected" : ""} onClick={() => setSort("breakeven")}>回本时间</button>
          </div>
        </div>

        <div className="column-head">
          <span>资产</span><span>执行路径 / 双腿行情</span><span>资金 APY</span><span>开仓价差</span><span>预计回本</span>
        </div>

        <div className="cards">
          {loading && !data && <div className="state"><RefreshCw className="spin" />正在扫描两个交易所的永续合约…</div>}
          {error && <div className="state error">行情连接失败：{error}<button onClick={load}>重试</button></div>}
          {!loading && !error && rows.length === 0 && <div className="state">当前筛选条件下暂无套利机会</div>}
          {rows.map((item, i) => <OpportunityCard key={item.id} item={item} index={i} onTrade={setTradeItem} />)}
        </div>
      </section>

      <section className="spread-workspace" id="spread-arbitrage">
        <div className="section-head">
          <div>
            <div className="section-kicker"><ArrowDownUp size={15} /> PRICE SPREAD ARBITRAGE</div>
            <h2>价差套利 <span>{spreadRows.length}</span></h2>
          </div>
          <div className="configured">仅展示可执行价差 &gt; 0.5% · Funding 成本按当前费率估算</div>
        </div>
        <div className="spread-column-head"><span>资产</span><span>执行路径</span><span>价差</span><span>Funding 成本</span><span>成本承受时间</span></div>
        <div className="spread-list">
          {!loading && !error && spreadRows.length === 0 && <div className="state compact">当前没有价差超过 0.5% 的共同合约</div>}
          {spreadRows.map((item, index) => <SpreadOpportunityCard key={`spread-${item.id}`} item={item} index={index} onTrade={setTradeItem} />)}
        </div>
        <p className="spread-note">价差收益依赖两所价格最终收敛；Funding 成本按当前费率和结算周期线性估算，实际费率可能在持仓期间变化。</p>
      </section>

      <section className="methodology" id="methodology">
        <div className="method-title"><Info size={18} /><div><b>计算口径</b><span>帮助你正确理解展示数据</span></div></div>
        <div><span>01</span><p><b>可执行价格</b>Long 使用卖一价，Short 使用买一价；Long 低于 Short 时价差为正。</p></div>
        <div><span>02</span><p><b>资金 APY</b>按各合约实际结算周期换算小时净收益，再按 365 天单利年化。</p></div>
        <div><span>03</span><p><b>预计回本</b>以资金费率覆盖价差与双腿开平仓 taker 手续费；Binance 0.05%，Bybit 0.055%。</p></div>
      </section>

      <footer><span><ShieldCheck size={15} />数据仅供研究，不构成投资建议</span><span>ARBIVIEW / 2026</span></footer>
      {tradeItem && <TradeModal item={tradeItem} mode={account?.mode} onClose={() => setTradeItem(null)} onSubmit={openTrade} busy={tradeBusy} />}
      {adjustment && <AdjustModal position={adjustment.position} type={adjustment.type} onClose={() => setAdjustment(null)} onSubmit={adjustPosition} busy={tradeBusy} />}
    </main>
  );
}
