"use client";

import { useCallback, useEffect, useMemo, useState } from "react";
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
            ? "真实交易模式：确认后将立即向 Binance 与 Bybit 提交真实市价单。"
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

function AccountBoard({ account, positions, onClose, busyId }) {
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
      <div className="position-list">
        {positions.length === 0 && <div className="state compact">暂无持仓，可从下方机会列表建立模拟双腿仓位</div>}
        {positions.map((p) => (
          <article className="position-row" key={p.id}>
            <div><b>{p.token}/USDT</b><span>{new Date(p.openedAt).toLocaleString("zh-CN")}</span></div>
            <div><span>LONG · {p.long.exchange}</span><b>{p.long.quantity} @ {price(p.long.entryPrice)}</b></div>
            <div><span>SHORT · {p.short.exchange}</span><b>{p.short.quantity} @ {price(p.short.entryPrice)}</b></div>
            <div><span>名义价值 / 杠杆</span><b>{price(p.notionalUsdt)} · {p.leverage}×</b></div>
            <div><span>未实现盈亏</span><b className={p.unrealizedPnl >= 0 ? "positive" : "negative"}>{price(p.unrealizedPnl)}</b></div>
            <button disabled={busyId === p.id} onClick={() => onClose(p.id)}>{busyId === p.id ? "平仓中…" : "双腿平仓"}</button>
          </article>
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
  const [closingId, setClosingId] = useState("");
  const [notice, setNotice] = useState("");

  const load = useCallback(async () => {
    setLoading(true);
    setError("");
    try {
      const response = await fetch("/backend/opportunities", { cache: "no-store" });
      const json = await response.json();
      if (!response.ok) throw new Error(json.detail || json.error);
      setData(json);
    } catch (e) {
      setError(e.message || "加载失败");
    } finally {
      setLoading(false);
    }
  }, []);

  const loadAccount = useCallback(async () => {
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
    } catch (e) {
      setNotice(`账户服务：${e.message}`);
    }
  }, []);

  useEffect(() => { load(); loadAccount(); }, [load, loadAccount]);

  async function openTrade(request) {
    setTradeBusy(true);
    try {
      const response = await fetch("/backend/trades/open", {
        method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(request)
      });
      const json = await response.json();
      if (!response.ok) throw new Error(json.error);
      setNotice(json.message);
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
      setNotice(json.message);
      await loadAccount();
    } catch (e) { setNotice(`平仓失败：${e.message}`); }
    finally { setClosingId(""); }
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

  const best = data?.opportunities?.[0];

  return (
    <main>
      <header className="topbar">
        <a className="brand" href="#"><span className="brand-mark">A</span>ARBIVIEW</a>
        <nav>
          <a href="#account">账户持仓</a>
          <a className="active" href="#opportunities">套利机会</a>
          <a href="#methodology">计算说明</a>
        </nav>
        <div className="live-status"><i /> LIVE <span>20s refresh</span></div>
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

      <AccountBoard account={account} positions={positions} onClose={closePosition} busyId={closingId} />

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

      <section className="methodology" id="methodology">
        <div className="method-title"><Info size={18} /><div><b>计算口径</b><span>帮助你正确理解展示数据</span></div></div>
        <div><span>01</span><p><b>可执行价格</b>Long 使用卖一价，Short 使用买一价；Long 低于 Short 时价差为正。</p></div>
        <div><span>02</span><p><b>资金 APY</b>按各合约实际结算周期换算小时净收益，再按 365 天单利年化。</p></div>
        <div><span>03</span><p><b>预计回本</b>以资金费率覆盖价差与双腿开平仓 taker 手续费；Binance 0.05%，Bybit 0.055%。</p></div>
      </section>

      <footer><span><ShieldCheck size={15} />数据仅供研究，不构成投资建议</span><span>ARBIVIEW / 2026</span></footer>
      {tradeItem && <TradeModal item={tradeItem} mode={account?.mode} onClose={() => setTradeItem(null)} onSubmit={openTrade} busy={tradeBusy} />}
    </main>
  );
}
