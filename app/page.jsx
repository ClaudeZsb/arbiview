"use client";

import { useCallback, useEffect, useMemo, useState } from "react";
import {
  ArrowDownUp, ArrowRight, ChevronDown, Clock3, Info, RefreshCw,
  Search, ShieldCheck, SlidersHorizontal, Sparkles, TrendingUp
} from "lucide-react";

const formatter = new Intl.NumberFormat("en-US", { maximumFractionDigits: 8 });

function price(value) {
  if (value >= 1000) return `$${formatter.format(value)}`;
  if (value >= 1) return `$${value.toLocaleString("en-US", { maximumFractionDigits: 4 })}`;
  return `$${value.toLocaleString("en-US", { maximumSignificantDigits: 6 })}`;
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

function OpportunityCard({ item, index }) {
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
    </article>
  );
}

export default function Dashboard() {
  const [data, setData] = useState(null);
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(true);
  const [sort, setSort] = useState("apy");
  const [query, setQuery] = useState("");
  const [minApy, setMinApy] = useState(0);

  const load = useCallback(async () => {
    setLoading(true);
    setError("");
    try {
      const response = await fetch("/api/opportunities", { cache: "no-store" });
      const json = await response.json();
      if (!response.ok) throw new Error(json.detail || json.error);
      setData(json);
    } catch (e) {
      setError(e.message || "加载失败");
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => { load(); }, [load]);

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
          {rows.map((item, i) => <OpportunityCard key={item.id} item={item} index={i} />)}
        </div>
      </section>

      <section className="methodology" id="methodology">
        <div className="method-title"><Info size={18} /><div><b>计算口径</b><span>帮助你正确理解展示数据</span></div></div>
        <div><span>01</span><p><b>可执行价格</b>Long 使用卖一价，Short 使用买一价；Long 低于 Short 时价差为正。</p></div>
        <div><span>02</span><p><b>资金 APY</b>按各合约实际结算周期换算小时净收益，再按 365 天单利年化。</p></div>
        <div><span>03</span><p><b>预计回本</b>以资金费率覆盖价差与双腿开平仓 taker 手续费；Binance 0.05%，Bybit 0.055%。</p></div>
      </section>

      <footer><span><ShieldCheck size={15} />数据仅供研究，不构成投资建议</span><span>ARBIVIEW / 2026</span></footer>
    </main>
  );
}
