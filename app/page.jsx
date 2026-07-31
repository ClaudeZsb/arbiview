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
  if (value === undefined || value === null || !Number.isFinite(Number(value))) return "—";
  return `$${Number(value || 0).toLocaleString("en-US", {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2
  })}`;
}

function compactMoney(value) {
  const amount = Number(value);
  if (!Number.isFinite(amount) || amount <= 0) return "—";
  if (amount >= 1e9) return `$${(amount / 1e9).toFixed(amount >= 1e10 ? 1 : 2)}B`;
  if (amount >= 1e6) return `$${(amount / 1e6).toFixed(amount >= 1e7 ? 1 : 2)}M`;
  if (amount >= 1e3) return `$${(amount / 1e3).toFixed(amount >= 1e4 ? 1 : 2)}K`;
  return money(amount);
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

async function readJson(response, label = "接口") {
  const text = await response.text();
  try {
    return JSON.parse(text);
  } catch {
    const detail = text.trim().slice(0, 160) || response.statusText || "空响应";
    throw new Error(`${label}返回非 JSON（HTTP ${response.status}）：${detail}`);
  }
}

function ExchangeLogo({ name }) {
  return <span className={`exchange-logo ${name.toLowerCase()}`}>{name === "Binance" ? "◈" : "◆"}</span>;
}

function Leg({ type, leg }) {
  const market = leg.market === "spot" ? "SPOT" : "PERP";
  return (
    <div className="leg">
      <div className="leg-head">
        <span className={`side ${type}`}>{type === "long" ? "LONG" : "SHORT"}</span>
        <span className="exchange"><ExchangeLogo name={leg.exchange} />{leg.exchange} · {market}</span>
      </div>
      <div className="leg-price">{price(type === "long" ? leg.ask : leg.bid)}</div>
      <div className="leg-meta">
        {leg.market === "spot"
          ? <span>现货腿 <b>{type === "long" ? "买入持有" : "借币卖出"}</b></span>
          : <span>资金费率 <b className={leg.rate < 0 ? "positive" : ""}>{pct(leg.rate, 4)}</b> / {leg.intervalHours}h</span>}
        <span>下次结算 <b>{time(leg.nextFundingTime)}</b></span>
        <span>24h 成交额 <b>{compactMoney(leg.volume24hUsdt)}</b></span>
      </div>
    </div>
  );
}

function AssetMeta({ token }) {
  return (
    <div className="asset-meta">
      <span>{token.name}</span>
      <div className="asset-tags">
        {(token.tags || []).map((tag) => <b key={tag} className={`asset-tag ${tag}`}>{tag}</b>)}
        {token.rank && <b className="asset-tag cmc-rank">CMC #{token.rank}</b>}
      </div>
    </div>
  );
}

function OpportunityCard({ item, index, onTrade, expanded, onToggle, history, historyLoading }) {
  const executable = item.executionSupported !== false;
  const disabledReason = item.short.market === "spot"
    ? "当前币种没有可用的 Binance 杠杆借币额度"
    : "该现货交易方向尚未接入真实执行";
  return (
    <article className={`opportunity-shell ${expanded ? "expanded" : ""}`}>
      <div className="opportunity-card" role="button" tabIndex={0} onClick={onToggle} onKeyDown={(event) => {
        if (event.key === "Enter" || event.key === " ") onToggle();
      }}>
        <div className="rank">#{String(index + 1).padStart(2, "0")}</div>
        <div className="asset">
          <div className="asset-icon">{item.token.symbol.slice(0, 2)}</div>
          <div>
            <div className="asset-symbol">{item.token.symbol}<span>/USDT</span></div>
            <AssetMeta token={item.token} />
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
          <small>净收益 {pct(item.fundingPerHour, 4)} / 小时</small>
          {item.borrowInterestPerHour > 0 &&
            <small className="borrow-cost">借币成本 −{pct(item.borrowInterestPerHour, 4).replace("+", "")} / 小时</small>}
        </div>
      <div className="metric spread">
        <span className="metric-label">当前 / 24h 加权价差</span>
        <strong className={item.spread >= 0 ? "positive" : "negative"}>{pct(item.spread)}</strong>
        <small>加权基准 {pct(item.averageSpread24h)} · 偏离 <b className={item.spreadVsAverage >= 0 ? "positive" : "negative"}>{pct(item.spreadVsAverage)}</b></small>
        </div>
      <div className="metric breakeven">
        <span className="metric-label">预计回本</span>
        <strong>{duration(item.breakEvenHours)}</strong>
        <small>覆盖价差偏离与双边费用</small>
        </div>
        <div className="opportunity-actions">
          <button className="trade-button" disabled={!executable} title={executable ? "" : disabledReason}
            onClick={(event) => { event.stopPropagation(); if (executable) onTrade(item); }}>
            <Zap size={13} />{executable ? "开仓" : "仅观察"}
          </button>
          <ChevronDown className="opportunity-chevron" size={16} />
        </div>
      </div>
      {expanded && <OpportunityHistoryDetails item={item} data={history} loading={historyLoading} />}
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
        <div><div className="asset-symbol">{item.token.symbol}<span>/USDT</span></div><AssetMeta token={item.token} /></div>
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
  const [leverage, setLeverage] = useState(10);
  const [spreadGuard, setSpreadGuard] = useState(false);
  const [spreadThreshold, setSpreadThreshold] = useState(Number((item.spread * 100).toFixed(4)));
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
        <label className="guard-toggle">买入模式<select value={spreadGuard ? "guard" : "market"} onChange={(e) => setSpreadGuard(e.target.value === "guard")}>
          <option value="market">市价模式</option>
          <option value="guard">保价差限价模式</option>
        </select></label>
        {spreadGuard && <>
          <label>最低可执行价差（%）<input type="number" step="0.01" value={spreadThreshold} onChange={(e) => setSpreadThreshold(Number(e.target.value))} /></label>
        </>}
        <div className={`trade-warning ${isLive ? "live" : "paper"}`}><ShieldCheck size={16} />
          {isLive
            ? spreadGuard
              ? `保价差模式：仅当累计成交价差满足门槛时按盘口提交 IOC 限价单，单腿每次最多 $50；部分成交立即市价补腿，产生的价差欠账由下一批追回。`
              : "真实交易模式：两腿将并发提交市价单；不足目标时优先补仓，差额超过 10 USDT 时自动减仓对齐。"
            : mode === "paper"
              ? "模拟交易模式：只在后端记录模拟仓位，不会向交易所发送订单。"
              : "尚未读取后端交易模式，暂时禁止提交。"}
        </div>
        <button className={`confirm-trade ${isLive ? "live" : ""}`} disabled={busy || !modeKnown} onClick={() => onSubmit({
          opportunityId: item.id, notionalUsdt: notional, leverage, spreadGuard,
          spreadThreshold: spreadThreshold / 100, orderNotionalUsdt: 0, intervalSeconds: 0.25
        })}>
          {busy ? "正在建立任务…" : spreadGuard ? "启动保价差限价开仓" : isLive ? "确认真实下单" : "确认模拟开仓"}
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

function BatchIncreasePanel({ position, action, task, onClose, onStart, onCancel, busy }) {
  const isReduce = action === "reduce";
  const maximumReduction = Math.max(0, position.notionalUsdt - 10);
  const [target, setTarget] = useState(isReduce ? Math.min(1000, maximumReduction) : 1000);
  const [orderNotional, setOrderNotional] = useState(Math.min(100, isReduce ? maximumReduction : 100));
  const [intervalSeconds, setIntervalSeconds] = useState(2);
  const [spreadGuard, setSpreadGuard] = useState(false);
  const [noLossGuard, setNoLossGuard] = useState(false);
  const initialLongClose = position.long.closePrice || position.long.markPrice;
  const initialShortClose = position.short.closePrice || position.short.markPrice;
  const initialCloseSpread = initialLongClose > 0 && initialShortClose > 0
    ? (initialShortClose - initialLongClose) / initialLongClose
    : (position.currentSpread || 0);
  const [spreadThreshold, setSpreadThreshold] = useState(Number((((isReduce ? initialCloseSpread : position.currentSpread) || 0) * 100).toFixed(4)));
  const logRef = useRef(null);
  const progress = task
    ? Math.min(100, task.completedNotionalUsdt / task.targetNotionalUsdt * 100)
    : 0;
  const active = task && ["queued", "running", "cancelling"].includes(task.status);
  useEffect(() => {
    if (logRef.current) logRef.current.scrollTop = logRef.current.scrollHeight;
  }, [task?.logs?.length]);
  return (
    <aside className="batch-panel">
      <div className="batch-panel-head">
        <div>
          <div className="section-kicker"><Zap size={14} /> BATCH {isReduce ? "REDUCE" : "INCREASE"}</div>
          <h3>{position.token} 双腿批量{isReduce ? "减仓" : "加仓"}</h3>
        </div>
        <button className="batch-close" disabled={active} title={active ? "任务运行中，请先停止或等待完成" : "关闭"} onClick={onClose}><X size={18} /></button>
      </div>
      <div className="batch-route">
        <span><i className="side-dot long" />LONG · {position.long.exchange} · {position.long.market === "spot" ? "SPOT" : "PERP"}</span>
        <ArrowRight size={14} />
        <span><i className="side-dot short" />SHORT · {position.short.exchange} · {position.short.market === "spot" ? "SPOT" : "PERP"}</span>
      </div>
      {!task && <>
        <label>每腿目标{isReduce ? "减仓" : "加仓"}金额（USDT）<input type="number" min="10" max={isReduce ? maximumReduction : undefined} step="100" value={target} onChange={(event) => setTarget(Number(event.target.value))} /></label>
        {!isReduce && <label>买入模式<select value={spreadGuard ? "guard" : "market"} onChange={(event) => setSpreadGuard(event.target.value === "guard")}>
          <option value="market">市价模式</option>
          <option value="guard">保价差限价模式</option>
        </select></label>}
        {!isReduce && spreadGuard && <label>最低可执行价差（%）<input type="number" step="0.01" value={spreadThreshold} onChange={(event) => setSpreadThreshold(Number(event.target.value))} /></label>}
        {isReduce && <label>平仓模式<select value={noLossGuard ? "no-loss" : "market"} onChange={(event) => setNoLossGuard(event.target.value === "no-loss")}>
          <option value="market">市价批量减仓</option>
          <option value="no-loss">保不亏限价平仓</option>
        </select></label>}
        {isReduce && noLossGuard && <label>最高可执行平仓价差（%）<input type="number" step="0.01" value={spreadThreshold} onChange={(event) => setSpreadThreshold(Number(event.target.value))} /><small>默认取当前可平仓价差；数值调低表示等待更有利、潜在盈利更高的退出价差。</small></label>}
        {(!spreadGuard && !noLossGuard) && <label>单次每腿下单金额（USDT）<input type="number" min="10" step="10" value={orderNotional} onChange={(event) => setOrderNotional(Number(event.target.value))} /></label>}
        {(!spreadGuard && !noLossGuard) && <label>批次间隔（秒）<input type="number" min="0.5" max="3600" step="0.5" value={intervalSeconds} onChange={(event) => setIntervalSeconds(Number(event.target.value))} /></label>}
        <div className="batch-estimate">
          {spreadGuard && !isReduce
            ? "按实时盘口动态下单，单腿每次最多 $50；成交差额立即市价补腿，不利价差由下一批提高门槛追回"
            : noLossGuard
              ? "同时满足目标平仓价差和累计仓位盈亏不为负才下单；不计资金费和手续费。单腿每次最多 $50，部分成交立即市价补腿，价差及盈亏欠账由下一批追回"
            : <>预计 {Math.ceil(target / Math.max(orderNotional, 1))} 批，约 {duration(Math.max(0, Math.ceil(target / Math.max(orderNotional, 1)) - 1) * intervalSeconds / 3600)}</>}
        </div>
        <button
          className="batch-start"
          disabled={busy || target < 10 || ((!spreadGuard && !noLossGuard) && (orderNotional < 10 || orderNotional > target || intervalSeconds < 0.5)) || (isReduce && target > maximumReduction)}
          onClick={() => onStart({ targetNotionalUsdt: target, orderNotionalUsdt: spreadGuard || noLossGuard ? 0 : orderNotional, intervalSeconds: spreadGuard || noLossGuard ? 0.25 : intervalSeconds, spreadGuard, spreadThreshold: spreadThreshold / 100, noLossGuard, closeSpreadThreshold: noLossGuard ? spreadThreshold / 100 : undefined })}
        >
          {busy ? "正在创建任务…" : `启动批量${isReduce ? "减仓" : "加仓"}`}
        </button>
      </>}
      {task && <>
        <div className="batch-summary">
          <div><span>状态</span><b className={`batch-status ${task.status}`}>{task.status}</b></div>
          <div><span>完成金额</span><b>{money(task.completedNotionalUsdt)} / {money(task.targetNotionalUsdt)}</b></div>
          <div><span>批次</span><b>{task.spreadGuard || task.noLossGuard ? `${task.completedBatches} 次动态下单` : `${task.completedBatches} / ${task.totalBatches}`}</b></div>
        </div>
        {task.spreadGuard && <div className="batch-estimate">
          保价差限价 · 原门槛 {pct(task.spreadThreshold, 4)} · 当前要求 {task.effectiveSpreadThreshold == null ? "读取中" : pct(task.effectiveSpreadThreshold, 4)} · 当前盘口 {task.currentSpread == null ? "读取中" : pct(task.currentSpread, 4)} · 累计成交 {task.cumulativeFilledSpread == null ? "暂无" : pct(task.cumulativeFilledSpread, 4)} · 已等待 {task.spreadWaitCount} 次
        </div>}
        {task.noLossGuard && <div className="batch-estimate">
          保价差且不亏平仓 · 原门槛 {task.spreadThreshold == null ? "读取中" : pct(task.spreadThreshold, 4)} · 当前要求 {task.effectiveSpreadThreshold == null ? "读取中" : pct(task.effectiveSpreadThreshold, 4)} · 当前平仓价差 {task.currentSpread == null ? "读取中" : pct(task.currentSpread, 4)} · 累计成交 {task.cumulativeFilledSpread == null ? "暂无" : pct(task.cumulativeFilledSpread, 4)} · 当前可配对仓位盈亏 {task.currentClosePnlUsdt == null ? "读取中" : money(task.currentClosePnlUsdt)} · 已等待 {task.spreadWaitCount} 次
        </div>}
        <div className="batch-progress"><i style={{ width: `${progress}%` }} /></div>
        <div className="batch-progress-label"><b>{progress.toFixed(1)}%</b><span>{task.spreadGuard ? "单腿硬顶 $50 · 差额立即市价补齐 · 下一批追回价差欠账" : task.noLossGuard ? "价差与仓位盈亏双重保护 · 差额立即市价补齐 · 下一批追回欠账" : `单笔 ${money(task.orderNotionalUsdt)} · 间隔 ${task.intervalSeconds}s`}</span></div>
        {task.error && <div className="batch-error">{task.error}</div>}
        <div className="batch-log" ref={logRef}>
          {task.logs.length === 0 && <div className="batch-log-empty">等待第一批成交…</div>}
          {task.logs.map((log) => (
            <div className={`batch-log-row ${log.status === "failed" ? "failed" : ""}`} key={`${log.sequence}-${log.orderId}`}>
              <time>{new Date(log.timestamp).toLocaleTimeString("zh-CN", { hour12: false })}</time>
              <i className={`side-dot ${log.side}`} />
              <div>
                <b>#{log.batch} {log.exchange} · {log.side.toUpperCase()} {log.token}</b>
                <span>{money(log.notionalUsdt)} @ {price(log.averagePrice)} · {log.message}</span>
              </div>
            </div>
          ))}
        </div>
        {active && <button className="batch-cancel" disabled={task.status === "cancelling"} onClick={onCancel}>{task.status === "cancelling" ? "正在停止…" : "完成当前订单后停止"}</button>}
      </>}
    </aside>
  );
}

function HistoryLineChart({ points, fields, title, subtitle, valueLabel }) {
  const [hoverIndex, setHoverIndex] = useState(null);
  if (points.length < 2) return <div className="spread-chart-empty">暂无足够历史数据</div>;
  const width = 900;
  const height = 190;
  const pad = { top: 18, right: 22, bottom: 32, left: 58 };
  const values = points.flatMap((point) => fields.map((field) => point[field.key])).filter(Number.isFinite);
  if (values.length < 2) return <div className="spread-chart-empty">交易所暂未返回该时段历史数据</div>;
  const rawMin = Math.min(...values, 0);
  const rawMax = Math.max(...values, 0);
  const margin = Math.max((rawMax - rawMin) * 0.12, 0.0001);
  const min = rawMin - margin;
  const max = rawMax + margin;
  const x = (index) => pad.left + index * (width - pad.left - pad.right) / (points.length - 1);
  const y = (value) => pad.top + (max - value) * (height - pad.top - pad.bottom) / (max - min);
  const hovered = hoverIndex == null ? null : points[hoverIndex];
  const hoverX = hoverIndex == null ? null : x(hoverIndex);
  const longestTooltipLine = Math.max(
    16,
    ...fields.map((field) => `${field.label}：${valueLabel(0)}`.length)
  );
  const tooltipWidth = Math.min(330, Math.max(205, 34 + longestTooltipLine * 8.5));
  const tooltipHeight = 28 + fields.length * 18;
  const tooltipX = hoverX == null ? 0 : Math.min(Math.max(hoverX + 10, pad.left), width - tooltipWidth - 8);
  return (
    <div className="opportunity-history-chart">
      <div className="history-chart-title"><div><b>{title}</b><span>{subtitle}</span></div>
        <div className="history-legend">{fields.map((field) => <span key={field.key}><i style={{ background: field.color }} />{field.label}</span>)}</div>
      </div>
      <svg viewBox={`0 0 ${width} ${height}`} onPointerLeave={() => setHoverIndex(null)} onPointerMove={(event) => {
        const bounds = event.currentTarget.getBoundingClientRect();
        const svgX = (event.clientX - bounds.left) / bounds.width * width;
        const index = Math.round((svgX - pad.left) / (width - pad.left - pad.right) * (points.length - 1));
        setHoverIndex(Math.max(0, Math.min(points.length - 1, index)));
      }}>
        <line className="spread-zero" x1={pad.left} x2={width - pad.right} y1={y(0)} y2={y(0)} />
        {[min, (min + max) / 2, max].map((value) => <g key={value}>
          <line className="spread-grid" x1={pad.left} x2={width - pad.right} y1={y(value)} y2={y(value)} />
          <text x={pad.left - 8} y={y(value) + 3} textAnchor="end">{valueLabel(value)}</text>
        </g>)}
        {fields.map((field) => {
          const segments = [];
          let segment = [];
          points.forEach((point, index) => {
            if (Number.isFinite(point[field.key])) segment.push([index, point[field.key]]);
            else if (segment.length) { segments.push(segment); segment = []; }
          });
          if (segment.length) segments.push(segment);
          return <g key={field.key}>{segments.map((part, partIndex) => (
            <path key={partIndex} fill="none" stroke={field.color} strokeWidth="2.3" d={part.map(([index, value], pointIndex) => `${pointIndex ? "L" : "M"} ${x(index).toFixed(2)} ${y(value).toFixed(2)}`).join(" ")} />
          ))}</g>;
        })}
        {points.map((point, index) => (index === 0 || index === points.length - 1 || index % 6 === 0) && (
          <text key={point.timestamp} x={x(index)} y={height - 9} textAnchor="middle">{new Date(point.timestamp).toLocaleTimeString("zh-CN", { hour: "2-digit", minute: "2-digit" })}</text>
        ))}
        <rect className="history-hover-area" x={pad.left} y={pad.top} width={width - pad.left - pad.right} height={height - pad.top - pad.bottom} />
        {hovered && <g className="history-tooltip">
          <line x1={hoverX} x2={hoverX} y1={pad.top} y2={height - pad.bottom} />
          {fields.map((field) => Number.isFinite(hovered[field.key]) && <circle key={field.key} cx={hoverX} cy={y(hovered[field.key])} r="4" fill="white" stroke={field.color} strokeWidth="2.5" />)}
          <rect x={tooltipX} y="5" width={tooltipWidth} height={tooltipHeight} rx="4" />
          <text x={tooltipX + 10} y="21">{new Date(hovered.timestamp).toLocaleString("zh-CN", { month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit" })}</text>
          {fields.map((field, index) => <text key={field.key} x={tooltipX + 10} y={40 + index * 18}>
            <tspan fill={field.color}>●</tspan><tspan> {field.label}：{Number.isFinite(hovered[field.key]) ? valueLabel(hovered[field.key]) : "—"}</tspan>
          </text>)}
        </g>}
      </svg>
    </div>
  );
}

function PositionHistoryDetails({ position, data, loading }) {
  if (loading) return <div className="opportunity-history-loading"><RefreshCw className="spin" size={15} />读取 {position.token} 近 24 小时价差与资金费…</div>;
  if (!data?.points?.length) return <div className="opportunity-history-loading">展开后读取近 24 小时价差与资金费历史</div>;
  if (data.opportunityId) {
    const points = data.points.map((point) => ({
      ...point,
      directionalSpread: point.directionalSpreadPercent,
      longFundingPercent: Number.isFinite(point.longFundingRate) ? point.longFundingRate * 100 : null,
      shortFundingPercent: Number.isFinite(point.shortFundingRate) ? point.shortFundingRate * 100 : null
    }));
    const marketLabel = (leg) => `${leg.exchange} · ${leg.market === "spot" ? "SPOT" : "PERP"}`;
    const fundingFields = [
      position.long.market === "perpetual" && { key: "longFundingPercent", label: `${marketLabel(position.long)} · LONG`, color: "#d8a000" },
      position.short.market === "perpetual" && { key: "shortFundingPercent", label: `${marketLabel(position.short)} · SHORT`, color: "#dc5d43" }
    ].filter(Boolean);
    return <div className="position-history">
      <HistoryLineChart points={points} fields={[
        { key: "directionalSpread", label: `${marketLabel(position.long)} LONG → ${marketLabel(position.short)} SHORT`, color: "#087d5a" }
      ]} title={`${position.token} 近 24h 方向价差`} subtitle="每小时收盘；SHORT 价格高于 LONG 为正，低于 LONG 为负" valueLabel={(value) => `${value.toFixed(3)}%`} />
      <HistoryLineChart points={points} fields={fundingFields}
        title={position.long.market === "spot" || position.short.market === "spot" ? "永续腿历史资金费率" : "两所历史资金费率"}
        subtitle={data.fundingNote || "每小时展示最近一次已知结算费率"} valueLabel={(value) => `${value.toFixed(4)}%`} />
    </div>;
  }
  const longIsBinance = position.long.exchange === "Binance";
  const points = data.points.map((point) => {
    const longPrice = longIsBinance ? point.binanceClose : point.bybitClose;
    const shortPrice = longIsBinance ? point.bybitClose : point.binanceClose;
    return {
      ...point,
      directionalSpread: longPrice > 0 ? (shortPrice - longPrice) / longPrice * 100 : null,
      binanceFundingPercent: Number.isFinite(point.binanceFundingRate) ? point.binanceFundingRate * 100 : null,
      bybitFundingPercent: Number.isFinite(point.bybitFundingRate) ? point.bybitFundingRate * 100 : null
    };
  });
  return <div className="position-history">
    <HistoryLineChart points={points} fields={[
      { key: "directionalSpread", label: `${position.long.exchange} LONG → ${position.short.exchange} SHORT`, color: "#087d5a" }
    ]} title={`${position.token} 近 24h 方向价差`} subtitle="每小时收盘；SHORT 价格高于 LONG 为正，低于 LONG 为负" valueLabel={(value) => `${value.toFixed(3)}%`} />
    <HistoryLineChart points={points} fields={[
      { key: "binanceFundingPercent", label: `Binance${position.long.exchange === "Binance" ? " · LONG" : " · SHORT"}`, color: "#d8a000" },
      { key: "bybitFundingPercent", label: `Bybit${position.long.exchange === "Bybit" ? " · LONG" : " · SHORT"}`, color: "#dc5d43" }
    ]} title={`${position.token} 近 24h 两所资金费率`} subtitle={data.fundingNote || "每小时展示最近一次已知结算费率"} valueLabel={(value) => `${value.toFixed(4)}%`} />
  </div>;
}

function OpportunityHistoryDetails({ item, data, loading }) {
  if (loading) return <div className="opportunity-history-loading"><RefreshCw className="spin" size={15} />读取 {item.token.symbol} 近 24 小时历史…</div>;
  if (!data?.points?.length) return <div className="opportunity-history-loading">暂无历史数据</div>;
  const points = data.points.map((point) => {
    return {
      ...point,
      directionalSpread: point.directionalSpreadPercent,
      longFundingPercent: Number.isFinite(point.longFundingRate) ? point.longFundingRate * 100 : null,
      shortFundingPercent: Number.isFinite(point.shortFundingRate) ? point.shortFundingRate * 100 : null
    };
  });
  const marketLabel = (leg) => `${leg.exchange} · ${leg.market === "spot" ? "SPOT" : "PERP"}`;
  const fundingFields = [
    item.long.market === "perpetual" && { key: "longFundingPercent", label: `${marketLabel(item.long)} · LONG`, color: "#d8a000" },
    item.short.market === "perpetual" && { key: "shortFundingPercent", label: `${marketLabel(item.short)} · SHORT`, color: "#dc5d43" }
  ].filter(Boolean);
  return <div className="opportunity-history">
    <HistoryLineChart points={points} fields={[{ key: "directionalSpread", label: `${marketLabel(item.long)} LONG → ${marketLabel(item.short)} SHORT`, color: "#087d5a" }]}
      title={`${item.token.symbol} 近 24h 方向价差`}
      subtitle="每小时收盘；SHORT 价格高于 LONG 为正，低于 LONG 为负"
      valueLabel={(value) => `${value.toFixed(2)}%`} />
    <HistoryLineChart points={points} fields={fundingFields}
      title={item.routeType === "spot_perpetual" ? "永续腿历史资金费率" : "两所历史资金费率"}
      subtitle={data.fundingNote || "按小时展示最近一次已知结算费率"}
      valueLabel={(value) => `${value.toFixed(3)}%`} />
  </div>;
}

function AutoCloseControl({ position, rule, busy, onSet, onCancel }) {
  const [threshold, setThreshold] = useState(rule?.thresholdApyPercent ?? 300);
  const [orderNotional, setOrderNotional] = useState(rule?.orderNotionalUsdt ?? 100);
  const [intervalSeconds, setIntervalSeconds] = useState(rule?.intervalSeconds ?? 2);
  const statusText = {
    armed: "监控中",
    triggered: "已触发",
    closing: "平仓中",
    completed: "已完成",
    failed: "失败",
    cancelled: "已取消",
    replaced: "已替换"
  }[rule?.status] || rule?.status;
  return (
    <div className="auto-close-control">
      <div className="auto-close-head">
        <div><b>APY 阈值自动平仓</b><span>连续 3 次（约 45 秒）低于阈值后触发双腿批量退出</span></div>
        {rule && <strong className={`auto-close-status ${rule.status}`}>{statusText}</strong>}
      </div>
      <div className="auto-close-fields">
        <label>触发 APY 低于<input type="number" value={threshold} onChange={(event) => setThreshold(Number(event.target.value))} /><span>%</span></label>
        <label>单笔金额<input type="number" min="10" step="10" value={orderNotional} onChange={(event) => setOrderNotional(Number(event.target.value))} /><span>USDT</span></label>
        <label>下单间隔<input type="number" min="0.5" step="0.5" value={intervalSeconds} onChange={(event) => setIntervalSeconds(Number(event.target.value))} /><span>秒</span></label>
        <button disabled={busy || orderNotional < 10 || intervalSeconds < 0.5} onClick={() => onSet(position.id, { thresholdApyPercent: threshold, orderNotionalUsdt: orderNotional, intervalSeconds })}>
          {busy ? "保存中…" : rule?.status === "armed" ? "更新规则" : "启用自动平仓"}
        </button>
        {rule?.status === "armed" && <button className="cancel" disabled={busy} onClick={() => onCancel(rule.id)}>取消</button>}
      </div>
      {rule && (
        <div className="auto-close-meta">
          <span>当前 APY：{rule.currentApyPercent == null ? "等待有效行情" : `${rule.currentApyPercent.toFixed(2)}%`}</span>
          <span>低于阈值连续读数：{rule.consecutiveLowReadings}/3</span>
          <span>已平仓：{money(rule.completedNotionalUsdt)}</span>
          {rule.error && <span className="negative">错误：{rule.error}</span>}
        </div>
      )}
    </div>
  );
}

function AdvisorCard({ recommendation }) {
  if (recommendation === undefined) return <section className="advisor-card loading"><RefreshCw className="spin" size={16} />正在读取最近一条策略建议…</section>;
  if (recommendation === null) return <section className="advisor-card loading">暂无非等待策略建议；下一次出现入场或止盈建议后会保留在这里</section>;
  const entry = recommendation.entry;
  const actionText = {
    enter: "允许入场",
    take_profit_allowed: "允许止盈退出",
    hold: "继续等待"
  }[recommendation.action] || recommendation.action;
  return <section className={`advisor-card ${recommendation.action}`}>
    <div className="advisor-heading">
      <div><span>STRATEGY ADVISOR</span><h3>最新策略建议</h3></div>
      <b>{actionText}</b>
    </div>
    <p>{recommendation.reason}</p>
    {entry && <div className="advisor-entry">
      <strong>{entry.token}</strong>
      <span>LONG {entry.longExchange} → SHORT {entry.shortExchange}</span>
      <span>APY <b>{entry.apyPercent.toFixed(2)}%</b> · APY 排名 #{entry.apyRank}</span>
      <span>回本 <b>{duration(entry.breakEvenHours)}</b> · 价差偏离 <b>{entry.spreadVsAveragePercent >= 0 ? "+" : ""}{entry.spreadVsAveragePercent.toFixed(3)}%</b></span>
    </div>}
    {recommendation.positions?.map((position) => <div className="advisor-position" key={position.positionId}>
      <strong>{position.token}</strong>
      <span className={position.takeProfitAllowed ? "positive" : ""}>{position.takeProfitAllowed ? "允许止盈" : "继续持有"}</span>
      <span>净收益 <b>{money(position.netProfitUsdt)}</b> / 门槛 {money(position.takeProfitThresholdUsdt)}</span>
      <small>Funding {money(position.fundingReceivedUsdt)} + 未实现 {money(position.unrealizedPnlUsdt)} − 手续费 {money(position.estimatedFeesUsdt)}</small>
    </div>)}
    <div className="advisor-footer">
      <span>下一整点 {new Date(recommendation.nextSettlementAt).toLocaleTimeString("zh-CN", { hour: "2-digit", minute: "2-digit", hour12: false })} · {recommendation.entryWindowOpen ? "入场窗口已开启" : "整点前 5 分钟开启入场窗口"}</span>
      <time>更新于 {new Date(recommendation.generatedAt).toLocaleTimeString("zh-CN", { hour12: false })}</time>
    </div>
  </section>;
}

function AccountBoard({ account, positions, protection, histories, historyLoading, onLoadHistory, autoCloseRules, onSetAutoClose, onCancelAutoClose, autoCloseBusy, onClose, onAdjust, busyId }) {
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
        <div><span>套利仓位近 {account?.realizedPeriodDays || 7} 日已实现</span><strong className={(account?.realizedPnl || 0) >= 0 ? "positive" : "negative"}>{money(account?.realizedPnl)}</strong></div>
        <div><span>活跃套利仓位</span><strong>{account ? account.activePositions : "—"}</strong></div>
      </div>
      <div className="exchange-balances">
        {(account?.exchanges || []).map((item) => (
          <article key={item.exchange}>
            <div className="exchange-balance-head"><ExchangeLogo name={item.exchange} /><b>{item.exchange}</b><span>{item.exchange === "Bybit" ? "统一交易账户" : "USDT-M 永续账户"}</span></div>
            <div><span>可用金额</span><strong>{money(item.availableUsdt)}</strong></div>
            <div><span>账户权益</span><b>{money(item.equityUsdt)}</b></div>
            <div><span>未实现盈亏</span><b className={item.unrealizedPnl >= 0 ? "positive" : "negative"}>{money(item.unrealizedPnl)}</b></div>
            <div><span>套利合约近 {account?.realizedPeriodDays || 7} 日已实现</span><b className={item.realizedPnl >= 0 ? "positive" : "negative"}>{money(item.realizedPnl)}</b><small>平仓 {money(item.closedPositionPnl)} + Funding {money(item.fundingIncome)} − 手续费 {money(item.tradingFees)}</small></div>
          </article>
        ))}
      </div>
      <p className="balance-note">未实现盈亏仅统计当前未平仓仓位；已实现盈亏只统计当前跨所套利合约近 {account?.realizedPeriodDays || 7} 日的平仓损益加 Funding 净额再减交易手续费，不含账户内其他策略。Bybit 可用金额为 Unified Account 的 USD 口径，可能低于账户权益。</p>
      {protection?.enabled && (
        <div className="protection-status">
          <b><ShieldCheck size={14} /> 双腿保护运行中</b>
          <span>名义价值偏差同时超过 {protection.tolerancePercent}% 和 {money(protection.minimumDifferenceUsdt)} 才保护 · 孤腿退出每单 {money(protection.orderNotionalUsdt)} / {protection.intervalSeconds}s</span>
          <small>已保护：{protection.protectedTokens?.join("、") || "等待识别双腿仓位"}</small>
        </div>
      )}
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
        {positions.length === 0 && <div className="state compact">{account ? "暂无持仓，可从下方机会列表建立模拟双腿仓位" : "正在读取账户与持仓…"}</div>}
        {positions.map((p) => (
          <details className="position-item" key={p.id} onToggle={(event) => {
            if (event.currentTarget.open) onLoadHistory(p);
          }}>
            <summary className="position-row">
              <div className="position-token"><ChevronDown size={14} /><div><b>{p.token}/USDT</b><span>{p.openedAt > 0 ? new Date(p.openedAt).toLocaleString("zh-CN") : "交易所实时仓位"}</span></div></div>
              <div><span>LONG · {p.long.exchange} · {p.long.market === "spot" ? "SPOT" : "PERP"}</span><b>{p.long.quantity} @ {price(p.long.entryPrice)}</b></div>
              <div><span>SHORT · {p.short.exchange} · {p.short.market === "spot" ? "SPOT" : "PERP"}</span><b>{p.short.quantity} @ {price(p.short.entryPrice)}</b></div>
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
              <div className="position-market-metrics">
                <span>当前资费净差 <b>{p.currentFundingPerHour == null ? "行情暂不可用" : `${pct(p.currentFundingPerHour, 4)} / 小时`}</b></span>
                <span>当前方向价差 <b className={(p.currentSpread ?? 0) >= 0 ? "positive" : "negative"}>{p.currentSpread == null ? "行情暂不可用" : pct(p.currentSpread, 3)}</b></span>
                <span>当前净 APY <b className={(p.currentApy ?? 0) >= 0 ? "positive" : "negative"}>{p.currentApy == null ? "行情暂不可用" : pct(p.currentApy, 1)}</b></span>
              </div>
              <div className="position-detail-head">
                <span>方向 / 交易所</span>
                <span>可平仓价</span>
                <span>策略参考价</span>
                <span>最新成交价</span>
                <span>开仓均价</span>
                <span>仓位数量</span>
                <span>Funding Rate</span>
                <span>未实现收益</span>
                <span>累计 Funding / 借币利息</span>
              </div>
              {[p.long, p.short].map((leg) => (
                <div className="position-leg-detail" key={`${leg.exchange}-${leg.market}-${leg.side}`}>
                  <div className="leg-detail-title"><i className={`side-dot ${leg.side}`} /><b>{leg.side.toUpperCase()} · {leg.exchange} · {leg.market === "spot" ? "SPOT" : "PERP"}</b></div>
                  <b>{price(leg.closePrice ?? leg.markPrice)}</b>
                  <b title={leg.usesLastPrice ? `Mark ${price(leg.rawMarkPrice)} 与 Last 偏差超过 0.1%，已使用 Last` : "使用 Mark Price"}>{price(leg.markPrice)}{leg.usesLastPrice ? " · LAST" : ""}</b>
                  <b>{price(leg.lastPrice ?? leg.markPrice)}</b>
                  <b>{price(leg.entryPrice)}</b>
                  <b>{formatter.format(leg.quantity)}</b>
                  <b className={leg.fundingRate >= 0 ? "positive" : "negative"}>{pct(leg.fundingRate)}</b>
                  <b className={leg.unrealizedPnl >= 0 ? "positive" : "negative"}>{money(leg.unrealizedPnl)}</b>
                  <b className={leg.fundingEarned >= 0 ? "positive" : "negative"}>{money(leg.fundingEarned)}</b>
                </div>
              ))}
              <p>永续腿显示近 7 日 Funding；现货杠杆腿显示当前累计借币利息折算值。正数表示收到，负数表示成本。</p>
              <AutoCloseControl
                position={p}
                rule={autoCloseRules.find((rule) => rule.positionId === p.id && ["armed", "triggered", "closing"].includes(rule.status)) || autoCloseRules.find((rule) => rule.positionId === p.id)}
                busy={autoCloseBusy === p.id}
                onSet={onSetAutoClose}
                onCancel={onCancelAutoClose}
              />
              <PositionHistoryDetails position={p} data={histories[p.id] || histories[p.long.symbol]} loading={historyLoading === p.id} />
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
  const [expandedOpportunity, setExpandedOpportunity] = useState("");
  const [opportunityHistories, setOpportunityHistories] = useState({});
  const [historyLoading, setHistoryLoading] = useState("");
  const [account, setAccount] = useState(null);
  const [positions, setPositions] = useState([]);
  const [autoCloseRules, setAutoCloseRules] = useState([]);
  const [autoCloseBusy, setAutoCloseBusy] = useState("");
  const [protection, setProtection] = useState(null);
  const [advisor, setAdvisor] = useState(undefined);
  const [tradeItem, setTradeItem] = useState(null);
  const [tradeBusy, setTradeBusy] = useState(false);
  const [adjustment, setAdjustment] = useState(null);
  const [batchPanel, setBatchPanel] = useState(null);
  const [batchTask, setBatchTask] = useState(null);
  const [closingId, setClosingId] = useState("");
  const [notice, setNotice] = useState("");
  const [streamStatus, setStreamStatus] = useState({ Binance: "idle", Bybit: "idle" });
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
      const [summaryResponse, positionsResponse, protectionResponse, autoCloseResponse] = await Promise.all([
        fetch("/backend/account/summary", { cache: "no-store" }),
        fetch("/backend/positions", { cache: "no-store" }),
        fetch("/backend/account/hedge-protection", { cache: "no-store" }),
        fetch("/backend/auto-close", { cache: "no-store" })
      ]);
      const [summary, active, protectionStatus, rules] = await Promise.all([
        readJson(summaryResponse, "账户汇总接口"),
        readJson(positionsResponse, "持仓接口"),
        readJson(protectionResponse, "仓位保护接口"),
        readJson(autoCloseResponse, "自动平仓接口")
      ]);
      if (summaryResponse.ok) setAccount(summary);
      if (positionsResponse.ok) setPositions(active);
      if (protectionResponse.ok) setProtection(protectionStatus);
      if (autoCloseResponse.ok) setAutoCloseRules(rules);
      if (!summaryResponse.ok || !positionsResponse.ok || !protectionResponse.ok || !autoCloseResponse.ok) {
        throw new Error(summary.error || active.error || protectionStatus.error || rules.error);
      }
      accountBackoffUntil.current = 0;
    } catch (e) {
      accountBackoffUntil.current = Date.now() + 15_000;
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
      accountBackoffUntil.current = Date.now() + 15_000;
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
      accountBackoffUntil.current = Date.now() + 15_000;
      setNotice(`持仓服务：${e.message}`);
    }
  }, []);

  const loadAutoCloseRules = useCallback(async () => {
    try {
      const response = await fetch("/backend/auto-close", { cache: "no-store" });
      const rules = await response.json();
      if (!response.ok) throw new Error(rules.error);
      setAutoCloseRules(rules);
    } catch (e) {
      setNotice(`自动平仓：${e.message}`);
    }
  }, []);

  const loadAdvisor = useCallback(async () => {
    try {
      const response = await fetch("/backend/advisor/latest", { cache: "no-store" });
      const recommendation = await readJson(response, "策略建议接口");
      if (!response.ok) throw new Error(recommendation.error || `HTTP ${response.status}`);
      setAdvisor(recommendation);
    } catch (error) {
      setNotice(`策略建议：${error.message}`);
    }
  }, []);

  useEffect(() => {
    load();
    loadAccount();
    loadAdvisor();
    const opportunityTimer = window.setInterval(load, 20_000);
    const accountTimer = window.setInterval(loadAccountSummary, 20_000);
    const autoCloseTimer = window.setInterval(loadAutoCloseRules, 15_000);
    const advisorTimer = window.setInterval(loadAdvisor, 15_000);
    const now = new Date();
    const nextFundingRefresh = new Date(now);
    nextFundingRefresh.setMinutes(2, 0, 0);
    if (nextFundingRefresh <= now) nextFundingRefresh.setHours(nextFundingRefresh.getHours() + 1);
    let fundingInterval;
    const fundingTimer = window.setTimeout(() => {
      loadFullPositions();
      fundingInterval = window.setInterval(loadFullPositions, 60 * 60_000);
    }, nextFundingRefresh.getTime() - now.getTime());
    return () => {
      window.clearInterval(opportunityTimer);
      window.clearInterval(accountTimer);
      window.clearInterval(autoCloseTimer);
      window.clearInterval(advisorTimer);
      window.clearTimeout(fundingTimer);
      if (fundingInterval) window.clearInterval(fundingInterval);
    };
  }, [load, loadAccount, loadAccountSummary, loadAdvisor, loadAutoCloseRules, loadFullPositions]);

  const positionSubscriptionKey = useMemo(
    () => account?.mode === "live" ? positions
      .flatMap((position) => [position.long, position.short])
      .map((leg) => `${leg.exchange}:${leg.symbol}`)
      .sort()
      .join("|") : "",
    [account?.mode, positions]
  );

  useEffect(() => {
    const subscriptions = new Set(positionSubscriptionKey.split("|").filter(Boolean));
    if (subscriptions.size === 0) return undefined;
    setStreamStatus({ Binance: "connecting", Bybit: "connecting" });
    let stopped = false;
    const refreshQuotes = async () => {
      try {
        const response = await fetch("/backend/position-quotes", { cache: "no-store" });
        const quotes = await response.json();
        if (!response.ok) throw new Error(quotes.error);
        if (stopped) return;
        const relevant = new Map(
          quotes
            .filter((quote) => subscriptions.has(`${quote.exchange}:${quote.symbol}`))
            .map((quote) => [`${quote.exchange}:${quote.symbol}`, quote])
        );
        setPositions((current) => current.map((position) => {
          const updateLeg = (leg) => {
            const quote = relevant.get(`${leg.exchange}:${leg.symbol}`);
            if (!quote) return leg;
            const markPrice = Number(quote.referencePrice);
            const closePrice = leg.side === "long"
              ? Number(quote.bidPrice)
              : Number(quote.askPrice);
            const unrealizedPnl = leg.side === "long"
              ? (markPrice - leg.entryPrice) * leg.quantity
              : (leg.entryPrice - markPrice) * leg.quantity;
            return {
              ...leg,
              markPrice,
              rawMarkPrice: Number(quote.markPrice),
              lastPrice: Number(quote.lastPrice),
              usesLastPrice: Boolean(quote.usesLastPrice),
              closePrice,
              fundingRate: Number(quote.fundingRate),
              unrealizedPnl
            };
          };
          const long = updateLeg(position.long);
          const short = updateLeg(position.short);
          return { ...position, long, short, unrealizedPnl: long.unrealizedPnl + short.unrealizedPnl };
        }));
        setStreamStatus({
          Binance: [...relevant.values()].some((quote) => quote.exchange === "Binance") ? "live" : "offline",
          Bybit: [...relevant.values()].some((quote) => quote.exchange === "Bybit") ? "live" : "offline"
        });
      } catch {
        if (!stopped) setStreamStatus({ Binance: "reconnecting", Bybit: "reconnecting" });
      }
    };
    refreshQuotes();
    const timer = window.setInterval(refreshQuotes, 2_000);
    return () => {
      stopped = true;
      window.clearInterval(timer);
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
      const guarded = request.spreadGuard;
      const body = guarded ? {
        opportunityId: request.opportunityId,
        targetNotionalUsdt: request.notionalUsdt,
        orderNotionalUsdt: request.orderNotionalUsdt,
        intervalSeconds: request.intervalSeconds,
        leverage: request.leverage,
        spreadGuard: true,
        spreadThreshold: request.spreadThreshold
      } : request;
      const response = await fetch(guarded ? "/backend/trades/batch-increase" : "/backend/trades/open", {
        method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body)
      });
      const json = await response.json();
      if (!response.ok) throw new Error(json.error);
      if (guarded) {
        const item = tradeItem;
        setBatchPanel({
          type: "increase",
          position: {
            id: "", token: item.token.symbol, leverage: request.leverage, notionalUsdt: 0,
            currentSpread: item.spread, long: item.long, short: item.short
          }
        });
        setBatchTask(json);
        setTradeItem(null);
        setNotice("保价差限价开仓任务已启动；价差不满足门槛时会保持等待");
        return;
      }
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
        const opportunity = [...(data?.opportunities || []), ...(data?.spotOpportunities || [])].find((item) =>
          item.token.symbol === adjustment.position.token &&
          item.long.exchange === adjustment.position.long.exchange &&
          item.long.market === adjustment.position.long.market &&
          item.short.exchange === adjustment.position.short.exchange
          && item.short.market === adjustment.position.short.market
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

  async function startBatchIncrease(settings) {
    if (!batchPanel) return;
    setTradeBusy(true);
    try {
      let path;
      let body;
      if (batchPanel.type === "increase") {
        const opportunity = [...(data?.opportunities || []), ...(data?.spotOpportunities || [])].find((item) =>
          item.token.symbol === batchPanel.position.token &&
          item.long.exchange === batchPanel.position.long.exchange &&
          item.long.market === batchPanel.position.long.market &&
          item.short.exchange === batchPanel.position.short.exchange
          && item.short.market === batchPanel.position.short.market
        );
        if (!opportunity) throw new Error("当前机会列表中没有与该持仓方向一致的机会，无法安全加仓");
        path = "/backend/trades/batch-increase";
        body = {
          opportunityId: opportunity.id,
          leverage: batchPanel.position.leverage,
          ...settings
        };
      } else {
        path = "/backend/trades/batch-reduce";
        body = { positionId: batchPanel.position.id, ...settings };
      }
      const response = await fetch(path, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
        signal: AbortSignal.timeout(20000)
      });
      const task = await response.json();
      if (!response.ok) throw new Error(task.error);
      setBatchTask(task);
    } catch (error) {
      try {
        const recoveryResponse = await fetch("/backend/trades/batch-tasks", {
          cache: "no-store",
          signal: AbortSignal.timeout(10000)
        });
        const tasks = await recoveryResponse.json();
        const action = batchPanel.type === "reduce" ? "reduce" : "increase";
        const recovered = recoveryResponse.ok && tasks.find((task) =>
          task.action === action &&
          task.token === batchPanel.position.token &&
          Math.abs(task.targetNotionalUsdt - settings.targetNotionalUsdt) < 0.01 &&
          Math.abs(task.orderNotionalUsdt - settings.orderNotionalUsdt) < 0.01 &&
          Math.abs(task.intervalSeconds - settings.intervalSeconds) < 0.01 &&
          Date.now() - task.startedAt < 10 * 60 * 1000
        );
        if (recovered) {
          setBatchTask(recovered);
          setNotice("创建响应超时，已自动接管后端正在执行的批量任务");
          return;
        }
      } catch {
        // Keep the original creation error when recovery is unavailable.
      }
      setNotice(`批量${batchPanel.type === "reduce" ? "减仓" : "加仓"}启动失败：${error.message}`);
    } finally {
      setTradeBusy(false);
    }
  }

  const refreshBatchTask = useCallback(async (taskId) => {
    try {
      const response = await fetch(`/backend/trades/batch-increase/${taskId}`, { cache: "no-store" });
      const task = await response.json().catch(() => ({}));
      if (!response.ok) {
        const message = task.error || `HTTP ${response.status}`;
        if (response.status === 404 || message.includes("batch task not found")) {
          setBatchTask((current) => current?.id === taskId ? {
            ...current,
            status: "expired",
            error: "后端已重启或任务记录已过期；请核对当前仓位后重新创建任务"
          } : current);
          setNotice("批量任务记录已失效，已停止轮询；请核对当前仓位后重新创建任务");
          await loadAccount();
          return;
        }
        throw new Error(message);
      }
      setBatchTask(task);
      if (task.currentPosition) {
        setPositions((current) => {
          const index = current.findIndex((position) =>
            position.id === task.currentPosition.id ||
            (
              position.token === task.currentPosition.token &&
              position.long.exchange === task.currentPosition.long.exchange &&
              position.short.exchange === task.currentPosition.short.exchange
            )
          );
          if (index < 0) return [...current, task.currentPosition];
          const previous = current[index];
          const updated = {
            ...task.currentPosition,
            long: { ...task.currentPosition.long, closePrice: previous.long.closePrice },
            short: { ...task.currentPosition.short, closePrice: previous.short.closePrice }
          };
          return current.map((position, positionIndex) => positionIndex === index ? updated : position);
        });
      }
      if (["completed", "failed", "cancelled"].includes(task.status)) await loadAccount();
    } catch (error) {
      setNotice(`批量仓位任务进度读取失败：${error.message}`);
    }
  }, [loadAccount]);

  useEffect(() => {
    if (!batchTask?.id || !["queued", "running", "cancelling"].includes(batchTask.status)) return undefined;
    const timer = window.setInterval(() => refreshBatchTask(batchTask.id), 1000);
    return () => window.clearInterval(timer);
  }, [batchTask?.id, batchTask?.status, refreshBatchTask]);

  async function cancelBatchIncrease() {
    if (!batchTask?.id) return;
    try {
      const response = await fetch(`/backend/trades/batch-increase/${batchTask.id}/cancel`, { method: "POST" });
      const task = await response.json().catch(() => ({}));
      if (!response.ok) {
        const message = task.error || `HTTP ${response.status}`;
        if (response.status === 404 || message.includes("batch task not found")) {
          setBatchTask((current) => current ? {
            ...current,
            status: "expired",
            error: "任务记录已过期，无需再发送停止请求"
          } : current);
          setNotice("任务记录已过期，已解除窗口锁定");
          return;
        }
        throw new Error(message);
      }
      setBatchTask(task);
    } catch (error) {
      setNotice(`停止批量加仓失败：${error.message}`);
    }
  }

  async function setAutoClose(positionId, request) {
    setAutoCloseBusy(positionId);
    try {
      const response = await fetch(`/backend/positions/${positionId}/auto-close`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(request)
      });
      const rule = await response.json();
      if (!response.ok) throw new Error(rule.error);
      setNotice(`${rule.token} 自动平仓已启用：APY 低于 ${rule.thresholdApyPercent}% 时，每单 ${money(rule.orderNotionalUsdt)} / ${rule.intervalSeconds}s`);
      await loadAutoCloseRules();
    } catch (error) {
      setNotice(`设置自动平仓失败：${error.message}`);
    } finally {
      setAutoCloseBusy("");
    }
  }

  async function cancelAutoClose(ruleId) {
    const rule = autoCloseRules.find((item) => item.id === ruleId);
    setAutoCloseBusy(rule?.positionId || ruleId);
    try {
      const response = await fetch(`/backend/auto-close/${ruleId}/cancel`, { method: "POST" });
      const result = await response.json();
      if (!response.ok) throw new Error(result.error);
      setNotice(`${result.token} 自动平仓规则已取消`);
      await loadAutoCloseRules();
    } catch (error) {
      setNotice(`取消自动平仓失败：${error.message}`);
    } finally {
      setAutoCloseBusy("");
    }
  }

  async function toggleOpportunity(item) {
    if (expandedOpportunity === item.id) {
      setExpandedOpportunity("");
      return;
    }
    setExpandedOpportunity(item.id);
    if (opportunityHistories[item.id]) return;
    setHistoryLoading(item.id);
    try {
      const response = await fetch(`/backend/opportunity-history/${encodeURIComponent(item.id)}`, { cache: "no-store" });
      const history = await readJson(response, "机会历史接口");
      if (!response.ok) throw new Error(history.error);
      setOpportunityHistories((current) => ({ ...current, [item.id]: history }));
    } catch (error) {
      setNotice(`${item.token.symbol} 历史行情：${error.message}`);
    } finally {
      setHistoryLoading("");
    }
  }

  async function loadPositionHistory(position) {
    if (opportunityHistories[position.id]) return;
    setHistoryLoading(position.id);
    try {
      const opportunity = [...(data?.opportunities || []), ...(data?.spotOpportunities || [])].find((item) =>
        item.token.symbol === position.token &&
        item.long.exchange === position.long.exchange &&
        item.long.market === position.long.market &&
        item.short.exchange === position.short.exchange &&
        item.short.market === position.short.market
      );
      const endpoint = opportunity
        ? `/backend/opportunity-history/${encodeURIComponent(opportunity.id)}`
        : `/backend/spread-history/${encodeURIComponent(position.long.symbol)}`;
      const response = await fetch(endpoint, { cache: "no-store" });
      const history = await readJson(response, "持仓历史接口");
      if (!response.ok) throw new Error(history.error);
      setOpportunityHistories((current) => ({ ...current, [position.id]: history }));
    } catch (error) {
      setNotice(`${position.token} 资金费历史：${error.message}`);
    } finally {
      setHistoryLoading("");
    }
  }

  const rows = useMemo(() => {
    const list = (data?.opportunities || []).filter((x) =>
      [x.token.symbol, x.token.name, ...(x.token.tags || [])]
        .some((value) => value.toLowerCase().includes(query.toLowerCase())) &&
      x.apy * 100 >= minApy
    );
    return [...list].sort((a, b) =>
      sort === "apy" ? b.apy - a.apy : a.breakEvenHours - b.breakEvenHours
    );
  }, [data, query, minApy, sort]);
  const spotRows = useMemo(() => {
    const list = (data?.spotOpportunities || []).filter((x) =>
      [x.token.symbol, x.token.name, ...(x.token.tags || [])]
        .some((value) => value.toLowerCase().includes(query.toLowerCase())) &&
      x.apy * 100 >= minApy
    );
    return [...list].sort((a, b) =>
      sort === "apy" ? b.apy - a.apy : a.breakEvenHours - b.breakEvenHours
    );
  }, [data, query, minApy, sort]);
  const spreadRows = useMemo(
    () => (data?.spreadOpportunities || [])
      .filter((item) => [item.token.symbol, item.token.name, ...(item.token.tags || [])]
        .some((value) => value.toLowerCase().includes(query.toLowerCase())))
      .sort((a, b) => b.spread - a.spread),
    [data, query]
  );
  useEffect(() => {
    setExpandedOpportunity("");
  }, [query, minApy, sort]);

  const best = [...(data?.opportunities || []), ...(data?.spotOpportunities || [])]
    .sort((a, b) => b.apy - a.apy)[0];

  return (
    <main>
      <header className="topbar">
        <a className="brand" href="#"><span className="brand-mark">A</span>ARBIVIEW</a>
        <nav>
          <a href="#account">账户持仓</a>
          <a className="active" href="#opportunities">跨所永续</a>
          <a href="#spot-opportunities">现货–永续</a>
          <a href="#spread-arbitrage">价差套利</a>
          <a href="#methodology">计算说明</a>
        </nav>
        <div className="live-status"><i /> LIVE <span>机会 20s · WS B:{streamStatus.Binance.toUpperCase()} Y:{streamStatus.Bybit.toUpperCase()}</span></div>
      </header>

      <section className="hero">
        <div className="hero-copy">
          <div className="eyebrow"><Sparkles size={14} /> PERPETUAL FUNDING ARBITRAGE</div>
          <h1>捕捉双腿<br /><em>资金费率差</em></h1>
          <p>同时扫描 Binance 与 Bybit 跨所永续，以及同所现货–永续套利机会。</p>
        </div>
        <div className="hero-stats">
          <div><span>交易所合约</span><strong>{data?.universeSize ?? "—"}</strong><small>全部 USDT 永续</small></div>
          <div><span>共同合约</span><strong>{data?.matchedPairs ?? "—"}</strong><small>BINANCE × BYBIT</small></div>
          <div className="highlight"><span>最高资金 APY</span><strong>{best ? pct(best.apy, 1) : "—"}</strong><small>{best ? `${best.token.symbol} · ${best.long.exchange} → ${best.short.exchange}` : "正在扫描"}</small></div>
        </div>
      </section>

      {notice && <div className="notice"><span>{notice}</span><button onClick={() => setNotice("")}><X size={14} /></button></div>}

      <AdvisorCard recommendation={advisor} />

      <AccountBoard account={account} positions={positions} protection={protection} histories={opportunityHistories} historyLoading={historyLoading} onLoadHistory={loadPositionHistory} autoCloseRules={autoCloseRules} onSetAutoClose={setAutoClose} onCancelAutoClose={cancelAutoClose} autoCloseBusy={autoCloseBusy} onClose={closePosition} onAdjust={(position, type) => {
        if (type === "increase" || type === "reduce") {
          if (batchTask && ["queued", "running", "cancelling"].includes(batchTask.status)) {
            setNotice("已有批量仓位任务正在执行，请先等待完成或停止任务");
            return;
          }
          setBatchPanel({ position, type });
          setBatchTask(null);
        } else {
          setAdjustment({ position, type });
        }
      }} busyId={closingId} />

      <section className="workspace" id="opportunities">
        <div className="section-head">
          <div>
            <div className="section-kicker"><TrendingUp size={15} /> CROSS-EXCHANGE PERPETUAL</div>
            <h2>跨所永续套利 <span>{rows.length}</span></h2>
            <div className="configured">仅排名下一整点实际发生结算的机会 · APY 只计该整点结算腿</div>
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
          <span>资产</span><span>执行路径 / 双腿行情</span><span>资金 APY</span><span>当前 / 均值</span><span>预计回本</span>
        </div>

        <div className="cards">
          {loading && !data && <div className="state"><RefreshCw className="spin" />正在扫描两个交易所的永续合约…</div>}
          {error && <div className="state error">行情连接失败：{error}<button onClick={load}>重试</button></div>}
          {!loading && !error && rows.length === 0 && <div className="state">当前筛选条件下暂无套利机会</div>}
          {rows.map((item, i) => <OpportunityCard
            key={item.id}
            item={item}
            index={i}
            onTrade={setTradeItem}
            expanded={expandedOpportunity === item.id}
            onToggle={() => toggleOpportunity(item)}
            history={opportunityHistories[item.id]}
            historyLoading={historyLoading === item.id}
          />)}
        </div>
      </section>

      <section className="workspace" id="spot-opportunities">
        <div className="section-head">
          <div>
            <div className="section-kicker"><TrendingUp size={15} /> SPOT × PERPETUAL</div>
            <h2>同所现货–永续套利 <span>{spotRows.length}</span></h2>
          </div>
          <div className="configured">净 APY 前 10 · 做空现货已过滤无可借库存</div>
        </div>

        <div className="column-head">
          <span>资产</span><span>执行路径 / 双腿行情</span><span>资金 APY</span><span>当前 / 均值</span><span>预计回本</span>
        </div>

        <div className="cards">
          {loading && !data && <div className="state"><RefreshCw className="spin" />正在扫描现货与永续市场…</div>}
          {!loading && !error && spotRows.length === 0 && <div className="state">当前筛选条件下暂无同所现货–永续机会</div>}
          {spotRows.map((item, i) => <OpportunityCard
            key={item.id}
            item={item}
            index={i}
            onTrade={setTradeItem}
            expanded={expandedOpportunity === item.id}
            onToggle={() => toggleOpportunity(item)}
            history={opportunityHistories[item.id]}
            historyLoading={historyLoading === item.id}
          />)}
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
        <div><span>02</span><p><b>资金 APY</b>跨所永续只计下一整点实际结算腿的完整费率；若两所周期不同，未在该整点结算的腿不计入。现货–永续仍按合约周期折算。</p></div>
        <div><span>03</span><p><b>预计回本</b>以资金费率覆盖“当前方向价差 − 24h 时间加权方向价差”及双腿开平仓 taker 手续费；加权半衰期 6 小时，越近权重越高。Binance 0.05%，Bybit 0.055%。</p></div>
      </section>

      <footer><span><ShieldCheck size={15} />数据仅供研究，不构成投资建议</span><span>ARBIVIEW / 2026</span></footer>
      {tradeItem && <TradeModal item={tradeItem} mode={account?.mode} onClose={() => setTradeItem(null)} onSubmit={openTrade} busy={tradeBusy} />}
      {adjustment && <AdjustModal position={adjustment.position} type={adjustment.type} onClose={() => setAdjustment(null)} onSubmit={adjustPosition} busy={tradeBusy} />}
      {batchPanel && <BatchIncreasePanel
        position={batchPanel.position}
        action={batchPanel.type}
        task={batchTask}
        busy={tradeBusy}
        onStart={startBatchIncrease}
        onCancel={cancelBatchIncrease}
        onClose={() => {
          setBatchPanel(null);
          setBatchTask(null);
        }}
      />}
    </main>
  );
}
