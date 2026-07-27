use crate::{
    config::{Config, ExchangeCredentials, TradingMode},
    market::MarketService,
    models::*,
};
use anyhow::{anyhow, bail, Context, Result};
use hmac::{Hmac, Mac};
use reqwest::{Client, Method};
use serde_json::{json, Value};
use sha2::Sha256;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};
use tokio::sync::{Mutex, RwLock};
use uuid::Uuid;

type HmacSha256 = Hmac<Sha256>;
type FundingTotals = HashMap<String, f64>;
type FundingCache = Option<(i64, IncomeSummary, IncomeSummary)>;

#[derive(Clone, Default)]
struct IncomeSummary {
    funding_by_symbol: FundingTotals,
    trading_fees_by_symbol: FundingTotals,
    closed_pnl_by_symbol: FundingTotals,
    closed_position_pnl: f64,
    funding_income: f64,
    trading_fees: f64,
}

impl IncomeSummary {
    fn realized_pnl(&self) -> f64 {
        self.closed_position_pnl + self.funding_income - self.trading_fees
    }

    fn for_symbols(&self, symbols: &HashSet<String>) -> Self {
        let funding_by_symbol = self
            .funding_by_symbol
            .iter()
            .filter(|(symbol, _)| symbols.contains(*symbol))
            .map(|(symbol, value)| (symbol.clone(), *value))
            .collect::<HashMap<_, _>>();
        let trading_fees_by_symbol = self
            .trading_fees_by_symbol
            .iter()
            .filter(|(symbol, _)| symbols.contains(*symbol))
            .map(|(symbol, value)| (symbol.clone(), *value))
            .collect::<HashMap<_, _>>();
        let closed_pnl_by_symbol = self
            .closed_pnl_by_symbol
            .iter()
            .filter(|(symbol, _)| symbols.contains(*symbol))
            .map(|(symbol, value)| (symbol.clone(), *value))
            .collect::<HashMap<_, _>>();
        Self {
            closed_position_pnl: closed_pnl_by_symbol.values().sum(),
            funding_income: funding_by_symbol.values().sum(),
            trading_fees: trading_fees_by_symbol.values().sum(),
            funding_by_symbol,
            trading_fees_by_symbol,
            closed_pnl_by_symbol,
        }
    }
}

const MAX_RECONCILIATION_ATTEMPTS: u8 = 3;
const HEDGE_PROTECTION_ORDER_USDT: f64 = 100.0;
const HEDGE_PROTECTION_INTERVAL_SECONDS: f64 = 1.0;
const HEDGE_PROTECTION_TOLERANCE_RATIO: f64 = 0.03;
const POSITION_RECONCILIATION_TOLERANCE_RATIO: f64 = 0.01;
const HEDGE_PROTECTION_MINIMUM_DIFFERENCE_USDT: f64 = 10.0;
const FUNDING_REFRESH_DELAY_SECONDS: i64 = 2 * 60;

struct PhaseOneResult {
    state: Option<PositionLeg>,
    attempts: u8,
    anomalies: u8,
    orders: Vec<OrderExecution>,
}

struct PhaseTwoResult {
    long: Option<PositionLeg>,
    short: Option<PositionLeg>,
    attempts: u8,
    anomalies: u8,
    orders: Vec<OrderExecution>,
}

#[derive(Clone)]
struct ProtectedRoute {
    token: String,
    symbol: String,
    long_exchange: String,
    short_exchange: String,
}

#[derive(Clone)]
pub struct TradingService {
    config: Config,
    client: Client,
    market: MarketService,
    paper_positions: Arc<RwLock<HashMap<String, Position>>>,
    funding_cache: Arc<RwLock<FundingCache>>,
    income_refresh_lock: Arc<Mutex<()>>,
    batch_tasks: Arc<RwLock<HashMap<String, BatchIncreaseTask>>>,
    auto_close_rules: Arc<RwLock<HashMap<String, AutoCloseRule>>>,
    protected_routes: Arc<RwLock<HashMap<String, ProtectedRoute>>>,
    protection_events: Arc<RwLock<Vec<HedgeProtectionEvent>>>,
    execution_lock: Arc<Mutex<()>>,
}

impl TradingService {
    pub fn new(config: Config, market: MarketService) -> Result<Self> {
        let auto_close_rules = load_auto_close_rules(&config)?;
        Ok(Self {
            config,
            market,
            client: Client::builder().timeout(Duration::from_secs(15)).build()?,
            paper_positions: Arc::new(RwLock::new(HashMap::new())),
            funding_cache: Arc::new(RwLock::new(None)),
            income_refresh_lock: Arc::new(Mutex::new(())),
            batch_tasks: Arc::new(RwLock::new(HashMap::new())),
            auto_close_rules: Arc::new(RwLock::new(auto_close_rules)),
            protected_routes: Arc::new(RwLock::new(HashMap::new())),
            protection_events: Arc::new(RwLock::new(Vec::new())),
            execution_lock: Arc::new(Mutex::new(())),
        })
    }

    pub fn spawn_auto_close_monitor(&self) {
        let service = self.clone();
        tokio::spawn(async move {
            // Give market/account services time to warm up before the first evaluation.
            tokio::time::sleep(Duration::from_secs(5)).await;
            loop {
                if let Err(error) = service.evaluate_auto_close_rules().await {
                    tracing::warn!("auto-close evaluation failed: {error:#}");
                }
                tokio::time::sleep(Duration::from_secs(15)).await;
            }
        });
    }

    pub fn spawn_hedge_protection_monitor(&self) {
        if self.config.trading_mode != TradingMode::Live {
            return;
        }
        let service = self.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(3)).await;
            loop {
                if let Err(error) = service.inspect_hedge_protection().await {
                    tracing::warn!("hedge protection inspection failed: {error:#}");
                }
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        });
    }

    pub async fn hedge_protection_status(&self) -> HedgeProtectionStatus {
        let mut protected_tokens = self
            .protected_routes
            .read()
            .await
            .values()
            .map(|route| route.token.clone())
            .collect::<Vec<_>>();
        protected_tokens.sort();
        protected_tokens.dedup();
        HedgeProtectionStatus {
            enabled: self.config.trading_mode == TradingMode::Live,
            tolerance_percent: HEDGE_PROTECTION_TOLERANCE_RATIO * 100.0,
            minimum_difference_usdt: HEDGE_PROTECTION_MINIMUM_DIFFERENCE_USDT,
            order_notional_usdt: HEDGE_PROTECTION_ORDER_USDT,
            interval_seconds: HEDGE_PROTECTION_INTERVAL_SECONDS,
            protected_tokens,
            events: self.protection_events.read().await.clone(),
        }
    }

    async fn inspect_hedge_protection(&self) -> Result<()> {
        let (binance, bybit, quotes) = tokio::try_join!(
            self.binance_positions(),
            self.bybit_positions(),
            self.market.position_quotes()
        )?;
        let mut legs = binance.into_iter().chain(bybit).collect::<Vec<_>>();
        apply_position_quotes(&mut legs, &quotes);
        self.remember_protected_routes(&legs).await;
        if self
            .batch_tasks
            .read()
            .await
            .values()
            .any(|task| matches!(task.status.as_str(), "queued" | "running" | "cancelling"))
        {
            return Ok(());
        }
        let routes = self
            .protected_routes
            .read()
            .await
            .values()
            .cloned()
            .collect::<Vec<_>>();
        for route in routes {
            let long = find_route_leg(&legs, &route.long_exchange, &route.symbol, "long");
            let short = find_route_leg(&legs, &route.short_exchange, &route.symbol, "short");
            let needs_protection = match (&long, &short) {
                (Some(long), Some(short)) => hedge_notional_needs_protection(long, short),
                (Some(_), None) | (None, Some(_)) => true,
                (None, None) => false,
            };
            if !needs_protection {
                continue;
            }
            let Ok(_guard) = self.execution_lock.try_lock() else {
                continue;
            };
            // Re-read under the execution lock. A normal trade may have completed
            // between the initial observation and acquiring the lock.
            let (binance, bybit, quotes) = tokio::try_join!(
                self.binance_positions(),
                self.bybit_positions(),
                self.market.position_quotes()
            )?;
            let mut actual = binance.into_iter().chain(bybit).collect::<Vec<_>>();
            apply_position_quotes(&mut actual, &quotes);
            let long = find_route_leg(&actual, &route.long_exchange, &route.symbol, "long");
            let short = find_route_leg(&actual, &route.short_exchange, &route.symbol, "short");
            let still_unbalanced = match (&long, &short) {
                (Some(long), Some(short)) => hedge_notional_needs_protection(long, short),
                (Some(_), None) | (None, Some(_)) => true,
                (None, None) => false,
            };
            if still_unbalanced {
                self.run_hedge_protection(route).await;
            }
        }
        Ok(())
    }

    async fn remember_protected_routes(&self, legs: &[PositionLeg]) {
        let mut routes = self.protected_routes.write().await;
        for long in legs.iter().filter(|leg| leg.side == "long") {
            if let Some(short) = legs.iter().find(|leg| {
                leg.symbol == long.symbol && leg.side == "short" && leg.exchange != long.exchange
            }) {
                let token = long.symbol.trim_end_matches("USDT").to_string();
                routes.insert(
                    token.clone(),
                    ProtectedRoute {
                        token,
                        symbol: long.symbol.clone(),
                        long_exchange: long.exchange.clone(),
                        short_exchange: short.exchange.clone(),
                    },
                );
            }
        }
    }

    async fn run_hedge_protection(&self, route: ProtectedRoute) {
        let event_id = Uuid::new_v4().to_string();
        let now = chrono::Utc::now().timestamp_millis();
        let initial = HedgeProtectionEvent {
            id: event_id.clone(),
            token: route.token.clone(),
            event_type: "detecting".into(),
            status: "running".into(),
            message: "检测到双腿名义价值偏差超过 1%".into(),
            started_at: now,
            updated_at: now,
            initial_long_notional_usdt: None,
            initial_short_notional_usdt: None,
            final_long_notional_usdt: None,
            final_short_notional_usdt: None,
            orders: vec![],
        };
        self.push_protection_event(initial).await;
        let market_legs = match self.market.trading_legs().await {
            Ok(legs) => legs,
            Err(error) => {
                self.fail_protection_event(&event_id, format!("合约元数据读取失败：{error:#}"))
                    .await;
                return;
            }
        };
        loop {
            let actual = match tokio::try_join!(
                self.binance_positions(),
                self.bybit_positions(),
                self.market.position_quotes()
            ) {
                Ok((binance, bybit, quotes)) => {
                    let mut legs = binance.into_iter().chain(bybit).collect::<Vec<_>>();
                    apply_position_quotes(&mut legs, &quotes);
                    legs
                }
                Err(error) => {
                    self.fail_protection_event(&event_id, format!("仓位读取失败：{error:#}"))
                        .await;
                    return;
                }
            };
            let long = find_route_leg(&actual, &route.long_exchange, &route.symbol, "long");
            let short = find_route_leg(&actual, &route.short_exchange, &route.symbol, "short");
            let long_notional = long.as_ref().map(position_notional).unwrap_or(0.0);
            let short_notional = short.as_ref().map(position_notional).unwrap_or(0.0);
            self.update_protection_event(&event_id, |event| {
                if event.initial_long_notional_usdt.is_none() {
                    event.initial_long_notional_usdt = Some(long_notional);
                    event.initial_short_notional_usdt = Some(short_notional);
                }
                event.final_long_notional_usdt = Some(long_notional);
                event.final_short_notional_usdt = Some(short_notional);
            })
            .await;
            let (target, event_type, message) = match (long, short) {
                (None, None) => {
                    self.complete_protection_event(&event_id, "双腿仓位均已归零")
                        .await;
                    return;
                }
                (Some(leg), None) | (None, Some(leg)) => {
                    (leg, "orphan_exit", "检测到一条腿归零，正在批量退出剩余腿")
                }
                (Some(long), Some(short)) => {
                    if !hedge_notional_needs_protection(&long, &short) {
                        self.complete_protection_event(
                            &event_id,
                            "当前差额未同时超过 1% 与 10 USDT，保护停止",
                        )
                        .await;
                        return;
                    }
                    if position_notional(&long) > position_notional(&short) {
                        (long, "mismatch_reduce", "正在削减名义价值更大的 LONG 腿")
                    } else {
                        (short, "mismatch_reduce", "正在削减名义价值更大的 SHORT 腿")
                    }
                }
            };
            self.update_protection_event(&event_id, |event| {
                event.event_type = event_type.into();
                event.message = message.into();
            })
            .await;
            let current_notional = position_notional(&target);
            let other_notional = actual
                .iter()
                .find(|leg| {
                    leg.symbol == target.symbol
                        && leg.exchange != target.exchange
                        && leg.side != target.side
                })
                .map(position_notional)
                .unwrap_or(0.0);
            let excess_notional = if event_type == "orphan_exit" {
                current_notional
            } else {
                (current_notional - other_notional).max(0.0)
            };
            let metadata = match market_legs
                .iter()
                .find(|leg| leg.exchange == target.exchange && leg.symbol == target.symbol)
            {
                Some(metadata) => metadata,
                None => {
                    self.fail_protection_event(&event_id, "找不到剩余腿的合约元数据".into())
                        .await;
                    return;
                }
            };
            let quantity = protection_order_quantity(
                &target,
                excess_notional,
                metadata.qty_step,
                event_type == "orphan_exit",
            );
            if quantity <= 0.0 {
                self.fail_protection_event(&event_id, "保护订单数量计算为零".into())
                    .await;
                return;
            }
            let side = if target.side == "long" { "Sell" } else { "Buy" };
            match self
                .place(&target.exchange, &target.symbol, side, quantity, true, 0.0)
                .await
            {
                Ok(order) => {
                    self.update_protection_event(&event_id, |event| event.orders.push(order))
                        .await;
                }
                Err(error) => {
                    self.fail_protection_event(
                        &event_id,
                        format!("保护性 reduceOnly 订单失败：{error:#}"),
                    )
                    .await;
                    return;
                }
            }
            tokio::time::sleep(Duration::from_secs_f64(HEDGE_PROTECTION_INTERVAL_SECONDS)).await;
        }
    }

    async fn push_protection_event(&self, event: HedgeProtectionEvent) {
        let mut events = self.protection_events.write().await;
        events.push(event);
        if events.len() > 100 {
            events.remove(0);
        }
    }

    async fn update_protection_event<F>(&self, id: &str, update: F)
    where
        F: FnOnce(&mut HedgeProtectionEvent),
    {
        if let Some(event) = self
            .protection_events
            .write()
            .await
            .iter_mut()
            .find(|event| event.id == id)
        {
            update(event);
            event.updated_at = chrono::Utc::now().timestamp_millis();
        }
    }

    async fn complete_protection_event(&self, id: &str, message: &str) {
        self.update_protection_event(id, |event| {
            event.status = "completed".into();
            event.message = message.into();
        })
        .await;
    }

    async fn fail_protection_event(&self, id: &str, message: String) {
        tracing::error!(event_id = id, %message, "hedge protection failed");
        self.update_protection_event(id, |event| {
            event.status = "failed".into();
            event.message = message;
        })
        .await;
    }

    pub async fn set_auto_close(
        &self,
        position_id: &str,
        threshold_apy_percent: f64,
        order_notional_usdt: f64,
        interval_seconds: f64,
    ) -> Result<AutoCloseRule> {
        if !(-100_000.0..=100_000.0).contains(&threshold_apy_percent) {
            bail!("threshold APY must be between -100000% and 100000%");
        }
        if !(10.0..=1_000_000.0).contains(&order_notional_usdt) {
            bail!("order notional must be between 10 and 1,000,000 USDT");
        }
        if !(0.5..=3_600.0).contains(&interval_seconds) {
            bail!("interval must be between 0.5 and 3,600 seconds");
        }
        let position = self
            .positions()
            .await?
            .into_iter()
            .find(|position| position.id == position_id)
            .ok_or_else(|| anyhow!("position not found"))?;
        let now = chrono::Utc::now().timestamp_millis();
        let rule = AutoCloseRule {
            id: Uuid::new_v4().to_string(),
            position_id: position.id.clone(),
            token: position.token,
            threshold_apy_percent,
            order_notional_usdt,
            interval_seconds,
            status: "armed".into(),
            current_apy_percent: None,
            consecutive_low_readings: 0,
            completed_notional_usdt: 0.0,
            created_at: now,
            updated_at: now,
            triggered_at: None,
            error: None,
        };
        let mut rules = self.auto_close_rules.write().await;
        for existing in rules
            .values_mut()
            .filter(|existing| existing.position_id == position.id && existing.status == "armed")
        {
            existing.status = "replaced".into();
            existing.updated_at = now;
        }
        rules.insert(rule.id.clone(), rule.clone());
        drop(rules);
        self.persist_auto_close_rules().await;
        Ok(rule)
    }

    pub async fn auto_close_rules(&self) -> Vec<AutoCloseRule> {
        let mut rules = self
            .auto_close_rules
            .read()
            .await
            .values()
            .cloned()
            .collect::<Vec<_>>();
        rules.sort_by_key(|rule| std::cmp::Reverse(rule.created_at));
        rules
    }

    pub async fn cancel_auto_close(&self, id: &str) -> Result<AutoCloseRule> {
        let mut rules = self.auto_close_rules.write().await;
        let rule = rules
            .get_mut(id)
            .ok_or_else(|| anyhow!("auto-close rule not found"))?;
        if rule.status != "armed" {
            bail!("only an armed auto-close rule can be cancelled");
        }
        rule.status = "cancelled".into();
        rule.updated_at = chrono::Utc::now().timestamp_millis();
        let result = rule.clone();
        drop(rules);
        self.persist_auto_close_rules().await;
        Ok(result)
    }

    async fn evaluate_auto_close_rules(&self) -> Result<()> {
        let armed = self
            .auto_close_rules
            .read()
            .await
            .values()
            .filter(|rule| rule.status == "armed")
            .cloned()
            .collect::<Vec<_>>();
        if armed.is_empty() {
            return Ok(());
        }
        let positions = self.positions().await?;
        let opportunities = self.market.opportunities().await?;
        for rule in armed {
            let Some(position) = positions
                .iter()
                .find(|position| position.id == rule.position_id)
            else {
                self.update_auto_close(&rule.id, |rule| rule.status = "completed".into())
                    .await;
                continue;
            };
            let Some(current_apy_percent) =
                held_route_apy_percent(position, &opportunities.opportunities)
            else {
                // Absence means the token/route was not present in a complete
                // market scan. It is missing data, never evidence of a 0% APY.
                self.update_auto_close(&rule.id, |rule| {
                    rule.current_apy_percent = None;
                    rule.consecutive_low_readings = 0;
                })
                .await;
                tracing::warn!(
                    token = %position.token,
                    rule_id = %rule.id,
                    "auto-close skipped because held-route APY is unavailable"
                );
                continue;
            };
            let consecutive_low_readings = {
                let mut rules = self.auto_close_rules.write().await;
                let Some(stored) = rules.get_mut(&rule.id) else {
                    continue;
                };
                stored.current_apy_percent = Some(current_apy_percent);
                if current_apy_percent < stored.threshold_apy_percent {
                    stored.consecutive_low_readings =
                        stored.consecutive_low_readings.saturating_add(1);
                } else {
                    stored.consecutive_low_readings = 0;
                }
                stored.updated_at = chrono::Utc::now().timestamp_millis();
                stored.consecutive_low_readings
            };
            self.persist_auto_close_rules().await;
            if consecutive_low_readings >= 3 {
                let claimed = {
                    let mut rules = self.auto_close_rules.write().await;
                    let Some(stored) = rules.get_mut(&rule.id) else {
                        continue;
                    };
                    if stored.status != "armed" {
                        false
                    } else {
                        let now = chrono::Utc::now().timestamp_millis();
                        stored.status = "triggered".into();
                        stored.triggered_at = Some(now);
                        stored.updated_at = now;
                        true
                    }
                };
                if claimed {
                    let service = self.clone();
                    tokio::spawn(async move {
                        service.run_auto_close(rule).await;
                    });
                }
            }
        }
        Ok(())
    }

    async fn run_auto_close(&self, rule: AutoCloseRule) {
        self.update_auto_close(&rule.id, |rule| rule.status = "closing".into())
            .await;
        loop {
            let position = match self.positions().await.map(|positions| {
                positions
                    .into_iter()
                    .find(|position| position.id == rule.position_id)
            }) {
                Ok(Some(position)) => position,
                Ok(None) => {
                    self.update_auto_close(&rule.id, |rule| rule.status = "completed".into())
                        .await;
                    return;
                }
                Err(error) => {
                    self.fail_auto_close(&rule.id, error).await;
                    return;
                }
            };
            let remaining = position.notional_usdt;
            let result = match auto_close_reduction(
                remaining,
                rule.order_notional_usdt,
                self.config.position_tolerance_usdt,
            ) {
                Some(amount) => {
                    self.reduce(
                        &rule.position_id,
                        AdjustPositionRequest {
                            notional_usdt: amount,
                        },
                    )
                    .await
                }
                None => self.close(&rule.position_id).await,
            };
            match result {
                Ok(response) => {
                    let reduced = if response.position.status == "closed" {
                        remaining
                    } else {
                        (remaining - response.position.notional_usdt).max(0.0)
                    };
                    self.update_auto_close(&rule.id, |rule| {
                        rule.completed_notional_usdt += reduced
                    })
                    .await;
                    if response.position.status == "closed" {
                        self.update_auto_close(&rule.id, |rule| rule.status = "completed".into())
                            .await;
                        return;
                    }
                }
                Err(error) => {
                    self.fail_auto_close(&rule.id, error).await;
                    return;
                }
            }
            tokio::time::sleep(Duration::from_secs_f64(rule.interval_seconds)).await;
        }
    }

    async fn update_auto_close<F>(&self, id: &str, update: F)
    where
        F: FnOnce(&mut AutoCloseRule),
    {
        if let Some(rule) = self.auto_close_rules.write().await.get_mut(id) {
            update(rule);
            rule.updated_at = chrono::Utc::now().timestamp_millis();
        }
        self.persist_auto_close_rules().await;
    }

    async fn fail_auto_close(&self, id: &str, error: anyhow::Error) {
        self.update_auto_close(id, |rule| {
            rule.status = "failed".into();
            rule.error = Some(format!("{error:#}"));
        })
        .await;
    }

    async fn persist_auto_close_rules(&self) {
        let Some(path) = self.config.auto_close_state_path.as_ref() else {
            return;
        };
        let rules = self.auto_close_rules.read().await;
        let data = match serde_json::to_vec_pretty(&rules.values().collect::<Vec<_>>()) {
            Ok(data) => data,
            Err(error) => {
                tracing::error!("failed to serialize auto-close rules: {error}");
                return;
            }
        };
        if let Some(parent) = path.parent() {
            if let Err(error) = std::fs::create_dir_all(parent) {
                tracing::error!("failed to create auto-close state directory: {error}");
                return;
            }
        }
        let temporary = path.with_extension("tmp");
        if let Err(error) =
            std::fs::write(&temporary, data).and_then(|_| std::fs::rename(&temporary, path))
        {
            tracing::error!("failed to persist auto-close rules: {error}");
        }
    }

    pub async fn open(&self, request: OpenTradeRequest) -> Result<TradeResponse> {
        let opportunity = self.resolve_opportunity(&request.opportunity_id).await?;
        self.open_opportunity(opportunity, request).await
    }

    async fn resolve_opportunity(&self, opportunity_id: &str) -> Result<Opportunity> {
        let snapshot = self.market.opportunities().await?;
        snapshot
            .opportunities
            .into_iter()
            .chain(snapshot.spread_opportunities)
            .find(|x| x.id == opportunity_id)
            .ok_or_else(|| anyhow!("opportunity is no longer available"))
    }

    async fn open_opportunity(
        &self,
        opportunity: Opportunity,
        request: OpenTradeRequest,
    ) -> Result<TradeResponse> {
        if !(10.0..=1_000_000.0).contains(&request.notional_usdt) {
            bail!("notionalUsdt must be between 10 and 1,000,000");
        }
        if !(1..=20).contains(&request.leverage) {
            bail!("leverage must be between 1 and 20");
        }
        match self.config.trading_mode {
            TradingMode::Paper => self.open_paper(opportunity, request).await,
            TradingMode::Live => {
                let _guard = self.execution_lock.lock().await;
                self.open_live(opportunity, request).await
            }
        }
    }

    pub async fn start_batch_increase(
        &self,
        request: BatchIncreaseRequest,
    ) -> Result<BatchIncreaseTask> {
        if !(10.0..=1_000_000.0).contains(&request.target_notional_usdt) {
            bail!("targetNotionalUsdt must be between 10 and 1,000,000");
        }
        if !(10.0..=request.target_notional_usdt).contains(&request.order_notional_usdt) {
            bail!("orderNotionalUsdt must be between 10 and targetNotionalUsdt");
        }
        if !(0.5..=3_600.0).contains(&request.interval_seconds) {
            bail!("intervalSeconds must be between 0.5 and 3,600");
        }
        if !(1..=20).contains(&request.leverage) {
            bail!("leverage must be between 1 and 20");
        }
        let opportunity = self.resolve_opportunity(&request.opportunity_id).await?;
        let mut tasks = self.batch_tasks.write().await;
        if let Some(existing) = tasks.values().find(|task| {
            task.action == "increase"
                && task.token == opportunity.token.symbol
                && matches!(task.status.as_str(), "queued" | "running" | "cancelling")
        }) {
            return Ok(existing.clone());
        }
        if tasks
            .values()
            .any(|task| matches!(task.status.as_str(), "queued" | "running" | "cancelling"))
        {
            bail!("another batch position task is already active");
        }
        let now = chrono::Utc::now().timestamp_millis();
        let total_batches =
            (request.target_notional_usdt / request.order_notional_usdt).ceil() as usize;
        let task = BatchIncreaseTask {
            id: Uuid::new_v4().to_string(),
            action: "increase".into(),
            status: "queued".into(),
            token: opportunity.token.symbol.clone(),
            long_exchange: opportunity.long.exchange.clone(),
            short_exchange: opportunity.short.exchange.clone(),
            target_notional_usdt: request.target_notional_usdt,
            order_notional_usdt: request.order_notional_usdt,
            interval_seconds: request.interval_seconds,
            completed_notional_usdt: 0.0,
            completed_batches: 0,
            total_batches,
            started_at: now,
            updated_at: now,
            cancel_requested: false,
            error: None,
            current_position: None,
            logs: vec![],
        };
        tasks.insert(task.id.clone(), task.clone());
        drop(tasks);

        let service = self.clone();
        let task_id = task.id.clone();
        tokio::spawn(async move {
            service
                .run_batch_increase(task_id, opportunity, request)
                .await;
        });
        Ok(task)
    }

    pub async fn start_batch_reduce(
        &self,
        request: BatchReduceRequest,
    ) -> Result<BatchIncreaseTask> {
        if !(10.0..=1_000_000.0).contains(&request.target_notional_usdt) {
            bail!("targetNotionalUsdt must be between 10 and 1,000,000");
        }
        if !(10.0..=request.target_notional_usdt).contains(&request.order_notional_usdt) {
            bail!("orderNotionalUsdt must be between 10 and targetNotionalUsdt");
        }
        if !(0.5..=3_600.0).contains(&request.interval_seconds) {
            bail!("intervalSeconds must be between 0.5 and 3,600");
        }
        let position = self
            .positions()
            .await?
            .into_iter()
            .find(|position| position.id == request.position_id)
            .ok_or_else(|| anyhow!("position not found"))?;
        let maximum_reduction =
            (position.notional_usdt - self.config.position_tolerance_usdt).max(0.0);
        if request.target_notional_usdt > maximum_reduction {
            bail!(
                "targetNotionalUsdt must leave at least {:.2} USDT per leg; use close for a full exit",
                self.config.position_tolerance_usdt
            );
        }
        let mut tasks = self.batch_tasks.write().await;
        if let Some(existing) = tasks.values().find(|task| {
            task.action == "reduce"
                && task.token == position.token
                && matches!(task.status.as_str(), "queued" | "running" | "cancelling")
        }) {
            return Ok(existing.clone());
        }
        if tasks
            .values()
            .any(|task| matches!(task.status.as_str(), "queued" | "running" | "cancelling"))
        {
            bail!("another batch position task is already active");
        }
        let now = chrono::Utc::now().timestamp_millis();
        let total_batches =
            (request.target_notional_usdt / request.order_notional_usdt).ceil() as usize;
        let task = BatchIncreaseTask {
            id: Uuid::new_v4().to_string(),
            action: "reduce".into(),
            status: "queued".into(),
            token: position.token.clone(),
            long_exchange: position.long.exchange.clone(),
            short_exchange: position.short.exchange.clone(),
            target_notional_usdt: request.target_notional_usdt,
            order_notional_usdt: request.order_notional_usdt,
            interval_seconds: request.interval_seconds,
            completed_notional_usdt: 0.0,
            completed_batches: 0,
            total_batches,
            started_at: now,
            updated_at: now,
            cancel_requested: false,
            error: None,
            current_position: None,
            logs: vec![],
        };
        tasks.insert(task.id.clone(), task.clone());
        drop(tasks);

        let service = self.clone();
        let task_id = task.id.clone();
        tokio::spawn(async move {
            service.run_batch_reduce(task_id, position, request).await;
        });
        Ok(task)
    }

    pub async fn batch_task(&self, id: &str) -> Result<BatchIncreaseTask> {
        self.batch_tasks
            .read()
            .await
            .get(id)
            .cloned()
            .ok_or_else(|| anyhow!("batch task not found"))
    }

    pub async fn batch_tasks(&self) -> Vec<BatchIncreaseTask> {
        let mut tasks = self
            .batch_tasks
            .read()
            .await
            .values()
            .cloned()
            .collect::<Vec<_>>();
        tasks.sort_by_key(|task| std::cmp::Reverse(task.started_at));
        tasks
    }

    pub async fn cancel_batch_task(&self, id: &str) -> Result<BatchIncreaseTask> {
        let mut tasks = self.batch_tasks.write().await;
        let task = tasks
            .get_mut(id)
            .ok_or_else(|| anyhow!("batch task not found"))?;
        if matches!(task.status.as_str(), "queued" | "running") {
            task.cancel_requested = true;
            task.status = "cancelling".into();
            task.updated_at = chrono::Utc::now().timestamp_millis();
        }
        Ok(task.clone())
    }

    async fn run_batch_increase(
        &self,
        task_id: String,
        opportunity: Opportunity,
        request: BatchIncreaseRequest,
    ) {
        self.update_batch(&task_id, |task| task.status = "running".into())
            .await;
        let mut completed = 0.0;
        let mut batch = 0usize;
        while completed + 0.001 < request.target_notional_usdt {
            let cancelled = self
                .batch_tasks
                .read()
                .await
                .get(&task_id)
                .map(|task| task.cancel_requested)
                .unwrap_or(true);
            if cancelled {
                self.update_batch(&task_id, |task| {
                    task.status = "cancelled".into();
                })
                .await;
                return;
            }
            batch += 1;
            let batch_notional = request
                .order_notional_usdt
                .min(request.target_notional_usdt - completed);
            let open_request = OpenTradeRequest {
                opportunity_id: request.opportunity_id.clone(),
                notional_usdt: batch_notional,
                leverage: request.leverage,
            };
            match self
                .open_opportunity(opportunity.clone(), open_request)
                .await
            {
                Ok(response) => {
                    completed += batch_notional;
                    let logs = batch_logs(&opportunity, batch, &response, batch_notional);
                    let current_position = response.position.clone();
                    self.update_batch(&task_id, |task| {
                        task.completed_notional_usdt = completed.min(task.target_notional_usdt);
                        task.completed_batches = batch;
                        task.current_position = Some(current_position);
                        for mut log in logs {
                            log.sequence = task.logs.len() + 1;
                            task.logs.push(log);
                        }
                    })
                    .await;
                }
                Err(error) => {
                    let message = format!("{error:#}");
                    self.update_batch(&task_id, |task| {
                        task.status = "failed".into();
                        task.error = Some(message.clone());
                        task.logs.push(BatchExecutionLog {
                            sequence: task.logs.len() + 1,
                            batch,
                            timestamp: chrono::Utc::now().timestamp_millis(),
                            exchange: "System".into(),
                            side: "error".into(),
                            token: task.token.clone(),
                            notional_usdt: 0.0,
                            executed_quantity: 0.0,
                            average_price: 0.0,
                            status: "failed".into(),
                            order_id: String::new(),
                            message: message.clone(),
                        });
                    })
                    .await;
                    return;
                }
            }
            if completed + 0.001 < request.target_notional_usdt {
                tokio::time::sleep(Duration::from_secs_f64(request.interval_seconds)).await;
            }
        }
        self.update_batch(&task_id, |task| task.status = "completed".into())
            .await;
    }

    async fn run_batch_reduce(
        &self,
        task_id: String,
        position: Position,
        request: BatchReduceRequest,
    ) {
        self.update_batch(&task_id, |task| task.status = "running".into())
            .await;
        let mut completed = 0.0;
        let mut batch = 0usize;
        while completed + 0.001 < request.target_notional_usdt {
            let cancelled = self
                .batch_tasks
                .read()
                .await
                .get(&task_id)
                .map(|task| task.cancel_requested)
                .unwrap_or(true);
            if cancelled {
                self.update_batch(&task_id, |task| task.status = "cancelled".into())
                    .await;
                return;
            }
            batch += 1;
            let batch_notional = request
                .order_notional_usdt
                .min(request.target_notional_usdt - completed);
            match self
                .reduce(
                    &request.position_id,
                    AdjustPositionRequest {
                        notional_usdt: batch_notional,
                    },
                )
                .await
            {
                Ok(response) => {
                    completed += batch_notional;
                    let logs = batch_reduce_logs(&position, batch, &response, batch_notional);
                    let current_position = response.position.clone();
                    self.update_batch(&task_id, |task| {
                        task.completed_notional_usdt = completed.min(task.target_notional_usdt);
                        task.completed_batches = batch;
                        task.current_position = Some(current_position);
                        for mut log in logs {
                            log.sequence = task.logs.len() + 1;
                            task.logs.push(log);
                        }
                    })
                    .await;
                }
                Err(error) => {
                    let message = format!("{error:#}");
                    self.update_batch(&task_id, |task| {
                        task.status = "failed".into();
                        task.error = Some(message.clone());
                        task.logs.push(BatchExecutionLog {
                            sequence: task.logs.len() + 1,
                            batch,
                            timestamp: chrono::Utc::now().timestamp_millis(),
                            exchange: "System".into(),
                            side: "error".into(),
                            token: task.token.clone(),
                            notional_usdt: 0.0,
                            executed_quantity: 0.0,
                            average_price: 0.0,
                            status: "failed".into(),
                            order_id: String::new(),
                            message: message.clone(),
                        });
                    })
                    .await;
                    return;
                }
            }
            if completed + 0.001 < request.target_notional_usdt {
                tokio::time::sleep(Duration::from_secs_f64(request.interval_seconds)).await;
            }
        }
        self.update_batch(&task_id, |task| task.status = "completed".into())
            .await;
    }

    async fn update_batch<F>(&self, id: &str, update: F)
    where
        F: FnOnce(&mut BatchIncreaseTask),
    {
        if let Some(task) = self.batch_tasks.write().await.get_mut(id) {
            update(task);
            task.updated_at = chrono::Utc::now().timestamp_millis();
        }
    }

    pub async fn close(&self, id: &str) -> Result<TradeResponse> {
        match self.config.trading_mode {
            TradingMode::Paper => {
                let mut positions = self.paper_positions.write().await;
                let mut position = positions
                    .remove(id)
                    .ok_or_else(|| anyhow!("position not found"))?;
                position.status = "closed".into();
                Ok(TradeResponse {
                    position,
                    mode: "paper".into(),
                    message: "模拟仓位已平仓".into(),
                    execution: None,
                })
            }
            TradingMode::Live => {
                let _guard = self.execution_lock.lock().await;
                self.close_live(id).await
            }
        }
    }

    pub async fn reduce(&self, id: &str, request: AdjustPositionRequest) -> Result<TradeResponse> {
        if request.notional_usdt < 10.0 {
            bail!("notionalUsdt must be at least 10");
        }
        match self.config.trading_mode {
            TradingMode::Paper => self.reduce_paper(id, request.notional_usdt).await,
            TradingMode::Live => {
                let _guard = self.execution_lock.lock().await;
                self.reduce_live(id, request.notional_usdt).await
            }
        }
    }

    pub async fn adjust_leverage(&self, id: &str, leverage: u8) -> Result<TradeResponse> {
        if !(1..=20).contains(&leverage) {
            bail!("leverage must be between 1 and 20");
        }
        match self.config.trading_mode {
            TradingMode::Paper => {
                let mut positions = self.paper_positions.write().await;
                let position = positions
                    .get_mut(id)
                    .ok_or_else(|| anyhow!("position not found"))?;
                position.leverage = leverage;
                position.long.leverage = leverage;
                position.short.leverage = leverage;
                Ok(TradeResponse {
                    position: position.clone(),
                    mode: "paper".into(),
                    message: format!("模拟双腿杠杆已调整为 {leverage}×"),
                    execution: None,
                })
            }
            TradingMode::Live => {
                let position = self
                    .live_positions()
                    .await?
                    .into_iter()
                    .find(|position| position.id == id)
                    .ok_or_else(|| anyhow!("position not found"))?;
                tokio::try_join!(
                    self.set_leverage(&position.long.exchange, &position.long.symbol, leverage),
                    self.set_leverage(&position.short.exchange, &position.short.symbol, leverage)
                )
                .context("failed to adjust both legs' leverage")?;
                let mut adjusted = position;
                adjusted.leverage = leverage;
                adjusted.long.leverage = leverage;
                adjusted.short.leverage = leverage;
                Ok(TradeResponse {
                    position: adjusted,
                    mode: "live".into(),
                    message: format!("两家交易所杠杆已调整为 {leverage}×"),
                    execution: None,
                })
            }
        }
    }

    pub async fn positions(&self) -> Result<Vec<Position>> {
        match self.config.trading_mode {
            TradingMode::Paper => Ok(self
                .paper_positions
                .read()
                .await
                .values()
                .cloned()
                .collect()),
            TradingMode::Live => self.live_positions().await,
        }
    }

    pub async fn account_summary(&self) -> Result<AccountSummary> {
        let configured_exchanges = [
            self.config.binance.as_ref().map(|_| "Binance".to_string()),
            self.config.bybit.as_ref().map(|_| "Bybit".to_string()),
        ]
        .into_iter()
        .flatten()
        .collect();
        if self.config.trading_mode == TradingMode::Paper {
            let positions = self.positions().await?;
            let unrealized_pnl = positions.iter().map(|x| x.unrealized_pnl).sum();
            return Ok(AccountSummary {
                mode: "paper".into(),
                configured_exchanges,
                exchanges: paper_exchange_balances(&positions),
                equity_usdt: 100_000.0 + unrealized_pnl,
                available_usdt: 100_000.0
                    - positions
                        .iter()
                        .map(|x| x.notional_usdt * 2.0 / x.leverage as f64)
                        .sum::<f64>(),
                unrealized_pnl,
                realized_pnl: 0.0,
                closed_position_pnl: 0.0,
                funding_income: 0.0,
                trading_fees: 0.0,
                realized_period_days: 7,
                active_positions: positions.len(),
                unhedged_legs: vec![],
            });
        }
        let (
            binance,
            bybit,
            mut binance_legs,
            mut bybit_legs,
            (binance_income, bybit_income),
            quotes,
        ) = tokio::try_join!(
            self.binance_balance(),
            self.bybit_balance(),
            self.binance_positions(),
            self.bybit_positions(),
            self.income_summary(),
            self.market.position_quotes()
        )?;
        apply_position_quotes(&mut binance_legs, &quotes);
        apply_position_quotes(&mut bybit_legs, &quotes);
        let binance_effective_upl = binance_legs.iter().map(|leg| leg.unrealized_pnl).sum();
        let bybit_effective_upl = bybit_legs.iter().map(|leg| leg.unrealized_pnl).sum();
        let all_legs: Vec<PositionLeg> = binance_legs.into_iter().chain(bybit_legs).collect();
        let active_positions = all_legs
            .iter()
            .filter(|leg| leg.side == "long")
            .filter(|leg| {
                all_legs.iter().any(|other| {
                    other.symbol == leg.symbol
                        && other.side == "short"
                        && other.exchange != leg.exchange
                })
            })
            .count();
        let managed_symbols = all_legs
            .iter()
            .filter(|leg| {
                all_legs.iter().any(|other| {
                    other.symbol == leg.symbol
                        && other.side != leg.side
                        && other.exchange != leg.exchange
                })
            })
            .map(|leg| leg.symbol.clone())
            .collect::<HashSet<_>>();
        let binance_income = binance_income.for_symbols(&managed_symbols);
        let bybit_income = bybit_income.for_symbols(&managed_symbols);
        let unrealized_pnl = all_legs.iter().map(|leg| leg.unrealized_pnl).sum();
        let unhedged_legs = all_legs
            .iter()
            .filter(|leg| {
                !all_legs.iter().any(|other| {
                    other.exchange != leg.exchange
                        && other.symbol == leg.symbol
                        && other.side != leg.side
                })
            })
            .cloned()
            .collect();
        Ok(AccountSummary {
            mode: "live".into(),
            configured_exchanges,
            exchanges: vec![
                ExchangeBalance {
                    exchange: "Binance".into(),
                    equity_usdt: binance.0,
                    available_usdt: binance.1,
                    unrealized_pnl: binance_effective_upl,
                    realized_pnl: binance_income.realized_pnl(),
                    closed_position_pnl: binance_income.closed_position_pnl,
                    funding_income: binance_income.funding_income,
                    trading_fees: binance_income.trading_fees,
                },
                ExchangeBalance {
                    exchange: "Bybit".into(),
                    equity_usdt: bybit.0,
                    available_usdt: bybit.1,
                    unrealized_pnl: bybit_effective_upl,
                    realized_pnl: bybit_income.realized_pnl(),
                    closed_position_pnl: bybit_income.closed_position_pnl,
                    funding_income: bybit_income.funding_income,
                    trading_fees: bybit_income.trading_fees,
                },
            ],
            equity_usdt: binance.0 + bybit.0,
            available_usdt: binance.1 + bybit.1,
            unrealized_pnl,
            realized_pnl: binance_income.realized_pnl() + bybit_income.realized_pnl(),
            closed_position_pnl: binance_income.closed_position_pnl
                + bybit_income.closed_position_pnl,
            funding_income: binance_income.funding_income + bybit_income.funding_income,
            trading_fees: binance_income.trading_fees + bybit_income.trading_fees,
            realized_period_days: 7,
            active_positions,
            unhedged_legs,
        })
    }

    async fn open_paper(
        &self,
        opportunity: Opportunity,
        request: OpenTradeRequest,
    ) -> Result<TradeResponse> {
        let quantity_long = floor_step(
            request.notional_usdt / opportunity.long.ask,
            opportunity.long.qty_step,
        );
        let quantity_short = floor_step(
            request.notional_usdt / opportunity.short.bid,
            opportunity.short.qty_step,
        );
        let mut positions = self.paper_positions.write().await;
        if let Some(position) = positions.values_mut().find(|position| {
            position.token == opportunity.token.symbol
                && position.long.exchange == opportunity.long.exchange
                && position.short.exchange == opportunity.short.exchange
        }) {
            let long_total = position.long.quantity + quantity_long;
            let short_total = position.short.quantity + quantity_short;
            position.long.entry_price = weighted_average(
                position.long.entry_price,
                position.long.quantity,
                opportunity.long.ask,
                quantity_long,
            );
            position.short.entry_price = weighted_average(
                position.short.entry_price,
                position.short.quantity,
                opportunity.short.bid,
                quantity_short,
            );
            position.long.quantity = long_total;
            position.short.quantity = short_total;
            position.notional_usdt += request.notional_usdt;
            return Ok(TradeResponse {
                position: position.clone(),
                mode: "paper".into(),
                message: "模拟双腿已加仓".into(),
                execution: None,
            });
        }
        let position = Position {
            id: Uuid::new_v4().to_string(),
            token: opportunity.token.symbol,
            status: "open".into(),
            opened_at: chrono::Utc::now().timestamp_millis(),
            notional_usdt: request.notional_usdt,
            leverage: request.leverage,
            long: PositionLeg {
                exchange: opportunity.long.exchange,
                symbol: opportunity.long.symbol,
                side: "long".into(),
                quantity: quantity_long,
                entry_price: opportunity.long.ask,
                mark_price: opportunity.long.mark,
                unrealized_pnl: 0.0,
                funding_earned: 0.0,
                funding_rate: opportunity.long.rate,
                leverage: request.leverage,
            },
            short: PositionLeg {
                exchange: opportunity.short.exchange,
                symbol: opportunity.short.symbol,
                side: "short".into(),
                quantity: quantity_short,
                entry_price: opportunity.short.bid,
                mark_price: opportunity.short.mark,
                unrealized_pnl: 0.0,
                funding_earned: 0.0,
                funding_rate: opportunity.short.rate,
                leverage: request.leverage,
            },
            funding_earned: 0.0,
            unrealized_pnl: 0.0,
        };
        positions.insert(position.id.clone(), position.clone());
        Ok(TradeResponse {
            position,
            mode: "paper".into(),
            message: "模拟双腿仓位已建立，未发送真实订单".into(),
            execution: None,
        })
    }

    async fn preflight_balance(
        &self,
        opportunity: &Opportunity,
        request: &OpenTradeRequest,
    ) -> Result<()> {
        let (binance, bybit) = tokio::try_join!(self.binance_balance(), self.bybit_balance())
            .context("balance preflight failed; no orders were sent")?;
        let required = request.notional_usdt / request.leverage as f64 * 1.05;
        for exchange in [&opportunity.long.exchange, &opportunity.short.exchange] {
            let available = if exchange == "Binance" {
                binance.1
            } else if exchange == "Bybit" {
                bybit.1
            } else {
                bail!("unsupported exchange {exchange}");
            };
            if available < required {
                bail!(
                    "{exchange} available balance {:.2} is below required safety amount {:.2}; no orders were sent",
                    available,
                    required
                );
            }
        }
        Ok(())
    }

    fn ensure_slippage(
        &self,
        order: &OrderExecution,
        expected_price: f64,
        is_buy: bool,
    ) -> Result<()> {
        if order.executed_quantity <= 0.0 || order.average_price <= 0.0 {
            bail!(
                "{} order returned invalid fill data; clientOrderId={}",
                order.exchange,
                order.client_order_id
            );
        }
        let adverse = if is_buy {
            (order.average_price - expected_price) / expected_price
        } else {
            (expected_price - order.average_price) / expected_price
        }
        .max(0.0);
        let actual_bps = adverse * 10_000.0;
        if actual_bps > self.config.max_slippage_bps as f64 {
            bail!(
                "{} fill slippage {:.1}bps exceeded limit {}bps; clientOrderId={}",
                order.exchange,
                actual_bps,
                self.config.max_slippage_bps,
                order.client_order_id
            );
        }
        Ok(())
    }

    async fn open_live(
        &self,
        opportunity: Opportunity,
        request: OpenTradeRequest,
    ) -> Result<TradeResponse> {
        let long_qty = floor_step(
            request.notional_usdt / opportunity.long.ask,
            opportunity.long.qty_step,
        );
        let short_qty = floor_step(
            request.notional_usdt / opportunity.short.bid,
            opportunity.short.qty_step,
        );
        if long_qty <= 0.0 || short_qty <= 0.0 {
            bail!("calculated order quantity is zero");
        }

        let (existing_long, existing_short) = tokio::try_join!(
            self.actual_leg(&opportunity.long, "long"),
            self.actual_leg(&opportunity.short, "short")
        )
        .context("failed to verify existing positions; no orders were sent")?;
        let (current_long, current_short, is_increase) = match (existing_long, existing_short) {
            (None, None) => (0.0, 0.0, false),
            (Some(long), Some(short)) => {
                (position_notional(&long), position_notional(&short), true)
            }
            _ => bail!(
                "NAKED_EXPOSURE: {} has only one expected leg; repair it before increasing",
                opportunity.token.symbol
            ),
        };
        let long_target = current_long + request.notional_usdt;
        let short_target = current_short + request.notional_usdt;
        self.preflight_balance(&opportunity, &request).await?;
        tokio::try_join!(
            self.set_leverage(
                &opportunity.long.exchange,
                &opportunity.long.symbol,
                request.leverage
            ),
            self.set_leverage(
                &opportunity.short.exchange,
                &opportunity.short.symbol,
                request.leverage
            )
        )
        .context("failed to configure leverage; no orders were sent")?;

        let mut report = ExecutionReport {
            outcome: "executing".into(),
            naked_exposure: false,
            max_slippage_bps: self.config.max_slippage_bps,
            target_notional_usdt: request.notional_usdt,
            long_notional_usdt: 0.0,
            short_notional_usdt: 0.0,
            tolerance_usdt: self.config.position_tolerance_usdt,
            balanced: false,
            phase_one_long_attempts: 0,
            phase_one_short_attempts: 0,
            phase_one_long_anomalies: 0,
            phase_one_short_anomalies: 0,
            phase_two_attempts: 0,
            phase_two_anomalies: 0,
            alert: false,
            orders: vec![],
            supplement_orders: vec![],
            rebalance_orders: vec![],
            compensation_orders: vec![],
        };

        let (long_result, short_result) = tokio::join!(
            self.place(
                &opportunity.long.exchange,
                &opportunity.long.symbol,
                "Buy",
                long_qty,
                false,
                opportunity.long.ask,
            ),
            self.place(
                &opportunity.short.exchange,
                &opportunity.short.symbol,
                "Sell",
                short_qty,
                false,
                opportunity.short.bid,
            )
        );
        match long_result {
            Ok(order) => {
                if let Err(error) = self.ensure_slippage(&order, opportunity.long.ask, true) {
                    tracing::warn!(error = %error, "initial long fill exceeded slippage limit; reconciliation will use actual positions");
                }
                report.orders.push(order);
            }
            Err(error) => {
                tracing::warn!(error = %error, "initial long order failed; actual position reconciliation will decide the next action")
            }
        }
        match short_result {
            Ok(order) => {
                if let Err(error) = self.ensure_slippage(&order, opportunity.short.bid, false) {
                    tracing::warn!(error = %error, "initial short fill exceeded slippage limit; reconciliation will use actual positions");
                }
                report.orders.push(order);
            }
            Err(error) => {
                tracing::warn!(error = %error, "initial short order failed; actual position reconciliation will decide the next action")
            }
        }

        let (long_phase, short_phase) = tokio::join!(
            self.align_leg_to_target(&opportunity.long, "long", "Buy", long_target),
            self.align_leg_to_target(&opportunity.short, "short", "Sell", short_target)
        );
        report.phase_one_long_attempts = long_phase.attempts;
        report.phase_one_short_attempts = short_phase.attempts;
        report.phase_one_long_anomalies = long_phase.anomalies;
        report.phase_one_short_anomalies = short_phase.anomalies;
        report.supplement_orders.extend(long_phase.orders);
        report.supplement_orders.extend(short_phase.orders);

        let phase_two = self
            .reduce_larger_leg(
                &opportunity.long,
                &opportunity.short,
                long_phase.state,
                short_phase.state,
            )
            .await;
        report.phase_two_attempts = phase_two.attempts;
        report.phase_two_anomalies = phase_two.anomalies;
        report.rebalance_orders.extend(phase_two.orders);
        let long = phase_two.long;
        let short = phase_two.short;
        report.long_notional_usdt = long.as_ref().map(position_notional).unwrap_or(0.0);
        report.short_notional_usdt = short.as_ref().map(position_notional).unwrap_or(0.0);
        report.balanced = paired_positions_are_balanced(long.as_ref(), short.as_ref());
        if !report.balanced {
            report.naked_exposure = true;
            report.alert = true;
            let mismatch = (report.long_notional_usdt - report.short_notional_usdt).abs();
            let mismatch_percent = match (&long, &short) {
                (Some(long), Some(short)) => hedge_notional_imbalance_ratio(long, short) * 100.0,
                _ => 100.0,
            };
            bail!(
                "NAKED_EXPOSURE: phase two exhausted after {} attempts ({} anomalies); actual position mismatch {:.2} USDT ({:.2}%) exceeds both 10 USDT and 1%",
                report.phase_two_attempts,
                report.phase_two_anomalies,
                mismatch,
                mismatch_percent
            );
        }
        let long =
            long.ok_or_else(|| anyhow!("NAKED_EXPOSURE: actual long position is missing"))?;
        let short =
            short.ok_or_else(|| anyhow!("NAKED_EXPOSURE: actual short position is missing"))?;
        report.outcome = "filled_and_balanced".into();
        let position = Position {
            id: format!("live-{}", opportunity.token.symbol),
            token: opportunity.token.symbol,
            status: "open".into(),
            opened_at: chrono::Utc::now().timestamp_millis(),
            notional_usdt: (report.long_notional_usdt + report.short_notional_usdt) / 2.0,
            leverage: request.leverage,
            long,
            short,
            funding_earned: 0.0,
            unrealized_pnl: 0.0,
        };
        Ok(TradeResponse {
            position,
            mode: "live".into(),
            message: if is_increase {
                "双腿已并发加仓并完成仓位对齐".into()
            } else {
                "双腿已并发成交并完成仓位对齐".into()
            },
            execution: Some(report),
        })
    }

    async fn align_leg_to_target(
        &self,
        leg: &Leg,
        position_side: &str,
        order_side: &str,
        target_usdt: f64,
    ) -> PhaseOneResult {
        let mut result = PhaseOneResult {
            state: None,
            attempts: 0,
            anomalies: 0,
            orders: vec![],
        };
        while result.attempts < MAX_RECONCILIATION_ATTEMPTS {
            let state = match self.actual_leg(leg, position_side).await {
                Ok(state) => state,
                Err(error) => {
                    result.anomalies += 1;
                    result.attempts += 1;
                    tracing::warn!(exchange = %leg.exchange, symbol = %leg.symbol, error = %error, "phase one actual-position query failed");
                    continue;
                }
            };
            let actual = state.as_ref().map(position_notional).unwrap_or(0.0);
            result.state = state;
            let missing = target_usdt - actual;
            if missing <= self.config.position_tolerance_usdt {
                break;
            }
            let reference = result
                .state
                .as_ref()
                .map(|x| x.mark_price)
                .filter(|x| *x > 0.0)
                .unwrap_or(if order_side == "Buy" {
                    leg.ask
                } else {
                    leg.bid
                });
            let quantity = floor_step(missing / reference, leg.qty_step);
            result.attempts += 1;
            if quantity <= 0.0 {
                result.anomalies += 1;
                break;
            }
            match self
                .place(
                    &leg.exchange,
                    &leg.symbol,
                    order_side,
                    quantity,
                    false,
                    reference,
                )
                .await
            {
                Ok(order) => {
                    if let Err(error) = self.ensure_slippage(&order, reference, order_side == "Buy")
                    {
                        result.anomalies += 1;
                        tracing::warn!(exchange = %leg.exchange, symbol = %leg.symbol, error = %error, "phase one supplement exceeded slippage limit");
                    }
                    result.orders.push(order);
                }
                Err(error) => {
                    result.anomalies += 1;
                    tracing::warn!(exchange = %leg.exchange, symbol = %leg.symbol, error = %error, "phase one supplement failed");
                }
            }
        }
        match self.actual_leg(leg, position_side).await {
            Ok(state) => {
                let actual = state.as_ref().map(position_notional).unwrap_or(0.0);
                if target_usdt - actual > self.config.position_tolerance_usdt {
                    result.anomalies = result.anomalies.saturating_add(1);
                }
                result.state = state;
            }
            Err(error) => {
                result.anomalies = result.anomalies.saturating_add(1);
                tracing::warn!(exchange = %leg.exchange, symbol = %leg.symbol, error = %error, "phase one final actual-position query failed");
            }
        }
        result
    }

    async fn reduce_larger_leg(
        &self,
        long_leg: &Leg,
        short_leg: &Leg,
        initial_long: Option<PositionLeg>,
        initial_short: Option<PositionLeg>,
    ) -> PhaseTwoResult {
        let mut result = PhaseTwoResult {
            long: initial_long,
            short: initial_short,
            attempts: 0,
            anomalies: 0,
            orders: vec![],
        };
        while result.attempts < MAX_RECONCILIATION_ATTEMPTS {
            let (long_state, short_state) = tokio::join!(
                self.actual_leg(long_leg, "long"),
                self.actual_leg(short_leg, "short")
            );
            let (long, short) = match (long_state, short_state) {
                (Ok(long), Ok(short)) => (long, short),
                (long_error, short_error) => {
                    result.attempts += 1;
                    result.anomalies += 1;
                    tracing::warn!(long = ?long_error.err(), short = ?short_error.err(), "phase two actual-position query failed");
                    continue;
                }
            };
            let long_notional = long.as_ref().map(position_notional).unwrap_or(0.0);
            let short_notional = short.as_ref().map(position_notional).unwrap_or(0.0);
            result.long = long;
            result.short = short;
            if let (Some(long), Some(short)) = (&result.long, &result.short) {
                if !hedge_notional_mismatch_exceeds(
                    long,
                    short,
                    POSITION_RECONCILIATION_TOLERANCE_RATIO,
                ) {
                    break;
                }
            } else if long_notional <= f64::EPSILON && short_notional <= f64::EPSILON {
                break;
            }
            let difference = (long_notional - short_notional).abs();
            let (leg, side, reference) = if long_notional > short_notional {
                (long_leg, "Sell", result.long.as_ref().map(|x| x.mark_price))
            } else {
                (
                    short_leg,
                    "Buy",
                    result.short.as_ref().map(|x| x.mark_price),
                )
            };
            result.attempts += 1;
            let reference = reference.filter(|x| *x > 0.0).unwrap_or(leg.mark);
            let quantity = floor_step(difference / reference, leg.qty_step);
            if quantity <= 0.0 {
                result.anomalies += 1;
                continue;
            }
            match self
                .place(&leg.exchange, &leg.symbol, side, quantity, true, 0.0)
                .await
            {
                Ok(order) => result.orders.push(order),
                Err(error) => {
                    result.anomalies += 1;
                    tracing::error!(exchange = %leg.exchange, symbol = %leg.symbol, error = %error, "phase two reduction failed");
                }
            }
        }
        let (long, short) = tokio::join!(
            self.actual_leg(long_leg, "long"),
            self.actual_leg(short_leg, "short")
        );
        if let Ok(state) = long {
            result.long = state;
        } else {
            result.anomalies = result.anomalies.saturating_add(1);
        }
        if let Ok(state) = short {
            result.short = state;
        } else {
            result.anomalies = result.anomalies.saturating_add(1);
        }
        result
    }

    async fn reduce_paper(&self, id: &str, notional_usdt: f64) -> Result<TradeResponse> {
        let mut positions = self.paper_positions.write().await;
        let position = positions
            .get_mut(id)
            .ok_or_else(|| anyhow!("position not found"))?;
        if notional_usdt >= position.notional_usdt {
            bail!("reduction must be smaller than the position; use close for a full exit");
        }
        let remaining = position.notional_usdt - notional_usdt;
        let ratio = remaining / position.notional_usdt;
        position.long.quantity *= ratio;
        position.short.quantity *= ratio;
        position.notional_usdt = remaining;
        Ok(TradeResponse {
            position: position.clone(),
            mode: "paper".into(),
            message: "模拟双腿已减仓".into(),
            execution: None,
        })
    }

    async fn reduce_live(&self, id: &str, notional_usdt: f64) -> Result<TradeResponse> {
        let position = self
            .live_positions()
            .await?
            .into_iter()
            .find(|position| position.id == id)
            .ok_or_else(|| anyhow!("position not found"))?;
        let original_long = position_notional(&position.long);
        let original_short = position_notional(&position.short);
        let original_long_quantity = position.long.quantity;
        let original_short_quantity = position.short.quantity;
        if notional_usdt >= original_long.min(original_short) {
            bail!("reduction is too close to the full position; use close for a full exit");
        }

        let legs = self.market.trading_legs().await?;
        let long_leg = legs
            .iter()
            .find(|leg| {
                leg.exchange == position.long.exchange && leg.symbol == position.long.symbol
            })
            .cloned()
            .ok_or_else(|| anyhow!("long contract metadata unavailable"))?;
        let short_leg = legs
            .iter()
            .find(|leg| {
                leg.exchange == position.short.exchange && leg.symbol == position.short.symbol
            })
            .cloned()
            .ok_or_else(|| anyhow!("short contract metadata unavailable"))?;
        let long_qty = floor_step(notional_usdt / position.long.mark_price, long_leg.qty_step);
        let short_qty = floor_step(
            notional_usdt / position.short.mark_price,
            short_leg.qty_step,
        );
        if long_qty <= 0.0 || short_qty <= 0.0 {
            bail!("calculated reduction quantity is zero");
        }

        let mut report = ExecutionReport {
            outcome: "reducing".into(),
            naked_exposure: false,
            max_slippage_bps: self.config.max_slippage_bps,
            target_notional_usdt: notional_usdt,
            long_notional_usdt: original_long,
            short_notional_usdt: original_short,
            tolerance_usdt: self.config.position_tolerance_usdt,
            balanced: false,
            phase_one_long_attempts: 0,
            phase_one_short_attempts: 0,
            phase_one_long_anomalies: 0,
            phase_one_short_anomalies: 0,
            phase_two_attempts: 0,
            phase_two_anomalies: 0,
            alert: false,
            orders: vec![],
            supplement_orders: vec![],
            rebalance_orders: vec![],
            compensation_orders: vec![],
        };
        let (long_result, short_result) = tokio::join!(
            self.place(
                &position.long.exchange,
                &position.long.symbol,
                "Sell",
                long_qty,
                true,
                0.0
            ),
            self.place(
                &position.short.exchange,
                &position.short.symbol,
                "Buy",
                short_qty,
                true,
                0.0
            )
        );
        for result in [long_result, short_result] {
            match result {
                Ok(order) => report.orders.push(order),
                Err(error) => {
                    tracing::warn!(error = %error, "initial reduction failed; actual-position reconciliation will decide the next action")
                }
            }
        }

        let phase_two = self
            .reduce_larger_leg(&long_leg, &short_leg, None, None)
            .await;
        report.phase_two_attempts = phase_two.attempts;
        report.phase_two_anomalies = phase_two.anomalies;
        report.rebalance_orders.extend(phase_two.orders);
        let long = phase_two.long;
        let short = phase_two.short;
        report.long_notional_usdt = long.as_ref().map(position_notional).unwrap_or(0.0);
        report.short_notional_usdt = short.as_ref().map(position_notional).unwrap_or(0.0);
        report.balanced = paired_positions_are_balanced(long.as_ref(), short.as_ref());
        if !report.balanced {
            let mismatch = (report.long_notional_usdt - report.short_notional_usdt).abs();
            let mismatch_percent = match (&long, &short) {
                (Some(long), Some(short)) => hedge_notional_imbalance_ratio(long, short) * 100.0,
                _ => 100.0,
            };
            bail!(
                "NAKED_EXPOSURE: reduction reconciliation failed; actual mismatch is {:.2} USDT ({:.2}%)",
                mismatch,
                mismatch_percent
            );
        }
        let long_reduced_usdt = reduced_notional_at_reference(
            original_long_quantity,
            long.as_ref().map(|leg| leg.quantity).unwrap_or(0.0),
            position.long.mark_price,
        );
        let short_reduced_usdt = reduced_notional_at_reference(
            original_short_quantity,
            short.as_ref().map(|leg| leg.quantity).unwrap_or(0.0),
            position.short.mark_price,
        );
        if long_reduced_usdt < notional_usdt - self.config.position_tolerance_usdt
            || short_reduced_usdt < notional_usdt - self.config.position_tolerance_usdt
        {
            bail!(
                "reduction target was not reached; target {:.2} USDT, actual LONG {:.2} / SHORT {:.2} USDT",
                notional_usdt,
                long_reduced_usdt,
                short_reduced_usdt
            );
        }
        let long =
            long.ok_or_else(|| anyhow!("position was fully closed; refresh the position list"))?;
        let short =
            short.ok_or_else(|| anyhow!("position was fully closed; refresh the position list"))?;
        report.outcome = "reduced_and_balanced".into();
        let reduced = Position {
            id: position.id,
            token: position.token,
            status: "open".into(),
            opened_at: position.opened_at,
            notional_usdt: (report.long_notional_usdt + report.short_notional_usdt) / 2.0,
            leverage: position.leverage,
            funding_earned: position.funding_earned,
            unrealized_pnl: long.unrealized_pnl + short.unrealized_pnl,
            long,
            short,
        };
        Ok(TradeResponse {
            position: reduced,
            mode: "live".into(),
            message: "双腿已并发减仓并完成仓位对齐".into(),
            execution: Some(report),
        })
    }

    async fn close_live(&self, id: &str) -> Result<TradeResponse> {
        let positions = self.live_positions().await?;
        let position = positions
            .into_iter()
            .find(|p| p.id == id)
            .ok_or_else(|| anyhow!("position not found"))?;
        let mut report = ExecutionReport {
            outcome: "closing".into(),
            naked_exposure: false,
            max_slippage_bps: self.config.max_slippage_bps,
            target_notional_usdt: position.notional_usdt,
            long_notional_usdt: position.long.quantity * position.long.mark_price,
            short_notional_usdt: position.short.quantity * position.short.mark_price,
            tolerance_usdt: self.config.position_tolerance_usdt,
            balanced: true,
            phase_one_long_attempts: 0,
            phase_one_short_attempts: 0,
            phase_one_long_anomalies: 0,
            phase_one_short_anomalies: 0,
            phase_two_attempts: 0,
            phase_two_anomalies: 0,
            alert: false,
            orders: vec![],
            supplement_orders: vec![],
            rebalance_orders: vec![],
            compensation_orders: vec![],
        };
        let long_close = self
            .place(
                &position.long.exchange,
                &position.long.symbol,
                "Sell",
                position.long.quantity,
                true,
                0.0,
            )
            .await?;
        report.orders.push(long_close);
        match self
            .place(
                &position.short.exchange,
                &position.short.symbol,
                "Buy",
                position.short.quantity,
                true,
                0.0,
            )
            .await
        {
            Ok(order) => report.orders.push(order),
            Err(error) => {
                return Err(error.context(
                    "NAKED_EXPOSURE: long leg close confirmed, short leg close failed; manual intervention required",
                ));
            }
        }
        report.outcome = "closed".into();
        let mut closed = position;
        closed.status = "closed".into();
        Ok(TradeResponse {
            position: closed,
            mode: "live".into(),
            message: "双腿平仓单已确认成交".into(),
            execution: Some(report),
        })
    }

    async fn place(
        &self,
        exchange: &str,
        symbol: &str,
        side: &str,
        quantity: f64,
        reduce_only: bool,
        _expected_price: f64,
    ) -> Result<OrderExecution> {
        let client_order_id = format!("av{}", Uuid::new_v4().simple());
        match exchange {
            "Binance" => {
                self.binance_order(
                    symbol,
                    if side == "Buy" { "BUY" } else { "SELL" },
                    quantity,
                    reduce_only,
                    &client_order_id,
                )
                .await
            }
            "Bybit" => {
                self.bybit_order(symbol, side, quantity, reduce_only, &client_order_id)
                    .await
            }
            _ => bail!("unsupported exchange {exchange}"),
        }
    }

    async fn set_leverage(&self, exchange: &str, symbol: &str, leverage: u8) -> Result<()> {
        match exchange {
            "Binance" => {
                let creds = self
                    .config
                    .binance
                    .as_ref()
                    .ok_or_else(|| anyhow!("Binance credentials missing"))?;
                let query = format!(
                    "symbol={symbol}&leverage={leverage}&timestamp={}",
                    chrono::Utc::now().timestamp_millis()
                );
                self.binance_signed(Method::POST, "/fapi/v1/leverage", &query, creds)
                    .await?;
                Ok(())
            }
            "Bybit" => {
                let body = json!({
                    "category": "linear",
                    "symbol": symbol,
                    "buyLeverage": leverage.to_string(),
                    "sellLeverage": leverage.to_string()
                });
                let response = self
                    .bybit_signed(Method::POST, "/v5/position/set-leverage", Some(body))
                    .await?;
                let code = response["retCode"].as_i64().unwrap_or(-1);
                // 110043 means leverage was already set to the requested value.
                if code != 0 && code != 110043 {
                    bail!("Bybit leverage rejected: {}", response["retMsg"]);
                }
                Ok(())
            }
            _ => bail!("unsupported exchange {exchange}"),
        }
    }

    async fn binance_order(
        &self,
        symbol: &str,
        side: &str,
        qty: f64,
        reduce_only: bool,
        client_order_id: &str,
    ) -> Result<OrderExecution> {
        let creds = self
            .config
            .binance
            .as_ref()
            .ok_or_else(|| anyhow!("Binance credentials missing"))?;
        let mode_query = format!("timestamp={}", chrono::Utc::now().timestamp_millis());
        let mode = self
            .binance_signed(
                Method::GET,
                "/fapi/v1/positionSide/dual",
                &mode_query,
                creds,
            )
            .await?;
        let hedge_mode = mode["dualSidePosition"].as_bool().unwrap_or(false);
        let position_side = if (!reduce_only && side == "BUY") || (reduce_only && side == "SELL") {
            "LONG"
        } else {
            "SHORT"
        };
        let mode_params = if hedge_mode {
            format!("&positionSide={position_side}")
        } else {
            format!("&reduceOnly={reduce_only}")
        };
        let query = format!(
            "symbol={symbol}&side={side}&type=MARKET&quantity={}&newClientOrderId={client_order_id}&newOrderRespType=RESULT{mode_params}&timestamp={}",
            format_qty(qty),
            chrono::Utc::now().timestamp_millis()
        );
        let mut response = match self
            .binance_signed(Method::POST, "/fapi/v1/order", &query, creds)
            .await
        {
            Ok(response) => response,
            Err(submit_error) => {
                tokio::time::sleep(Duration::from_millis(300)).await;
                let status_query = format!(
                    "symbol={symbol}&origClientOrderId={client_order_id}&timestamp={}",
                    chrono::Utc::now().timestamp_millis()
                );
                self.binance_signed(Method::GET, "/fapi/v1/order", &status_query, creds)
                    .await
                    .with_context(|| {
                        format!(
                            "Binance submit result unknown ({submit_error:#}); query clientOrderId={client_order_id} before any retry"
                        )
                    })?
            }
        };
        let status_query = || {
            format!(
                "symbol={symbol}&origClientOrderId={client_order_id}&timestamp={}",
                chrono::Utc::now().timestamp_millis()
            )
        };
        let mut fill = binance_fill(&response);
        if fill.1 <= 0.0 || fill.2 <= 0.0 {
            for _ in 0..8 {
                tokio::time::sleep(Duration::from_millis(250)).await;
                response = self
                    .binance_signed(Method::GET, "/fapi/v1/order", &status_query(), creds)
                    .await?;
                fill = binance_fill(&response);
                if fill.0 == "FILLED" && fill.1 > 0.0 && fill.2 > 0.0 {
                    break;
                }
            }
        }
        if fill.1 <= 0.0 || fill.2 <= 0.0 {
            bail!(
                "Binance order fill data unavailable after status polling: status={}, clientOrderId={client_order_id}",
                fill.0
            );
        }
        Ok(OrderExecution {
            exchange: "Binance".into(),
            client_order_id: client_order_id.into(),
            order_id: response["orderId"].to_string().trim_matches('"').into(),
            status: fill.0,
            executed_quantity: fill.1,
            average_price: fill.2,
        })
    }

    async fn bybit_order(
        &self,
        symbol: &str,
        side: &str,
        qty: f64,
        reduce_only: bool,
        client_order_id: &str,
    ) -> Result<OrderExecution> {
        let position_idx = self.bybit_position_idx(symbol, side, reduce_only).await?;
        let body = json!({
            "category": "linear", "symbol": symbol, "side": side,
            "orderType": "Market", "qty": format_qty(qty),
            "reduceOnly": reduce_only,
            "orderLinkId": client_order_id,
            "positionIdx": position_idx,
        });
        let submit = self
            .bybit_signed(Method::POST, "/v5/order/create", Some(body))
            .await;
        if let Ok(response) = &submit {
            if response["retCode"].as_i64().unwrap_or(-1) != 0 {
                bail!("Bybit order rejected: {}", response["retMsg"]);
            }
        }
        let order_id = submit
            .as_ref()
            .ok()
            .and_then(|response| response["result"]["orderId"].as_str())
            .unwrap_or("")
            .to_string();
        let mut partial_quantity = 0.0;
        let mut partial_average_price = 0.0;
        for _ in 0..8 {
            tokio::time::sleep(Duration::from_millis(250)).await;
            let path = format!(
                "/v5/order/realtime?category=linear&symbol={symbol}&orderLinkId={client_order_id}"
            );
            let status_response = self.bybit_signed(Method::GET, &path, None).await?;
            if status_response["retCode"].as_i64().unwrap_or(-1) != 0 {
                bail!("Bybit order status failed: {}", status_response["retMsg"]);
            }
            if let Some(order) = status_response["result"]["list"]
                .as_array()
                .and_then(|orders| orders.first())
            {
                let status = order["orderStatus"].as_str().unwrap_or("Unknown");
                partial_quantity = number(&order["cumExecQty"]).unwrap_or(0.0);
                partial_average_price = number(&order["avgPrice"]).unwrap_or(0.0);
                if status == "Filled" {
                    return Ok(OrderExecution {
                        exchange: "Bybit".into(),
                        client_order_id: client_order_id.into(),
                        order_id,
                        status: status.into(),
                        executed_quantity: number(&order["cumExecQty"]).unwrap_or(0.0),
                        average_price: number(&order["avgPrice"]).unwrap_or(0.0),
                    });
                }
                if matches!(status, "Rejected" | "Cancelled" | "Deactivated") {
                    bail!(
                        "Bybit order ended with status={status}, clientOrderId={client_order_id}"
                    );
                }
            }
        }
        if let Err(submit_error) = submit {
            bail!(
                "Bybit submit result unknown ({submit_error:#}); confirmation timed out; query orderLinkId={client_order_id} before any retry"
            );
        }
        if partial_quantity > 0.0 {
            return Ok(OrderExecution {
                exchange: "Bybit".into(),
                client_order_id: client_order_id.into(),
                order_id,
                status: "PartiallyFilled".into(),
                executed_quantity: partial_quantity,
                average_price: partial_average_price,
            });
        }
        bail!("Bybit order confirmation timed out; query orderLinkId={client_order_id} before any retry")
    }

    async fn bybit_position_idx(&self, symbol: &str, side: &str, reduce_only: bool) -> Result<i64> {
        let path = format!("/v5/position/list?category=linear&symbol={symbol}");
        let response = self.bybit_signed(Method::GET, &path, None).await?;
        if response["retCode"].as_i64().unwrap_or(-1) != 0 {
            bail!("Bybit position mode query failed: {}", response["retMsg"]);
        }
        let hedge_mode = response["result"]["list"].as_array().is_some_and(|items| {
            items
                .iter()
                .any(|item| item["positionIdx"].as_i64().unwrap_or(0) > 0)
        });
        if !hedge_mode {
            return Ok(0);
        }
        let is_long = (!reduce_only && side == "Buy") || (reduce_only && side == "Sell");
        Ok(if is_long { 1 } else { 2 })
    }

    async fn actual_leg(&self, leg: &Leg, side: &str) -> Result<Option<PositionLeg>> {
        let mut positions = match leg.exchange.as_str() {
            "Binance" => self.binance_positions().await?,
            "Bybit" => self.bybit_positions().await?,
            exchange => bail!("unsupported exchange {exchange}"),
        };
        let quotes = self.market.position_quotes().await?;
        apply_position_quotes(&mut positions, &quotes);
        Ok(positions
            .into_iter()
            .find(|position| position.symbol == leg.symbol && position.side == side))
    }

    async fn live_positions(&self) -> Result<Vec<Position>> {
        let (mut binance, mut bybit, (binance_income, bybit_income), quotes) = tokio::try_join!(
            self.binance_positions(),
            self.bybit_positions(),
            self.income_summary(),
            self.market.position_quotes()
        )?;
        apply_position_quotes(&mut binance, &quotes);
        apply_position_quotes(&mut bybit, &quotes);
        let funding_rates = quotes
            .iter()
            .map(|quote| {
                (
                    format!("{}:{}", quote.exchange, quote.symbol),
                    quote.funding_rate,
                )
            })
            .collect::<HashMap<_, _>>();
        for leg in &mut binance {
            leg.funding_earned = *binance_income
                .funding_by_symbol
                .get(&leg.symbol)
                .unwrap_or(&0.0);
            leg.funding_rate = *funding_rates
                .get(&format!("Binance:{}", leg.symbol))
                .unwrap_or(&0.0);
        }
        for leg in &mut bybit {
            leg.funding_earned = *bybit_income
                .funding_by_symbol
                .get(&leg.symbol)
                .unwrap_or(&0.0);
            leg.funding_rate = *funding_rates
                .get(&format!("Bybit:{}", leg.symbol))
                .unwrap_or(&0.0);
        }
        let mut grouped: HashMap<String, Vec<PositionLeg>> = HashMap::new();
        for leg in binance.into_iter().chain(bybit) {
            grouped
                .entry(leg.symbol.trim_end_matches("USDT").to_string())
                .or_default()
                .push(leg);
        }
        Ok(grouped
            .into_iter()
            .filter_map(|(token, legs)| {
                let long = legs.iter().find(|x| x.side == "long")?.clone();
                let short = legs.iter().find(|x| x.side == "short")?.clone();
                let pnl = long.unrealized_pnl + short.unrealized_pnl;
                let funding_earned = long.funding_earned + short.funding_earned;
                Some(Position {
                    id: format!("live-{token}"),
                    token,
                    status: "open".into(),
                    opened_at: 0,
                    notional_usdt: long.quantity * long.entry_price,
                    leverage: long.leverage,
                    long,
                    short,
                    funding_earned,
                    unrealized_pnl: pnl,
                })
            })
            .collect())
    }

    async fn binance_positions(&self) -> Result<Vec<PositionLeg>> {
        let creds = self
            .config
            .binance
            .as_ref()
            .ok_or_else(|| anyhow!("Binance credentials missing"))?;
        let query = format!("timestamp={}", chrono::Utc::now().timestamp_millis());
        let json = self
            .binance_signed(Method::GET, "/fapi/v2/positionRisk", &query, creds)
            .await?;
        Ok(json
            .as_array()
            .unwrap_or(&vec![])
            .iter()
            .filter_map(|x| {
                let amount: f64 = x["positionAmt"].as_str()?.parse().ok()?;
                if amount == 0.0 {
                    return None;
                }
                Some(PositionLeg {
                    exchange: "Binance".into(),
                    symbol: x["symbol"].as_str()?.into(),
                    side: if amount > 0.0 { "long" } else { "short" }.into(),
                    quantity: amount.abs(),
                    entry_price: number(&x["entryPrice"])?,
                    mark_price: number(&x["markPrice"])?,
                    unrealized_pnl: number(&x["unRealizedProfit"]).unwrap_or(0.0),
                    funding_earned: 0.0,
                    funding_rate: 0.0,
                    leverage: number(&x["leverage"]).unwrap_or(1.0) as u8,
                })
            })
            .collect())
    }

    async fn bybit_positions(&self) -> Result<Vec<PositionLeg>> {
        let json = self
            .bybit_signed(
                Method::GET,
                "/v5/position/list?category=linear&settleCoin=USDT",
                None,
            )
            .await?;
        Ok(json["result"]["list"]
            .as_array()
            .unwrap_or(&vec![])
            .iter()
            .filter_map(|x| {
                let qty = number(&x["size"])?;
                if qty == 0.0 {
                    return None;
                }
                Some(PositionLeg {
                    exchange: "Bybit".into(),
                    symbol: x["symbol"].as_str()?.into(),
                    side: if x["side"] == "Buy" { "long" } else { "short" }.into(),
                    quantity: qty,
                    entry_price: number(&x["avgPrice"])?,
                    mark_price: number(&x["markPrice"])?,
                    unrealized_pnl: number(&x["unrealisedPnl"]).unwrap_or(0.0),
                    funding_earned: 0.0,
                    funding_rate: 0.0,
                    leverage: number(&x["leverage"]).unwrap_or(1.0) as u8,
                })
            })
            .collect())
    }

    async fn binance_income_summary(&self) -> Result<IncomeSummary> {
        let creds = self
            .config
            .binance
            .as_ref()
            .ok_or_else(|| anyhow!("Binance credentials missing"))?;
        let now = chrono::Utc::now().timestamp_millis();
        let start = now - 7 * 24 * 60 * 60 * 1000;
        let funding_query = format!(
            "incomeType=FUNDING_FEE&startTime={start}&endTime={now}&limit=1000&timestamp={now}"
        );
        let commission_query = format!(
            "incomeType=COMMISSION&startTime={start}&endTime={now}&limit=1000&timestamp={now}"
        );
        let realized_query = format!(
            "incomeType=REALIZED_PNL&startTime={start}&endTime={now}&limit=1000&timestamp={now}"
        );
        let (funding, commissions, realized) = tokio::try_join!(
            self.binance_signed(Method::GET, "/fapi/v1/income", &funding_query, creds),
            self.binance_signed(Method::GET, "/fapi/v1/income", &commission_query, creds),
            self.binance_signed(Method::GET, "/fapi/v1/income", &realized_query, creds)
        )?;
        let mut summary = IncomeSummary::default();
        for item in funding.as_array().unwrap_or(&vec![]) {
            if let (Some(symbol), Some(income)) = (item["symbol"].as_str(), number(&item["income"]))
            {
                *summary
                    .funding_by_symbol
                    .entry(symbol.to_string())
                    .or_insert(0.0) += income;
                summary.funding_income += income;
            }
        }
        for item in commissions.as_array().unwrap_or(&vec![]) {
            if let (Some(symbol), Some(commission)) =
                (item["symbol"].as_str(), number(&item["income"]))
            {
                let expense = -commission;
                *summary
                    .trading_fees_by_symbol
                    .entry(symbol.to_string())
                    .or_insert(0.0) += expense;
                summary.trading_fees += expense;
            }
        }
        for item in realized.as_array().unwrap_or(&vec![]) {
            if let (Some(symbol), Some(pnl)) = (item["symbol"].as_str(), number(&item["income"])) {
                *summary
                    .closed_pnl_by_symbol
                    .entry(symbol.to_string())
                    .or_insert(0.0) += pnl;
                summary.closed_position_pnl += pnl;
            }
        }
        Ok(summary)
    }

    async fn income_summary(&self) -> Result<(IncomeSummary, IncomeSummary)> {
        let refresh_bucket = funding_refresh_bucket(chrono::Utc::now().timestamp_millis());
        if let Some((cached_bucket, binance, bybit)) = self.funding_cache.read().await.as_ref() {
            if *cached_bucket == refresh_bucket {
                return Ok((binance.clone(), bybit.clone()));
            }
        }
        let _refresh_guard = self.income_refresh_lock.lock().await;
        if let Some((cached_bucket, binance, bybit)) = self.funding_cache.read().await.as_ref() {
            if *cached_bucket == refresh_bucket {
                return Ok((binance.clone(), bybit.clone()));
            }
        }
        let (binance, bybit) =
            tokio::try_join!(self.binance_income_summary(), self.bybit_income_summary())?;
        *self.funding_cache.write().await = Some((refresh_bucket, binance.clone(), bybit.clone()));
        Ok((binance, bybit))
    }

    async fn bybit_income_summary(&self) -> Result<IncomeSummary> {
        let now = chrono::Utc::now().timestamp_millis();
        let start = now - 7 * 24 * 60 * 60 * 1000;
        let mut cursor = None;
        let mut summary = IncomeSummary::default();
        for _ in 0..20 {
            let mut path = format!(
                "/v5/account/transaction-log?accountType=UNIFIED&category=linear&currency=USDT&startTime={start}&endTime={now}&limit=50"
            );
            if let Some(value) = cursor.as_deref() {
                path.push_str("&cursor=");
                path.push_str(value);
            }
            let json = self.bybit_signed(Method::GET, &path, None).await?;
            if json["retCode"].as_i64().unwrap_or(-1) != 0 {
                bail!("Bybit transaction log rejected: {}", json["retMsg"]);
            }
            for item in json["result"]["list"].as_array().unwrap_or(&vec![]) {
                if let (Some(symbol), Some(funding)) =
                    (item["symbol"].as_str(), number(&item["funding"]))
                {
                    *summary
                        .funding_by_symbol
                        .entry(symbol.to_string())
                        .or_insert(0.0) += funding;
                    summary.funding_income += funding;
                }
                if let (Some(symbol), Some(fee)) = (item["symbol"].as_str(), number(&item["fee"])) {
                    *summary
                        .trading_fees_by_symbol
                        .entry(symbol.to_string())
                        .or_insert(0.0) += fee;
                    summary.trading_fees += fee;
                }
            }
            cursor = json["result"]["nextPageCursor"]
                .as_str()
                .filter(|value| !value.is_empty())
                .map(str::to_string);
            if cursor.is_none() {
                break;
            }
        }
        let mut cursor = None;
        for _ in 0..20 {
            let mut path = format!(
                "/v5/position/closed-pnl?category=linear&startTime={start}&endTime={now}&limit=100"
            );
            if let Some(value) = cursor.as_deref() {
                path.push_str("&cursor=");
                path.push_str(value);
            }
            let json = self.bybit_signed(Method::GET, &path, None).await?;
            if json["retCode"].as_i64().unwrap_or(-1) != 0 {
                bail!("Bybit closed PnL rejected: {}", json["retMsg"]);
            }
            let records = json["result"]["list"]
                .as_array()
                .map(Vec::as_slice)
                .unwrap_or(&[]);
            for item in records {
                if let (Some(symbol), Some(pnl)) =
                    (item["symbol"].as_str(), number(&item["closedPnl"]))
                {
                    *summary
                        .closed_pnl_by_symbol
                        .entry(symbol.to_string())
                        .or_insert(0.0) += pnl;
                    summary.closed_position_pnl += pnl;
                }
            }
            cursor = json["result"]["nextPageCursor"]
                .as_str()
                .filter(|value| !value.is_empty() && !records.is_empty())
                .map(str::to_string);
            if cursor.is_none() {
                break;
            }
        }
        Ok(summary)
    }

    async fn binance_balance(&self) -> Result<(f64, f64, f64)> {
        let creds = self
            .config
            .binance
            .as_ref()
            .ok_or_else(|| anyhow!("Binance credentials missing"))?;
        let query = format!("timestamp={}", chrono::Utc::now().timestamp_millis());
        let json = self
            .binance_signed(Method::GET, "/fapi/v2/account", &query, creds)
            .await?;
        Ok((
            number(&json["totalWalletBalance"]).unwrap_or(0.0),
            number(&json["availableBalance"]).unwrap_or(0.0),
            number(&json["totalUnrealizedProfit"]).unwrap_or(0.0),
        ))
    }

    async fn bybit_balance(&self) -> Result<(f64, f64, f64)> {
        let json = self
            .bybit_signed(
                Method::GET,
                "/v5/account/wallet-balance?accountType=UNIFIED",
                None,
            )
            .await?;
        let account = json["result"]["list"]
            .as_array()
            .and_then(|x| x.first())
            .ok_or_else(|| anyhow!("Bybit wallet missing"))?;
        Ok((
            number(&account["totalEquity"]).unwrap_or(0.0),
            number(&account["totalAvailableBalance"]).unwrap_or(0.0),
            number(&account["totalPerpUPL"]).unwrap_or(0.0),
        ))
    }

    async fn binance_signed(
        &self,
        method: Method,
        path: &str,
        query: &str,
        creds: &ExchangeCredentials,
    ) -> Result<Value> {
        let signed_query = if query.contains("recvWindow=") {
            query.to_string()
        } else {
            format!("{query}&recvWindow=10000")
        };
        let signature = sign(&creds.api_secret, &signed_query);
        let url = format!("https://fapi.binance.com{path}?{signed_query}&signature={signature}");
        let response = self
            .client
            .request(method, url)
            .header("X-MBX-APIKEY", &creds.api_key)
            .send()
            .await
            .map_err(|error| anyhow!("Binance request failed: {}", error.without_url()))?;
        let status = response.status();
        let body = response.text().await?;
        if !status.is_success() {
            let detail = serde_json::from_str::<Value>(&body)
                .ok()
                .and_then(|json| json["msg"].as_str().map(str::to_string))
                .unwrap_or_else(|| "request rejected".into());
            bail!("Binance API HTTP {status}: {detail}");
        }
        serde_json::from_str(&body).context("invalid Binance API response")
    }

    async fn bybit_signed(&self, method: Method, path: &str, body: Option<Value>) -> Result<Value> {
        let creds = self
            .config
            .bybit
            .as_ref()
            .ok_or_else(|| anyhow!("Bybit credentials missing"))?;
        let timestamp = chrono::Utc::now().timestamp_millis().to_string();
        let recv_window = "5000";
        let payload = if method == Method::GET {
            path.split_once('?').map(|x| x.1).unwrap_or("").to_string()
        } else {
            serde_json::to_string(body.as_ref().unwrap_or(&json!({})))?
        };
        let signature = sign(
            &creds.api_secret,
            &format!("{timestamp}{}{recv_window}{payload}", creds.api_key),
        );
        let url = format!("https://api.bybit.com{path}");
        let mut request = self
            .client
            .request(method.clone(), url)
            .header("X-BAPI-API-KEY", &creds.api_key)
            .header("X-BAPI-SIGN", signature)
            .header("X-BAPI-TIMESTAMP", timestamp)
            .header("X-BAPI-RECV-WINDOW", recv_window);
        if method == Method::POST {
            request = request
                .header("Content-Type", "application/json")
                .body(payload);
        }
        let response = request
            .send()
            .await
            .map_err(|error| anyhow!("Bybit request failed: {}", error.without_url()))?;
        let status = response.status();
        let body = response.text().await?;
        if !status.is_success() {
            let detail = serde_json::from_str::<Value>(&body)
                .ok()
                .and_then(|json| json["retMsg"].as_str().map(str::to_string))
                .unwrap_or_else(|| "request rejected".into());
            bail!("Bybit API HTTP {status}: {detail}");
        }
        serde_json::from_str(&body).context("invalid Bybit API response")
    }
}

fn batch_logs(
    opportunity: &Opportunity,
    batch: usize,
    response: &TradeResponse,
    batch_notional: f64,
) -> Vec<BatchExecutionLog> {
    let timestamp = chrono::Utc::now().timestamp_millis();
    if let Some(report) = &response.execution {
        return report
            .orders
            .iter()
            .chain(&report.supplement_orders)
            .chain(&report.rebalance_orders)
            .chain(&report.compensation_orders)
            .map(|order| {
                let side = if order.exchange == opportunity.long.exchange {
                    "long"
                } else if order.exchange == opportunity.short.exchange {
                    "short"
                } else {
                    "unknown"
                };
                BatchExecutionLog {
                    sequence: 0,
                    batch,
                    timestamp,
                    exchange: order.exchange.clone(),
                    side: side.into(),
                    token: opportunity.token.symbol.clone(),
                    notional_usdt: order.executed_quantity * order.average_price,
                    executed_quantity: order.executed_quantity,
                    average_price: order.average_price,
                    status: order.status.clone(),
                    order_id: order.order_id.clone(),
                    message: if report
                        .orders
                        .iter()
                        .any(|item| item.client_order_id == order.client_order_id)
                    {
                        "批次市价单成交".into()
                    } else if report
                        .supplement_orders
                        .iter()
                        .any(|item| item.client_order_id == order.client_order_id)
                    {
                        "目标仓位补单".into()
                    } else {
                        "双腿差额对齐".into()
                    },
                }
            })
            .collect();
    }
    [
        (&opportunity.long.exchange, "long", opportunity.long.ask),
        (&opportunity.short.exchange, "short", opportunity.short.bid),
    ]
    .into_iter()
    .map(|(exchange, side, price)| BatchExecutionLog {
        sequence: 0,
        batch,
        timestamp,
        exchange: exchange.clone(),
        side: side.into(),
        token: opportunity.token.symbol.clone(),
        notional_usdt: batch_notional,
        executed_quantity: batch_notional / price,
        average_price: price,
        status: "FILLED".into(),
        order_id: format!("paper-{batch}-{side}"),
        message: "模拟批次成交".into(),
    })
    .collect()
}

fn batch_reduce_logs(
    position: &Position,
    batch: usize,
    response: &TradeResponse,
    batch_notional: f64,
) -> Vec<BatchExecutionLog> {
    let timestamp = chrono::Utc::now().timestamp_millis();
    if let Some(report) = &response.execution {
        return report
            .orders
            .iter()
            .chain(&report.supplement_orders)
            .chain(&report.rebalance_orders)
            .chain(&report.compensation_orders)
            .map(|order| {
                let side = if order.exchange == position.long.exchange {
                    "long"
                } else if order.exchange == position.short.exchange {
                    "short"
                } else {
                    "unknown"
                };
                BatchExecutionLog {
                    sequence: 0,
                    batch,
                    timestamp,
                    exchange: order.exchange.clone(),
                    side: side.into(),
                    token: position.token.clone(),
                    notional_usdt: order.executed_quantity * order.average_price,
                    executed_quantity: order.executed_quantity,
                    average_price: order.average_price,
                    status: order.status.clone(),
                    order_id: order.order_id.clone(),
                    message: if report
                        .orders
                        .iter()
                        .any(|item| item.client_order_id == order.client_order_id)
                    {
                        "批次减仓市价单成交".into()
                    } else {
                        "减仓后双腿差额对齐".into()
                    },
                }
            })
            .collect();
    }
    [
        (&position.long.exchange, "long", position.long.mark_price),
        (&position.short.exchange, "short", position.short.mark_price),
    ]
    .into_iter()
    .map(|(exchange, side, price)| BatchExecutionLog {
        sequence: 0,
        batch,
        timestamp,
        exchange: exchange.clone(),
        side: side.into(),
        token: position.token.clone(),
        notional_usdt: batch_notional,
        executed_quantity: batch_notional / price,
        average_price: price,
        status: "FILLED".into(),
        order_id: format!("paper-reduce-{batch}-{side}"),
        message: "模拟批次减仓成交".into(),
    })
    .collect()
}

fn paper_exchange_balances(positions: &[Position]) -> Vec<ExchangeBalance> {
    ["Binance", "Bybit"]
        .into_iter()
        .map(|exchange| {
            let used_margin = positions
                .iter()
                .filter(|position| {
                    position.long.exchange == exchange || position.short.exchange == exchange
                })
                .map(|position| position.notional_usdt / position.leverage as f64)
                .sum::<f64>();
            let unrealized_pnl = positions
                .iter()
                .flat_map(|position| [&position.long, &position.short])
                .filter(|leg| leg.exchange == exchange)
                .map(|leg| leg.unrealized_pnl)
                .sum::<f64>();
            ExchangeBalance {
                exchange: exchange.into(),
                equity_usdt: 50_000.0 + unrealized_pnl,
                available_usdt: 50_000.0 - used_margin,
                unrealized_pnl,
                realized_pnl: 0.0,
                closed_position_pnl: 0.0,
                funding_income: 0.0,
                trading_fees: 0.0,
            }
        })
        .collect()
}

fn sign(secret: &str, message: &str) -> String {
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).expect("HMAC accepts any key");
    mac.update(message.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

fn floor_step(value: f64, step: f64) -> f64 {
    if step <= 0.0 {
        value
    } else {
        (value / step).floor() * step
    }
}

fn format_qty(value: f64) -> String {
    let result = format!("{value:.12}");
    result
        .trim_end_matches('0')
        .trim_end_matches('.')
        .to_string()
}

fn number(value: &Value) -> Option<f64> {
    value
        .as_str()
        .and_then(|x| x.parse().ok())
        .or_else(|| value.as_f64())
}

fn binance_fill(response: &Value) -> (String, f64, f64) {
    let status = response["status"].as_str().unwrap_or("UNKNOWN").to_string();
    let quantity = number(&response["executedQty"])
        .or_else(|| number(&response["cumQty"]))
        .unwrap_or(0.0);
    let average_price = number(&response["avgPrice"])
        .filter(|price| *price > 0.0)
        .or_else(|| {
            let quote = number(&response["cumQuote"])?;
            (quantity > 0.0).then_some(quote / quantity)
        })
        .unwrap_or(0.0);
    (status, quantity, average_price)
}

fn position_notional(leg: &PositionLeg) -> f64 {
    leg.quantity * leg.mark_price
}

fn reduced_notional_at_reference(
    original_quantity: f64,
    final_quantity: f64,
    reference_price: f64,
) -> f64 {
    (original_quantity - final_quantity).max(0.0) * reference_price
}

fn apply_position_quotes(legs: &mut [PositionLeg], quotes: &[PositionQuote]) {
    for leg in legs {
        let Some(quote) = quotes
            .iter()
            .find(|quote| quote.exchange == leg.exchange && quote.symbol == leg.symbol)
        else {
            continue;
        };
        leg.mark_price = quote.reference_price;
        leg.unrealized_pnl = if leg.side == "long" {
            (quote.reference_price - leg.entry_price) * leg.quantity
        } else {
            (leg.entry_price - quote.reference_price) * leg.quantity
        };
        leg.funding_rate = quote.funding_rate;
    }
}

fn funding_refresh_bucket(timestamp_millis: i64) -> i64 {
    (timestamp_millis / 1_000 - FUNDING_REFRESH_DELAY_SECONDS).div_euclid(60 * 60)
}

fn hedge_notional_imbalance_ratio(long: &PositionLeg, short: &PositionLeg) -> f64 {
    let long_notional = position_notional(long);
    let short_notional = position_notional(short);
    let larger = long_notional.max(short_notional);
    if larger <= f64::EPSILON {
        0.0
    } else {
        (long_notional - short_notional).abs() / larger
    }
}

fn hedge_notional_needs_protection(long: &PositionLeg, short: &PositionLeg) -> bool {
    hedge_notional_mismatch_exceeds(long, short, HEDGE_PROTECTION_TOLERANCE_RATIO)
}

fn hedge_notional_mismatch_exceeds(
    long: &PositionLeg,
    short: &PositionLeg,
    tolerance_ratio: f64,
) -> bool {
    let difference = (position_notional(long) - position_notional(short)).abs();
    difference > HEDGE_PROTECTION_MINIMUM_DIFFERENCE_USDT
        && hedge_notional_imbalance_ratio(long, short) > tolerance_ratio
}

fn paired_positions_are_balanced(long: Option<&PositionLeg>, short: Option<&PositionLeg>) -> bool {
    matches!((long, short), (Some(long), Some(short)) if !hedge_notional_mismatch_exceeds(
        long,
        short,
        POSITION_RECONCILIATION_TOLERANCE_RATIO
    ))
}

fn find_route_leg(
    legs: &[PositionLeg],
    exchange: &str,
    symbol: &str,
    side: &str,
) -> Option<PositionLeg> {
    legs.iter()
        .find(|leg| {
            leg.exchange == exchange
                && leg.symbol == symbol
                && leg.side == side
                && position_notional(leg) > f64::EPSILON
        })
        .cloned()
}

fn protection_order_quantity(
    leg: &PositionLeg,
    excess_notional: f64,
    qty_step: f64,
    close_entire_leg: bool,
) -> f64 {
    if close_entire_leg && position_notional(leg) <= HEDGE_PROTECTION_ORDER_USDT {
        leg.quantity
    } else {
        floor_step(
            HEDGE_PROTECTION_ORDER_USDT.min(excess_notional) / leg.mark_price,
            qty_step,
        )
    }
}

fn weighted_average(
    first_price: f64,
    first_quantity: f64,
    second_price: f64,
    second_quantity: f64,
) -> f64 {
    let total = first_quantity + second_quantity;
    if total <= 0.0 {
        0.0
    } else {
        (first_price * first_quantity + second_price * second_quantity) / total
    }
}

fn load_auto_close_rules(config: &Config) -> Result<HashMap<String, AutoCloseRule>> {
    let Some(path) = config.auto_close_state_path.as_ref() else {
        return Ok(HashMap::new());
    };
    let data = match std::fs::read(path) {
        Ok(data) => data,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(HashMap::new()),
        Err(error) => {
            return Err(error).context("failed to read auto-close state");
        }
    };
    let mut rules = serde_json::from_slice::<Vec<AutoCloseRule>>(&data)
        .context("failed to parse auto-close state")?;
    for rule in &mut rules {
        // A process restart interrupts an in-flight close loop. Re-arm it so
        // the monitor resumes safely from the exchange's actual remaining size.
        if matches!(rule.status.as_str(), "triggered" | "closing") {
            rule.status = "armed".into();
        }
    }
    Ok(rules
        .into_iter()
        .map(|rule| (rule.id.clone(), rule))
        .collect())
}

fn held_route_apy_percent(position: &Position, opportunities: &[Opportunity]) -> Option<f64> {
    if let Some(opportunity) = opportunities.iter().find(|opportunity| {
        opportunity.token.symbol == position.token
            && opportunity.long.exchange == position.long.exchange
            && opportunity.short.exchange == position.short.exchange
    }) {
        return Some(opportunity.apy * 100.0);
    }
    // The scanner only emits positive funding routes. If the reverse route is
    // present, the held route has the exact negative APY.
    opportunities
        .iter()
        .find(|opportunity| {
            opportunity.token.symbol == position.token
                && opportunity.long.exchange == position.short.exchange
                && opportunity.short.exchange == position.long.exchange
        })
        .map(|opportunity| -opportunity.apy * 100.0)
}

fn auto_close_reduction(remaining: f64, order_notional: f64, tolerance: f64) -> Option<f64> {
    if remaining <= order_notional {
        return None;
    }
    let amount = order_notional.min((remaining - tolerance).max(0.0));
    (amount >= 10.0).then_some(amount)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quantity_rounding_never_exceeds_requested_amount() {
        let quantity = floor_step(12.3456, 0.01);
        assert!(quantity <= 12.3456);
        assert!((quantity - 12.34).abs() < 1e-9);
    }

    #[test]
    fn binance_fill_uses_cumulative_quote_when_average_price_is_zero() {
        let response = json!({
            "status": "FILLED",
            "executedQty": "34.57",
            "cumQuote": "99.80359000",
            "avgPrice": "0"
        });
        let (status, quantity, average_price) = binance_fill(&response);
        assert_eq!(status, "FILLED");
        assert!((quantity - 34.57).abs() < 1e-9);
        assert!((average_price - 2.887).abs() < 1e-9);
    }

    #[test]
    fn auto_close_batches_never_exceed_configured_order_size() {
        assert_eq!(auto_close_reduction(1_005.0, 100.0, 10.0), Some(100.0));
        // Preserve the normal 10 USDT tolerance before the final full-close order.
        assert_eq!(auto_close_reduction(105.0, 100.0, 10.0), Some(95.0));
        assert_eq!(auto_close_reduction(10.0, 100.0, 10.0), None);
    }

    #[test]
    fn hedge_protection_caps_orders_and_closes_the_final_remainder() {
        let mut leg = test_position().long;
        leg.quantity = 100.0;
        leg.mark_price = 10.0;
        assert_eq!(protection_order_quantity(&leg, 1_000.0, 0.01, false), 10.0);
        assert_eq!(protection_order_quantity(&leg, 50.0, 0.01, false), 5.0);

        leg.quantity = 8.0;
        assert_eq!(protection_order_quantity(&leg, 8.0, 0.01, true), 8.0);
    }

    #[test]
    fn hedge_protection_uses_relative_notional_difference_without_dividing_by_zero() {
        let mut long = test_position().long;
        let mut short = test_position().short;
        long.quantity = 50.0;
        short.quantity = 50.0;
        long.mark_price = 4.50;
        short.mark_price = 4.00;
        assert!((hedge_notional_imbalance_ratio(&long, &short) - (25.0 / 225.0)).abs() < 1e-9);

        long.quantity = 0.0;
        short.quantity = 0.0;
        assert_eq!(hedge_notional_imbalance_ratio(&long, &short), 0.0);
    }

    #[test]
    fn hedge_protection_ignores_differences_at_or_below_ten_usdt() {
        let mut long = test_position().long;
        let mut short = test_position().short;
        long.quantity = 2.0;
        short.quantity = 1.0;
        long.mark_price = 10.0;
        short.mark_price = 10.0;
        assert!(!hedge_notional_needs_protection(&long, &short));

        long.quantity = 2.01;
        assert!(hedge_notional_needs_protection(&long, &short));
    }

    #[test]
    fn hedge_protection_requires_more_than_three_percent_imbalance() {
        let mut long = test_position().long;
        let mut short = test_position().short;
        long.mark_price = 10.0;
        short.mark_price = 10.0;
        long.quantity = 100.0;
        short.quantity = 98.0;
        assert!(!hedge_notional_needs_protection(&long, &short));

        short.quantity = 96.0;
        assert!(hedge_notional_needs_protection(&long, &short));
    }

    #[test]
    fn paired_positions_accept_small_relative_mismatch_above_ten_usdt() {
        let mut long = test_position().long;
        let mut short = test_position().short;
        long.quantity = 500.0;
        short.quantity = 498.4;
        long.mark_price = 10.0;
        short.mark_price = 10.0;
        assert!(paired_positions_are_balanced(Some(&long), Some(&short)));
        assert!(!paired_positions_are_balanced(Some(&long), None));
    }

    #[test]
    fn reduction_progress_uses_quantity_change_at_a_stable_reference_price() {
        assert!((reduced_notional_at_reference(100.0, 90.0, 10.0) - 100.0).abs() < 1e-9);
        assert_eq!(reduced_notional_at_reference(100.0, 100.5, 10.0), 0.0);
    }

    #[test]
    fn auto_close_never_treats_missing_market_data_as_zero_apy() {
        let position = test_position();
        assert_eq!(held_route_apy_percent(&position, &[]), None);

        let reverse = Opportunity {
            id: "DEXE-Bybit-Binance".into(),
            token: Token {
                symbol: "DEXE".into(),
                name: "DeXe".into(),
                rank: Some(100),
                tags: vec!["cmc200".into()],
            },
            long: test_market_leg("Bybit"),
            short: test_market_leg("Binance"),
            funding_per_hour: 0.001,
            apy: 4.0,
            spread: 0.0,
            fees: 0.0,
            break_even_hours: 0.0,
        };
        assert_eq!(held_route_apy_percent(&position, &[reverse]), Some(-400.0));
    }

    fn test_position() -> Position {
        Position {
            id: "live-DEXE".into(),
            token: "DEXE".into(),
            status: "open".into(),
            opened_at: 0,
            notional_usdt: 100.0,
            leverage: 1,
            long: PositionLeg {
                exchange: "Binance".into(),
                symbol: "DEXEUSDT".into(),
                side: "long".into(),
                quantity: 10.0,
                entry_price: 10.0,
                mark_price: 10.0,
                unrealized_pnl: 0.0,
                funding_earned: 0.0,
                funding_rate: 0.0,
                leverage: 1,
            },
            short: PositionLeg {
                exchange: "Bybit".into(),
                symbol: "DEXEUSDT".into(),
                side: "short".into(),
                quantity: 10.0,
                entry_price: 10.0,
                mark_price: 10.0,
                unrealized_pnl: 0.0,
                funding_earned: 0.0,
                funding_rate: 0.0,
                leverage: 1,
            },
            funding_earned: 0.0,
            unrealized_pnl: 0.0,
        }
    }

    fn test_market_leg(exchange: &str) -> Leg {
        Leg {
            exchange: exchange.into(),
            base: "DEXE".into(),
            symbol: "DEXEUSDT".into(),
            bid: 10.0,
            ask: 10.0,
            mark: 10.0,
            rate: 0.0,
            interval_hours: 8.0,
            next_funding_time: 0,
            qty_step: 0.01,
            tags: vec![],
            volume_24h_usdt: 1_000_000.0,
        }
    }

    #[test]
    fn realized_pnl_is_funding_less_trading_fees() {
        let summary = IncomeSummary {
            funding_income: 12.5,
            trading_fees: 3.25,
            ..Default::default()
        };
        assert!((summary.realized_pnl() - 9.25).abs() < 1e-9);
    }

    #[test]
    fn income_summary_can_be_scoped_to_managed_symbols() {
        let summary = IncomeSummary {
            funding_by_symbol: HashMap::from([("DEXEUSDT".into(), 8.0), ("XMRUSDT".into(), -2.0)]),
            trading_fees_by_symbol: HashMap::from([
                ("DEXEUSDT".into(), 0.7),
                ("XMRUSDT".into(), 4.0),
            ]),
            funding_income: 6.0,
            trading_fees: 4.7,
            ..Default::default()
        };
        let filtered = summary.for_symbols(&HashSet::from(["DEXEUSDT".into()]));
        assert!((filtered.funding_income - 8.0).abs() < 1e-9);
        assert!((filtered.trading_fees - 0.7).abs() < 1e-9);
        assert!((filtered.realized_pnl() - 7.3).abs() < 1e-9);
    }

    #[test]
    fn funding_cache_rolls_over_two_minutes_after_the_hour() {
        let hour = 60 * 60 * 1_000;
        assert_eq!(
            funding_refresh_bucket(hour + 119_999),
            funding_refresh_bucket(hour - 1)
        );
        assert_eq!(
            funding_refresh_bucket(hour + 120_000),
            funding_refresh_bucket(hour - 1) + 1
        );
    }

    #[test]
    fn realized_pnl_includes_partial_close_pnl() {
        let summary = IncomeSummary {
            closed_position_pnl: 4.5,
            funding_income: 2.0,
            trading_fees: 0.75,
            ..Default::default()
        };
        assert!((summary.realized_pnl() - 5.75).abs() < 1e-9);
    }
}
