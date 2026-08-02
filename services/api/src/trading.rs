use crate::{
    account_stream::{
        spawn_binance_account_stream, spawn_bybit_account_stream, AccountBalance, AccountStore,
    },
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
    collections::{HashMap, HashSet, VecDeque},
    sync::Arc,
    time::Duration,
};
use tokio::sync::{Mutex, RwLock};
use uuid::Uuid;

type HmacSha256 = Hmac<Sha256>;
type FundingTotals = HashMap<String, f64>;
type FundingCache = Option<(i64, IncomeSummary, IncomeSummary)>;
const SPREAD_GUARD_POLL_SECONDS: f64 = 0.25;
const POSITION_SETTLEMENT_CHECKS: usize = 3;
const SPREAD_GUARD_ORDER_CAP_USDT: f64 = 50.0;
const SPREAD_RECOVERY_BATCHES: usize = 10;

#[derive(Debug, Clone, Copy, Default)]
struct SpreadRecovery {
    debt_usdt: f64,
    batches_remaining: usize,
}

impl SpreadRecovery {
    fn open_threshold(&self, threshold: f64, next_notional: f64, available_batches: usize) -> f64 {
        threshold + self.installment_ratio(next_notional, available_batches)
    }

    fn close_threshold(&self, threshold: f64, next_notional: f64, available_batches: usize) -> f64 {
        threshold - self.installment_ratio(next_notional, available_batches)
    }

    fn record_open_cumulative(
        &mut self,
        threshold: f64,
        completed_notional: f64,
        cumulative_edge_value: f64,
    ) {
        self.record_debt((threshold * completed_notional - cumulative_edge_value).max(0.0));
    }

    fn record_close_cumulative(
        &mut self,
        threshold: f64,
        completed_notional: f64,
        cumulative_spread_value: f64,
    ) {
        self.record_debt((cumulative_spread_value - threshold * completed_notional).max(0.0));
    }

    fn installment_ratio(&self, next_notional: f64, available_batches: usize) -> f64 {
        let installments = self.batches_remaining.min(available_batches);
        if self.debt_usdt <= f64::EPSILON || installments == 0 || next_notional <= f64::EPSILON {
            0.0
        } else {
            self.debt_usdt / (next_notional * installments as f64)
        }
    }

    fn record_debt(&mut self, updated_debt: f64) {
        if updated_debt <= f64::EPSILON {
            self.debt_usdt = 0.0;
            self.batches_remaining = 0;
            return;
        }
        let new_adverse_fill =
            self.debt_usdt <= f64::EPSILON || updated_debt > self.debt_usdt + f64::EPSILON;
        self.debt_usdt = updated_debt;
        self.batches_remaining = if new_adverse_fill {
            SPREAD_RECOVERY_BATCHES
        } else {
            self.batches_remaining.saturating_sub(1).max(1)
        };
    }
}

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

#[derive(Clone, Copy, PartialEq, Eq)]
enum GuardFillSide {
    Long,
    Short,
}

struct GuardUnmatchedFill {
    notional_usdt: f64,
    price: f64,
}

#[derive(Clone)]
struct CloseUnmatchedFill {
    notional_usdt: f64,
    pnl_ratio: f64,
    price: f64,
}

#[derive(Clone)]
struct ProtectedRoute {
    token: String,
    symbol: String,
    long_exchange: String,
    long_market: String,
    short_exchange: String,
    short_market: String,
}

#[derive(Clone)]
pub struct TradingService {
    config: Config,
    client: Client,
    market: MarketService,
    paper_positions: Arc<RwLock<HashMap<String, Position>>>,
    funding_cache: Arc<RwLock<FundingCache>>,
    margin_position_lock: Arc<Mutex<()>>,
    income_refresh_lock: Arc<Mutex<()>>,
    batch_tasks: Arc<RwLock<HashMap<String, BatchIncreaseTask>>>,
    auto_close_rules: Arc<RwLock<HashMap<String, AutoCloseRule>>>,
    managed_symbols: Arc<RwLock<HashSet<String>>>,
    protected_routes: Arc<RwLock<HashMap<String, ProtectedRoute>>>,
    position_funding_baselines: Arc<RwLock<HashMap<String, f64>>>,
    protection_events: Arc<RwLock<Vec<HedgeProtectionEvent>>>,
    execution_lock: Arc<Mutex<()>>,
    account_store: AccountStore,
}

impl TradingService {
    pub fn new(config: Config, market: MarketService) -> Result<Self> {
        let auto_close_rules = load_auto_close_rules(&config)?;
        let mut managed_symbols = load_managed_symbols(&config)?;
        let position_funding_baselines = load_position_funding_baselines(&config)?;
        managed_symbols.extend(
            auto_close_rules
                .values()
                .map(|rule| format!("{}USDT", rule.token.to_ascii_uppercase())),
        );
        Ok(Self {
            config,
            market,
            client: Client::builder().timeout(Duration::from_secs(15)).build()?,
            paper_positions: Arc::new(RwLock::new(HashMap::new())),
            funding_cache: Arc::new(RwLock::new(None)),
            margin_position_lock: Arc::new(Mutex::new(())),
            income_refresh_lock: Arc::new(Mutex::new(())),
            batch_tasks: Arc::new(RwLock::new(HashMap::new())),
            auto_close_rules: Arc::new(RwLock::new(auto_close_rules)),
            managed_symbols: Arc::new(RwLock::new(managed_symbols)),
            protected_routes: Arc::new(RwLock::new(HashMap::new())),
            position_funding_baselines: Arc::new(RwLock::new(position_funding_baselines)),
            protection_events: Arc::new(RwLock::new(Vec::new())),
            execution_lock: Arc::new(Mutex::new(())),
            account_store: AccountStore::default(),
        })
    }

    pub fn spawn_account_streams(&self) {
        if self.config.trading_mode != TradingMode::Live {
            return;
        }
        let service = self.clone();
        tokio::spawn(async move {
            let (binance_balance, binance_positions, bybit_balance, bybit_positions) = tokio::join!(
                service.fetch_binance_balance(),
                service.fetch_binance_positions(),
                service.fetch_bybit_balance(),
                service.fetch_bybit_positions()
            );
            match (binance_balance, binance_positions) {
                (Ok(balance), Ok(positions)) => {
                    service.account_store.seed_binance(balance, positions).await;
                }
                (balance, positions) => tracing::warn!(
                    "Binance account stream bootstrap failed: balance={:?}, positions={:?}",
                    balance.err(),
                    positions.err()
                ),
            }
            match (bybit_balance, bybit_positions) {
                (Ok(balance), Ok(positions)) => {
                    service.account_store.seed_bybit(balance, positions).await;
                }
                (balance, positions) => tracing::warn!(
                    "Bybit account stream bootstrap failed: balance={:?}, positions={:?}",
                    balance.err(),
                    positions.err()
                ),
            }
            if let Some(credentials) = service.config.binance.clone() {
                spawn_binance_account_stream(
                    service.client.clone(),
                    credentials,
                    service.account_store.clone(),
                );
            }
            if let Some(credentials) = service.config.bybit.clone() {
                spawn_bybit_account_stream(credentials, service.account_store.clone());
            }
            let mut binance_connected = false;
            let mut bybit_connected = false;
            let mut margin_refresh_seconds = 30u64;
            let mut account_reconcile_seconds = 0u64;
            loop {
                tokio::time::sleep(Duration::from_secs(1)).await;
                let current_binance = service.account_store.connected("Binance").await;
                let current_bybit = service.account_store.connected("Bybit").await;
                if current_binance && !binance_connected {
                    match tokio::try_join!(
                        service.fetch_binance_balance(),
                        service.fetch_binance_positions()
                    ) {
                        Ok((balance, positions)) => {
                            service.account_store.seed_binance(balance, positions).await
                        }
                        Err(error) => {
                            tracing::warn!("Binance reconnect snapshot failed: {error:#}")
                        }
                    }
                }
                if current_bybit && !bybit_connected {
                    match tokio::try_join!(
                        service.fetch_bybit_balance(),
                        service.fetch_bybit_positions()
                    ) {
                        Ok((balance, positions)) => {
                            service.account_store.seed_bybit(balance, positions).await
                        }
                        Err(error) => tracing::warn!("Bybit reconnect snapshot failed: {error:#}"),
                    }
                }
                binance_connected = current_binance;
                bybit_connected = current_bybit;
                margin_refresh_seconds = margin_refresh_seconds.saturating_add(1);
                account_reconcile_seconds = account_reconcile_seconds.saturating_add(1);
                if margin_refresh_seconds >= 30 {
                    match service.fetch_binance_margin_positions().await {
                        Ok(positions) => {
                            service.account_store.seed_binance_margin(positions).await;
                            margin_refresh_seconds = 0;
                        }
                        Err(error) => {
                            tracing::warn!("Binance margin snapshot refresh failed: {error:#}");
                            margin_refresh_seconds = 25;
                        }
                    }
                }
                if account_reconcile_seconds >= 30 {
                    let (binance, bybit) = tokio::join!(
                        async {
                            tokio::try_join!(
                                service.fetch_binance_balance(),
                                service.fetch_binance_positions()
                            )
                        },
                        async {
                            tokio::try_join!(
                                service.fetch_bybit_balance(),
                                service.fetch_bybit_positions()
                            )
                        }
                    );
                    match binance {
                        Ok((balance, positions)) => {
                            service.account_store.seed_binance(balance, positions).await
                        }
                        Err(error) => {
                            tracing::warn!("Binance account reconciliation failed: {error:#}")
                        }
                    }
                    match bybit {
                        Ok((balance, positions)) => {
                            service.account_store.seed_bybit(balance, positions).await
                        }
                        Err(error) => {
                            tracing::warn!("Bybit account reconciliation failed: {error:#}")
                        }
                    }
                    account_reconcile_seconds = 0;
                }
            }
        });
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
            loop {
                service.account_store.wait_for_position_update().await;
                // Debounce the two exchanges' near-simultaneous account events so a
                // normal paired fill is observed as a pair instead of a transient orphan.
                tokio::time::sleep(Duration::from_millis(500)).await;
                if let Err(error) = service.inspect_hedge_protection().await {
                    tracing::warn!("hedge protection inspection failed: {error:#}");
                }
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
            tolerance_percent: 0.0,
            minimum_difference_usdt: 0.0,
            order_notional_usdt: HEDGE_PROTECTION_ORDER_USDT,
            interval_seconds: HEDGE_PROTECTION_INTERVAL_SECONDS,
            protected_tokens,
            events: self.protection_events.read().await.clone(),
        }
    }

    pub async fn account_stream_status(&self) -> Value {
        json!({
            "binance": {
                "connected": self.account_store.connected("Binance").await
            },
            "bybit": {
                "connected": self.account_store.connected("Bybit").await
            }
        })
    }

    async fn inspect_hedge_protection(&self) -> Result<()> {
        let (binance, bybit, margin, quotes) = tokio::try_join!(
            self.binance_positions(),
            self.bybit_positions(),
            self.binance_margin_positions(),
            self.market.position_quotes()
        )?;
        let mut legs = binance
            .into_iter()
            .chain(bybit)
            .chain(margin)
            .collect::<Vec<_>>();
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
            let long = find_route_leg(
                &legs,
                &route.long_exchange,
                &route.long_market,
                &route.symbol,
                "long",
            );
            let short = find_route_leg(
                &legs,
                &route.short_exchange,
                &route.short_market,
                &route.symbol,
                "short",
            );
            let needs_protection = matches!((&long, &short), (Some(_), None) | (None, Some(_)));
            if !needs_protection {
                continue;
            }
            let Ok(_guard) = self.execution_lock.try_lock() else {
                continue;
            };
            // Re-read under the execution lock. A normal trade may have completed
            // between the initial observation and acquiring the lock.
            let (binance, bybit, margin, quotes) = tokio::try_join!(
                self.binance_positions(),
                self.bybit_positions(),
                self.binance_margin_positions(),
                self.market.position_quotes()
            )?;
            let mut actual = binance
                .into_iter()
                .chain(bybit)
                .chain(margin)
                .collect::<Vec<_>>();
            apply_position_quotes(&mut actual, &quotes);
            let long = find_route_leg(
                &actual,
                &route.long_exchange,
                &route.long_market,
                &route.symbol,
                "long",
            );
            let short = find_route_leg(
                &actual,
                &route.short_exchange,
                &route.short_market,
                &route.symbol,
                "short",
            );
            let still_unbalanced = matches!((&long, &short), (Some(_), None) | (None, Some(_)));
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
                leg.symbol == long.symbol
                    && leg.side == "short"
                    && (leg.exchange != long.exchange || leg.market != long.market)
            }) {
                let token = long.symbol.trim_end_matches("USDT").to_string();
                routes.insert(
                    token.clone(),
                    ProtectedRoute {
                        token,
                        symbol: long.symbol.clone(),
                        long_exchange: long.exchange.clone(),
                        long_market: long.market.clone(),
                        short_exchange: short.exchange.clone(),
                        short_market: short.market.clone(),
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
            message: "检测到一条腿不存在，准备退出剩余腿".into(),
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
            let actual = match self.authoritative_protection_positions().await {
                Ok(legs) => legs,
                Err(error) => {
                    self.fail_protection_event(&event_id, format!("仓位读取失败：{error:#}"))
                        .await;
                    return;
                }
            };
            let long = find_route_leg(
                &actual,
                &route.long_exchange,
                &route.long_market,
                &route.symbol,
                "long",
            );
            let short = find_route_leg(
                &actual,
                &route.short_exchange,
                &route.short_market,
                &route.symbol,
                "short",
            );
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
            let target = match (long, short) {
                (None, None) => {
                    self.complete_protection_event(&event_id, "双腿仓位均已归零")
                        .await;
                    return;
                }
                (Some(leg), None) | (None, Some(leg)) => leg,
                (Some(_), Some(_)) => {
                    self.complete_protection_event(&event_id, "双腿均存在，孤腿保护停止")
                        .await;
                    return;
                }
            };
            self.update_protection_event(&event_id, |event| {
                event.event_type = "orphan_exit".into();
                event.message = "检测到一条腿归零，正在批量退出剩余腿".into();
            })
            .await;
            let current_notional = position_notional(&target);
            let metadata = match market_legs.iter().find(|leg| {
                leg.exchange == target.exchange
                    && leg.market == target.market
                    && leg.symbol == target.symbol
            }) {
                Some(metadata) => metadata,
                None => {
                    self.fail_protection_event(&event_id, "找不到剩余腿的合约元数据".into())
                        .await;
                    return;
                }
            };
            let quantity =
                protection_order_quantity(&target, current_notional, metadata.qty_step, true);
            if quantity <= 0.0 {
                self.fail_protection_event(&event_id, "保护订单数量计算为零".into())
                    .await;
                return;
            }
            let side = if target.side == "long" { "Sell" } else { "Buy" };
            match self
                .place(
                    &target.exchange,
                    &target.market,
                    &target.symbol,
                    side,
                    quantity,
                    true,
                )
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

    async fn authoritative_protection_positions(&self) -> Result<Vec<PositionLeg>> {
        let (binance, bybit, margin, quotes) = tokio::try_join!(
            self.fetch_binance_positions(),
            self.fetch_bybit_positions(),
            self.binance_margin_positions(),
            self.market.position_quotes()
        )?;
        self.account_store
            .seed_binance(self.fetch_binance_balance().await?, binance.clone())
            .await;
        self.account_store
            .seed_bybit(self.fetch_bybit_balance().await?, bybit.clone())
            .await;
        let mut legs = binance
            .into_iter()
            .chain(bybit)
            .chain(margin)
            .collect::<Vec<_>>();
        apply_position_quotes(&mut legs, &quotes);
        Ok(legs)
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
        for rule in armed {
            let Some(position) = positions
                .iter()
                .find(|position| position.id == rule.position_id)
            else {
                self.update_auto_close(&rule.id, |rule| rule.status = "completed".into())
                    .await;
                continue;
            };
            let Some(current_apy_percent) = position.current_apy.map(|apy| apy * 100.0) else {
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

    async fn remember_managed_symbol(&self, symbol: &str) {
        let symbol = symbol.trim().to_ascii_uppercase();
        if symbol.is_empty() {
            return;
        }
        let inserted = self.managed_symbols.write().await.insert(symbol);
        if inserted {
            self.persist_managed_symbols().await;
        }
    }

    async fn persist_managed_symbols(&self) {
        let Some(path) = self.config.managed_symbols_state_path.as_ref() else {
            return;
        };
        let mut symbols = self
            .managed_symbols
            .read()
            .await
            .iter()
            .cloned()
            .collect::<Vec<_>>();
        symbols.sort();
        let data = match serde_json::to_vec_pretty(&symbols) {
            Ok(data) => data,
            Err(error) => {
                tracing::error!("failed to serialize managed symbols: {error}");
                return;
            }
        };
        if let Some(parent) = path.parent() {
            if let Err(error) = std::fs::create_dir_all(parent) {
                tracing::error!("failed to create managed-symbol state directory: {error}");
                return;
            }
        }
        let temporary = path.with_extension("tmp");
        if let Err(error) =
            std::fs::write(&temporary, data).and_then(|_| std::fs::rename(&temporary, path))
        {
            tracing::error!("failed to persist managed symbols: {error}");
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
            .chain(snapshot.spot_opportunities)
            .chain(snapshot.spread_opportunities)
            .find(|x| x.id == opportunity_id)
            .ok_or_else(|| anyhow!("opportunity is no longer available"))
    }

    async fn open_opportunity(
        &self,
        opportunity: Opportunity,
        request: OpenTradeRequest,
    ) -> Result<TradeResponse> {
        let opportunity = if request.spread_guard {
            let refreshed = self.market.refresh_opportunity_quotes(&opportunity).await?;
            let threshold = request.spread_threshold.unwrap_or(opportunity.spread);
            if !spread_guard_allows(refreshed.spread, threshold) {
                bail!(
                    "SPREAD_WAIT: current executable spread {:.4}% is below threshold {:.4}%",
                    refreshed.spread * 100.0,
                    threshold * 100.0
                );
            }
            refreshed
        } else {
            opportunity
        };
        if !opportunity.execution_supported {
            bail!("this opportunity is observation-only or currently has no borrowable liquidity");
        }
        if !(10.0..=1_000_000.0).contains(&request.notional_usdt) {
            bail!("notionalUsdt must be between 10 and 1,000,000");
        }
        if !(1..=20).contains(&request.leverage) {
            bail!("leverage must be between 1 and 20");
        }
        let managed_symbol = opportunity.long.symbol.clone();
        let result = match self.config.trading_mode {
            TradingMode::Paper => self.open_paper(opportunity, request).await,
            TradingMode::Live => {
                let _guard = self.execution_lock.lock().await;
                self.open_live(opportunity, request).await
            }
        };
        if result.is_ok() {
            self.remember_managed_symbol(&managed_symbol).await;
        }
        result
    }

    pub async fn start_batch_increase(
        &self,
        request: BatchIncreaseRequest,
    ) -> Result<BatchIncreaseTask> {
        if !(10.0..=1_000_000.0).contains(&request.target_notional_usdt) {
            bail!("targetNotionalUsdt must be between 10 and 1,000,000");
        }
        if !request.spread_guard
            && !(10.0..=request.target_notional_usdt).contains(&request.order_notional_usdt)
        {
            bail!("orderNotionalUsdt must be between 10 and targetNotionalUsdt");
        }
        if !request.spread_guard && !(0.5..=3_600.0).contains(&request.interval_seconds) {
            bail!("intervalSeconds must be between 0.5 and 3,600");
        }
        if !(1..=20).contains(&request.leverage) {
            bail!("leverage must be between 1 and 20");
        }
        let opportunity = self.resolve_opportunity(&request.opportunity_id).await?;
        if !opportunity.execution_supported {
            bail!("this opportunity is observation-only or currently has no borrowable liquidity");
        }
        self.remember_managed_symbol(&opportunity.long.symbol).await;
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
        let total_batches = if request.spread_guard {
            0
        } else {
            (request.target_notional_usdt / request.order_notional_usdt).ceil() as usize
        };
        let task = BatchIncreaseTask {
            id: Uuid::new_v4().to_string(),
            action: "increase".into(),
            status: "queued".into(),
            token: opportunity.token.symbol.clone(),
            long_exchange: opportunity.long.exchange.clone(),
            short_exchange: opportunity.short.exchange.clone(),
            target_notional_usdt: request.target_notional_usdt,
            order_notional_usdt: if request.spread_guard {
                0.0
            } else {
                request.order_notional_usdt
            },
            interval_seconds: if request.spread_guard {
                SPREAD_GUARD_POLL_SECONDS
            } else {
                request.interval_seconds
            },
            leverage: Some(request.leverage),
            spread_guard: request.spread_guard,
            spread_threshold: request
                .spread_guard
                .then(|| request.spread_threshold.unwrap_or(opportunity.spread)),
            current_spread: Some(opportunity.spread),
            effective_spread_threshold: request
                .spread_guard
                .then(|| request.spread_threshold.unwrap_or(opportunity.spread)),
            cumulative_filled_spread: None,
            spread_wait_count: 0,
            no_loss_guard: false,
            current_close_pnl_usdt: None,
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
        mut request: BatchReduceRequest,
    ) -> Result<BatchIncreaseTask> {
        let position = self
            .positions()
            .await?
            .into_iter()
            .find(|position| position.id == request.position_id)
            .ok_or_else(|| anyhow!("position not found"))?;
        if request.close_all {
            request.target_notional_usdt = position.notional_usdt;
        }
        if !(10.0..=1_000_000.0).contains(&request.target_notional_usdt) {
            bail!("targetNotionalUsdt must be between 10 and 1,000,000");
        }
        if !request.no_loss_guard
            && !(10.0..=request.target_notional_usdt).contains(&request.order_notional_usdt)
        {
            bail!("orderNotionalUsdt must be between 10 and targetNotionalUsdt");
        }
        if !request.no_loss_guard && !(0.5..=3_600.0).contains(&request.interval_seconds) {
            bail!("intervalSeconds must be between 0.5 and 3,600");
        }
        let maximum_reduction =
            (position.notional_usdt - self.config.position_tolerance_usdt).max(0.0);
        if !request.close_all && request.target_notional_usdt > maximum_reduction {
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
        let total_batches = if request.no_loss_guard {
            0
        } else {
            (request.target_notional_usdt / request.order_notional_usdt).ceil() as usize
        };
        let task = BatchIncreaseTask {
            id: Uuid::new_v4().to_string(),
            action: "reduce".into(),
            status: "queued".into(),
            token: position.token.clone(),
            long_exchange: position.long.exchange.clone(),
            short_exchange: position.short.exchange.clone(),
            target_notional_usdt: request.target_notional_usdt,
            order_notional_usdt: if request.no_loss_guard {
                0.0
            } else {
                request.order_notional_usdt
            },
            interval_seconds: if request.no_loss_guard {
                SPREAD_GUARD_POLL_SECONDS
            } else {
                request.interval_seconds
            },
            leverage: None,
            spread_guard: false,
            spread_threshold: request
                .no_loss_guard
                .then_some(request.close_spread_threshold)
                .flatten(),
            current_spread: None,
            effective_spread_threshold: None,
            cumulative_filled_spread: None,
            spread_wait_count: 0,
            no_loss_guard: request.no_loss_guard,
            current_close_pnl_usdt: None,
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
        if request.spread_guard && matches!(self.config.trading_mode, TradingMode::Live) {
            self.run_spread_guard_increase(task_id, opportunity, request)
                .await;
            return;
        }
        self.update_batch(&task_id, |task| task.status = "running".into())
            .await;
        let base_notional = self
            .positions()
            .await
            .ok()
            .and_then(|positions| {
                positions
                    .into_iter()
                    .find(|position| position.token == opportunity.token.symbol)
            })
            .map(|position| position.notional_usdt)
            .unwrap_or(0.0);
        let mut completed = 0.0;
        let mut batch = 0usize;
        while completed + 0.001 < request.target_notional_usdt {
            if request.spread_guard && request.target_notional_usdt - completed <= 10.0 {
                break;
            }
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
            let batch_opportunity = if request.spread_guard {
                match self.market.refresh_opportunity_quotes(&opportunity).await {
                    Ok(refreshed) => {
                        let threshold = request.spread_threshold.unwrap_or(opportunity.spread);
                        self.update_batch(&task_id, |task| {
                            task.current_spread = Some(refreshed.spread);
                        })
                        .await;
                        if !spread_guard_allows(refreshed.spread, threshold) {
                            self.update_batch(&task_id, |task| {
                                task.spread_wait_count += 1;
                            })
                            .await;
                            tokio::time::sleep(Duration::from_secs_f64(SPREAD_GUARD_POLL_SECONDS))
                                .await;
                            continue;
                        }
                        refreshed
                    }
                    Err(error) => {
                        tracing::warn!("spread-guard quote refresh failed: {error:#}");
                        tokio::time::sleep(Duration::from_secs_f64(SPREAD_GUARD_POLL_SECONDS))
                            .await;
                        continue;
                    }
                }
            } else {
                opportunity.clone()
            };
            batch += 1;
            let remaining = request.target_notional_usdt - completed;
            let batch_notional = if request.spread_guard {
                executable_top_notional(&batch_opportunity).min(remaining)
            } else {
                request.order_notional_usdt.min(remaining)
            };
            if batch_notional < 10.0 {
                self.update_batch(&task_id, |task| {
                    task.spread_wait_count += 1;
                })
                .await;
                tokio::time::sleep(Duration::from_secs_f64(SPREAD_GUARD_POLL_SECONDS)).await;
                continue;
            }
            let open_request = OpenTradeRequest {
                opportunity_id: request.opportunity_id.clone(),
                notional_usdt: batch_notional,
                leverage: request.leverage,
                spread_guard: request.spread_guard,
                spread_threshold: request.spread_threshold.or(Some(opportunity.spread)),
            };
            match self
                .open_opportunity(batch_opportunity.clone(), open_request)
                .await
            {
                Ok(response) => {
                    completed = (response.position.notional_usdt - base_notional)
                        .max(0.0)
                        .min(request.target_notional_usdt);
                    let logs = batch_logs(&batch_opportunity, batch, &response, batch_notional);
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
                    if request.spread_guard
                        && (message.contains("SPREAD_WAIT") || message.contains("SPREAD_NO_FILL"))
                    {
                        batch = batch.saturating_sub(1);
                        self.update_batch(&task_id, |task| {
                            task.spread_wait_count += 1;
                            task.error = None;
                        })
                        .await;
                        tokio::time::sleep(Duration::from_secs_f64(SPREAD_GUARD_POLL_SECONDS))
                            .await;
                        continue;
                    }
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
            if !request.spread_guard && completed + 0.001 < request.target_notional_usdt {
                tokio::time::sleep(Duration::from_secs_f64(request.interval_seconds)).await;
            }
        }
        self.update_batch(&task_id, |task| task.status = "completed".into())
            .await;
    }

    async fn run_spread_guard_increase(
        &self,
        task_id: String,
        opportunity: Opportunity,
        request: BatchIncreaseRequest,
    ) {
        self.update_batch(&task_id, |task| task.status = "running".into())
            .await;
        let preflight = OpenTradeRequest {
            opportunity_id: request.opportunity_id.clone(),
            notional_usdt: request.target_notional_usdt,
            leverage: request.leverage,
            spread_guard: true,
            spread_threshold: request.spread_threshold,
        };
        if let Err(error) = self.preflight_balance(&opportunity, &preflight).await {
            self.fail_batch_task(&task_id, 0, format!("{error:#}"))
                .await;
            return;
        }
        if let Err(error) = tokio::try_join!(
            self.set_leverage(
                &opportunity.long.exchange,
                &opportunity.long.market,
                &opportunity.long.symbol,
                request.leverage
            ),
            self.set_leverage(
                &opportunity.short.exchange,
                &opportunity.short.market,
                &opportunity.short.symbol,
                request.leverage
            )
        ) {
            self.fail_batch_task(
                &task_id,
                0,
                format!("failed to configure leverage; no orders were sent: {error:#}"),
            )
            .await;
            return;
        }

        let threshold = request.spread_threshold.unwrap_or(opportunity.spread);
        let mut unmatched_long = VecDeque::new();
        let mut unmatched_short = VecDeque::new();
        let mut completed = 0.0;
        let mut cumulative_edge_value = 0.0;
        let mut spread_recovery = SpreadRecovery::default();
        let mut batch = 0usize;
        let mut consecutive_empty_attempts = 0u32;
        while request.target_notional_usdt - completed > 10.0 {
            if self.batch_cancelled(&task_id).await {
                self.update_batch(&task_id, |task| task.status = "cancelled".into())
                    .await;
                return;
            }
            let quote = match self.market.refresh_opportunity_quotes(&opportunity).await {
                Ok(quote) => quote,
                Err(error) => {
                    tracing::warn!("spread-guard quote refresh failed: {error:#}");
                    self.record_spread_wait(&task_id, None).await;
                    tokio::time::sleep(Duration::from_secs_f64(SPREAD_GUARD_POLL_SECONDS)).await;
                    continue;
                }
            };
            self.update_batch(&task_id, |task| task.current_spread = Some(quote.spread))
                .await;
            let remaining = (request.target_notional_usdt - completed).max(0.0);
            let desired = SPREAD_GUARD_ORDER_CAP_USDT.min(remaining);
            let available_batches =
                (remaining / SPREAD_GUARD_ORDER_CAP_USDT).ceil().max(1.0) as usize;
            let effective_threshold =
                spread_recovery.open_threshold(threshold, desired, available_batches);
            self.update_batch(&task_id, |task| {
                task.effective_spread_threshold = Some(effective_threshold)
            })
            .await;
            if !spread_guard_allows(quote.spread, effective_threshold) {
                self.record_spread_wait(&task_id, Some(quote.spread)).await;
                tokio::time::sleep(Duration::from_secs_f64(SPREAD_GUARD_POLL_SECONDS)).await;
                continue;
            }

            let long_notional = desired;
            let short_notional = desired;
            let long_qty = floor_step(long_notional / quote.long.ask, opportunity.long.qty_step);
            let short_qty =
                floor_step(short_notional / quote.short.bid, opportunity.short.qty_step);
            if long_qty <= 0.0 && short_qty <= 0.0 {
                self.record_spread_wait(&task_id, Some(quote.spread)).await;
                tokio::time::sleep(Duration::from_secs_f64(SPREAD_GUARD_POLL_SECONDS)).await;
                continue;
            }

            batch += 1;
            let _execution_guard = self.execution_lock.lock().await;
            let (long_result, short_result) = match (long_qty > 0.0, short_qty > 0.0) {
                (true, true) => {
                    let (long, short) = tokio::join!(
                        self.place_limit_ioc(
                            &quote.long.exchange,
                            &quote.long.market,
                            &quote.long.symbol,
                            "Buy",
                            long_qty,
                            quote.long.ask,
                        ),
                        self.place_limit_ioc(
                            &quote.short.exchange,
                            &quote.short.market,
                            &quote.short.symbol,
                            "Sell",
                            short_qty,
                            quote.short.bid,
                        )
                    );
                    (Some(long), Some(short))
                }
                (true, false) => (
                    Some(
                        self.place_limit_ioc(
                            &quote.long.exchange,
                            &quote.long.market,
                            &quote.long.symbol,
                            "Buy",
                            long_qty,
                            quote.long.ask,
                        )
                        .await,
                    ),
                    None,
                ),
                (false, true) => (
                    None,
                    Some(
                        self.place_limit_ioc(
                            &quote.short.exchange,
                            &quote.short.market,
                            &quote.short.symbol,
                            "Sell",
                            short_qty,
                            quote.short.bid,
                        )
                        .await,
                    ),
                ),
                (false, false) => unreachable!(),
            };
            drop(_execution_guard);

            let long_error = long_result
                .as_ref()
                .and_then(|result| result.as_ref().err())
                .map(|error| format!("{error:#}"));
            let short_error = short_result
                .as_ref()
                .and_then(|result| result.as_ref().err())
                .map(|error| format!("{error:#}"));
            let long_result = long_result.and_then(Result::ok);
            let short_result = short_result.and_then(Result::ok);
            if long_result.is_none() && short_result.is_none() {
                consecutive_empty_attempts += 1;
                tracing::warn!(
                    token = opportunity.token.symbol,
                    attempt = batch,
                    long_error = long_error.as_deref().unwrap_or("not submitted"),
                    short_error = short_error.as_deref().unwrap_or("not submitted"),
                    "spread-guard IOC attempt produced no fills"
                );
                self.record_spread_wait(&task_id, Some(quote.spread)).await;
                let unexpected_error = long_error
                    .iter()
                    .chain(short_error.iter())
                    .find(|error| !is_ioc_no_fill(error));
                if consecutive_empty_attempts >= 3 {
                    if let Some(error) = unexpected_error {
                        self.fail_batch_task(
                            &task_id,
                            batch,
                            format!(
                                "IOC order failed repeatedly; no position was opened in the last {} attempts: {error}",
                                consecutive_empty_attempts
                            ),
                        )
                        .await;
                        return;
                    }
                }
                let backoff_millis =
                    (250u64.saturating_mul(1u64 << consecutive_empty_attempts.min(4))).min(5_000);
                tokio::time::sleep(Duration::from_millis(backoff_millis)).await;
                continue;
            }
            consecutive_empty_attempts = 0;

            let mut logs = Vec::new();
            if let Some(order) = long_result {
                let notional = order.executed_quantity * order.average_price;
                if notional > 0.0 {
                    unmatched_long.push_back(GuardUnmatchedFill {
                        notional_usdt: notional,
                        price: order.average_price,
                    });
                    logs.push(guard_batch_log(&opportunity, batch, "long", &order, false));
                }
            }
            if let Some(order) = short_result {
                let notional = order.executed_quantity * order.average_price;
                if notional > 0.0 {
                    unmatched_short.push_back(GuardUnmatchedFill {
                        notional_usdt: notional,
                        price: order.average_price,
                    });
                    logs.push(guard_batch_log(&opportunity, batch, "short", &order, false));
                }
            }
            let (mut matched, mut edge_value) =
                match_guard_fills_with_edge(&mut unmatched_long, &mut unmatched_short);
            for attempt in 1..=MAX_RECONCILIATION_ATTEMPTS {
                let long_excess = unmatched_total(&unmatched_long);
                let short_excess = unmatched_total(&unmatched_short);
                let difference = (long_excess - short_excess).abs();
                if difference <= HEDGE_PROTECTION_MINIMUM_DIFFERENCE_USDT {
                    break;
                }
                let _supplement_guard = self.execution_lock.lock().await;
                let supplement = if long_excess > short_excess {
                    let quantity =
                        floor_step(difference / quote.short.bid, opportunity.short.qty_step);
                    if quantity <= 0.0 {
                        None
                    } else {
                        self.place(
                            &quote.short.exchange,
                            &quote.short.market,
                            &quote.short.symbol,
                            "Sell",
                            quantity,
                            false,
                        )
                        .await
                        .ok()
                        .map(|order| (GuardFillSide::Short, order))
                    }
                } else {
                    let quantity =
                        floor_step(difference / quote.long.ask, opportunity.long.qty_step);
                    if quantity <= 0.0 {
                        None
                    } else {
                        self.place(
                            &quote.long.exchange,
                            &quote.long.market,
                            &quote.long.symbol,
                            "Buy",
                            quantity,
                            false,
                        )
                        .await
                        .ok()
                        .map(|order| (GuardFillSide::Long, order))
                    }
                };
                drop(_supplement_guard);
                let Some((side, order)) = supplement else {
                    tracing::warn!(
                        token = %opportunity.token.symbol,
                        attempt,
                        "immediate market leg supplement failed"
                    );
                    continue;
                };
                let notional = order.executed_quantity * order.average_price;
                if side == GuardFillSide::Long {
                    unmatched_long.push_back(GuardUnmatchedFill {
                        notional_usdt: notional,
                        price: order.average_price,
                    });
                    logs.push(guard_market_supplement_log(
                        &opportunity,
                        batch,
                        "long",
                        &order,
                    ));
                } else {
                    unmatched_short.push_back(GuardUnmatchedFill {
                        notional_usdt: notional,
                        price: order.average_price,
                    });
                    logs.push(guard_market_supplement_log(
                        &opportunity,
                        batch,
                        "short",
                        &order,
                    ));
                }
                let (newly_matched, new_edge_value) =
                    match_guard_fills_with_edge(&mut unmatched_long, &mut unmatched_short);
                matched += newly_matched;
                edge_value += new_edge_value;
            }
            let residual =
                (unmatched_total(&unmatched_long) - unmatched_total(&unmatched_short)).abs();
            if residual > HEDGE_PROTECTION_MINIMUM_DIFFERENCE_USDT {
                self.fail_batch_task(
                    &task_id,
                    batch,
                    format!(
                        "NAKED_EXPOSURE: immediate market supplement failed after {} attempts; residual mismatch {:.2} USDT",
                        MAX_RECONCILIATION_ATTEMPTS, residual
                    ),
                )
                .await;
                return;
            }
            completed = (completed + matched).min(request.target_notional_usdt);
            cumulative_edge_value += edge_value;
            spread_recovery.record_open_cumulative(threshold, completed, cumulative_edge_value);
            let cumulative_spread =
                (completed > f64::EPSILON).then_some(cumulative_edge_value / completed);
            let current_position = self.positions().await.ok().and_then(|positions| {
                positions
                    .into_iter()
                    .find(|position| position.token == opportunity.token.symbol)
            });
            self.update_batch(&task_id, |task| {
                task.completed_notional_usdt = completed;
                task.completed_batches = batch;
                task.current_position = current_position;
                task.cumulative_filled_spread = cumulative_spread;
                for mut log in logs {
                    log.sequence = task.logs.len() + 1;
                    task.logs.push(log);
                }
            })
            .await;
            if matched <= f64::EPSILON {
                tokio::time::sleep(Duration::from_secs_f64(SPREAD_GUARD_POLL_SECONDS)).await;
            }
        }
        self.update_batch(&task_id, |task| task.status = "completed".into())
            .await;
    }

    async fn batch_cancelled(&self, task_id: &str) -> bool {
        self.batch_tasks
            .read()
            .await
            .get(task_id)
            .map(|task| task.cancel_requested)
            .unwrap_or(true)
    }

    async fn record_spread_wait(&self, task_id: &str, spread: Option<f64>) {
        self.update_batch(task_id, |task| {
            task.spread_wait_count += 1;
            if let Some(spread) = spread {
                task.current_spread = Some(spread);
            }
        })
        .await;
    }

    async fn fail_batch_task(&self, task_id: &str, batch: usize, message: String) {
        self.update_batch(task_id, |task| {
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
                message,
            });
        })
        .await;
    }

    async fn run_batch_reduce(
        &self,
        task_id: String,
        position: Position,
        request: BatchReduceRequest,
    ) {
        if request.no_loss_guard && matches!(self.config.trading_mode, TradingMode::Live) {
            self.run_no_loss_reduce(task_id, position, request).await;
            return;
        }
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
            let remaining = request.target_notional_usdt - completed;
            let final_full_close = request.close_all && remaining <= request.order_notional_usdt;
            let batch_notional = request.order_notional_usdt.min(remaining);
            let result = if final_full_close {
                self.close(&request.position_id).await
            } else {
                self.reduce(
                    &request.position_id,
                    AdjustPositionRequest {
                        notional_usdt: batch_notional,
                    },
                )
                .await
            };
            match result {
                Ok(response) => {
                    completed += batch_notional;
                    let logs = batch_reduce_logs(&position, batch, &response, batch_notional);
                    let current_position = (!final_full_close).then_some(response.position.clone());
                    self.update_batch(&task_id, |task| {
                        if final_full_close {
                            completed = task.target_notional_usdt;
                        }
                        task.completed_notional_usdt = completed.min(task.target_notional_usdt);
                        task.completed_batches = batch;
                        task.current_position = current_position;
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

    async fn run_no_loss_reduce(
        &self,
        task_id: String,
        position: Position,
        request: BatchReduceRequest,
    ) {
        self.update_batch(&task_id, |task| task.status = "running".into())
            .await;
        let legs = match self.market.trading_legs().await {
            Ok(legs) => legs,
            Err(error) => {
                self.fail_batch_task(&task_id, 0, format!("{error:#}"))
                    .await;
                return;
            }
        };
        let Some(long_leg) = legs
            .iter()
            .find(|leg| {
                leg.exchange == position.long.exchange
                    && leg.market == position.long.market
                    && leg.symbol == position.long.symbol
            })
            .cloned()
        else {
            self.fail_batch_task(&task_id, 0, "long contract metadata unavailable".into())
                .await;
            return;
        };
        let Some(short_leg) = legs
            .iter()
            .find(|leg| {
                leg.exchange == position.short.exchange
                    && leg.market == position.short.market
                    && leg.symbol == position.short.symbol
            })
            .cloned()
        else {
            self.fail_batch_task(&task_id, 0, "short contract metadata unavailable".into())
                .await;
            return;
        };

        let mut unmatched_long = VecDeque::<CloseUnmatchedFill>::new();
        let mut unmatched_short = VecDeque::<CloseUnmatchedFill>::new();
        let mut completed = 0.0;
        let mut realized_position_pnl = 0.0;
        let mut close_spread_threshold = request.close_spread_threshold;
        let mut cumulative_close_spread_value = 0.0;
        let mut spread_recovery = SpreadRecovery::default();
        let mut batch = 0usize;
        let completion_tolerance = if request.close_all { 0.001 } else { 10.0 };
        while request.target_notional_usdt - completed > completion_tolerance {
            if self.batch_cancelled(&task_id).await {
                self.update_batch(&task_id, |task| task.status = "cancelled".into())
                    .await;
                return;
            }
            let (long_quote, short_quote) = match tokio::try_join!(
                self.market.refresh_leg_quote(&long_leg),
                self.market.refresh_leg_quote(&short_leg)
            ) {
                Ok(quotes) => quotes,
                Err(error) => {
                    tracing::warn!("no-loss close quote refresh failed: {error:#}");
                    self.record_spread_wait(&task_id, None).await;
                    tokio::time::sleep(Duration::from_secs_f64(SPREAD_GUARD_POLL_SECONDS)).await;
                    continue;
                }
            };
            let long_pnl_ratio =
                (long_quote.bid - position.long.entry_price) / long_quote.bid.max(f64::EPSILON);
            let short_pnl_ratio =
                (position.short.entry_price - short_quote.ask) / short_quote.ask.max(f64::EPSILON);
            let current_close_spread =
                (short_quote.ask - long_quote.bid) / long_quote.bid.max(f64::EPSILON);
            let original_threshold = *close_spread_threshold.get_or_insert(current_close_spread);
            let remaining = (request.target_notional_usdt - completed).max(0.0);
            let desired = SPREAD_GUARD_ORDER_CAP_USDT.min(remaining);
            let available_batches =
                (remaining / SPREAD_GUARD_ORDER_CAP_USDT).ceil().max(1.0) as usize;
            let effective_threshold =
                spread_recovery.close_threshold(original_threshold, desired, available_batches);
            self.update_batch(&task_id, |task| {
                task.spread_threshold = Some(original_threshold);
                task.current_spread = Some(current_close_spread);
                task.effective_spread_threshold = Some(effective_threshold);
            })
            .await;
            if !close_spread_guard_allows(current_close_spread, effective_threshold) {
                self.record_spread_wait(&task_id, Some(current_close_spread))
                    .await;
                tokio::time::sleep(Duration::from_secs_f64(SPREAD_GUARD_POLL_SECONDS)).await;
                continue;
            }
            let long_notional = desired;
            let short_notional = desired;
            let mut projected_long = unmatched_long.clone();
            let mut projected_short = unmatched_short.clone();
            if long_notional > 0.0 {
                projected_long.push_back(CloseUnmatchedFill {
                    notional_usdt: long_notional,
                    pnl_ratio: long_pnl_ratio,
                    price: long_quote.bid,
                });
            }
            if short_notional > 0.0 {
                projected_short.push_back(CloseUnmatchedFill {
                    notional_usdt: short_notional,
                    pnl_ratio: short_pnl_ratio,
                    price: short_quote.ask,
                });
            }
            let (_, projected_pnl, _) =
                match_close_fills(&mut projected_long, &mut projected_short);
            self.update_batch(&task_id, |task| {
                task.current_close_pnl_usdt = Some(realized_position_pnl + projected_pnl)
            })
            .await;
            if !no_loss_next_batch_allowed(realized_position_pnl, projected_pnl) {
                self.record_spread_wait(&task_id, None).await;
                tokio::time::sleep(Duration::from_secs_f64(SPREAD_GUARD_POLL_SECONDS)).await;
                continue;
            }

            let long_qty = floor_step(long_notional / long_quote.bid, long_leg.qty_step)
                .min(position.long.quantity);
            let short_qty = floor_step(short_notional / short_quote.ask, short_leg.qty_step)
                .min(position.short.quantity);
            if long_qty <= 0.0 && short_qty <= 0.0 {
                self.record_spread_wait(&task_id, None).await;
                tokio::time::sleep(Duration::from_secs_f64(SPREAD_GUARD_POLL_SECONDS)).await;
                continue;
            }
            batch += 1;
            let _execution_guard = self.execution_lock.lock().await;
            let (long_result, short_result) = match (long_qty > 0.0, short_qty > 0.0) {
                (true, true) => {
                    let (long, short) = tokio::join!(
                        self.place_reduce_limit_ioc(&long_leg, "Sell", long_qty, long_quote.bid),
                        self.place_reduce_limit_ioc(&short_leg, "Buy", short_qty, short_quote.ask)
                    );
                    (long.ok(), short.ok())
                }
                (true, false) => (
                    self.place_reduce_limit_ioc(&long_leg, "Sell", long_qty, long_quote.bid)
                        .await
                        .ok(),
                    None,
                ),
                (false, true) => (
                    None,
                    self.place_reduce_limit_ioc(&short_leg, "Buy", short_qty, short_quote.ask)
                        .await
                        .ok(),
                ),
                (false, false) => unreachable!(),
            };
            drop(_execution_guard);

            let mut logs = Vec::new();
            if let Some(order) = long_result {
                let notional = order.executed_quantity * order.average_price;
                if notional > 0.0 {
                    unmatched_long.push_back(CloseUnmatchedFill {
                        notional_usdt: notional,
                        pnl_ratio: (order.average_price - position.long.entry_price)
                            / order.average_price,
                        price: order.average_price,
                    });
                    logs.push(no_loss_batch_log(&position, batch, "long", &order, false));
                }
            }
            if let Some(order) = short_result {
                let notional = order.executed_quantity * order.average_price;
                if notional > 0.0 {
                    unmatched_short.push_back(CloseUnmatchedFill {
                        notional_usdt: notional,
                        pnl_ratio: (position.short.entry_price - order.average_price)
                            / order.average_price,
                        price: order.average_price,
                    });
                    logs.push(no_loss_batch_log(&position, batch, "short", &order, false));
                }
            }
            let (mut matched, mut matched_pnl, mut matched_spread_value) =
                match_close_fills(&mut unmatched_long, &mut unmatched_short);
            for attempt in 1..=MAX_RECONCILIATION_ATTEMPTS {
                let long_excess = close_unmatched_total(&unmatched_long);
                let short_excess = close_unmatched_total(&unmatched_short);
                let difference = (long_excess - short_excess).abs();
                if difference <= HEDGE_PROTECTION_MINIMUM_DIFFERENCE_USDT {
                    break;
                }
                let _supplement_guard = self.execution_lock.lock().await;
                let supplement = if long_excess > short_excess {
                    let quantity = floor_step(difference / short_quote.ask, short_leg.qty_step);
                    if quantity <= 0.0 {
                        None
                    } else {
                        self.place(
                            &short_leg.exchange,
                            &short_leg.market,
                            &short_leg.symbol,
                            "Buy",
                            quantity,
                            true,
                        )
                        .await
                        .ok()
                        .map(|order| ("short", order))
                    }
                } else {
                    let quantity = floor_step(difference / long_quote.bid, long_leg.qty_step);
                    if quantity <= 0.0 {
                        None
                    } else {
                        self.place(
                            &long_leg.exchange,
                            &long_leg.market,
                            &long_leg.symbol,
                            "Sell",
                            quantity,
                            true,
                        )
                        .await
                        .ok()
                        .map(|order| ("long", order))
                    }
                };
                drop(_supplement_guard);
                let Some((side, order)) = supplement else {
                    tracing::warn!(
                        token = %position.token,
                        attempt,
                        "immediate market close-leg supplement failed"
                    );
                    continue;
                };
                let notional = order.executed_quantity * order.average_price;
                if side == "long" {
                    unmatched_long.push_back(CloseUnmatchedFill {
                        notional_usdt: notional,
                        pnl_ratio: (order.average_price - position.long.entry_price)
                            / order.average_price,
                        price: order.average_price,
                    });
                } else {
                    unmatched_short.push_back(CloseUnmatchedFill {
                        notional_usdt: notional,
                        pnl_ratio: (position.short.entry_price - order.average_price)
                            / order.average_price,
                        price: order.average_price,
                    });
                }
                logs.push(no_loss_market_supplement_log(
                    &position, batch, side, &order,
                ));
                let (newly_matched, new_pnl, new_spread_value) =
                    match_close_fills(&mut unmatched_long, &mut unmatched_short);
                matched += newly_matched;
                matched_pnl += new_pnl;
                matched_spread_value += new_spread_value;
            }
            let residual = (close_unmatched_total(&unmatched_long)
                - close_unmatched_total(&unmatched_short))
            .abs();
            if residual > HEDGE_PROTECTION_MINIMUM_DIFFERENCE_USDT {
                let fully_closed = request.close_all
                    && self.positions().await.ok().is_some_and(|positions| {
                        !positions.iter().any(|item| item.id == position.id)
                    });
                if fully_closed {
                    self.update_batch(&task_id, |task| {
                        task.completed_notional_usdt = task.target_notional_usdt;
                        task.completed_batches = batch;
                        task.current_position = None;
                        for mut log in logs {
                            log.sequence = task.logs.len() + 1;
                            task.logs.push(log);
                        }
                        task.status = "completed".into();
                    })
                    .await;
                    return;
                }
                self.fail_batch_task(
                    &task_id,
                    batch,
                    format!(
                        "NAKED_EXPOSURE: immediate market close supplement failed after {} attempts; residual mismatch {:.2} USDT",
                        MAX_RECONCILIATION_ATTEMPTS, residual
                    ),
                )
                .await;
                return;
            }
            completed = (completed + matched).min(request.target_notional_usdt);
            realized_position_pnl += matched_pnl;
            cumulative_close_spread_value += matched_spread_value;
            spread_recovery.record_close_cumulative(
                original_threshold,
                completed,
                cumulative_close_spread_value,
            );
            let cumulative_close_spread =
                (completed > f64::EPSILON).then_some(cumulative_close_spread_value / completed);
            let current_position =
                self.positions().await.ok().and_then(|positions| {
                    positions.into_iter().find(|item| item.id == position.id)
                });
            self.update_batch(&task_id, |task| {
                task.completed_notional_usdt = completed;
                task.completed_batches = batch;
                task.current_position = current_position;
                task.current_close_pnl_usdt = Some(realized_position_pnl);
                task.cumulative_filled_spread = cumulative_close_spread;
                for mut log in logs {
                    log.sequence = task.logs.len() + 1;
                    task.logs.push(log);
                }
            })
            .await;
            if matched <= f64::EPSILON {
                tokio::time::sleep(Duration::from_secs_f64(SPREAD_GUARD_POLL_SECONDS)).await;
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
                    self.set_leverage(
                        &position.long.exchange,
                        &position.long.market,
                        &position.long.symbol,
                        leverage
                    ),
                    self.set_leverage(
                        &position.short.exchange,
                        &position.short.market,
                        &position.short.symbol,
                        leverage
                    )
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
        let mut positions = match self.config.trading_mode {
            TradingMode::Paper => Ok(self
                .paper_positions
                .read()
                .await
                .values()
                .cloned()
                .collect()),
            TradingMode::Live => self.live_positions().await,
        }?;
        for position in &mut positions {
            update_position_roi(position);
            match self.market.position_market_metrics(position).await {
                Ok((funding_per_hour, spread, apy)) => {
                    position.current_funding_per_hour = Some(funding_per_hour);
                    position.current_spread = Some(spread);
                    position.current_apy = Some(apy);
                }
                Err(error) => {
                    position.current_funding_per_hour = None;
                    position.current_spread = None;
                    position.current_apy = None;
                    tracing::warn!(
                        token = %position.token,
                        "held-route market metrics unavailable: {error:#}"
                    );
                }
            }
        }
        Ok(positions)
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
            margin_legs,
            (binance_income, bybit_income),
            quotes,
        ) = tokio::try_join!(
            self.binance_balance(),
            self.bybit_balance(),
            self.binance_positions(),
            self.bybit_positions(),
            self.binance_margin_positions(),
            self.income_summary(),
            self.market.position_quotes()
        )?;
        apply_position_quotes(&mut binance_legs, &quotes);
        apply_position_quotes(&mut bybit_legs, &quotes);
        binance_legs.extend(margin_legs);
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
                        && (other.exchange != leg.exchange || other.market != leg.market)
                })
            })
            .count();
        let active_managed_symbols = all_legs
            .iter()
            .filter(|leg| {
                all_legs.iter().any(|other| {
                    other.symbol == leg.symbol
                        && other.side != leg.side
                        && (other.exchange != leg.exchange || other.market != leg.market)
                })
            })
            .map(|leg| leg.symbol.clone())
            .collect::<HashSet<_>>();
        for symbol in &active_managed_symbols {
            self.remember_managed_symbol(symbol).await;
        }
        let managed_symbols = self.managed_symbols.read().await.clone();
        let binance_income = binance_income.for_symbols(&managed_symbols);
        let bybit_income = bybit_income.for_symbols(&managed_symbols);
        let unrealized_pnl = all_legs.iter().map(|leg| leg.unrealized_pnl).sum();
        let unhedged_legs = all_legs
            .iter()
            .filter(|leg| {
                !all_legs.iter().any(|other| {
                    (other.exchange != leg.exchange || other.market != leg.market)
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
                && position.long.market == opportunity.long.market
                && position.short.exchange == opportunity.short.exchange
                && position.short.market == opportunity.short.market
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
                market: opportunity.long.market,
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
                market: opportunity.short.market,
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
            current_pnl_usdt: 0.0,
            current_roi: 0.0,
            roi_basis_usdt: request.notional_usdt * 2.0 / f64::from(request.leverage),
            current_funding_per_hour: None,
            current_spread: None,
            current_apy: None,
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
        if opportunity.short.exchange == "Binance" && opportunity.short.market == "spot" {
            let required_quantity = request.notional_usdt / opportunity.short.bid;
            let maximum = self
                .binance_margin_max_borrowable(&opportunity.short.base)
                .await
                .context("Binance margin borrow preflight failed; no orders were sent")?;
            if maximum + opportunity.short.qty_step < required_quantity {
                bail!(
                    "Binance margin maximum borrowable {} is below required {} {}; no orders were sent",
                    format_qty(maximum),
                    format_qty(required_quantity),
                    opportunity.short.base
                );
            }
        }
        Ok(())
    }

    async fn binance_margin_max_borrowable(&self, asset: &str) -> Result<f64> {
        let creds = self
            .config
            .binance
            .as_ref()
            .ok_or_else(|| anyhow!("Binance credentials missing"))?;
        let query = format!(
            "asset={asset}&timestamp={}",
            chrono::Utc::now().timestamp_millis()
        );
        let response = self
            .binance_spot_signed(Method::GET, "/sapi/v1/margin/maxBorrowable", &query, creds)
            .await?;
        number(&response["amount"])
            .ok_or_else(|| anyhow!("Binance returned no maxBorrowable amount for {asset}"))
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
                &opportunity.long.market,
                &opportunity.long.symbol,
                request.leverage
            ),
            self.set_leverage(
                &opportunity.short.exchange,
                &opportunity.short.market,
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

        let (long_result, short_result) = if request.spread_guard {
            tokio::join!(
                self.place_limit_ioc(
                    &opportunity.long.exchange,
                    &opportunity.long.market,
                    &opportunity.long.symbol,
                    "Buy",
                    long_qty,
                    opportunity.long.ask,
                ),
                self.place_limit_ioc(
                    &opportunity.short.exchange,
                    &opportunity.short.market,
                    &opportunity.short.symbol,
                    "Sell",
                    short_qty,
                    opportunity.short.bid,
                )
            )
        } else {
            tokio::join!(
                self.place(
                    &opportunity.long.exchange,
                    &opportunity.long.market,
                    &opportunity.long.symbol,
                    "Buy",
                    long_qty,
                    false,
                ),
                self.place(
                    &opportunity.short.exchange,
                    &opportunity.short.market,
                    &opportunity.short.symbol,
                    "Sell",
                    short_qty,
                    false,
                )
            )
        };
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

        let (long_phase, short_phase) = if request.spread_guard {
            let (long, short) = tokio::join!(
                self.actual_leg(&opportunity.long, "long"),
                self.actual_leg(&opportunity.short, "short")
            );
            (
                PhaseOneResult {
                    state: long.unwrap_or(None),
                    attempts: 0,
                    anomalies: 0,
                    orders: vec![],
                },
                PhaseOneResult {
                    state: short.unwrap_or(None),
                    attempts: 0,
                    anomalies: 0,
                    orders: vec![],
                },
            )
        } else {
            tokio::join!(
                self.align_leg_to_target(&opportunity.long, "long", "Buy", long_target),
                self.align_leg_to_target(&opportunity.short, "short", "Sell", short_target)
            )
        };
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
            let mismatch = (report.long_notional_usdt - report.short_notional_usdt).abs();
            let mismatch_percent = match (&long, &short) {
                (Some(long), Some(short)) => hedge_notional_imbalance_ratio(long, short) * 100.0,
                _ if mismatch > f64::EPSILON => 100.0,
                _ => 0.0,
            };
            if notional_mismatch_exceeds_values(
                report.long_notional_usdt,
                report.short_notional_usdt,
                POSITION_RECONCILIATION_TOLERANCE_RATIO,
            ) {
                report.naked_exposure = true;
                report.alert = true;
                bail!(
                    "NAKED_EXPOSURE: phase two exhausted after {} attempts ({} anomalies); actual position mismatch {:.6} USDT ({:.2}%) exceeds both 10 USDT and 1%",
                    report.phase_two_attempts,
                    report.phase_two_anomalies,
                    mismatch,
                    mismatch_percent
                );
            }
            if request.spread_guard {
                bail!(
                    "SPREAD_NO_FILL: exchange position synchronization produced only {:.6} USDT of residual mismatch",
                    mismatch
                );
            }
            bail!(
                "position reconciliation returned an incomplete leg with {:.6} USDT residual mismatch, within protection tolerance",
                mismatch
            );
        }
        let added_notional = report.long_notional_usdt.min(report.short_notional_usdt)
            - current_long.min(current_short);
        if request.spread_guard && added_notional < 1.0 {
            bail!("SPREAD_NO_FILL: IOC limit orders produced no balanced fill");
        }
        let long =
            long.ok_or_else(|| anyhow!("NAKED_EXPOSURE: actual long position is missing"))?;
        let short =
            short.ok_or_else(|| anyhow!("NAKED_EXPOSURE: actual short position is missing"))?;
        report.outcome = "filled_and_balanced".into();
        let roi_basis_usdt = position_margin_basis(&long, &short);
        let mut position = Position {
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
            current_pnl_usdt: 0.0,
            current_roi: 0.0,
            roi_basis_usdt,
            current_funding_per_hour: None,
            current_spread: None,
            current_apy: None,
        };
        update_position_roi(&mut position);
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
                    &leg.market,
                    &leg.symbol,
                    order_side,
                    quantity,
                    false,
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
        let mut empty_checks = 0usize;
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
                empty_checks += 1;
                if empty_checks >= POSITION_SETTLEMENT_CHECKS {
                    break;
                }
                tokio::time::sleep(Duration::from_secs_f64(SPREAD_GUARD_POLL_SECONDS)).await;
                continue;
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
                .place(
                    &leg.exchange,
                    &leg.market,
                    &leg.symbol,
                    side,
                    quantity,
                    true,
                )
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
                leg.exchange == position.long.exchange
                    && leg.symbol == position.long.symbol
                    && leg.market == position.long.market
            })
            .cloned()
            .ok_or_else(|| anyhow!("long contract metadata unavailable"))?;
        let short_leg = legs
            .iter()
            .find(|leg| {
                leg.exchange == position.short.exchange
                    && leg.symbol == position.short.symbol
                    && leg.market == position.short.market
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
                &position.long.market,
                &position.long.symbol,
                "Sell",
                long_qty,
                true
            ),
            self.place(
                &position.short.exchange,
                &position.short.market,
                &position.short.symbol,
                "Buy",
                short_qty,
                true
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
        let roi_basis_usdt = position_margin_basis(&long, &short);
        let mut reduced = Position {
            id: position.id,
            token: position.token,
            status: "open".into(),
            opened_at: position.opened_at,
            notional_usdt: (report.long_notional_usdt + report.short_notional_usdt) / 2.0,
            leverage: position.leverage,
            funding_earned: position.funding_earned,
            unrealized_pnl: long.unrealized_pnl + short.unrealized_pnl,
            current_pnl_usdt: 0.0,
            current_roi: 0.0,
            roi_basis_usdt,
            current_funding_per_hour: position.current_funding_per_hour,
            current_spread: position.current_spread,
            current_apy: position.current_apy,
            long,
            short,
        };
        update_position_roi(&mut reduced);
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
                &position.long.market,
                &position.long.symbol,
                "Sell",
                position.long.quantity,
                true,
            )
            .await?;
        report.orders.push(long_close);
        match self
            .place(
                &position.short.exchange,
                &position.short.market,
                &position.short.symbol,
                "Buy",
                position.short.quantity,
                true,
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
        if position.short.market == "spot" {
            let metadata = self
                .market
                .trading_legs()
                .await?
                .into_iter()
                .find(|leg| {
                    leg.exchange == position.short.exchange
                        && leg.market == position.short.market
                        && leg.symbol == position.short.symbol
                })
                .ok_or_else(|| anyhow!("spot margin metadata unavailable after close"))?;
            for _ in 0..MAX_RECONCILIATION_ATTEMPTS {
                let Some(residual) = self.actual_leg(&metadata, "short").await? else {
                    break;
                };
                let quantity = ceil_step(residual.quantity, metadata.qty_step);
                let order = self
                    .place(
                        &metadata.exchange,
                        &metadata.market,
                        &metadata.symbol,
                        "Buy",
                        quantity,
                        true,
                    )
                    .await
                    .context("Binance margin residual debt repayment failed")?;
                report.supplement_orders.push(order);
            }
            if self.actual_leg(&metadata, "short").await?.is_some() {
                bail!("Binance margin debt remains after three repayment attempts");
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
        market: &str,
        symbol: &str,
        side: &str,
        quantity: f64,
        reduce_only: bool,
    ) -> Result<OrderExecution> {
        let client_order_id = format!("av{}", Uuid::new_v4().simple());
        match (exchange, market) {
            ("Binance", "perpetual") => {
                self.binance_order(
                    symbol,
                    if side == "Buy" { "BUY" } else { "SELL" },
                    quantity,
                    reduce_only,
                    &client_order_id,
                    None,
                )
                .await
            }
            ("Binance", "spot") => {
                self.binance_margin_order(
                    symbol,
                    if side == "Buy" { "BUY" } else { "SELL" },
                    quantity,
                    reduce_only,
                    &client_order_id,
                    None,
                )
                .await
            }
            ("Bybit", "perpetual") => {
                self.bybit_order(symbol, side, quantity, reduce_only, &client_order_id, None)
                    .await
            }
            _ => bail!("unsupported {exchange} {market} market"),
        }
    }

    async fn place_limit_ioc(
        &self,
        exchange: &str,
        market: &str,
        symbol: &str,
        side: &str,
        quantity: f64,
        price: f64,
    ) -> Result<OrderExecution> {
        let client_order_id = format!("av{}", Uuid::new_v4().simple());
        match (exchange, market) {
            ("Binance", "perpetual") => {
                self.binance_order(
                    symbol,
                    if side == "Buy" { "BUY" } else { "SELL" },
                    quantity,
                    false,
                    &client_order_id,
                    Some(price),
                )
                .await
            }
            ("Binance", "spot") => {
                self.binance_margin_order(
                    symbol,
                    if side == "Buy" { "BUY" } else { "SELL" },
                    quantity,
                    false,
                    &client_order_id,
                    Some(price),
                )
                .await
            }
            ("Bybit", "perpetual") => {
                self.bybit_order(symbol, side, quantity, false, &client_order_id, Some(price))
                    .await
            }
            _ => bail!("unsupported {exchange} {market} market"),
        }
    }

    async fn place_reduce_limit_ioc(
        &self,
        leg: &Leg,
        side: &str,
        quantity: f64,
        price: f64,
    ) -> Result<OrderExecution> {
        let client_order_id = format!("av{}", Uuid::new_v4().simple());
        match (leg.exchange.as_str(), leg.market.as_str()) {
            ("Binance", "perpetual") => {
                self.binance_order(
                    &leg.symbol,
                    if side == "Buy" { "BUY" } else { "SELL" },
                    quantity,
                    true,
                    &client_order_id,
                    Some(price),
                )
                .await
            }
            ("Binance", "spot") => {
                self.binance_margin_order(
                    &leg.symbol,
                    if side == "Buy" { "BUY" } else { "SELL" },
                    quantity,
                    true,
                    &client_order_id,
                    Some(price),
                )
                .await
            }
            ("Bybit", "perpetual") => {
                self.bybit_order(
                    &leg.symbol,
                    side,
                    quantity,
                    true,
                    &client_order_id,
                    Some(price),
                )
                .await
            }
            _ => bail!("unsupported {} {} market", leg.exchange, leg.market),
        }
    }

    async fn set_leverage(
        &self,
        exchange: &str,
        market: &str,
        symbol: &str,
        leverage: u8,
    ) -> Result<()> {
        if market == "spot" {
            return Ok(());
        }
        let result: Result<()> = match exchange {
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
        };
        result?;
        self.account_store
            .set_leverage_hint(exchange, symbol, leverage)
            .await;
        Ok(())
    }

    async fn binance_order(
        &self,
        symbol: &str,
        side: &str,
        qty: f64,
        reduce_only: bool,
        client_order_id: &str,
        limit_price: Option<f64>,
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
        let order_params = limit_price.map_or_else(
            || "type=MARKET".to_string(),
            |price| format!("type=LIMIT&timeInForce=IOC&price={}", format_price(price)),
        );
        let query = format!(
            "symbol={symbol}&side={side}&{order_params}&quantity={}&newClientOrderId={client_order_id}&newOrderRespType=RESULT{mode_params}&timestamp={}",
            format_qty(qty),
            chrono::Utc::now().timestamp_millis()
        );
        let mut response = match self
            .binance_signed(Method::POST, "/fapi/v1/order", &query, creds)
            .await
        {
            Ok(response) => response,
            Err(submit_error) => {
                if let Some(order) = self
                    .account_store
                    .wait_for_order(client_order_id, Duration::from_secs(2))
                    .await
                {
                    return confirmed_order_or_error(order);
                }
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
            if let Some(order) = self
                .account_store
                .wait_for_order(client_order_id, Duration::from_secs(2))
                .await
            {
                return confirmed_order_or_error(order);
            }
            response = self
                .binance_signed(Method::GET, "/fapi/v1/order", &status_query(), creds)
                .await?;
            fill = binance_fill(&response);
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

    async fn binance_margin_order(
        &self,
        symbol: &str,
        side: &str,
        qty: f64,
        reduce_only: bool,
        client_order_id: &str,
        limit_price: Option<f64>,
    ) -> Result<OrderExecution> {
        let creds = self
            .config
            .binance
            .as_ref()
            .ok_or_else(|| anyhow!("Binance credentials missing"))?;
        if (!reduce_only && side != "SELL") || (reduce_only && side != "BUY") {
            bail!("Binance spot-margin route only supports opening SELL and repayment BUY");
        }
        let side_effect = if reduce_only {
            "AUTO_REPAY"
        } else {
            "AUTO_BORROW_REPAY"
        };
        let order_params = limit_price.map_or_else(
            || "type=MARKET".to_string(),
            |price| format!("type=LIMIT&timeInForce=IOC&price={}", format_price(price)),
        );
        let query = format!(
            "symbol={symbol}&isIsolated=FALSE&side={side}&{order_params}&quantity={}&sideEffectType={side_effect}&newClientOrderId={client_order_id}&newOrderRespType=FULL&timestamp={}",
            format_qty(qty),
            chrono::Utc::now().timestamp_millis()
        );
        let mut response = match self
            .binance_spot_signed(Method::POST, "/sapi/v1/margin/order", &query, creds)
            .await
        {
            Ok(response) => response,
            Err(submit_error) => {
                tokio::time::sleep(Duration::from_millis(300)).await;
                let status_query = format!(
                    "symbol={symbol}&isIsolated=FALSE&origClientOrderId={client_order_id}&timestamp={}",
                    chrono::Utc::now().timestamp_millis()
                );
                self.binance_spot_signed(
                    Method::GET,
                    "/sapi/v1/margin/order",
                    &status_query,
                    creds,
                )
                .await
                .with_context(|| {
                    format!(
                        "Binance margin submit result unknown ({submit_error:#}); query clientOrderId={client_order_id} before any retry"
                    )
                })?
            }
        };
        let status_query = || {
            format!(
                "symbol={symbol}&isIsolated=FALSE&origClientOrderId={client_order_id}&timestamp={}",
                chrono::Utc::now().timestamp_millis()
            )
        };
        let mut fill = binance_spot_fill(&response);
        if fill.1 <= 0.0 || fill.2 <= 0.0 {
            for _ in 0..8 {
                tokio::time::sleep(Duration::from_millis(250)).await;
                response = self
                    .binance_spot_signed(
                        Method::GET,
                        "/sapi/v1/margin/order",
                        &status_query(),
                        creds,
                    )
                    .await?;
                fill = binance_spot_fill(&response);
                if fill.0 == "FILLED" && fill.1 > 0.0 && fill.2 > 0.0 {
                    break;
                }
            }
        }
        if fill.1 <= 0.0 || fill.2 <= 0.0 {
            bail!(
                "Binance margin fill data unavailable: status={}, clientOrderId={client_order_id}",
                fill.0
            );
        }
        self.account_store.invalidate_binance_margin().await;
        Ok(OrderExecution {
            exchange: "Binance Spot Margin".into(),
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
        limit_price: Option<f64>,
    ) -> Result<OrderExecution> {
        let position_idx = self.bybit_position_idx(symbol, side, reduce_only).await?;
        let mut body = json!({
            "category": "linear", "symbol": symbol, "side": side,
            "orderType": if limit_price.is_some() { "Limit" } else { "Market" },
            "qty": format_qty(qty),
            "reduceOnly": reduce_only,
            "orderLinkId": client_order_id,
            "positionIdx": position_idx,
        });
        if let Some(price) = limit_price {
            body["price"] = Value::String(format_price(price));
            body["timeInForce"] = Value::String("IOC".into());
        }
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
        if let Some(order) = self
            .account_store
            .wait_for_order(client_order_id, Duration::from_secs(3))
            .await
        {
            return confirmed_order_or_error(order);
        }
        if let Err(submit_error) = submit {
            bail!(
                "Bybit submit result unknown ({submit_error:#}); confirmation timed out; query orderLinkId={client_order_id} before any retry"
            );
        }
        // Private order websocket is the primary confirmation path. A single
        // REST lookup remains as a disconnect/recovery fallback.
        let path = format!(
            "/v5/order/realtime?category=linear&symbol={symbol}&orderLinkId={client_order_id}"
        );
        let status_response = self.bybit_signed(Method::GET, &path, None).await?;
        if status_response["retCode"].as_i64().unwrap_or(-1) != 0 {
            bail!("Bybit order status failed: {}", status_response["retMsg"]);
        }
        if let Some(value) = status_response["result"]["list"]
            .as_array()
            .and_then(|orders| orders.first())
        {
            let order = OrderExecution {
                exchange: "Bybit".into(),
                client_order_id: client_order_id.into(),
                order_id,
                status: value["orderStatus"].as_str().unwrap_or("Unknown").into(),
                executed_quantity: number(&value["cumExecQty"]).unwrap_or(0.0),
                average_price: number(&value["avgPrice"]).unwrap_or(0.0),
            };
            return confirmed_order_or_error(order);
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
        if leg.exchange == "Binance" && leg.market == "spot" {
            if side != "short" {
                return Ok(None);
            }
            return self.binance_margin_short_position(leg).await;
        }
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

    async fn binance_margin_short_position(&self, leg: &Leg) -> Result<Option<PositionLeg>> {
        let creds = self
            .config
            .binance
            .as_ref()
            .ok_or_else(|| anyhow!("Binance credentials missing"))?;
        let query = format!("timestamp={}", chrono::Utc::now().timestamp_millis());
        let account = self
            .binance_spot_signed(Method::GET, "/sapi/v1/margin/account", &query, creds)
            .await?;
        let asset = account["userAssets"]
            .as_array()
            .and_then(|assets| assets.iter().find(|item| item["asset"] == leg.base))
            .ok_or_else(|| anyhow!("Binance margin asset {} is unavailable", leg.base))?;
        let borrowed = number(&asset["borrowed"]).unwrap_or(0.0);
        let interest = number(&asset["interest"]).unwrap_or(0.0);
        let debt = borrowed + interest;
        if debt <= leg.qty_step / 2.0 {
            return Ok(None);
        }
        let trade_query = format!(
            "symbol={}&limit=500&timestamp={}",
            leg.symbol,
            chrono::Utc::now().timestamp_millis()
        );
        let trades = self
            .binance_spot_signed(Method::GET, "/sapi/v1/margin/myTrades", &trade_query, creds)
            .await
            .unwrap_or(Value::Null);
        let entry_price = margin_short_entry_price(&trades, debt).unwrap_or(leg.mark);
        let mark_price = if leg.mark > 0.0 {
            leg.mark
        } else {
            (leg.bid + leg.ask) / 2.0
        };
        Ok(Some(PositionLeg {
            exchange: "Binance".into(),
            market: "spot".into(),
            symbol: leg.symbol.clone(),
            side: "short".into(),
            quantity: debt,
            entry_price,
            mark_price,
            unrealized_pnl: (entry_price - mark_price) * debt,
            funding_earned: -interest * mark_price,
            funding_rate: 0.0,
            leverage: 1,
        }))
    }

    async fn live_positions(&self) -> Result<Vec<Position>> {
        let (mut binance, mut bybit, margin, (binance_income, bybit_income), quotes) = tokio::try_join!(
            self.binance_positions(),
            self.bybit_positions(),
            self.binance_margin_positions(),
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
        self.apply_position_funding_baselines(
            &mut binance,
            &mut bybit,
            &binance_income,
            &bybit_income,
        )
        .await;
        for leg in &mut binance {
            leg.funding_rate = *funding_rates
                .get(&format!("Binance:{}", leg.symbol))
                .unwrap_or(&0.0);
        }
        for leg in &mut bybit {
            leg.funding_rate = *funding_rates
                .get(&format!("Bybit:{}", leg.symbol))
                .unwrap_or(&0.0);
        }
        binance.extend(margin);
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
                let roi_basis_usdt = position_margin_basis(&long, &short);
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
                    current_pnl_usdt: 0.0,
                    current_roi: 0.0,
                    roi_basis_usdt,
                    current_funding_per_hour: None,
                    current_spread: None,
                    current_apy: None,
                })
            })
            .collect())
    }

    async fn apply_position_funding_baselines(
        &self,
        binance: &mut [PositionLeg],
        bybit: &mut [PositionLeg],
        binance_income: &IncomeSummary,
        bybit_income: &IncomeSummary,
    ) {
        let mut baselines = self.position_funding_baselines.write().await;
        let mut active = HashSet::new();
        let mut changed = false;
        for (exchange, legs, income) in [
            ("Binance", binance, binance_income),
            ("Bybit", bybit, bybit_income),
        ] {
            for leg in legs {
                let key = position_funding_key(exchange, &leg.symbol, &leg.side);
                active.insert(key.clone());
                let total = *income.funding_by_symbol.get(&leg.symbol).unwrap_or(&0.0);
                let baseline = baselines.entry(key).or_insert_with(|| {
                    changed = true;
                    total
                });
                leg.funding_earned = total - *baseline;
            }
        }
        let previous_len = baselines.len();
        baselines.retain(|key, _| active.contains(key));
        changed |= baselines.len() != previous_len;
        drop(baselines);
        if changed {
            self.persist_position_funding_baselines().await;
        }
    }

    async fn persist_position_funding_baselines(&self) {
        let Some(path) = self.config.position_funding_state_path.as_ref() else {
            return;
        };
        let baselines = self.position_funding_baselines.read().await.clone();
        let data = match serde_json::to_vec_pretty(&baselines) {
            Ok(data) => data,
            Err(error) => {
                tracing::error!("failed to serialize position funding baselines: {error}");
                return;
            }
        };
        if let Some(parent) = path.parent() {
            if let Err(error) = std::fs::create_dir_all(parent) {
                tracing::error!("failed to create position funding state directory: {error}");
                return;
            }
        }
        let temporary = path.with_extension("tmp");
        if let Err(error) =
            std::fs::write(&temporary, data).and_then(|_| std::fs::rename(&temporary, path))
        {
            tracing::error!("failed to persist position funding baselines: {error}");
        }
    }

    async fn binance_margin_positions(&self) -> Result<Vec<PositionLeg>> {
        if let Some(positions) = self.account_store.binance_margin_positions().await {
            return Ok(positions);
        }
        let _guard = self.margin_position_lock.lock().await;
        if let Some(positions) = self.account_store.binance_margin_positions().await {
            return Ok(positions);
        }
        let positions = self.fetch_binance_margin_positions().await?;
        self.account_store
            .seed_binance_margin(positions.clone())
            .await;
        Ok(positions)
    }

    async fn fetch_binance_margin_positions(&self) -> Result<Vec<PositionLeg>> {
        let managed = self.managed_symbols.read().await.clone();
        if managed.is_empty() {
            return Ok(Vec::new());
        }
        let creds = self
            .config
            .binance
            .as_ref()
            .ok_or_else(|| anyhow!("Binance credentials missing"))?;
        let query = format!("timestamp={}", chrono::Utc::now().timestamp_millis());
        let (account, books) = tokio::try_join!(
            self.binance_spot_signed(Method::GET, "/sapi/v1/margin/account", &query, creds),
            async {
                self.client
                    .get("https://api.binance.com/api/v3/ticker/bookTicker")
                    .send()
                    .await?
                    .error_for_status()?
                    .json::<Value>()
                    .await
                    .map_err(anyhow::Error::from)
            }
        )?;
        let empty = Vec::new();
        let book_map = books
            .as_array()
            .unwrap_or(&empty)
            .iter()
            .filter_map(|item| {
                Some((
                    item["symbol"].as_str()?.to_string(),
                    (number(&item["bidPrice"])?, number(&item["askPrice"])?),
                ))
            })
            .collect::<HashMap<_, _>>();
        let mut positions = Vec::new();
        for symbol in managed {
            let base = symbol.trim_end_matches("USDT");
            let Some(asset) = account["userAssets"]
                .as_array()
                .and_then(|assets| assets.iter().find(|item| item["asset"] == base))
            else {
                continue;
            };
            let borrowed = number(&asset["borrowed"]).unwrap_or(0.0);
            let interest = number(&asset["interest"]).unwrap_or(0.0);
            let debt = borrowed + interest;
            if debt <= 0.0 {
                continue;
            }
            let Some((bid, ask)) = book_map.get(&symbol).copied() else {
                continue;
            };
            let trade_query = format!(
                "symbol={symbol}&limit=500&timestamp={}",
                chrono::Utc::now().timestamp_millis()
            );
            let trades = self
                .binance_spot_signed(Method::GET, "/sapi/v1/margin/myTrades", &trade_query, creds)
                .await
                .unwrap_or(Value::Null);
            let mark_price = (bid + ask) / 2.0;
            let entry_price = margin_short_entry_price(&trades, debt).unwrap_or(mark_price);
            positions.push(PositionLeg {
                exchange: "Binance".into(),
                market: "spot".into(),
                symbol,
                side: "short".into(),
                quantity: debt,
                entry_price,
                mark_price,
                unrealized_pnl: (entry_price - mark_price) * debt,
                funding_earned: -interest * mark_price,
                funding_rate: 0.0,
                leverage: 1,
            });
        }
        Ok(positions)
    }

    async fn binance_positions(&self) -> Result<Vec<PositionLeg>> {
        if let Some((_, positions)) = self.account_store.snapshot("Binance").await {
            return Ok(positions);
        }
        let (balance, positions) =
            tokio::try_join!(self.fetch_binance_balance(), self.fetch_binance_positions())?;
        self.account_store
            .seed_binance(balance, positions.clone())
            .await;
        Ok(positions)
    }

    async fn fetch_binance_positions(&self) -> Result<Vec<PositionLeg>> {
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
                    market: "perpetual".into(),
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
        if let Some((_, positions)) = self.account_store.snapshot("Bybit").await {
            return Ok(positions);
        }
        let (balance, positions) =
            tokio::try_join!(self.fetch_bybit_balance(), self.fetch_bybit_positions())?;
        self.account_store
            .seed_bybit(balance, positions.clone())
            .await;
        Ok(positions)
    }

    async fn fetch_bybit_positions(&self) -> Result<Vec<PositionLeg>> {
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
                    market: "perpetual".into(),
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
                if let (Some(symbol), Some(cash_flow)) =
                    (item["symbol"].as_str(), number(&item["cashFlow"]))
                {
                    *summary
                        .closed_pnl_by_symbol
                        .entry(symbol.to_string())
                        .or_insert(0.0) += cash_flow;
                    summary.closed_position_pnl += cash_flow;
                }
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
        Ok(summary)
    }

    async fn binance_balance(&self) -> Result<(f64, f64, f64)> {
        if let Some((balance, _)) = self.account_store.snapshot("Binance").await {
            return Ok((balance.equity, balance.available, balance.unrealized_pnl));
        }
        let (balance, positions) =
            tokio::try_join!(self.fetch_binance_balance(), self.fetch_binance_positions())?;
        self.account_store.seed_binance(balance, positions).await;
        Ok((balance.equity, balance.available, balance.unrealized_pnl))
    }

    async fn fetch_binance_balance(&self) -> Result<AccountBalance> {
        let creds = self
            .config
            .binance
            .as_ref()
            .ok_or_else(|| anyhow!("Binance credentials missing"))?;
        let query = format!("timestamp={}", chrono::Utc::now().timestamp_millis());
        let json = self
            .binance_signed(Method::GET, "/fapi/v2/account", &query, creds)
            .await?;
        Ok(AccountBalance {
            equity: number(&json["totalWalletBalance"]).unwrap_or(0.0),
            available: number(&json["availableBalance"]).unwrap_or(0.0),
            unrealized_pnl: number(&json["totalUnrealizedProfit"]).unwrap_or(0.0),
        })
    }

    async fn bybit_balance(&self) -> Result<(f64, f64, f64)> {
        if let Some((balance, _)) = self.account_store.snapshot("Bybit").await {
            return Ok((balance.equity, balance.available, balance.unrealized_pnl));
        }
        let (balance, positions) =
            tokio::try_join!(self.fetch_bybit_balance(), self.fetch_bybit_positions())?;
        self.account_store.seed_bybit(balance, positions).await;
        Ok((balance.equity, balance.available, balance.unrealized_pnl))
    }

    async fn fetch_bybit_balance(&self) -> Result<AccountBalance> {
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
        Ok(AccountBalance {
            equity: number(&account["totalEquity"]).unwrap_or(0.0),
            available: number(&account["totalAvailableBalance"]).unwrap_or(0.0),
            unrealized_pnl: number(&account["totalPerpUPL"]).unwrap_or(0.0),
        })
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

    async fn binance_spot_signed(
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
        let url = format!("https://api.binance.com{path}?{signed_query}&signature={signature}");
        let response = self
            .client
            .request(method, url)
            .header("X-MBX-APIKEY", &creds.api_key)
            .send()
            .await
            .map_err(|error| {
                anyhow!(
                    "Binance Spot Margin request failed: {}",
                    error.without_url()
                )
            })?;
        let status = response.status();
        let body = response.text().await?;
        if !status.is_success() {
            let detail = serde_json::from_str::<Value>(&body)
                .ok()
                .and_then(|json| json["msg"].as_str().map(str::to_string))
                .unwrap_or_else(|| "request rejected".into());
            bail!("Binance Spot Margin API HTTP {status}: {detail}");
        }
        serde_json::from_str(&body).context("invalid Binance Spot Margin API response")
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
                let side = if order.exchange.contains("Spot Margin") {
                    "short"
                } else if order.exchange == opportunity.long.exchange {
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

fn guard_batch_log(
    opportunity: &Opportunity,
    batch: usize,
    side: &str,
    order: &OrderExecution,
    is_compensation: bool,
) -> BatchExecutionLog {
    BatchExecutionLog {
        sequence: 0,
        batch,
        timestamp: chrono::Utc::now().timestamp_millis(),
        exchange: order.exchange.clone(),
        side: side.into(),
        token: opportunity.token.symbol.clone(),
        notional_usdt: order.executed_quantity * order.average_price,
        executed_quantity: order.executed_quantity,
        average_price: order.average_price,
        status: order.status.clone(),
        order_id: order.order_id.clone(),
        message: if is_compensation {
            "保价差补偿限价单成交".into()
        } else {
            "保价差 IOC 限价单成交".into()
        },
    }
}

fn guard_market_supplement_log(
    opportunity: &Opportunity,
    batch: usize,
    side: &str,
    order: &OrderExecution,
) -> BatchExecutionLog {
    BatchExecutionLog {
        sequence: 0,
        batch,
        timestamp: chrono::Utc::now().timestamp_millis(),
        exchange: order.exchange.clone(),
        side: side.into(),
        token: opportunity.token.symbol.clone(),
        notional_usdt: order.executed_quantity * order.average_price,
        executed_quantity: order.executed_quantity,
        average_price: order.average_price,
        status: order.status.clone(),
        order_id: order.order_id.clone(),
        message: "单腿差额立即市价补齐".into(),
    }
}

fn unmatched_total(fills: &VecDeque<GuardUnmatchedFill>) -> f64 {
    fills.iter().map(|fill| fill.notional_usdt).sum()
}

fn match_guard_fills_with_edge(
    long: &mut VecDeque<GuardUnmatchedFill>,
    short: &mut VecDeque<GuardUnmatchedFill>,
) -> (f64, f64) {
    let mut matched = 0.0;
    let mut edge_value = 0.0;
    while let (Some(long_fill), Some(short_fill)) = (long.front_mut(), short.front_mut()) {
        let amount = long_fill.notional_usdt.min(short_fill.notional_usdt);
        matched += amount;
        edge_value += amount * (short_fill.price - long_fill.price) / long_fill.price;
        long_fill.notional_usdt -= amount;
        short_fill.notional_usdt -= amount;
        if long_fill.notional_usdt <= f64::EPSILON {
            long.pop_front();
        }
        if short_fill.notional_usdt <= f64::EPSILON {
            short.pop_front();
        }
    }
    (matched, edge_value)
}

fn close_unmatched_total(fills: &VecDeque<CloseUnmatchedFill>) -> f64 {
    fills.iter().map(|fill| fill.notional_usdt).sum()
}

fn match_close_fills(
    long: &mut VecDeque<CloseUnmatchedFill>,
    short: &mut VecDeque<CloseUnmatchedFill>,
) -> (f64, f64, f64) {
    let mut matched = 0.0;
    let mut pnl = 0.0;
    let mut spread_value = 0.0;
    while let (Some(long_fill), Some(short_fill)) = (long.front_mut(), short.front_mut()) {
        let amount = long_fill.notional_usdt.min(short_fill.notional_usdt);
        matched += amount;
        pnl += amount * (long_fill.pnl_ratio + short_fill.pnl_ratio);
        spread_value +=
            amount * (short_fill.price - long_fill.price) / long_fill.price.max(f64::EPSILON);
        long_fill.notional_usdt -= amount;
        short_fill.notional_usdt -= amount;
        if long_fill.notional_usdt <= f64::EPSILON {
            long.pop_front();
        }
        if short_fill.notional_usdt <= f64::EPSILON {
            short.pop_front();
        }
    }
    (matched, pnl, spread_value)
}

fn no_loss_next_batch_allowed(realized_position_pnl: f64, projected_batch_pnl: f64) -> bool {
    realized_position_pnl + projected_batch_pnl >= -1e-8
}

fn no_loss_batch_log(
    position: &Position,
    batch: usize,
    side: &str,
    order: &OrderExecution,
    is_compensation: bool,
) -> BatchExecutionLog {
    BatchExecutionLog {
        sequence: 0,
        batch,
        timestamp: chrono::Utc::now().timestamp_millis(),
        exchange: order.exchange.clone(),
        side: side.into(),
        token: position.token.clone(),
        notional_usdt: order.executed_quantity * order.average_price,
        executed_quantity: order.executed_quantity,
        average_price: order.average_price,
        status: order.status.clone(),
        order_id: order.order_id.clone(),
        message: if is_compensation {
            "保不亏补偿限价平仓成交".into()
        } else {
            "保不亏 IOC 限价平仓成交".into()
        },
    }
}

fn no_loss_market_supplement_log(
    position: &Position,
    batch: usize,
    side: &str,
    order: &OrderExecution,
) -> BatchExecutionLog {
    BatchExecutionLog {
        sequence: 0,
        batch,
        timestamp: chrono::Utc::now().timestamp_millis(),
        exchange: order.exchange.clone(),
        side: side.into(),
        token: position.token.clone(),
        notional_usdt: order.executed_quantity * order.average_price,
        executed_quantity: order.executed_quantity,
        average_price: order.average_price,
        status: order.status.clone(),
        order_id: order.order_id.clone(),
        message: "单腿平仓差额立即市价补齐".into(),
    }
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
                let side = if order.exchange.contains("Spot Margin") {
                    "short"
                } else if order.exchange == position.long.exchange {
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

fn ceil_step(value: f64, step: f64) -> f64 {
    if step <= 0.0 {
        value
    } else {
        ((value / step).ceil() * step * 1e12).round() / 1e12
    }
}

fn format_qty(value: f64) -> String {
    let result = format!("{value:.12}");
    result
        .trim_end_matches('0')
        .trim_end_matches('.')
        .to_string()
}

fn format_price(value: f64) -> String {
    let raw = format!("{value:.12}");
    raw.trim_end_matches('0').trim_end_matches('.').to_string()
}

fn spread_guard_allows(current: f64, threshold: f64) -> bool {
    current + f64::EPSILON >= threshold
}

fn close_spread_guard_allows(current: f64, maximum: f64) -> bool {
    current <= maximum + 1e-12
}

fn is_ioc_no_fill(error: &str) -> bool {
    let normalized = error.to_ascii_lowercase();
    normalized.contains("status=expired")
        || normalized.contains("status=cancelled")
        || normalized.contains("status=canceled")
        || normalized.contains("order ended with status=cancelled")
        || normalized.contains("order ended with status=canceled")
}

fn confirmed_order_or_error(mut order: OrderExecution) -> Result<OrderExecution> {
    if order.executed_quantity > 0.0 && order.average_price > 0.0 {
        if matches!(
            order.status.as_str(),
            "CANCELED" | "EXPIRED" | "Cancelled" | "Canceled"
        ) {
            order.status = "PartiallyFilled".into();
        }
        return Ok(order);
    }
    bail!(
        "{} order ended with status={}, clientOrderId={}",
        order.exchange,
        order.status,
        order.client_order_id
    )
}

fn executable_top_notional(opportunity: &Opportunity) -> f64 {
    (opportunity.long.ask * opportunity.long.ask_quantity)
        .min(opportunity.short.bid * opportunity.short.bid_quantity)
        .max(0.0)
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

fn binance_spot_fill(response: &Value) -> (String, f64, f64) {
    let status = response["status"].as_str().unwrap_or("UNKNOWN").to_string();
    let quantity = number(&response["executedQty"]).unwrap_or(0.0);
    let average_price = number(&response["cummulativeQuoteQty"])
        .filter(|quote| *quote > 0.0)
        .and_then(|quote| (quantity > 0.0).then_some(quote / quantity))
        .or_else(|| {
            let fills = response["fills"].as_array()?;
            let (quote, quantity) = fills.iter().fold((0.0, 0.0), |totals, fill| {
                let qty = number(&fill["qty"]).unwrap_or(0.0);
                let price = number(&fill["price"]).unwrap_or(0.0);
                (totals.0 + qty * price, totals.1 + qty)
            });
            (quantity > 0.0).then_some(quote / quantity)
        })
        .unwrap_or(0.0);
    (status, quantity, average_price)
}

fn margin_short_entry_price(trades: &Value, current_debt: f64) -> Option<f64> {
    let mut rows = trades.as_array()?.iter().collect::<Vec<_>>();
    rows.sort_by_key(|trade| trade["time"].as_i64().unwrap_or(0));
    let mut short_quantity = 0.0;
    let mut short_proceeds = 0.0;
    for trade in rows {
        let quantity = number(&trade["qty"]).unwrap_or(0.0);
        let quote = number(&trade["quoteQty"])
            .unwrap_or_else(|| quantity * number(&trade["price"]).unwrap_or(0.0));
        if trade["isBuyer"].as_bool().unwrap_or(false) {
            if short_quantity > 0.0 {
                let reduced = quantity.min(short_quantity);
                let average = short_proceeds / short_quantity;
                short_quantity -= reduced;
                short_proceeds = (short_proceeds - reduced * average).max(0.0);
            }
        } else {
            short_quantity += quantity;
            short_proceeds += quote;
        }
    }
    if short_quantity <= 0.0 || current_debt <= 0.0 {
        None
    } else {
        Some(short_proceeds / short_quantity)
    }
}

fn position_notional(leg: &PositionLeg) -> f64 {
    leg.quantity * leg.mark_price
}

fn position_margin_basis(long: &PositionLeg, short: &PositionLeg) -> f64 {
    let margin =
        |leg: &PositionLeg| leg.quantity * leg.entry_price / f64::from(leg.leverage.max(1));
    margin(long) + margin(short)
}

fn update_position_roi(position: &mut Position) {
    position.current_pnl_usdt = position.unrealized_pnl + position.funding_earned;
    position.roi_basis_usdt = position_margin_basis(&position.long, &position.short);
    position.current_roi = if position.roi_basis_usdt > f64::EPSILON {
        position.current_pnl_usdt / position.roi_basis_usdt
    } else {
        0.0
    };
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

fn hedge_notional_mismatch_exceeds(
    long: &PositionLeg,
    short: &PositionLeg,
    tolerance_ratio: f64,
) -> bool {
    notional_mismatch_exceeds_values(
        position_notional(long),
        position_notional(short),
        tolerance_ratio,
    )
}

fn notional_mismatch_exceeds_values(
    long_notional: f64,
    short_notional: f64,
    tolerance_ratio: f64,
) -> bool {
    let difference = (long_notional - short_notional).abs();
    let larger = long_notional.max(short_notional);
    let ratio = if larger <= f64::EPSILON {
        0.0
    } else {
        difference / larger
    };
    difference > HEDGE_PROTECTION_MINIMUM_DIFFERENCE_USDT && ratio > tolerance_ratio
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
    market: &str,
    symbol: &str,
    side: &str,
) -> Option<PositionLeg> {
    legs.iter()
        .find(|leg| {
            leg.exchange == exchange
                && leg.market == market
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

fn load_managed_symbols(config: &Config) -> Result<HashSet<String>> {
    let Some(path) = config.managed_symbols_state_path.as_ref() else {
        return Ok(HashSet::new());
    };
    let data = match std::fs::read(path) {
        Ok(data) => data,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(HashSet::new()),
        Err(error) => return Err(error).context("failed to read managed-symbol state"),
    };
    let symbols = serde_json::from_slice::<Vec<String>>(&data)
        .context("failed to parse managed-symbol state")?;
    Ok(symbols
        .into_iter()
        .map(|symbol| symbol.trim().to_ascii_uppercase())
        .filter(|symbol| !symbol.is_empty())
        .collect())
}

fn load_position_funding_baselines(config: &Config) -> Result<HashMap<String, f64>> {
    let Some(path) = config.position_funding_state_path.as_ref() else {
        return Ok(HashMap::new());
    };
    let data = match std::fs::read(path) {
        Ok(data) => data,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(HashMap::new()),
        Err(error) => return Err(error).context("failed to read position funding state"),
    };
    serde_json::from_slice(&data).context("failed to parse position funding state")
}

fn position_funding_key(exchange: &str, symbol: &str, side: &str) -> String {
    format!("{exchange}:{symbol}:{side}")
}

#[cfg(test)]
fn held_route_apy_percent(position: &Position, opportunities: &[Opportunity]) -> Option<f64> {
    if let Some(opportunity) = opportunities.iter().find(|opportunity| {
        opportunity.token.symbol == position.token
            && opportunity.long.exchange == position.long.exchange
            && opportunity.long.market == position.long.market
            && opportunity.short.exchange == position.short.exchange
            && opportunity.short.market == position.short.market
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
                && opportunity.long.market == position.short.market
                && opportunity.short.exchange == position.long.exchange
                && opportunity.short.market == position.long.market
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
    fn binance_spot_fill_uses_cummulative_quote_quantity() {
        let response = json!({
            "status": "FILLED",
            "executedQty": "10",
            "cummulativeQuoteQty": "25"
        });
        let (status, quantity, average_price) = binance_spot_fill(&response);
        assert_eq!(status, "FILLED");
        assert_eq!(quantity, 10.0);
        assert_eq!(average_price, 2.5);
    }

    #[test]
    fn margin_short_entry_tracks_remaining_sell_inventory() {
        let trades = json!([
            {"time": 1, "isBuyer": false, "qty": "10", "quoteQty": "20"},
            {"time": 2, "isBuyer": true, "qty": "4", "quoteQty": "9"},
            {"time": 3, "isBuyer": false, "qty": "2", "quoteQty": "6"}
        ]);
        let entry = margin_short_entry_price(&trades, 8.0).unwrap();
        assert!((entry - 2.25).abs() < 1e-9);
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
    fn position_roi_uses_both_legs_margin_and_current_lifecycle_profit() {
        let mut position = test_position();
        position.long.leverage = 10;
        position.short.leverage = 5;
        position.unrealized_pnl = 4.0;
        position.funding_earned = 2.0;
        update_position_roi(&mut position);
        assert!((position.roi_basis_usdt - 30.0).abs() < 1e-9);
        assert!((position.current_pnl_usdt - 6.0).abs() < 1e-9);
        assert!((position.current_roi - 0.2).abs() < 1e-9);
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
            borrow_interest_per_hour: 0.0,
            apy: 4.0,
            apy_horizon_hours: 1,
            spread: 0.0,
            average_spread_24h: 0.0,
            spread_vs_average: 0.0,
            fees: 0.0,
            break_even_hours: 0.0,
            route_type: "cross_perpetual".into(),
            execution_supported: true,
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
                market: "perpetual".into(),
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
                market: "perpetual".into(),
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
            current_pnl_usdt: 0.0,
            current_roi: 0.0,
            roi_basis_usdt: 200.0,
            current_funding_per_hour: None,
            current_spread: None,
            current_apy: None,
        }
    }

    fn test_market_leg(exchange: &str) -> Leg {
        Leg {
            exchange: exchange.into(),
            market: "perpetual".into(),
            base: "DEXE".into(),
            symbol: "DEXEUSDT".into(),
            bid: 10.0,
            ask: 10.0,
            bid_quantity: 100.0,
            ask_quantity: 100.0,
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

    #[test]
    fn spread_guard_always_prefers_the_numerically_higher_spread() {
        assert!(spread_guard_allows(0.015, 0.01));
        assert!(!spread_guard_allows(0.005, 0.01));
        assert!(spread_guard_allows(-0.015, -0.02));
        assert!(!spread_guard_allows(-0.025, -0.02));
        assert!(spread_guard_allows(-0.02, -0.02));
    }

    #[test]
    fn close_spread_guard_prefers_the_numerically_lower_spread() {
        assert!(close_spread_guard_allows(-0.02, -0.015));
        assert!(close_spread_guard_allows(-0.015, -0.015));
        assert!(!close_spread_guard_allows(-0.01, -0.015));
    }

    #[test]
    fn spread_guard_uses_the_smaller_top_of_book_capacity() {
        let mut opportunity = Opportunity {
            id: "DEXE".into(),
            token: Token {
                symbol: "DEXE".into(),
                name: "DEXE".into(),
                rank: None,
                tags: vec![],
            },
            long: test_market_leg("Binance"),
            short: test_market_leg("Bybit"),
            funding_per_hour: 0.0,
            borrow_interest_per_hour: 0.0,
            apy: 0.0,
            apy_horizon_hours: 1,
            spread: 0.0,
            average_spread_24h: 0.0,
            spread_vs_average: 0.0,
            fees: 0.0,
            break_even_hours: 0.0,
            route_type: "perpetual_perpetual".into(),
            execution_supported: true,
        };
        opportunity.long.ask = 10.0;
        opportunity.long.ask_quantity = 30.0;
        opportunity.short.bid = 12.0;
        opportunity.short.bid_quantity = 20.0;
        assert!((executable_top_notional(&opportunity) - 240.0).abs() < 1e-9);
    }

    #[test]
    fn reconciliation_requires_both_absolute_and_relative_mismatch_thresholds() {
        assert!(!notional_mismatch_exceeds_values(0.004, 0.0, 0.01));
        assert!(!notional_mismatch_exceeds_values(1_000.0, 995.0, 0.01));
        assert!(!notional_mismatch_exceeds_values(2_000.0, 1_985.0, 0.01));
        assert!(notional_mismatch_exceeds_values(1_000.0, 980.0, 0.01));
        assert!(notional_mismatch_exceeds_values(20.0, 0.0, 0.01));
    }

    #[test]
    fn market_supplement_shortfall_is_amortized_over_ten_open_batches() {
        let mut long = VecDeque::from([GuardUnmatchedFill {
            notional_usdt: 20.0,
            price: 100.0,
        }]);
        let mut short = VecDeque::from([GuardUnmatchedFill {
            notional_usdt: 20.0,
            price: 100.5,
        }]);
        let (matched, edge_value) = match_guard_fills_with_edge(&mut long, &mut short);
        assert!((matched - 20.0).abs() < 1e-9);
        assert!((edge_value - 0.1).abs() < 1e-9);
        let mut recovery = SpreadRecovery::default();
        recovery.record_open_cumulative(0.01, matched, edge_value);
        assert_eq!(recovery.batches_remaining, 10);
        assert!((recovery.debt_usdt - 0.1).abs() < 1e-9);
        assert!((recovery.open_threshold(0.01, 20.0, 10) - 0.0105).abs() < 1e-9);
        assert!(long.is_empty());
        assert!(short.is_empty());
    }

    #[test]
    fn successful_open_batches_pay_down_the_scheduled_debt() {
        let mut recovery = SpreadRecovery::default();
        recovery.record_open_cumulative(0.01, 500.0, 4.0);
        assert!((recovery.debt_usdt - 1.0).abs() < 1e-9);
        let threshold = recovery.open_threshold(0.01, 50.0, 10);
        assert!((threshold - 0.012).abs() < 1e-9);
        recovery.record_open_cumulative(0.01, 550.0, 4.0 + 50.0 * threshold);
        assert!((recovery.debt_usdt - 0.9).abs() < 1e-9);
        assert_eq!(recovery.batches_remaining, 9);
        assert!((recovery.open_threshold(0.01, 50.0, 9) - 0.012).abs() < 1e-9);
    }

    #[test]
    fn close_spread_debt_is_amortized_toward_a_lower_maximum() {
        let mut recovery = SpreadRecovery::default();
        recovery.record_close_cumulative(0.01, 50.0, 0.75);
        assert_eq!(recovery.batches_remaining, 10);
        assert!((recovery.debt_usdt - 0.25).abs() < 1e-9);
        assert!((recovery.close_threshold(0.01, 50.0, 10) - 0.0095).abs() < 1e-9);
    }

    #[test]
    fn spread_recovery_uses_the_actual_remaining_batches_near_task_end() {
        let recovery = SpreadRecovery {
            debt_usdt: 1.0,
            batches_remaining: 10,
        };
        assert!((recovery.open_threshold(0.01, 50.0, 2) - 0.02).abs() < 1e-9);
    }

    #[test]
    fn cumulative_open_surplus_offsets_a_later_market_supplement_loss() {
        let mut recovery = SpreadRecovery::default();
        recovery.record_open_cumulative(0.013, 950.0, 950.0 * 0.0134);
        assert_eq!(recovery.debt_usdt, 0.0);

        recovery.record_open_cumulative(0.013, 1_000.0, 1_000.0 * 0.01338);
        assert_eq!(recovery.debt_usdt, 0.0);
        assert!((recovery.open_threshold(0.013, 50.0, 1) - 0.013).abs() < 1e-9);
    }

    #[test]
    fn no_loss_close_only_matches_batches_with_non_negative_position_pnl() {
        let mut long = VecDeque::from([CloseUnmatchedFill {
            notional_usdt: 50.0,
            pnl_ratio: 0.02,
            price: 100.0,
        }]);
        let mut short = VecDeque::from([CloseUnmatchedFill {
            notional_usdt: 50.0,
            pnl_ratio: -0.015,
            price: 101.0,
        }]);
        let (matched, pnl, spread_value) = match_close_fills(&mut long, &mut short);
        assert!((matched - 50.0).abs() < 1e-9);
        assert!((pnl - 0.25).abs() < 1e-9);
        assert!((spread_value - 0.5).abs() < 1e-9);

        let mut long = VecDeque::from([CloseUnmatchedFill {
            notional_usdt: 50.0,
            pnl_ratio: 0.01,
            price: 100.0,
        }]);
        let mut short = VecDeque::from([CloseUnmatchedFill {
            notional_usdt: 50.0,
            pnl_ratio: -0.02,
            price: 102.0,
        }]);
        let (_, pnl, _) = match_close_fills(&mut long, &mut short);
        assert!(pnl < 0.0);
        assert!(!no_loss_next_batch_allowed(-0.40, 0.30));
        assert!(no_loss_next_batch_allowed(-0.40, 0.40));
        assert!(no_loss_next_batch_allowed(0.20, -0.10));
    }
}
