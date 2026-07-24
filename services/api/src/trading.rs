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
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::sync::RwLock;
use uuid::Uuid;

type HmacSha256 = Hmac<Sha256>;

const MAX_RECONCILIATION_ATTEMPTS: u8 = 3;

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
pub struct TradingService {
    config: Config,
    client: Client,
    market: MarketService,
    paper_positions: Arc<RwLock<HashMap<String, Position>>>,
}

impl TradingService {
    pub fn new(config: Config, market: MarketService) -> Result<Self> {
        Ok(Self {
            config,
            market,
            client: Client::builder().timeout(Duration::from_secs(15)).build()?,
            paper_positions: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    pub async fn open(&self, request: OpenTradeRequest) -> Result<TradeResponse> {
        if !(10.0..=1_000_000.0).contains(&request.notional_usdt) {
            bail!("notionalUsdt must be between 10 and 1,000,000");
        }
        if !(1..=20).contains(&request.leverage) {
            bail!("leverage must be between 1 and 20");
        }
        let snapshot = self.market.opportunities().await?;
        let opportunity = snapshot
            .opportunities
            .into_iter()
            .find(|x| x.id == request.opportunity_id)
            .ok_or_else(|| anyhow!("opportunity is no longer available"))?;
        match self.config.trading_mode {
            TradingMode::Paper => self.open_paper(opportunity, request).await,
            TradingMode::Live => self.open_live(opportunity, request).await,
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
            TradingMode::Live => self.close_live(id).await,
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
        let positions = self.positions().await?;
        let configured_exchanges = [
            self.config.binance.as_ref().map(|_| "Binance".to_string()),
            self.config.bybit.as_ref().map(|_| "Bybit".to_string()),
        ]
        .into_iter()
        .flatten()
        .collect();
        if self.config.trading_mode == TradingMode::Paper {
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
                active_positions: positions.len(),
                unhedged_legs: vec![],
            });
        }
        let (binance, bybit, binance_legs, bybit_legs) = tokio::try_join!(
            self.binance_balance(),
            self.bybit_balance(),
            self.binance_positions(),
            self.bybit_positions()
        )?;
        let all_legs: Vec<PositionLeg> = binance_legs.into_iter().chain(bybit_legs).collect();
        let unhedged_legs = all_legs
            .iter()
            .filter(|leg| {
                let opposite = all_legs.iter().find(|other| {
                    other.exchange != leg.exchange
                        && other.symbol == leg.symbol
                        && other.side != leg.side
                });
                opposite.is_none_or(|other| {
                    (position_notional(leg) - position_notional(other)).abs()
                        > self.config.position_tolerance_usdt
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
                    unrealized_pnl: binance.2,
                },
                ExchangeBalance {
                    exchange: "Bybit".into(),
                    equity_usdt: bybit.0,
                    available_usdt: bybit.1,
                    unrealized_pnl: bybit.2,
                },
            ],
            equity_usdt: binance.0 + bybit.0,
            available_usdt: binance.1 + bybit.1,
            unrealized_pnl: positions.iter().map(|x| x.unrealized_pnl).sum(),
            active_positions: positions.len(),
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
            },
            short: PositionLeg {
                exchange: opportunity.short.exchange,
                symbol: opportunity.short.symbol,
                side: "short".into(),
                quantity: quantity_short,
                entry_price: opportunity.short.bid,
                mark_price: opportunity.short.mark,
                unrealized_pnl: 0.0,
            },
            funding_earned: 0.0,
            unrealized_pnl: 0.0,
        };
        self.paper_positions
            .write()
            .await
            .insert(position.id.clone(), position.clone());
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
        if existing_long.is_some() || existing_short.is_some() {
            bail!(
                "existing {} position detected; close it before opening a new hedge for this symbol",
                opportunity.token.symbol
            );
        }
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
            self.align_leg_to_target(&opportunity.long, "long", "Buy", request.notional_usdt),
            self.align_leg_to_target(&opportunity.short, "short", "Sell", request.notional_usdt)
        );
        report.phase_one_long_attempts = long_phase.attempts;
        report.phase_one_short_attempts = short_phase.attempts;
        report.phase_one_long_anomalies = long_phase.anomalies;
        report.phase_one_short_anomalies = short_phase.anomalies;
        report.supplement_orders.extend(long_phase.orders);
        report.supplement_orders.extend(short_phase.orders);

        let phase_two = self
            .reduce_larger_leg(&opportunity, long_phase.state, short_phase.state)
            .await;
        report.phase_two_attempts = phase_two.attempts;
        report.phase_two_anomalies = phase_two.anomalies;
        report.rebalance_orders.extend(phase_two.orders);
        let long = phase_two.long;
        let short = phase_two.short;
        report.long_notional_usdt = long.as_ref().map(position_notional).unwrap_or(0.0);
        report.short_notional_usdt = short.as_ref().map(position_notional).unwrap_or(0.0);
        report.balanced = (report.long_notional_usdt - report.short_notional_usdt).abs()
            <= self.config.position_tolerance_usdt;
        if !report.balanced {
            report.naked_exposure = true;
            report.alert = true;
            bail!(
                "NAKED_EXPOSURE: phase two exhausted after {} attempts ({} anomalies); actual position mismatch {:.2} USDT exceeds tolerance {:.2}",
                report.phase_two_attempts,
                report.phase_two_anomalies,
                (report.long_notional_usdt - report.short_notional_usdt).abs(),
                self.config.position_tolerance_usdt
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
            notional_usdt: request.notional_usdt,
            leverage: request.leverage,
            long,
            short,
            funding_earned: 0.0,
            unrealized_pnl: 0.0,
        };
        Ok(TradeResponse {
            position,
            mode: "live".into(),
            message: "双腿已并发成交并完成仓位对齐".into(),
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
        opportunity: &Opportunity,
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
                self.actual_leg(&opportunity.long, "long"),
                self.actual_leg(&opportunity.short, "short")
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
            let difference = (long_notional - short_notional).abs();
            if difference <= self.config.position_tolerance_usdt {
                break;
            }
            let (leg, side, reference) = if long_notional > short_notional {
                (
                    &opportunity.long,
                    "Sell",
                    result.long.as_ref().map(|x| x.mark_price),
                )
            } else {
                (
                    &opportunity.short,
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
            self.actual_leg(&opportunity.long, "long"),
            self.actual_leg(&opportunity.short, "short")
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
        let response = match self
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
        let status = response["status"].as_str().unwrap_or("UNKNOWN");
        let executed = number(&response["executedQty"]).unwrap_or(0.0);
        if status != "FILLED" && executed <= 0.0 {
            bail!("Binance order not filled: status={status}, clientOrderId={client_order_id}");
        }
        Ok(OrderExecution {
            exchange: "Binance".into(),
            client_order_id: client_order_id.into(),
            order_id: response["orderId"].to_string().trim_matches('"').into(),
            status: status.into(),
            executed_quantity: executed,
            average_price: number(&response["avgPrice"]).unwrap_or(0.0),
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
        let positions = match leg.exchange.as_str() {
            "Binance" => self.binance_positions().await?,
            "Bybit" => self.bybit_positions().await?,
            exchange => bail!("unsupported exchange {exchange}"),
        };
        Ok(positions
            .into_iter()
            .find(|position| position.symbol == leg.symbol && position.side == side))
    }

    async fn live_positions(&self) -> Result<Vec<Position>> {
        let (binance, bybit) = tokio::try_join!(self.binance_positions(), self.bybit_positions())?;
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
                Some(Position {
                    id: format!("live-{token}"),
                    token,
                    status: "open".into(),
                    opened_at: 0,
                    notional_usdt: long.quantity * long.entry_price,
                    leverage: 1,
                    long,
                    short,
                    funding_earned: 0.0,
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
                })
            })
            .collect())
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
        let signature = sign(&creds.api_secret, query);
        let url = format!("https://fapi.binance.com{path}?{query}&signature={signature}");
        self.client
            .request(method, url)
            .header("X-MBX-APIKEY", &creds.api_key)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await
            .map_err(Into::into)
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
        request
            .send()
            .await?
            .error_for_status()?
            .json()
            .await
            .map_err(Into::into)
    }
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

fn position_notional(leg: &PositionLeg) -> f64 {
    leg.quantity * leg.mark_price
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
}
