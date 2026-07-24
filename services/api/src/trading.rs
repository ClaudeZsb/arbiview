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

struct LegFill {
    quantity: f64,
    average_entry: f64,
}

impl LegFill {
    fn from_order(order: OrderExecution) -> Self {
        Self {
            quantity: order.executed_quantity,
            average_entry: order.average_price,
        }
    }

    fn add(&mut self, order: &OrderExecution) {
        let total = self.quantity + order.executed_quantity;
        if total > 0.0 {
            self.average_entry = (self.average_entry * self.quantity
                + order.average_price * order.executed_quantity)
                / total;
            self.quantity = total;
        }
    }

    fn reduce(&mut self, order: &OrderExecution) {
        self.quantity = (self.quantity - order.executed_quantity).max(0.0);
    }

    fn notional(&self, reference_price: f64) -> f64 {
        self.quantity * reference_price
    }

    fn average_price(&self) -> f64 {
        self.average_entry
    }

    fn as_execution(&self, exchange: &str) -> OrderExecution {
        OrderExecution {
            exchange: exchange.into(),
            client_order_id: "aggregate".into(),
            order_id: "aggregate".into(),
            status: "FILLED".into(),
            executed_quantity: self.quantity,
            average_price: self.average_entry,
        }
    }
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
        let (long_order, short_order) = match (long_result, short_result) {
            (Ok(long), Ok(short)) => (long, short),
            (Ok(long), Err(short_error)) => {
                return self
                    .compensate_single_leg(&opportunity.long, "Sell", long, short_error, "short")
                    .await;
            }
            (Err(long_error), Ok(short)) => {
                return self
                    .compensate_single_leg(&opportunity.short, "Buy", short, long_error, "long")
                    .await;
            }
            (Err(long_error), Err(short_error)) => {
                return Err(anyhow!(
                    "both concurrent legs failed; long=({long_error:#}); short=({short_error:#})"
                ));
            }
        };

        let mut long_fill = LegFill::from_order(long_order.clone());
        let mut short_fill = LegFill::from_order(short_order.clone());
        report.orders.extend([long_order, short_order]);

        self.supplement_leg(
            &opportunity.long,
            "Buy",
            &mut long_fill,
            request.notional_usdt,
            &mut report,
        )
        .await?;
        self.supplement_leg(
            &opportunity.short,
            "Sell",
            &mut short_fill,
            request.notional_usdt,
            &mut report,
        )
        .await?;
        self.rebalance_legs(&opportunity, &mut long_fill, &mut short_fill, &mut report)
            .await?;

        self.ensure_slippage(
            &long_fill.as_execution(&opportunity.long.exchange),
            opportunity.long.ask,
            true,
        )?;
        self.ensure_slippage(
            &short_fill.as_execution(&opportunity.short.exchange),
            opportunity.short.bid,
            false,
        )?;
        report.long_notional_usdt = long_fill.notional(opportunity.long.ask);
        report.short_notional_usdt = short_fill.notional(opportunity.short.bid);
        report.balanced = (report.long_notional_usdt - report.short_notional_usdt).abs()
            <= self.config.position_tolerance_usdt;
        if !report.balanced {
            bail!(
                "NAKED_EXPOSURE: final leg mismatch {:.2} USDT exceeds tolerance {:.2}",
                (report.long_notional_usdt - report.short_notional_usdt).abs(),
                self.config.position_tolerance_usdt
            );
        }
        report.outcome = "filled_and_balanced".into();
        let position = Position {
            id: format!("live-{}", opportunity.token.symbol),
            token: opportunity.token.symbol,
            status: "open".into(),
            opened_at: chrono::Utc::now().timestamp_millis(),
            notional_usdt: request.notional_usdt,
            leverage: request.leverage,
            long: PositionLeg {
                exchange: opportunity.long.exchange,
                symbol: opportunity.long.symbol,
                side: "long".into(),
                quantity: long_fill.quantity,
                entry_price: long_fill.average_price(),
                mark_price: opportunity.long.mark,
                unrealized_pnl: 0.0,
            },
            short: PositionLeg {
                exchange: opportunity.short.exchange,
                symbol: opportunity.short.symbol,
                side: "short".into(),
                quantity: short_fill.quantity,
                entry_price: short_fill.average_price(),
                mark_price: opportunity.short.mark,
                unrealized_pnl: 0.0,
            },
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

    async fn compensate_single_leg(
        &self,
        leg: &Leg,
        close_side: &str,
        order: OrderExecution,
        other_error: anyhow::Error,
        failed_side: &str,
    ) -> Result<TradeResponse> {
        let compensation = self
            .place(
                &leg.exchange,
                &leg.symbol,
                close_side,
                order.executed_quantity,
                true,
                0.0,
            )
            .await;
        match compensation {
            Ok(_) => Err(other_error.context(format!(
                "concurrent {failed_side} leg failed; opposite confirmed fill was compensated"
            ))),
            Err(compensation_error) => Err(anyhow!(
                "NAKED_EXPOSURE: concurrent {failed_side} leg failed ({other_error:#}); compensation failed ({compensation_error:#}); filledClientOrderId={}",
                order.client_order_id
            )),
        }
    }

    async fn supplement_leg(
        &self,
        leg: &Leg,
        side: &str,
        fill: &mut LegFill,
        target_usdt: f64,
        report: &mut ExecutionReport,
    ) -> Result<()> {
        let reference = if side == "Buy" { leg.ask } else { leg.bid };
        let missing = target_usdt - fill.notional(reference);
        if missing <= self.config.position_tolerance_usdt {
            return Ok(());
        }
        let quantity = floor_step(missing / reference, leg.qty_step);
        if quantity <= 0.0 {
            return Ok(());
        }
        match self
            .place(&leg.exchange, &leg.symbol, side, quantity, false, reference)
            .await
        {
            Ok(order) => {
                fill.add(&order);
                report.supplement_orders.push(order);
            }
            Err(error) => {
                let message = error.to_string();
                if message.contains("unknown") || message.contains("NAKED_EXPOSURE") {
                    return Err(error.context(
                        "NAKED_EXPOSURE: supplement result is uncertain; reconciliation stopped",
                    ));
                }
                tracing::warn!(
                    exchange = %leg.exchange,
                    symbol = %leg.symbol,
                    error = %error,
                    "target supplement failed; falling back to reducing the larger leg"
                );
            }
        }
        Ok(())
    }

    async fn rebalance_legs(
        &self,
        opportunity: &Opportunity,
        long_fill: &mut LegFill,
        short_fill: &mut LegFill,
        report: &mut ExecutionReport,
    ) -> Result<()> {
        let long_notional = long_fill.notional(opportunity.long.ask);
        let short_notional = short_fill.notional(opportunity.short.bid);
        let difference = (long_notional - short_notional).abs();
        if difference <= self.config.position_tolerance_usdt {
            return Ok(());
        }
        let (leg, side, fill, reference) = if long_notional > short_notional {
            (&opportunity.long, "Sell", long_fill, opportunity.long.ask)
        } else {
            (&opportunity.short, "Buy", short_fill, opportunity.short.bid)
        };
        let quantity = floor_step(difference / reference, leg.qty_step);
        if quantity <= 0.0 {
            return Ok(());
        }
        let order = self
            .place(&leg.exchange, &leg.symbol, side, quantity, true, 0.0)
            .await
            .context("NAKED_EXPOSURE: failed to reduce the larger leg during reconciliation")?;
        fill.reduce(&order);
        report.rebalance_orders.push(order);
        Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;

    fn order(quantity: f64, price: f64) -> OrderExecution {
        OrderExecution {
            exchange: "Test".into(),
            client_order_id: "client".into(),
            order_id: "order".into(),
            status: "Filled".into(),
            executed_quantity: quantity,
            average_price: price,
        }
    }

    #[test]
    fn aggregates_supplements_and_reductions() {
        let mut fill = LegFill::from_order(order(25.0, 2.0));
        fill.add(&order(25.0, 2.2));
        assert!((fill.quantity - 50.0).abs() < f64::EPSILON);
        assert!((fill.average_price() - 2.1).abs() < 1e-9);
        fill.reduce(&order(10.0, 2.1));
        assert!((fill.quantity - 40.0).abs() < f64::EPSILON);
    }

    #[test]
    fn quantity_rounding_never_exceeds_requested_amount() {
        let quantity = floor_step(12.3456, 0.01);
        assert!(quantity <= 12.3456);
        assert!((quantity - 12.34).abs() < 1e-9);
    }
}
