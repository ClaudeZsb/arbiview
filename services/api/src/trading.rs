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
            });
        }
        let (binance, bybit) = tokio::try_join!(self.binance_balance(), self.bybit_balance())?;
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
        })
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
        self.place(
            &opportunity.long.exchange,
            &opportunity.long.symbol,
            "Buy",
            long_qty,
            false,
        )
        .await
        .context("long leg failed; no short order was sent")?;
        if let Err(error) = self
            .place(
                &opportunity.short.exchange,
                &opportunity.short.symbol,
                "Sell",
                short_qty,
                false,
            )
            .await
        {
            let _ = self
                .place(
                    &opportunity.long.exchange,
                    &opportunity.long.symbol,
                    "Sell",
                    long_qty,
                    true,
                )
                .await;
            return Err(error.context("short leg failed; attempted to compensate the long leg"));
        }
        let position = Position {
            id: format!(
                "live-{}-{}",
                opportunity.token.symbol,
                chrono::Utc::now().timestamp_millis()
            ),
            token: opportunity.token.symbol,
            status: "open".into(),
            opened_at: chrono::Utc::now().timestamp_millis(),
            notional_usdt: request.notional_usdt,
            leverage: request.leverage,
            long: PositionLeg {
                exchange: opportunity.long.exchange,
                symbol: opportunity.long.symbol,
                side: "long".into(),
                quantity: long_qty,
                entry_price: opportunity.long.ask,
                mark_price: opportunity.long.mark,
                unrealized_pnl: 0.0,
            },
            short: PositionLeg {
                exchange: opportunity.short.exchange,
                symbol: opportunity.short.symbol,
                side: "short".into(),
                quantity: short_qty,
                entry_price: opportunity.short.bid,
                mark_price: opportunity.short.mark,
                unrealized_pnl: 0.0,
            },
            funding_earned: 0.0,
            unrealized_pnl: 0.0,
        };
        Ok(TradeResponse {
            position,
            mode: "live".into(),
            message: "双腿市价单已提交".into(),
        })
    }

    async fn close_live(&self, id: &str) -> Result<TradeResponse> {
        let positions = self.live_positions().await?;
        let position = positions
            .into_iter()
            .find(|p| p.id == id)
            .ok_or_else(|| anyhow!("position not found"))?;
        self.place(
            &position.long.exchange,
            &position.long.symbol,
            "Sell",
            position.long.quantity,
            true,
        )
        .await?;
        if let Err(error) = self
            .place(
                &position.short.exchange,
                &position.short.symbol,
                "Buy",
                position.short.quantity,
                true,
            )
            .await
        {
            return Err(error.context(
                "long leg closed, but short leg close failed; manual intervention required",
            ));
        }
        let mut closed = position;
        closed.status = "closed".into();
        Ok(TradeResponse {
            position: closed,
            mode: "live".into(),
            message: "双腿平仓单已提交".into(),
        })
    }

    async fn place(
        &self,
        exchange: &str,
        symbol: &str,
        side: &str,
        quantity: f64,
        reduce_only: bool,
    ) -> Result<()> {
        match exchange {
            "Binance" => {
                self.binance_order(
                    symbol,
                    if side == "Buy" { "BUY" } else { "SELL" },
                    quantity,
                    reduce_only,
                )
                .await
            }
            "Bybit" => self.bybit_order(symbol, side, quantity, reduce_only).await,
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
    ) -> Result<()> {
        let creds = self
            .config
            .binance
            .as_ref()
            .ok_or_else(|| anyhow!("Binance credentials missing"))?;
        let query = format!(
            "symbol={symbol}&side={side}&type=MARKET&quantity={}&reduceOnly={reduce_only}&timestamp={}",
            format_qty(qty), chrono::Utc::now().timestamp_millis()
        );
        self.binance_signed(Method::POST, "/fapi/v1/order", &query, creds)
            .await?;
        Ok(())
    }

    async fn bybit_order(
        &self,
        symbol: &str,
        side: &str,
        qty: f64,
        reduce_only: bool,
    ) -> Result<()> {
        let body = json!({
            "category": "linear", "symbol": symbol, "side": side,
            "orderType": "Market", "qty": format_qty(qty),
            "reduceOnly": reduce_only,
        });
        let response = self
            .bybit_signed(Method::POST, "/v5/order/create", Some(body))
            .await?;
        if response["retCode"].as_i64().unwrap_or(-1) != 0 {
            bail!("Bybit order rejected: {}", response["retMsg"]);
        }
        Ok(())
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
