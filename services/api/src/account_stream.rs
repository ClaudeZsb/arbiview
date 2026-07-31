use crate::{
    config::ExchangeCredentials,
    models::{OrderExecution, PositionLeg},
};
use anyhow::{anyhow, bail, Context, Result};
use futures_util::{SinkExt, StreamExt};
use hmac::{Hmac, Mac};
use reqwest::Client;
use serde_json::{json, Value};
use sha2::Sha256;
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::sync::{Notify, RwLock};
use tokio_tungstenite::{connect_async, tungstenite::Message};

type HmacSha256 = Hmac<Sha256>;

#[derive(Clone, Copy, Debug, Default)]
pub struct AccountBalance {
    pub equity: f64,
    pub available: f64,
    pub unrealized_pnl: f64,
}

#[derive(Clone, Debug, Default)]
struct ExchangeAccount {
    connected: bool,
    initialized: bool,
    balance: AccountBalance,
    positions: HashMap<String, PositionLeg>,
}

#[derive(Clone, Default)]
pub struct AccountStore {
    binance: Arc<RwLock<ExchangeAccount>>,
    bybit: Arc<RwLock<ExchangeAccount>>,
    binance_margin_positions: Arc<RwLock<Option<Vec<PositionLeg>>>>,
    leverage_hints: Arc<RwLock<HashMap<String, u8>>>,
    orders: Arc<RwLock<HashMap<String, OrderExecution>>>,
    order_notify: Arc<Notify>,
    position_notify: Arc<Notify>,
}

impl AccountStore {
    pub async fn seed_binance(&self, balance: AccountBalance, positions: Vec<PositionLeg>) {
        {
            let mut hints = self.leverage_hints.write().await;
            for position in &positions {
                hints.insert(format!("Binance:{}", position.symbol), position.leverage);
            }
        }
        let mut state = self.binance.write().await;
        state.balance = balance;
        state.positions = position_map(positions);
        state.initialized = true;
        self.position_notify.notify_one();
    }

    pub async fn seed_bybit(&self, balance: AccountBalance, positions: Vec<PositionLeg>) {
        let mut state = self.bybit.write().await;
        state.balance = balance;
        state.positions = position_map(positions);
        state.initialized = true;
        self.position_notify.notify_one();
    }

    pub async fn seed_binance_margin(&self, positions: Vec<PositionLeg>) {
        *self.binance_margin_positions.write().await = Some(positions);
    }

    pub async fn binance_margin_positions(&self) -> Option<Vec<PositionLeg>> {
        self.binance_margin_positions.read().await.clone()
    }

    pub async fn invalidate_binance_margin(&self) {
        *self.binance_margin_positions.write().await = None;
    }

    pub async fn set_leverage_hint(&self, exchange: &str, symbol: &str, leverage: u8) {
        self.leverage_hints
            .write()
            .await
            .insert(format!("{exchange}:{symbol}"), leverage);
    }

    pub async fn set_connected(&self, exchange: &str, connected: bool) {
        let state = if exchange == "Binance" {
            &self.binance
        } else {
            &self.bybit
        };
        state.write().await.connected = connected;
    }

    pub async fn connected(&self, exchange: &str) -> bool {
        if exchange == "Binance" {
            self.binance.read().await.connected
        } else {
            self.bybit.read().await.connected
        }
    }

    pub async fn wait_for_position_update(&self) {
        self.position_notify.notified().await;
    }

    pub async fn snapshot(&self, exchange: &str) -> Option<(AccountBalance, Vec<PositionLeg>)> {
        let state = if exchange == "Binance" {
            self.binance.read().await
        } else {
            self.bybit.read().await
        };
        (state.initialized && state.connected)
            .then(|| (state.balance, state.positions.values().cloned().collect()))
    }

    pub async fn apply_binance_event(&self, value: &Value) {
        match value["e"].as_str() {
            Some("ACCOUNT_UPDATE") => self.apply_binance_account(value).await,
            Some("ORDER_TRADE_UPDATE") => {
                if let Some(order) = binance_order_update(value) {
                    self.record_order(order).await;
                }
            }
            _ => {}
        }
    }

    async fn apply_binance_account(&self, value: &Value) {
        let account = &value["a"];
        let leverage_hints = self.leverage_hints.read().await.clone();
        let mut state = self.binance.write().await;
        let cross_wallet_balance = account["B"]
            .as_array()
            .and_then(|balances| balances.iter().find(|balance| balance["a"] == "USDT"))
            .and_then(|balance| {
                state.balance.equity = number(&balance["wb"]).unwrap_or(state.balance.equity);
                number(&balance["cw"])
            });
        if let Some(positions) = account["P"].as_array() {
            for position in positions {
                let Some(symbol) = position["s"].as_str() else {
                    continue;
                };
                let amount = number(&position["pa"]).unwrap_or(0.0);
                let position_side = position["ps"].as_str().unwrap_or("BOTH");
                let side = if position_side == "LONG" || (position_side == "BOTH" && amount > 0.0) {
                    "long"
                } else {
                    "short"
                };
                let key = position_key(symbol, side, "perpetual");
                if amount.abs() <= f64::EPSILON {
                    state.positions.remove(&key);
                    continue;
                }
                let unrealized_pnl = number(&position["up"]).unwrap_or(0.0);
                let leverage = state
                    .positions
                    .get(&key)
                    .map(|position| position.leverage)
                    .or_else(|| leverage_hints.get(&format!("Binance:{symbol}")).copied())
                    .unwrap_or(1);
                state.positions.insert(
                    key,
                    PositionLeg {
                        exchange: "Binance".into(),
                        market: "perpetual".into(),
                        symbol: symbol.into(),
                        side: side.into(),
                        quantity: amount.abs(),
                        entry_price: number(&position["ep"]).unwrap_or(0.0),
                        mark_price: 0.0,
                        unrealized_pnl,
                        funding_earned: 0.0,
                        funding_rate: 0.0,
                        leverage,
                    },
                );
            }
        }
        state.balance.unrealized_pnl = state
            .positions
            .values()
            .map(|position| position.unrealized_pnl)
            .sum();
        if let Some(cross_wallet_balance) = cross_wallet_balance {
            let initial_margin = state
                .positions
                .values()
                .filter(|position| position.market == "perpetual")
                .map(|position| {
                    position.quantity * position.entry_price / position.leverage.max(1) as f64
                })
                .sum::<f64>();
            state.balance.available = (cross_wallet_balance - initial_margin).max(0.0);
        }
        state.initialized = true;
        drop(state);
        self.position_notify.notify_one();
    }

    pub async fn apply_bybit_event(&self, value: &Value) {
        match value["topic"].as_str() {
            Some("wallet") => {
                let Some(account) = value["data"].as_array().and_then(|data| data.first()) else {
                    return;
                };
                let mut state = self.bybit.write().await;
                state.balance = AccountBalance {
                    equity: number(&account["totalEquity"]).unwrap_or(state.balance.equity),
                    available: number(&account["totalAvailableBalance"])
                        .unwrap_or(state.balance.available),
                    unrealized_pnl: number(&account["totalPerpUPL"])
                        .unwrap_or(state.balance.unrealized_pnl),
                };
                state.initialized = true;
                drop(state);
                self.position_notify.notify_one();
            }
            Some("position") => {
                let Some(positions) = value["data"].as_array() else {
                    return;
                };
                let mut state = self.bybit.write().await;
                for position in positions {
                    if position["category"] != "linear" {
                        continue;
                    }
                    let Some(symbol) = position["symbol"].as_str() else {
                        continue;
                    };
                    let side = if position["side"] == "Buy" {
                        "long"
                    } else {
                        "short"
                    };
                    let key = position_key(symbol, side, "perpetual");
                    let quantity = number(&position["size"]).unwrap_or(0.0);
                    if quantity <= f64::EPSILON {
                        state.positions.remove(&key);
                        continue;
                    }
                    state.positions.insert(
                        key,
                        PositionLeg {
                            exchange: "Bybit".into(),
                            market: "perpetual".into(),
                            symbol: symbol.into(),
                            side: side.into(),
                            quantity,
                            entry_price: number(&position["entryPrice"])
                                .or_else(|| number(&position["avgPrice"]))
                                .unwrap_or(0.0),
                            mark_price: number(&position["markPrice"]).unwrap_or(0.0),
                            unrealized_pnl: number(&position["unrealisedPnl"]).unwrap_or(0.0),
                            funding_earned: 0.0,
                            funding_rate: 0.0,
                            leverage: number(&position["leverage"]).unwrap_or(1.0) as u8,
                        },
                    );
                }
                state.balance.unrealized_pnl = state
                    .positions
                    .values()
                    .map(|position| position.unrealized_pnl)
                    .sum();
                state.initialized = true;
                drop(state);
                self.position_notify.notify_one();
            }
            Some("order") => {
                if let Some(orders) = value["data"].as_array() {
                    for value in orders {
                        if let Some(order) = bybit_order_update(value) {
                            self.record_order(order).await;
                        }
                    }
                }
            }
            _ => {}
        }
    }

    async fn record_order(&self, order: OrderExecution) {
        self.orders
            .write()
            .await
            .insert(order.client_order_id.clone(), order);
        self.order_notify.notify_waiters();
    }

    pub async fn wait_for_order(
        &self,
        client_order_id: &str,
        timeout: Duration,
    ) -> Option<OrderExecution> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if let Some(order) = self.orders.read().await.get(client_order_id).cloned() {
                if is_terminal_order_status(&order.status) {
                    return Some(order);
                }
            }
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return None;
            }
            if tokio::time::timeout(remaining, self.order_notify.notified())
                .await
                .is_err()
            {
                return None;
            }
        }
    }
}

pub fn spawn_binance_account_stream(
    client: Client,
    credentials: ExchangeCredentials,
    store: AccountStore,
) {
    tokio::spawn(async move {
        let mut retry = 1u64;
        loop {
            store.set_connected("Binance", false).await;
            match run_binance_account_stream(&client, &credentials, &store).await {
                Ok(()) => tracing::warn!("Binance account websocket closed"),
                Err(error) => tracing::warn!("Binance account websocket failed: {error:#}"),
            }
            tokio::time::sleep(Duration::from_secs(retry)).await;
            retry = (retry * 2).min(30);
        }
    });
}

async fn run_binance_account_stream(
    client: &Client,
    credentials: &ExchangeCredentials,
    store: &AccountStore,
) -> Result<()> {
    let listen_key = create_binance_listen_key(client, credentials).await?;
    let url = format!("wss://fstream.binance.com/ws/{listen_key}");
    let (mut socket, _) = tokio::time::timeout(Duration::from_secs(10), connect_async(&url))
        .await
        .context("Binance account websocket connect timed out")??;
    store.set_connected("Binance", true).await;
    tracing::info!("Binance account websocket connected");
    let mut keepalive = tokio::time::interval(Duration::from_secs(50 * 60));
    keepalive.tick().await;
    loop {
        tokio::select! {
            _ = keepalive.tick() => {
                client
                    .put("https://fapi.binance.com/fapi/v1/listenKey")
                    .header("X-MBX-APIKEY", &credentials.api_key)
                    .query(&[("listenKey", listen_key.as_str())])
                    .send().await?.error_for_status()?;
            }
            message = socket.next() => {
                let message = message.ok_or_else(|| anyhow!("Binance account stream ended"))??;
                match message {
                    Message::Text(text) => {
                        let value: Value = serde_json::from_str(text.as_ref())?;
                        store.apply_binance_event(&value).await;
                    }
                    Message::Ping(payload) => socket.send(Message::Pong(payload)).await?,
                    Message::Close(frame) => bail!("Binance account websocket closed: {frame:?}"),
                    _ => {}
                }
            }
        }
    }
}

async fn create_binance_listen_key(
    client: &Client,
    credentials: &ExchangeCredentials,
) -> Result<String> {
    let value = client
        .post("https://fapi.binance.com/fapi/v1/listenKey")
        .header("X-MBX-APIKEY", &credentials.api_key)
        .send()
        .await?
        .error_for_status()?
        .json::<Value>()
        .await?;
    value["listenKey"]
        .as_str()
        .map(str::to_string)
        .ok_or_else(|| anyhow!("Binance listen key is missing"))
}

pub fn spawn_bybit_account_stream(credentials: ExchangeCredentials, store: AccountStore) {
    tokio::spawn(async move {
        let mut retry = 1u64;
        loop {
            store.set_connected("Bybit", false).await;
            match run_bybit_account_stream(&credentials, &store).await {
                Ok(()) => tracing::warn!("Bybit account websocket closed"),
                Err(error) => tracing::warn!("Bybit account websocket failed: {error:#}"),
            }
            tokio::time::sleep(Duration::from_secs(retry)).await;
            retry = (retry * 2).min(30);
        }
    });
}

async fn run_bybit_account_stream(
    credentials: &ExchangeCredentials,
    store: &AccountStore,
) -> Result<()> {
    let (mut socket, _) = tokio::time::timeout(
        Duration::from_secs(10),
        connect_async("wss://stream.bybit.com/v5/private"),
    )
    .await
    .context("Bybit account websocket connect timed out")??;
    let expires = chrono::Utc::now().timestamp_millis() + 10_000;
    let signature = hmac_sign(&credentials.api_secret, &format!("GET/realtime{expires}"));
    socket
        .send(Message::Text(
            json!({
                "op": "auth",
                "args": [&credentials.api_key, expires, signature]
            })
            .to_string()
            .into(),
        ))
        .await?;
    let auth = tokio::time::timeout(Duration::from_secs(5), socket.next())
        .await
        .context("Bybit websocket auth timed out")?
        .ok_or_else(|| anyhow!("Bybit websocket ended during auth"))??;
    let Message::Text(auth) = auth else {
        bail!("Bybit websocket returned an invalid auth response");
    };
    let auth: Value = serde_json::from_str(auth.as_ref())?;
    if !auth["success"].as_bool().unwrap_or(false) {
        bail!("Bybit websocket auth rejected: {}", auth["ret_msg"]);
    }
    socket
        .send(Message::Text(
            json!({
                "op": "subscribe",
                "args": ["wallet", "position", "order", "execution"]
            })
            .to_string()
            .into(),
        ))
        .await?;
    store.set_connected("Bybit", true).await;
    tracing::info!("Bybit account websocket connected");
    let mut heartbeat = tokio::time::interval(Duration::from_secs(20));
    heartbeat.tick().await;
    loop {
        tokio::select! {
            _ = heartbeat.tick() => {
                socket.send(Message::Text(r#"{"op":"ping"}"#.into())).await?;
            }
            message = socket.next() => {
                let message = message.ok_or_else(|| anyhow!("Bybit account stream ended"))??;
                match message {
                    Message::Text(text) => {
                        let value: Value = serde_json::from_str(text.as_ref())?;
                        store.apply_bybit_event(&value).await;
                    }
                    Message::Ping(payload) => socket.send(Message::Pong(payload)).await?,
                    Message::Close(frame) => bail!("Bybit account websocket closed: {frame:?}"),
                    _ => {}
                }
            }
        }
    }
}

fn position_map(positions: Vec<PositionLeg>) -> HashMap<String, PositionLeg> {
    positions
        .into_iter()
        .map(|position| {
            (
                position_key(&position.symbol, &position.side, &position.market),
                position,
            )
        })
        .collect()
}

fn position_key(symbol: &str, side: &str, market: &str) -> String {
    format!("{market}:{symbol}:{side}")
}

fn binance_order_update(value: &Value) -> Option<OrderExecution> {
    let order = &value["o"];
    let executed_quantity = number(&order["z"]).unwrap_or(0.0);
    let average_price = number(&order["ap"]).unwrap_or(0.0);
    Some(OrderExecution {
        exchange: "Binance".into(),
        client_order_id: order["c"].as_str()?.into(),
        order_id: order["i"].to_string(),
        status: order["X"].as_str()?.into(),
        executed_quantity,
        average_price,
    })
}

fn bybit_order_update(value: &Value) -> Option<OrderExecution> {
    let client_order_id = value["orderLinkId"].as_str()?.to_string();
    let status = value["orderStatus"]
        .as_str()
        .or_else(|| value["execType"].as_str())
        .unwrap_or("Unknown");
    Some(OrderExecution {
        exchange: "Bybit".into(),
        client_order_id,
        order_id: value["orderId"].as_str().unwrap_or("").into(),
        status: status.into(),
        executed_quantity: number(&value["cumExecQty"])
            .or_else(|| number(&value["execQty"]))
            .unwrap_or(0.0),
        average_price: number(&value["avgPrice"])
            .or_else(|| number(&value["execPrice"]))
            .unwrap_or(0.0),
    })
}

fn is_terminal_order_status(status: &str) -> bool {
    matches!(
        status,
        "FILLED"
            | "CANCELED"
            | "EXPIRED"
            | "REJECTED"
            | "Filled"
            | "Cancelled"
            | "Rejected"
            | "Deactivated"
    )
}

fn number(value: &Value) -> Option<f64> {
    value
        .as_f64()
        .or_else(|| value.as_i64().map(|value| value as f64))
        .or_else(|| value.as_str().and_then(|value| value.parse().ok()))
}

fn hmac_sign(secret: &str, message: &str) -> String {
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).expect("HMAC key");
    mac.update(message.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn binance_account_update_merges_changed_positions() {
        let store = AccountStore::default();
        store
            .seed_binance(
                AccountBalance::default(),
                vec![PositionLeg {
                    exchange: "Binance".into(),
                    market: "perpetual".into(),
                    symbol: "BTCUSDT".into(),
                    side: "long".into(),
                    quantity: 1.0,
                    entry_price: 100.0,
                    mark_price: 101.0,
                    unrealized_pnl: 1.0,
                    funding_earned: 0.0,
                    funding_rate: 0.0,
                    leverage: 10,
                }],
            )
            .await;
        store.set_connected("Binance", true).await;
        store
            .apply_binance_event(&json!({
                "e": "ACCOUNT_UPDATE",
                "a": {
                    "B": [{"a":"USDT","wb":"999","cw":"900"}],
                    "P": [{"s":"ETHUSDT","pa":"2","ep":"20","up":"1","ps":"LONG"}]
                }
            }))
            .await;
        let (balance, positions) = store.snapshot("Binance").await.unwrap();
        assert_eq!(balance.equity, 999.0);
        assert_eq!(positions.len(), 2);
    }

    #[tokio::test]
    async fn bybit_position_zero_removes_only_changed_leg() {
        let store = AccountStore::default();
        store
            .seed_bybit(
                AccountBalance::default(),
                vec![PositionLeg {
                    exchange: "Bybit".into(),
                    market: "perpetual".into(),
                    symbol: "BTCUSDT".into(),
                    side: "short".into(),
                    quantity: 1.0,
                    entry_price: 100.0,
                    mark_price: 101.0,
                    unrealized_pnl: -1.0,
                    funding_earned: 0.0,
                    funding_rate: 0.0,
                    leverage: 10,
                }],
            )
            .await;
        store.set_connected("Bybit", true).await;
        store
            .apply_bybit_event(&json!({
                "topic":"position",
                "data":[{"category":"linear","symbol":"BTCUSDT","side":"Sell","size":"0"}]
            }))
            .await;
        assert!(store.snapshot("Bybit").await.unwrap().1.is_empty());
    }
}
