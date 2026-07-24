use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Token {
    pub symbol: String,
    pub name: String,
    pub rank: u32,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Leg {
    pub exchange: String,
    pub base: String,
    pub symbol: String,
    pub bid: f64,
    pub ask: f64,
    pub mark: f64,
    pub rate: f64,
    pub interval_hours: f64,
    pub next_funding_time: i64,
    pub qty_step: f64,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Opportunity {
    pub id: String,
    pub token: Token,
    pub long: Leg,
    pub short: Leg,
    pub funding_per_hour: f64,
    pub apy: f64,
    pub spread: f64,
    pub fees: f64,
    pub break_even_hours: f64,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OpportunitiesResponse {
    pub opportunities: Vec<Opportunity>,
    pub updated_at: i64,
    pub universe_size: usize,
    pub matched_pairs: usize,
    pub assumptions: FeeAssumptions,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FeeAssumptions {
    pub binance_taker_fee: f64,
    pub bybit_taker_fee: f64,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PositionLeg {
    pub exchange: String,
    pub symbol: String,
    pub side: String,
    pub quantity: f64,
    pub entry_price: f64,
    pub mark_price: f64,
    pub unrealized_pnl: f64,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Position {
    pub id: String,
    pub token: String,
    pub status: String,
    pub opened_at: i64,
    pub notional_usdt: f64,
    pub leverage: u8,
    pub long: PositionLeg,
    pub short: PositionLeg,
    pub funding_earned: f64,
    pub unrealized_pnl: f64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OpenTradeRequest {
    pub opportunity_id: String,
    pub notional_usdt: f64,
    #[serde(default = "default_leverage")]
    pub leverage: u8,
}

fn default_leverage() -> u8 {
    1
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TradeResponse {
    pub position: Position,
    pub mode: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub execution: Option<ExecutionReport>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OrderExecution {
    pub exchange: String,
    pub client_order_id: String,
    pub order_id: String,
    pub status: String,
    pub executed_quantity: f64,
    pub average_price: f64,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecutionReport {
    pub outcome: String,
    pub naked_exposure: bool,
    pub max_slippage_bps: u32,
    pub target_notional_usdt: f64,
    pub long_notional_usdt: f64,
    pub short_notional_usdt: f64,
    pub tolerance_usdt: f64,
    pub balanced: bool,
    pub orders: Vec<OrderExecution>,
    pub supplement_orders: Vec<OrderExecution>,
    pub rebalance_orders: Vec<OrderExecution>,
    pub compensation_orders: Vec<OrderExecution>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AccountSummary {
    pub mode: String,
    pub configured_exchanges: Vec<String>,
    pub exchanges: Vec<ExchangeBalance>,
    pub equity_usdt: f64,
    pub available_usdt: f64,
    pub unrealized_pnl: f64,
    pub active_positions: usize,
    pub unhedged_legs: Vec<PositionLeg>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExchangeBalance {
    pub exchange: String,
    pub equity_usdt: f64,
    pub available_usdt: f64,
    pub unrealized_pnl: f64,
}

#[derive(Debug, Deserialize)]
pub struct CmcResponse {
    pub data: Vec<CmcToken>,
}

#[derive(Debug, Deserialize)]
pub struct CmcToken {
    pub symbol: String,
    pub name: String,
    pub cmc_rank: u32,
}
