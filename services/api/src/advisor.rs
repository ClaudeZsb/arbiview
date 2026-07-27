use crate::{
    market::MarketService,
    models::{Opportunity, Position},
    trading::TradingService,
};
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::{path::Path, sync::Arc};
use tokio::sync::RwLock;

const ENTRY_WINDOW_MILLIS: i64 = 5 * 60 * 1_000;
const HOUR_MILLIS: i64 = 60 * 60 * 1_000;
const SETTLEMENT_TIME_TOLERANCE_MILLIS: i64 = 60 * 1_000;
const TAKE_PROFIT_RATIO: f64 = 0.01;
const BINANCE_TAKER_FEE: f64 = 0.0005;
const BYBIT_TAKER_FEE: f64 = 0.00055;

#[derive(Clone)]
pub struct AdvisorService {
    market: MarketService,
    trading: TradingService,
    latest_actionable: Arc<RwLock<Option<AdvisorResponse>>>,
    state_path: Option<std::path::PathBuf>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AdvisorResponse {
    pub action: String,
    pub generated_at: i64,
    pub next_settlement_at: i64,
    pub entry_window_open: bool,
    pub reason: String,
    pub entry: Option<EntryRecommendation>,
    pub positions: Vec<PositionRecommendation>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EntryRecommendation {
    pub opportunity_id: String,
    pub token: String,
    pub long_exchange: String,
    pub short_exchange: String,
    pub apy_percent: f64,
    pub break_even_hours: f64,
    pub spread_percent: f64,
    pub average_spread_24h_percent: f64,
    pub spread_vs_average_percent: f64,
    pub long_next_funding_time: i64,
    pub short_next_funding_time: i64,
    pub apy_rank: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PositionRecommendation {
    pub position_id: String,
    pub token: String,
    pub action: String,
    pub funding_received_usdt: f64,
    pub unrealized_pnl_usdt: f64,
    pub estimated_fees_usdt: f64,
    pub net_profit_usdt: f64,
    pub combined_notional_usdt: f64,
    pub take_profit_threshold_usdt: f64,
    pub take_profit_allowed: bool,
}

impl AdvisorService {
    pub fn new(
        market: MarketService,
        trading: TradingService,
        state_path: Option<std::path::PathBuf>,
    ) -> Result<Self> {
        let latest_actionable = load_latest_actionable(state_path.as_deref())?;
        Ok(Self {
            market,
            trading,
            latest_actionable: Arc::new(RwLock::new(latest_actionable)),
            state_path,
        })
    }

    pub async fn recommendation(&self) -> Result<AdvisorResponse> {
        let now = chrono::Utc::now().timestamp_millis();
        let (snapshot, positions) =
            tokio::try_join!(self.market.opportunities(), self.trading.positions())?;
        let recommendation = evaluate(now, &snapshot.opportunities, &positions);
        if recommendation.action != "hold" {
            *self.latest_actionable.write().await = Some(recommendation.clone());
            persist_latest_actionable(self.state_path.as_deref(), &recommendation)?;
        }
        Ok(recommendation)
    }

    pub async fn latest_actionable(&self) -> Result<Option<AdvisorResponse>> {
        self.recommendation().await?;
        Ok(self.latest_actionable.read().await.clone())
    }
}

fn load_latest_actionable(path: Option<&Path>) -> Result<Option<AdvisorResponse>> {
    let Some(path) = path else {
        return Ok(None);
    };
    match std::fs::read(path) {
        Ok(data) => serde_json::from_slice(&data)
            .map(Some)
            .context("failed to parse latest advisor recommendation"),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error).context("failed to read latest advisor recommendation"),
    }
}

fn persist_latest_actionable(path: Option<&Path>, recommendation: &AdvisorResponse) -> Result<()> {
    let Some(path) = path else {
        return Ok(());
    };
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).context("failed to create advisor state directory")?;
    }
    let data = serde_json::to_vec_pretty(recommendation)?;
    let temporary = path.with_extension("tmp");
    std::fs::write(&temporary, data).context("failed to write advisor state")?;
    std::fs::rename(&temporary, path).context("failed to replace advisor state")?;
    Ok(())
}

fn evaluate(now: i64, opportunities: &[Opportunity], positions: &[Position]) -> AdvisorResponse {
    let next_settlement_at = (now.div_euclid(HOUR_MILLIS) + 1) * HOUR_MILLIS;
    let entry_window_open = now >= next_settlement_at - ENTRY_WINDOW_MILLIS;
    if !positions.is_empty() {
        let position_recommendations = positions.iter().map(evaluate_position).collect::<Vec<_>>();
        let allowed = position_recommendations
            .iter()
            .filter(|position| position.take_profit_allowed)
            .count();
        return AdvisorResponse {
            action: if allowed > 0 {
                "take_profit_allowed".into()
            } else {
                "hold".into()
            },
            generated_at: now,
            next_settlement_at,
            entry_window_open,
            reason: if allowed > 0 {
                format!("{allowed} 个持仓的净收益已超过双腿名义价值之和的 1%")
            } else {
                "已有持仓，继续监控资金费、未实现盈亏与退出手续费".into()
            },
            entry: None,
            positions: position_recommendations,
        };
    }
    if !entry_window_open {
        return AdvisorResponse {
            action: "hold".into(),
            generated_at: now,
            next_settlement_at,
            entry_window_open,
            reason: "当前不在整点前 5 分钟的入场推荐窗口".into(),
            entry: None,
            positions: vec![],
        };
    }

    let mut ranked = opportunities
        .iter()
        .filter(|opportunity| settles_at(opportunity.long.next_funding_time, next_settlement_at))
        .filter(|opportunity| settles_at(opportunity.short.next_funding_time, next_settlement_at))
        .collect::<Vec<_>>();
    ranked.sort_by(|left, right| right.apy.total_cmp(&left.apy));
    ranked.truncate(10);
    let selected = ranked.iter().enumerate().min_by(|(_, left), (_, right)| {
        left.break_even_hours
            .total_cmp(&right.break_even_hours)
            .then_with(|| right.apy.total_cmp(&left.apy))
    });
    let Some((rank, opportunity)) = selected else {
        return AdvisorResponse {
            action: "hold".into(),
            generated_at: now,
            next_settlement_at,
            entry_window_open,
            reason: "没有找到两条腿都在下一整点结算的套利机会".into(),
            entry: None,
            positions: vec![],
        };
    };
    AdvisorResponse {
        action: "enter".into(),
        generated_at: now,
        next_settlement_at,
        entry_window_open,
        reason: "在下一整点结算的 APY 前 10 名中选择回本最快者；回本相同时选择 APY 更高者".into(),
        entry: Some(EntryRecommendation {
            opportunity_id: opportunity.id.clone(),
            token: opportunity.token.symbol.clone(),
            long_exchange: opportunity.long.exchange.clone(),
            short_exchange: opportunity.short.exchange.clone(),
            apy_percent: opportunity.apy * 100.0,
            break_even_hours: opportunity.break_even_hours,
            spread_percent: opportunity.spread * 100.0,
            average_spread_24h_percent: opportunity.average_spread_24h * 100.0,
            spread_vs_average_percent: opportunity.spread_vs_average * 100.0,
            long_next_funding_time: opportunity.long.next_funding_time,
            short_next_funding_time: opportunity.short.next_funding_time,
            apy_rank: rank + 1,
        }),
        positions: vec![],
    }
}

fn settles_at(actual: i64, expected: i64) -> bool {
    (actual - expected).abs() <= SETTLEMENT_TIME_TOLERANCE_MILLIS
}

fn evaluate_position(position: &Position) -> PositionRecommendation {
    let long_notional = position.long.quantity * position.long.mark_price;
    let short_notional = position.short.quantity * position.short.mark_price;
    let combined_notional = long_notional + short_notional;
    let fee_rate = |exchange: &str| {
        if exchange.eq_ignore_ascii_case("Binance") {
            BINANCE_TAKER_FEE
        } else {
            BYBIT_TAKER_FEE
        }
    };
    let entry_fees = position.long.quantity
        * position.long.entry_price
        * fee_rate(&position.long.exchange)
        + position.short.quantity * position.short.entry_price * fee_rate(&position.short.exchange);
    let exit_fees = long_notional * fee_rate(&position.long.exchange)
        + short_notional * fee_rate(&position.short.exchange);
    let estimated_fees = entry_fees + exit_fees;
    let funding_received = position.funding_earned;
    let net_profit = funding_received + position.unrealized_pnl - estimated_fees;
    let threshold = combined_notional * TAKE_PROFIT_RATIO;
    let take_profit_allowed = combined_notional > 0.0 && net_profit > threshold;
    PositionRecommendation {
        position_id: position.id.clone(),
        token: position.token.clone(),
        action: if take_profit_allowed {
            "take_profit_allowed".into()
        } else {
            "hold".into()
        },
        funding_received_usdt: funding_received,
        unrealized_pnl_usdt: position.unrealized_pnl,
        estimated_fees_usdt: estimated_fees,
        net_profit_usdt: net_profit,
        combined_notional_usdt: combined_notional,
        take_profit_threshold_usdt: threshold,
        take_profit_allowed,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{Leg, Token};

    fn opportunity(symbol: &str, apy: f64, break_even: f64, settlement: i64) -> Opportunity {
        let leg = |exchange: &str| Leg {
            exchange: exchange.into(),
            base: symbol.into(),
            symbol: format!("{symbol}USDT"),
            bid: 10.0,
            ask: 10.0,
            mark: 10.0,
            rate: 0.001,
            interval_hours: 1.0,
            next_funding_time: settlement,
            qty_step: 0.1,
            tags: vec![],
            volume_24h_usdt: 1_000_000.0,
        };
        Opportunity {
            id: symbol.into(),
            token: Token {
                symbol: symbol.into(),
                name: symbol.into(),
                rank: None,
                tags: vec![],
            },
            long: leg("Binance"),
            short: leg("Bybit"),
            funding_per_hour: apy / (365.0 * 24.0),
            apy,
            spread: 0.001,
            average_spread_24h: 0.0,
            spread_vs_average: 0.001,
            fees: 0.002,
            break_even_hours: break_even,
        }
    }

    #[test]
    fn only_recommends_entry_during_last_five_minutes() {
        let settlement = HOUR_MILLIS;
        let opportunities = vec![opportunity("AAA", 3.0, 2.0, settlement)];
        assert_eq!(
            evaluate(settlement - ENTRY_WINDOW_MILLIS - 1, &opportunities, &[]).action,
            "hold"
        );
        assert_eq!(
            evaluate(settlement - ENTRY_WINDOW_MILLIS, &opportunities, &[]).action,
            "enter"
        );
    }

    #[test]
    fn selects_fastest_break_even_from_apy_top_ten() {
        let settlement = HOUR_MILLIS;
        let mut opportunities = (0..11)
            .map(|index| {
                opportunity(
                    &format!("T{index}"),
                    20.0 - index as f64,
                    20.0 - index as f64,
                    settlement,
                )
            })
            .collect::<Vec<_>>();
        opportunities[4].break_even_hours = 0.5;
        opportunities[10].break_even_hours = 0.1;
        let result = evaluate(settlement - 1, &opportunities, &[]);
        assert_eq!(result.entry.unwrap().token, "T4");
    }

    #[test]
    fn position_requires_net_profit_above_one_percent() {
        let leg = |exchange: &str, side: &str| crate::models::PositionLeg {
            exchange: exchange.into(),
            symbol: "AAAUSDT".into(),
            side: side.into(),
            quantity: 100.0,
            entry_price: 10.0,
            mark_price: 10.0,
            unrealized_pnl: 0.0,
            funding_earned: 0.0,
            funding_rate: 0.0,
            leverage: 1,
        };
        let mut position = Position {
            id: "position".into(),
            token: "AAA".into(),
            status: "open".into(),
            opened_at: 0,
            notional_usdt: 1_000.0,
            leverage: 1,
            long: leg("Binance", "long"),
            short: leg("Bybit", "short"),
            funding_earned: 30.0,
            unrealized_pnl: 0.0,
        };
        assert!(evaluate_position(&position).take_profit_allowed);
        position.funding_earned = 20.0;
        assert!(!evaluate_position(&position).take_profit_allowed);
    }
}
