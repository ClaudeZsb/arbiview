use crate::{
    market::MarketService,
    models::{BatchIncreaseRequest, BatchReduceRequest},
    trading::TradingService,
};
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::{path::PathBuf, sync::Arc, time::Duration};
use tokio::sync::RwLock;

const ENTRY_DEVIATION: f64 = 0.01;
const ENTRY_NOTIONAL_USDT: f64 = 100.0;
const ENTRY_LEVERAGE: u8 = 5;
const ROI_STEP_PER_HOUR: f64 = 0.002;
const ROI_FLOOR_AFTER_HOURS: i64 = 5;
const STOP_LOSS_ROI: f64 = -0.02;
const MAX_HOLD_MILLIS: i64 = 10 * 60 * 60 * 1_000;
const REENTRY_COOLDOWN_MILLIS: i64 = 15 * 60 * 1_000;

fn take_profit_target(entry_deviation: f64, held_hours: i64) -> f64 {
    if held_hours >= ROI_FLOOR_AFTER_HOURS {
        0.0
    } else {
        (entry_deviation - held_hours.max(0) as f64 * ROI_STEP_PER_HOUR).max(0.0)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SpreadStrategyTrade {
    pub token: String,
    pub opportunity_id: String,
    pub status: String,
    pub signal_at: i64,
    pub opened_at: Option<i64>,
    pub closed_at: Option<i64>,
    pub entry_spread: f64,
    pub entry_deviation: f64,
    pub position_id: Option<String>,
    pub entry_task_id: Option<String>,
    pub exit_task_id: Option<String>,
    pub target_roi: Option<f64>,
    pub target_hour: Option<i64>,
    pub current_roi: Option<f64>,
    pub exit_reason: Option<String>,
    pub error: Option<String>,
}

#[derive(Clone)]
pub struct SpreadStrategyService {
    market: MarketService,
    trading: TradingService,
    state: Arc<RwLock<Option<SpreadStrategyTrade>>>,
    state_path: Option<PathBuf>,
    enabled: bool,
}

impl SpreadStrategyService {
    pub fn new(
        market: MarketService,
        trading: TradingService,
        state_path: Option<PathBuf>,
        enabled: bool,
    ) -> Result<Self> {
        let state = match state_path.as_deref() {
            Some(path) if path.exists() => Some(
                serde_json::from_slice(&std::fs::read(path)?)
                    .context("failed to parse spread strategy state")?,
            ),
            _ => None,
        };
        Ok(Self {
            market,
            trading,
            state: Arc::new(RwLock::new(state)),
            state_path,
            enabled,
        })
    }

    pub fn spawn(&self) {
        if !self.enabled {
            return;
        }
        let service = self.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(10)).await;
            loop {
                if let Err(error) = service.tick().await {
                    tracing::warn!("spread reversion strategy tick failed: {error:#}");
                }
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        });
    }

    pub async fn status(&self) -> Option<SpreadStrategyTrade> {
        self.state.read().await.clone()
    }

    async fn tick(&self) -> Result<()> {
        let current = self.state.read().await.clone();
        match current {
            None => self.scan_entry().await,
            Some(trade) if matches!(trade.status.as_str(), "closed" | "failed") => {
                let finished_at = trade.closed_at.unwrap_or(trade.signal_at);
                if chrono::Utc::now().timestamp_millis() - finished_at >= REENTRY_COOLDOWN_MILLIS {
                    self.replace_state(None).await?;
                }
                Ok(())
            }
            Some(trade) if trade.status == "entering" => self.monitor_entry(trade).await,
            Some(trade) if trade.status == "active" => self.monitor_active(trade).await,
            Some(trade) if trade.status == "market_exit" => self.monitor_market_exit(trade).await,
            Some(_) => Ok(()),
        }
    }

    async fn scan_entry(&self) -> Result<()> {
        let positions = self.trading.positions().await?;
        let snapshot = self.market.opportunities().await?;
        let Some(opportunity) = snapshot
            .spread_opportunities
            .into_iter()
            .find(|opportunity| {
                opportunity.execution_supported
                    && opportunity.spread_vs_average >= ENTRY_DEVIATION
                    && !positions
                        .iter()
                        .any(|position| position.token == opportunity.token.symbol)
            })
        else {
            return Ok(());
        };
        let now = chrono::Utc::now().timestamp_millis();
        let mut trade = SpreadStrategyTrade {
            token: opportunity.token.symbol.clone(),
            opportunity_id: opportunity.id.clone(),
            status: "entering".into(),
            signal_at: now,
            opened_at: None,
            closed_at: None,
            entry_spread: opportunity.spread,
            entry_deviation: opportunity.spread_vs_average,
            position_id: None,
            entry_task_id: None,
            exit_task_id: None,
            target_roi: None,
            target_hour: None,
            current_roi: None,
            exit_reason: None,
            error: None,
        };
        self.replace_state(Some(trade.clone())).await?;
        match self
            .trading
            .start_batch_increase(BatchIncreaseRequest {
                opportunity_id: opportunity.id,
                target_notional_usdt: ENTRY_NOTIONAL_USDT,
                order_notional_usdt: 0.0,
                interval_seconds: 0.0,
                leverage: ENTRY_LEVERAGE,
                spread_guard: true,
                spread_threshold: Some(opportunity.spread),
            })
            .await
        {
            Ok(task) => {
                trade.entry_task_id = Some(task.id);
                self.replace_state(Some(trade)).await
            }
            Err(error) => {
                self.fail(trade, format!("entry task failed: {error:#}"))
                    .await
            }
        }
    }

    async fn monitor_entry(&self, mut trade: SpreadStrategyTrade) -> Result<()> {
        let Some(task_id) = trade.entry_task_id.as_deref() else {
            return self.fail(trade, "entry task disappeared".into()).await;
        };
        let task = match self.trading.batch_task(task_id).await {
            Ok(task) => task,
            Err(_) => {
                let positions = self.trading.positions().await?;
                if let Some(position) = positions.into_iter().find(|p| p.token == trade.token) {
                    trade.status = "active".into();
                    trade.opened_at = Some(trade.signal_at);
                    trade.position_id = Some(position.id);
                    trade.entry_task_id = None;
                    return self.replace_state(Some(trade)).await;
                }
                return self.fail(trade, "entry task disappeared".into()).await;
            }
        };
        if matches!(task.status.as_str(), "failed" | "cancelled") {
            return self
                .fail(
                    trade,
                    task.error
                        .unwrap_or_else(|| format!("entry task {}", task.status)),
                )
                .await;
        }
        if task.status != "completed" {
            return Ok(());
        }
        let positions = self.trading.positions().await?;
        if let Some(position) = positions.into_iter().find(|p| p.token == trade.token) {
            trade.status = "active".into();
            trade.opened_at = Some(chrono::Utc::now().timestamp_millis());
            trade.position_id = Some(position.id);
            trade.entry_task_id = None;
            return self.replace_state(Some(trade)).await;
        }
        self.fail(trade, "entry completed but position is unavailable".into())
            .await
    }

    async fn monitor_active(&self, mut trade: SpreadStrategyTrade) -> Result<()> {
        let positions = self.trading.positions().await?;
        let Some(position) = positions.into_iter().find(|p| {
            trade.position_id.as_deref() == Some(p.id.as_str()) || p.token == trade.token
        }) else {
            trade.status = "closed".into();
            trade.closed_at = Some(chrono::Utc::now().timestamp_millis());
            return self.replace_state(Some(trade)).await;
        };
        trade.position_id = Some(position.id.clone());
        trade.current_roi = Some(position.current_roi);
        let now = chrono::Utc::now().timestamp_millis();
        let opened_at = trade.opened_at.unwrap_or(trade.signal_at);
        if position.current_roi <= STOP_LOSS_ROI {
            return self.begin_market_exit(trade, "stop_loss").await;
        }
        if now - opened_at >= MAX_HOLD_MILLIS {
            return self.begin_market_exit(trade, "max_hold_time").await;
        }
        let held_hours = ((now - opened_at).max(0) / (60 * 60 * 1_000)).min(ROI_FLOOR_AFTER_HOURS);
        let target_roi = take_profit_target(trade.entry_deviation, held_hours);
        if let Some(task_id) = trade.exit_task_id.clone() {
            match self.trading.batch_task(&task_id).await {
                Ok(task) if task.status == "completed" => {
                    trade.status = "closed".into();
                    trade.closed_at = Some(now);
                    trade.exit_reason = Some("take_profit".into());
                    return self.replace_state(Some(trade)).await;
                }
                Ok(task) if matches!(task.status.as_str(), "queued" | "running" | "cancelling") => {
                    if trade.target_hour != Some(held_hours) && task.status != "cancelling" {
                        self.trading.cancel_batch_task(&task_id).await?;
                        return self.replace_state(Some(trade)).await;
                    }
                    *self.state.write().await = Some(trade);
                    return Ok(());
                }
                _ => {
                    trade.exit_task_id = None;
                }
            }
        }
        let task = self
            .trading
            .start_batch_reduce(BatchReduceRequest {
                position_id: position.id,
                target_notional_usdt: position.notional_usdt,
                close_all: true,
                order_notional_usdt: 0.0,
                interval_seconds: 0.0,
                no_loss_guard: true,
                close_spread_threshold: None,
                minimum_roi: Some(target_roi),
            })
            .await?;
        trade.exit_task_id = Some(task.id);
        trade.target_roi = Some(target_roi);
        trade.target_hour = Some(held_hours);
        self.replace_state(Some(trade)).await
    }

    async fn begin_market_exit(&self, mut trade: SpreadStrategyTrade, reason: &str) -> Result<()> {
        if let Some(task_id) = trade.exit_task_id.as_deref() {
            let _ = self.trading.cancel_batch_task(task_id).await;
        }
        trade.status = "market_exit".into();
        trade.exit_reason = Some(reason.into());
        self.replace_state(Some(trade)).await
    }

    async fn monitor_market_exit(&self, mut trade: SpreadStrategyTrade) -> Result<()> {
        if let Some(task_id) = trade.exit_task_id.clone() {
            if self.trading.batch_task(&task_id).await.is_ok_and(|task| {
                matches!(task.status.as_str(), "queued" | "running" | "cancelling")
            }) {
                return Ok(());
            }
            trade.exit_task_id = None;
        }
        let positions = self.trading.positions().await?;
        let Some(position) = positions.into_iter().find(|p| p.token == trade.token) else {
            trade.status = "closed".into();
            trade.closed_at = Some(chrono::Utc::now().timestamp_millis());
            return self.replace_state(Some(trade)).await;
        };
        match self.trading.close(&position.id).await {
            Ok(_) => {
                trade.status = "closed".into();
                trade.closed_at = Some(chrono::Utc::now().timestamp_millis());
                self.replace_state(Some(trade)).await
            }
            Err(error) => {
                self.fail(trade, format!("market exit failed: {error:#}"))
                    .await
            }
        }
    }

    async fn fail(&self, mut trade: SpreadStrategyTrade, error: String) -> Result<()> {
        trade.status = "failed".into();
        trade.error = Some(error);
        trade.closed_at = Some(chrono::Utc::now().timestamp_millis());
        self.replace_state(Some(trade)).await
    }

    async fn replace_state(&self, value: Option<SpreadStrategyTrade>) -> Result<()> {
        *self.state.write().await = value.clone();
        let Some(path) = self.state_path.as_ref() else {
            return Ok(());
        };
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        match value {
            Some(value) => {
                let temporary = path.with_extension("tmp");
                std::fs::write(&temporary, serde_json::to_vec_pretty(&value)?)?;
                std::fs::rename(temporary, path)?;
            }
            None if path.exists() => std::fs::remove_file(path)?,
            None => {}
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::take_profit_target;

    #[test]
    fn take_profit_roi_drops_each_hour_and_reaches_zero_at_five_hours() {
        assert!((take_profit_target(0.012, 0) - 0.012).abs() < 1e-12);
        assert!((take_profit_target(0.012, 1) - 0.010).abs() < 1e-12);
        assert!((take_profit_target(0.012, 4) - 0.004).abs() < 1e-12);
        assert_eq!(take_profit_target(0.012, 5), 0.0);
        assert_eq!(take_profit_target(0.012, 8), 0.0);
    }
}
