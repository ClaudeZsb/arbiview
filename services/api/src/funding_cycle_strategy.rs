use crate::{
    market::MarketService,
    models::{BatchIncreaseRequest, BatchReduceRequest},
    telegram::TelegramNotifier,
    trading::TradingService,
};
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::{path::PathBuf, sync::Arc, time::Duration};
use tokio::sync::RwLock;

const SYMBOL: &str = "DEXE";
const HOUR_MILLIS: i64 = 60 * 60 * 1_000;
const YEAR_HOURS: f64 = 365.0 * 24.0;
const EXIT_APY_THRESHOLD: f64 = 1.0; // 100%
const ENTRY_HOURLY_RETURN_THRESHOLD: f64 = 0.002; // 0.2%
const ENTRY_TARGET_USDT: f64 = 5_000.0;
const ENTRY_ORDER_USDT: f64 = 100.0;
const ENTRY_INTERVAL_SECONDS: f64 = 30.0;
const ENTRY_LEVERAGE: u8 = 5;
const EXIT_INTERVAL_SECONDS: f64 = 3.0;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FundingCycleStrategyStatus {
    pub enabled: bool,
    pub symbol: String,
    pub state: String,
    pub last_exit_check_hour: Option<i64>,
    pub last_entry_check_hour: Option<i64>,
    pub exit_requested_hour: Option<i64>,
    pub cleanup_checked_hour: Option<i64>,
    pub entry_task_id: Option<String>,
    pub exit_task_id: Option<String>,
    pub signal_hourly_return: Option<f64>,
    pub signal_apy: Option<f64>,
    pub signal_direction: Option<String>,
    pub last_action_at: Option<i64>,
    pub last_action: Option<String>,
    pub error: Option<String>,
}

impl FundingCycleStrategyStatus {
    fn new(enabled: bool) -> Self {
        Self {
            enabled,
            symbol: SYMBOL.into(),
            state: if enabled { "waiting" } else { "disabled" }.into(),
            last_exit_check_hour: None,
            last_entry_check_hour: None,
            exit_requested_hour: None,
            cleanup_checked_hour: None,
            entry_task_id: None,
            exit_task_id: None,
            signal_hourly_return: None,
            signal_apy: None,
            signal_direction: None,
            last_action_at: None,
            last_action: None,
            error: None,
        }
    }
}

#[derive(Clone)]
pub struct FundingCycleStrategyService {
    market: MarketService,
    trading: TradingService,
    state: Arc<RwLock<FundingCycleStrategyStatus>>,
    state_path: Option<PathBuf>,
    notifier: Option<TelegramNotifier>,
    enabled: bool,
}

impl FundingCycleStrategyService {
    pub fn new(
        market: MarketService,
        trading: TradingService,
        state_path: Option<PathBuf>,
        enabled: bool,
        notifier: Option<TelegramNotifier>,
    ) -> Result<Self> {
        let mut state = match state_path.as_deref() {
            Some(path) if path.exists() => serde_json::from_slice(&std::fs::read(path)?)
                .context("failed to parse DEXE funding-cycle strategy state")?,
            _ => FundingCycleStrategyStatus::new(enabled),
        };
        state.enabled = enabled;
        if !enabled {
            state.state = "disabled".into();
        }
        Ok(Self {
            market,
            trading,
            state: Arc::new(RwLock::new(state)),
            state_path,
            notifier,
            enabled,
        })
    }

    pub fn spawn(&self) {
        if !self.enabled {
            return;
        }
        let service = self.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(3)).await;
            loop {
                if let Err(error) = service.tick().await {
                    tracing::warn!("DEXE funding-cycle strategy tick failed: {error:#}");
                    service.record_error(format!("{error:#}")).await;
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        });
    }

    pub async fn status(&self) -> FundingCycleStrategyStatus {
        self.state.read().await.clone()
    }

    async fn tick(&self) -> Result<()> {
        let now = chrono::Utc::now().timestamp_millis();
        let hour = now.div_euclid(HOUR_MILLIS);
        let offset_seconds = now.rem_euclid(HOUR_MILLIS) / 1_000;
        self.refresh_task_state().await?;

        // +00:01: assess whether the next boundary still pays the held route.
        if (1..15).contains(&offset_seconds)
            && self.state.read().await.last_exit_check_hour != Some(hour)
        {
            self.check_exit(hour, now).await?;
        }
        // +00:45: ensure a requested exit has no remainder.
        if (45..60).contains(&offset_seconds) {
            let state = self.state.read().await.clone();
            if state.exit_requested_hour == Some(hour) && state.cleanup_checked_hour != Some(hour) {
                self.cleanup_exit(hour, now).await?;
            }
        }
        // +01:00: enter for the next funding boundary if its actual one-hour
        // cashflow covers the configured round-trip fee allowance.
        if (60..75).contains(&offset_seconds)
            && self.state.read().await.last_entry_check_hour != Some(hour)
        {
            self.check_entry(hour, now).await?;
        }
        Ok(())
    }

    async fn check_exit(&self, hour: i64, now: i64) -> Result<()> {
        {
            let mut state = self.state.write().await;
            state.last_exit_check_hour = Some(hour);
            state.error = None;
        }
        self.persist().await?;
        let positions = self.trading.positions().await?;
        let Some(position) = positions
            .into_iter()
            .find(|position| position.token == SYMBOL)
        else {
            self.set_waiting("整点退出检查：当前空仓", now).await?;
            return Ok(());
        };
        let settlement_at = (hour + 1) * HOUR_MILLIS;
        let hourly_return = self
            .market
            .hourly_position_funding_return(&position, settlement_at)
            .await?;
        let apy = hourly_return * YEAR_HOURS;
        self.record_signal(
            hourly_return,
            apy,
            format!(
                "LONG {} / SHORT {}",
                position.long.exchange, position.short.exchange
            ),
        )
        .await?;
        if apy >= EXIT_APY_THRESHOLD {
            self.set_waiting(
                format!("持仓方向 APY {:.2}% ≥ 100%，继续持有", apy * 100.0),
                now,
            )
            .await?;
            return Ok(());
        }
        // A $5,000 entry can span more than one funding hour. If the next hour
        // no longer pays this direction, stop the remaining entry batches before
        // constructing the exit from the authoritative filled position.
        if !self.stop_active_symbol_tasks().await? {
            self.record_error("退出信号已触发，但 DEXE 批量任务未能及时停止".into())
                .await;
            return Ok(());
        }
        let positions = self.trading.refresh_positions_authoritatively().await?;
        let Some(position) = positions
            .into_iter()
            .find(|position| position.token == SYMBOL)
        else {
            self.set_waiting("退出信号触发时仓位已经关闭", now).await?;
            return Ok(());
        };
        let order_notional = (position.notional_usdt / 10.0).max(10.0);
        let task = self
            .trading
            .start_batch_reduce(BatchReduceRequest {
                position_id: position.id,
                target_notional_usdt: position.notional_usdt,
                close_all: true,
                order_notional_usdt: order_notional.min(position.notional_usdt),
                interval_seconds: EXIT_INTERVAL_SECONDS,
                no_loss_guard: false,
                close_spread_threshold: None,
                minimum_roi: None,
            })
            .await?;
        {
            let mut state = self.state.write().await;
            state.state = "exiting".into();
            state.exit_requested_hour = Some(hour);
            state.exit_task_id = Some(task.id.clone());
            state.last_action_at = Some(now);
            state.last_action = Some(format!(
                "APY {:.2}% < 100%，批量市价全平，每单约 ${:.2} / 3s",
                apy * 100.0,
                order_notional
            ));
        }
        self.persist().await?;
        self.notify(format!(
            "🔴 <b>DEXE 周期策略退出</b>\n持仓方向小时收益：{:+.4}%\nAPY：{:+.2}%\n批量市价全平：每单约 ${:.2} / 3s",
            hourly_return * 100.0,
            apy * 100.0,
            order_notional
        ))
        .await;
        Ok(())
    }

    async fn cleanup_exit(&self, hour: i64, now: i64) -> Result<()> {
        let task_id = self.state.read().await.exit_task_id.clone();
        if let Some(task_id) = task_id {
            if self.trading.batch_task(&task_id).await.is_ok_and(|task| {
                matches!(task.status.as_str(), "queued" | "running" | "cancelling")
            }) {
                self.trading.cancel_batch_task(&task_id).await?;
                for _ in 0..20 {
                    tokio::time::sleep(Duration::from_millis(250)).await;
                    if self.trading.batch_task(&task_id).await.is_ok_and(|task| {
                        !matches!(task.status.as_str(), "queued" | "running" | "cancelling")
                    }) {
                        break;
                    }
                }
            }
        }
        let positions = self.trading.refresh_positions_authoritatively().await?;
        let residual = positions
            .into_iter()
            .find(|position| position.token == SYMBOL);
        let action = if let Some(position) = residual {
            self.trading.close(&position.id).await?;
            format!(
                "整点 +45s 检测到残仓 ${:.2}，已市价全平",
                position.notional_usdt
            )
        } else {
            "整点 +45s 复核：已完全平仓".into()
        };
        {
            let mut state = self.state.write().await;
            state.cleanup_checked_hour = Some(hour);
            state.exit_task_id = None;
            state.state = "waiting".into();
            state.last_action_at = Some(now);
            state.last_action = Some(action);
        }
        self.persist().await
    }

    async fn check_entry(&self, hour: i64, now: i64) -> Result<()> {
        {
            let mut state = self.state.write().await;
            state.last_entry_check_hour = Some(hour);
            state.error = None;
        }
        self.persist().await?;
        if self
            .trading
            .positions()
            .await?
            .iter()
            .any(|position| position.token == SYMBOL)
        {
            self.set_waiting("整点 +1m 入场检查：已有 DEXE 仓位", now)
                .await?;
            return Ok(());
        }
        if self.has_active_batch_task().await {
            self.set_waiting("整点 +1m 入场检查：已有批量任务运行", now)
                .await?;
            return Ok(());
        }
        let settlement_at = (hour + 1) * HOUR_MILLIS;
        let Some(opportunity) = self
            .market
            .hourly_cross_funding_opportunity(SYMBOL, settlement_at)
            .await?
        else {
            self.set_waiting("下一整点两腿均无可套利结算费率", now)
                .await?;
            return Ok(());
        };
        self.record_signal(
            opportunity.funding_per_hour,
            opportunity.apy,
            format!(
                "LONG {} / SHORT {}",
                opportunity.long.exchange, opportunity.short.exchange
            ),
        )
        .await?;
        if opportunity.funding_per_hour <= ENTRY_HOURLY_RETURN_THRESHOLD {
            self.set_waiting(
                format!(
                    "小时收益 {:.4}% ≤ 0.2%，不入场",
                    opportunity.funding_per_hour * 100.0
                ),
                now,
            )
            .await?;
            return Ok(());
        }
        let task = self
            .trading
            .start_batch_increase_for_opportunity(
                BatchIncreaseRequest {
                    opportunity_id: opportunity.id.clone(),
                    target_notional_usdt: ENTRY_TARGET_USDT,
                    order_notional_usdt: ENTRY_ORDER_USDT,
                    interval_seconds: ENTRY_INTERVAL_SECONDS,
                    leverage: ENTRY_LEVERAGE,
                    spread_guard: false,
                    spread_threshold: None,
                },
                opportunity.clone(),
            )
            .await?;
        {
            let mut state = self.state.write().await;
            state.state = "entering".into();
            state.entry_task_id = Some(task.id.clone());
            state.last_action_at = Some(now);
            state.last_action = Some(format!(
                "小时收益 {:.4}% > 0.2%，批量市价入场 $5000",
                opportunity.funding_per_hour * 100.0
            ));
        }
        self.persist().await?;
        self.notify(format!(
            "🟢 <b>DEXE 周期策略入场</b>\nLONG {} / SHORT {}\n下一整点小时收益：{:+.4}%\n目标：每腿 $5000 · 5×\n批量市价：$100 / 30s",
            opportunity.long.exchange,
            opportunity.short.exchange,
            opportunity.funding_per_hour * 100.0
        ))
        .await;
        Ok(())
    }

    async fn refresh_task_state(&self) -> Result<()> {
        let snapshot = self.state.read().await.clone();
        if let Some(task_id) = snapshot.entry_task_id {
            if let Ok(task) = self.trading.batch_task(&task_id).await {
                if matches!(task.status.as_str(), "completed" | "failed" | "cancelled") {
                    let completed = task.status == "completed";
                    let completed_notional = task.completed_notional_usdt;
                    let task_error = task.error.clone();
                    let mut state = self.state.write().await;
                    state.entry_task_id = None;
                    state.state = if completed {
                        "holding".into()
                    } else {
                        "waiting".into()
                    };
                    state.error = task_error.clone();
                    drop(state);
                    self.persist().await?;
                    self.notify(if completed {
                        format!(
                            "✅ <b>DEXE 周期策略入场完成</b>\n每腿累计成交：约 ${completed_notional:.2}\n状态：持仓中"
                        )
                    } else {
                        format!(
                            "⚠️ <b>DEXE 周期策略入场{}</b>\n{}",
                            if task.status == "failed" { "失败" } else { "已取消" },
                            task_error.unwrap_or_else(|| "未提供错误信息".into())
                        )
                    })
                    .await;
                }
            }
        }
        let exit_task_id = self.state.read().await.exit_task_id.clone();
        if let Some(task_id) = exit_task_id {
            if let Ok(task) = self.trading.batch_task(&task_id).await {
                if matches!(task.status.as_str(), "completed" | "failed" | "cancelled") {
                    let completed = task.status == "completed";
                    let completed_notional = task.completed_notional_usdt;
                    let task_error = task.error.clone();
                    {
                        let mut state = self.state.write().await;
                        state.exit_task_id = None;
                        state.state = if completed {
                            "exit_verifying".into()
                        } else {
                            "waiting_cleanup".into()
                        };
                        state.error = task_error.clone();
                    }
                    self.persist().await?;
                    self.notify(if completed {
                        format!(
                            "✅ <b>DEXE 周期策略批量退场完成</b>\n每腿累计平仓：约 ${completed_notional:.2}\n将在整点 +45s 再次检查残仓"
                        )
                    } else {
                        format!(
                            "⚠️ <b>DEXE 周期策略批量退场{}</b>\n{}\n将在整点 +45s 尝试清理残仓",
                            if task.status == "failed" { "失败" } else { "已取消" },
                            task_error.unwrap_or_else(|| "未提供错误信息".into())
                        )
                    })
                    .await;
                }
            }
        }
        Ok(())
    }

    async fn has_active_batch_task(&self) -> bool {
        self.trading
            .batch_tasks()
            .await
            .iter()
            .any(|task| matches!(task.status.as_str(), "queued" | "running" | "cancelling"))
    }

    async fn stop_active_symbol_tasks(&self) -> Result<bool> {
        let active = self
            .trading
            .batch_tasks()
            .await
            .into_iter()
            .filter(|task| {
                task.token == SYMBOL
                    && matches!(task.status.as_str(), "queued" | "running" | "cancelling")
            })
            .collect::<Vec<_>>();
        for task in &active {
            if task.status != "cancelling" {
                self.trading.cancel_batch_task(&task.id).await?;
            }
        }
        for _ in 0..40 {
            let still_active = self.trading.batch_tasks().await.iter().any(|task| {
                task.token == SYMBOL
                    && matches!(task.status.as_str(), "queued" | "running" | "cancelling")
            });
            if !still_active {
                return Ok(true);
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        Ok(active.is_empty())
    }

    async fn record_signal(&self, hourly_return: f64, apy: f64, direction: String) -> Result<()> {
        {
            let mut state = self.state.write().await;
            state.signal_hourly_return = Some(hourly_return);
            state.signal_apy = Some(apy);
            state.signal_direction = Some(direction);
        }
        self.persist().await
    }

    async fn set_waiting(&self, action: impl Into<String>, now: i64) -> Result<()> {
        {
            let mut state = self.state.write().await;
            state.state = "waiting".into();
            state.last_action_at = Some(now);
            state.last_action = Some(action.into());
        }
        self.persist().await
    }

    async fn record_error(&self, error: String) {
        self.state.write().await.error = Some(error);
        let _ = self.persist().await;
    }

    async fn notify(&self, message: String) {
        if let Some(notifier) = &self.notifier {
            if let Err(error) = notifier.send_html(&message).await {
                tracing::warn!("DEXE funding-cycle Telegram notification failed: {error:#}");
            }
        }
    }

    async fn persist(&self) -> Result<()> {
        let Some(path) = self.state_path.as_ref() else {
            return Ok(());
        };
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let temporary = path.with_extension("tmp");
        std::fs::write(
            &temporary,
            serde_json::to_vec_pretty(&*self.state.read().await)?,
        )?;
        std::fs::rename(temporary, path)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{ENTRY_HOURLY_RETURN_THRESHOLD, EXIT_APY_THRESHOLD, YEAR_HOURS};

    #[test]
    fn configured_thresholds_match_the_strategy_specification() {
        assert_eq!(ENTRY_HOURLY_RETURN_THRESHOLD, 0.002);
        assert_eq!(EXIT_APY_THRESHOLD, 1.0);
        assert!((EXIT_APY_THRESHOLD / YEAR_HOURS - 0.000114155).abs() < 1e-9);
    }
}
