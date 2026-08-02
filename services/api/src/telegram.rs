use crate::{
    config::TelegramConfig,
    models::{
        AdjustPositionRequest, BatchIncreaseRequest, BatchIncreaseTask, BatchReduceRequest,
        OpenTradeRequest, Opportunity, Position,
    },
    AppState,
};
use anyhow::{anyhow, Context, Result};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{sync::Arc, time::Duration};

const API_TIMEOUT_SECONDS: u64 = 25;
const DEFAULT_NOTIONAL: f64 = 100.0;

#[derive(Debug, Deserialize)]
struct ApiResponse<T> {
    ok: bool,
    result: Option<T>,
    description: Option<String>,
}

#[derive(Debug, Deserialize)]
struct Update {
    update_id: i64,
    message: Option<Message>,
    callback_query: Option<CallbackQuery>,
}

#[derive(Debug, Deserialize)]
struct Message {
    chat: Chat,
    from: Option<User>,
    text: Option<String>,
    message_thread_id: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct Chat {
    id: i64,
}

#[derive(Debug, Deserialize)]
struct User {
    id: i64,
}

#[derive(Debug, Deserialize)]
struct CallbackQuery {
    id: String,
    from: User,
    message: Option<CallbackMessage>,
    data: Option<String>,
}

#[derive(Debug, Deserialize)]
struct CallbackMessage {
    chat: Chat,
    message_id: i64,
    message_thread_id: Option<i64>,
}

#[derive(Debug, Clone, Serialize)]
struct Button {
    text: String,
    callback_data: String,
}

type Keyboard = Vec<Vec<Button>>;

#[derive(Clone)]
struct TelegramBot {
    config: TelegramConfig,
    client: Client,
    state: Arc<AppState>,
}

pub fn spawn(config: TelegramConfig, state: Arc<AppState>) {
    let bot = TelegramBot {
        config,
        client: Client::builder()
            .timeout(Duration::from_secs(API_TIMEOUT_SECONDS + 5))
            .build()
            .expect("Telegram HTTP client"),
        state,
    };
    let polling_bot = bot.clone();
    tokio::spawn(async move {
        tracing::info!("Telegram bot enabled with long polling");
        if let Err(error) = polling_bot.run().await {
            tracing::error!("Telegram bot stopped: {error:#}");
        }
    });
}

impl TelegramBot {
    async fn run(&self) -> Result<()> {
        self.call::<Value>("deleteWebhook", json!({"drop_pending_updates": true}))
            .await?;
        self.call::<Value>(
            "setMyCommands",
            json!({
                "commands": [
                    {"command": "opportunities", "description": "查询当前资费套利机会"},
                    {"command": "positions", "description": "查询和管理当前仓位"},
                    {"command": "account", "description": "查询账户余额和盈亏"},
                    {"command": "protection", "description": "查询双腿保护状态和事件"},
                    {"command": "spread_strategy", "description": "查询价差回归策略状态"},
                    {"command": "open", "description": "开仓：TOKEN 金额 [杠杆]"},
                    {"command": "batch_open", "description": "批量开仓：TOKEN 目标 单笔 间隔 [杠杆]"},
                    {"command": "guard_open", "description": "保价差限价开仓：TOKEN 目标 [杠杆] [门槛%]"},
                    {"command": "reduce", "description": "减仓：TOKEN 金额"},
                    {"command": "batch_reduce", "description": "批量减仓：TOKEN 金额/all 单笔 间隔"},
                    {"command": "no_loss_close", "description": "保不亏限价平仓：TOKEN 金额/all"},
                    {"command": "leverage", "description": "调整杠杆：TOKEN 杠杆"},
                    {"command": "close", "description": "完全平仓：TOKEN"},
                    {"command": "autoclose", "description": "设置 APY 自动平仓"},
                    {"command": "autoclose_list", "description": "查询自动平仓规则"},
                    {"command": "autoclose_cancel", "description": "取消自动平仓规则"},
                    {"command": "help", "description": "显示使用帮助"}
                ]
            }),
        )
        .await?;
        let mut offset = 0i64;
        loop {
            match self
                .call::<Vec<Update>>(
                    "getUpdates",
                    json!({
                        "offset": offset,
                        "timeout": 20,
                        "allowed_updates": ["message", "callback_query"]
                    }),
                )
                .await
            {
                Ok(updates) => {
                    for update in updates {
                        offset = update.update_id + 1;
                        let bot = self.clone();
                        tokio::spawn(async move {
                            if let Err(error) = bot.handle_update(update).await {
                                tracing::warn!("Telegram update failed: {error:#}");
                            }
                        });
                    }
                }
                Err(error) => {
                    tracing::warn!("Telegram polling failed: {error:#}");
                    tokio::time::sleep(Duration::from_secs(3)).await;
                }
            }
        }
    }

    async fn handle_update(&self, update: Update) -> Result<()> {
        if let Some(message) = update.message {
            if !self.authorized(
                message.chat.id,
                message.from.as_ref().map(|user| user.id),
                message.message_thread_id,
            ) {
                tracing::info!("Rejected unauthorized Telegram message");
                return Ok(());
            }
            if let Some(text) = message.text {
                if let Err(error) = self.handle_command(&text).await {
                    self.send(&format!("❌ {}", html(&error.to_string())), vec![])
                        .await?;
                }
            }
        } else if let Some(query) = update.callback_query {
            let Some(message) = query.message.as_ref() else {
                return Ok(());
            };
            if !self.authorized(
                message.chat.id,
                Some(query.from.id),
                message.message_thread_id,
            ) {
                tracing::info!("Rejected unauthorized Telegram callback");
                return Ok(());
            }
            self.answer_callback(&query.id).await?;
            if let Some(data) = query.data.as_deref() {
                if let Err(error) = self.handle_callback(data, message.message_id).await {
                    self.send(&format!("❌ {}", html(&error.to_string())), vec![])
                        .await?;
                }
            }
        }
        Ok(())
    }

    fn authorized(&self, chat_id: i64, user_id: Option<i64>, topic_id: Option<i64>) -> bool {
        chat_id == self.config.chat_id
            && self
                .config
                .topic_id
                .is_none_or(|expected| topic_id == Some(expected))
            && (self.config.authorized_users.is_empty()
                || user_id.is_some_and(|id| self.config.authorized_users.contains(&id)))
    }

    async fn handle_command(&self, text: &str) -> Result<()> {
        let mut parts = text.split_whitespace();
        let command = parts.next().unwrap_or("").split('@').next().unwrap_or("");
        match command {
            "/start" | "/help" => self.send_help().await,
            "/opportunities" | "/opps" => self.show_opportunities().await,
            "/positions" | "/status" => self.show_positions().await,
            "/account" | "/balance" => self.show_account().await,
            "/protection" => self.show_protection().await,
            "/spread_strategy" => self.show_spread_strategy().await,
            "/open" => {
                let token = parts
                    .next()
                    .ok_or_else(|| anyhow!("用法：/open TOKEN 金额 [杠杆]"))?;
                let notional = parse_f64(parts.next(), "金额")?;
                let leverage = match parts.next() {
                    Some(value) => value.parse::<u8>()?,
                    None => self
                        .find_position_by_token(token)
                        .await
                        .map(|position| position.leverage)
                        .unwrap_or(5),
                };
                let opportunity = self.find_opportunity(token, None, None).await?;
                self.confirm_open(&opportunity, notional, leverage).await
            }
            "/batch_open" => {
                let token = parts.next().ok_or_else(|| {
                    anyhow!("用法：/batch_open TOKEN 目标金额 单笔金额 间隔秒数 [杠杆]")
                })?;
                let target = parse_f64(parts.next(), "目标金额")?;
                let order = parse_f64(parts.next(), "单笔金额")?;
                let interval = parse_f64(parts.next(), "间隔秒数")?;
                let leverage = match parts.next() {
                    Some(value) => value.parse::<u8>()?,
                    None => self
                        .find_position_by_token(token)
                        .await
                        .map(|position| position.leverage)
                        .unwrap_or(5),
                };
                let opportunity = self.find_opportunity(token, None, None).await?;
                self.confirm_batch_open(&opportunity, target, order, interval, leverage)
                    .await
            }
            "/guard_open" => {
                let token = parts.next().ok_or_else(|| {
                    anyhow!("用法：/guard_open TOKEN 目标金额 [杠杆] [最低价差%]")
                })?;
                let target = parse_f64(parts.next(), "目标金额")?;
                let leverage = match parts.next() {
                    Some(value) => value.parse::<u8>()?,
                    None => self
                        .find_position_by_token(token)
                        .await
                        .map(|position| position.leverage)
                        .unwrap_or(5),
                };
                let opportunity = self.find_opportunity(token, None, None).await?;
                let threshold = parts
                    .next()
                    .map(|value| value.parse::<f64>().map(|value| value / 100.0))
                    .transpose()?
                    .unwrap_or(opportunity.spread);
                self.confirm_guard_open(&opportunity, target, leverage, threshold)
                    .await
            }
            "/reduce" => {
                let token = parts
                    .next()
                    .ok_or_else(|| anyhow!("用法：/reduce TOKEN 金额"))?;
                let position = self.find_position_by_token(token).await?;
                let amount_raw = parts.next().ok_or_else(|| anyhow!("缺少金额"))?;
                if amount_raw.eq_ignore_ascii_case("all") {
                    return self
                        .send(
                            &format!(
                                "🚨 确认完全平掉 <b>{}</b> 的双腿仓位？此操作不可撤销。",
                                html(&position.token)
                            ),
                            vec![vec![
                                button("确认平仓", &format!("cx|{}", position.id)),
                                button("取消", &format!("p|{}", position.id)),
                            ]],
                        )
                        .await;
                }
                let amount = amount_raw.parse::<f64>().context("金额必须是数字或 all")?;
                self.send(
                    &format!(
                        "⚠️ 确认减少 <b>{}</b> 每腿 ${:.2}？",
                        html(&position.token),
                        amount
                    ),
                    vec![vec![
                        button("确认减仓", &format!("rx|{}|{amount}", position.id)),
                        button("取消", &format!("p|{}", position.id)),
                    ]],
                )
                .await
            }
            "/batch_reduce" => {
                let token = parts.next().ok_or_else(|| {
                    anyhow!("用法：/batch_reduce TOKEN 目标金额 单笔金额 间隔秒数")
                })?;
                let target_raw = parts.next().ok_or_else(|| anyhow!("缺少目标金额"))?;
                let order = parse_f64(parts.next(), "单笔金额")?;
                let interval = parse_f64(parts.next(), "间隔秒数")?;
                let position = self.find_position_by_token(token).await?;
                let (target, close_all) = parse_close_target(target_raw, &position)?;
                self.confirm_batch_reduce(&position, target, close_all, order, interval)
                    .await
            }
            "/no_loss_close" => {
                let token = parts.next().ok_or_else(|| {
                    anyhow!("用法：/no_loss_close TOKEN 目标金额 [最高平仓价差%]")
                })?;
                let target_raw = parts.next().ok_or_else(|| anyhow!("缺少目标金额"))?;
                let threshold = parts
                    .next()
                    .map(|value| value.parse::<f64>().map(|value| value / 100.0))
                    .transpose()
                    .context("最高平仓价差必须是数字")?;
                let position = self.find_position_by_token(token).await?;
                let (target, close_all) = parse_close_target(target_raw, &position)?;
                self.confirm_no_loss_close(&position, target, close_all, threshold)
                    .await
            }
            "/leverage" => {
                let token = parts
                    .next()
                    .ok_or_else(|| anyhow!("用法：/leverage TOKEN 杠杆"))?;
                let leverage = parts
                    .next()
                    .ok_or_else(|| anyhow!("缺少杠杆"))?
                    .parse::<u8>()?;
                if !(1..=20).contains(&leverage) {
                    return Err(anyhow!("杠杆范围为 1–20×"));
                }
                let position = self.find_position_by_token(token).await?;
                self.send(
                    &format!(
                        "确认将 <b>{}</b> 双腿杠杆调整为 {}×？",
                        html(&position.token),
                        leverage
                    ),
                    vec![vec![
                        button("确认调整", &format!("lx|{}|{leverage}", position.id)),
                        button("取消", &format!("p|{}", position.id)),
                    ]],
                )
                .await
            }
            "/close" => {
                let token = parts.next().ok_or_else(|| anyhow!("用法：/close TOKEN"))?;
                let position = self.find_position_by_token(token).await?;
                self.send(
                    &format!(
                        "🚨 确认完全平掉 <b>{}</b> 的双腿仓位？此操作不可撤销。",
                        html(&position.token)
                    ),
                    vec![vec![
                        button("确认平仓", &format!("cx|{}", position.id)),
                        button("取消", &format!("p|{}", position.id)),
                    ]],
                )
                .await
            }
            "/autoclose" => {
                let token = parts.next().ok_or_else(|| {
                    anyhow!("用法：/autoclose TOKEN APY阈值 [单笔金额] [间隔秒数]")
                })?;
                let threshold = parse_f64(parts.next(), "APY 阈值")?;
                let order_notional = parts.next().unwrap_or("100").parse::<f64>()?;
                let interval = parts.next().unwrap_or("2").parse::<f64>()?;
                let position = self.find_position_by_token(token).await?;
                self.confirm_auto_close(&position, threshold, order_notional, interval)
                    .await
            }
            "/autoclose_list" => self.show_auto_close_rules().await,
            "/autoclose_cancel" => {
                let id = parts
                    .next()
                    .ok_or_else(|| anyhow!("用法：/autoclose_cancel RULE_ID"))?;
                let rule = self.state.trading.cancel_auto_close(id).await?;
                self.send(
                    &format!("✅ 已取消 <b>{}</b> 的自动平仓规则", html(&rule.token)),
                    vec![vec![button("查看规则", "al")]],
                )
                .await
            }
            _ => {
                self.send("未知命令。使用 /help 查看可用命令。", vec![])
                    .await
            }
        }
    }

    async fn handle_callback(&self, data: &str, message_id: i64) -> Result<()> {
        let parts = data.split('|').collect::<Vec<_>>();
        match parts.as_slice() {
            ["opps"] => self.show_opportunities().await,
            ["pos"] => self.show_positions().await,
            ["acct"] => self.show_account().await,
            ["protect"] => self.show_protection().await,
            ["al"] => self.show_auto_close_rules().await,
            ["bt", id] => {
                let task = self.state.trading.batch_task(id).await?;
                self.show_batch_task(&task).await
            }
            ["o", token, long, short] => {
                let opportunity = self
                    .find_opportunity(token, Some(long), Some(short))
                    .await?;
                self.show_opportunity(&opportunity).await
            }
            ["oh", token, long, short] => {
                let opportunity = self
                    .find_opportunity(token, Some(long), Some(short))
                    .await?;
                let history = self
                    .state
                    .market
                    .opportunity_history(&opportunity.id)
                    .await?;
                self.show_market_history(
                    &opportunity.token.symbol,
                    &history,
                    &format!("o|{token}|{long}|{short}"),
                )
                .await
            }
            ["ph", id] => {
                let position = self.find_position(id).await?;
                let history = self.state.market.position_history(&position).await?;
                self.show_market_history(&position.token, &history, &format!("p|{id}"))
                    .await
            }
            ["oc", token, long, short, amount, leverage] => {
                let opportunity = self
                    .find_opportunity(token, Some(long), Some(short))
                    .await?;
                self.confirm_open(&opportunity, amount.parse()?, leverage.parse()?)
                    .await
            }
            ["ox", token, long, short, amount, leverage] => {
                let opportunity = self
                    .find_opportunity(token, Some(long), Some(short))
                    .await?;
                self.edit(message_id, "⏳ 正在同时提交双腿市价单…", vec![])
                    .await?;
                let response = self
                    .state
                    .trading
                    .open(OpenTradeRequest {
                        opportunity_id: opportunity.id,
                        notional_usdt: amount.parse()?,
                        leverage: leverage.parse()?,
                        spread_guard: false,
                        spread_threshold: None,
                    })
                    .await?;
                self.send(
                    &format!(
                        "✅ {}\n<b>{}</b> · 每腿 ${:.2} · {}×\n当前仓位 ${:.2}",
                        html(&response.message),
                        html(&response.position.token),
                        amount.parse::<f64>()?,
                        leverage,
                        response.position.notional_usdt
                    ),
                    position_keyboard(&response.position),
                )
                .await
            }
            ["boc", token, long, short, target, order, interval, leverage] => {
                let opportunity = self
                    .find_opportunity(token, Some(long), Some(short))
                    .await?;
                self.confirm_batch_open(
                    &opportunity,
                    target.parse()?,
                    order.parse()?,
                    interval.parse()?,
                    leverage.parse()?,
                )
                .await
            }
            ["box", token, long, short, target, order, interval, leverage] => {
                let opportunity = self
                    .find_opportunity(token, Some(long), Some(short))
                    .await?;
                let task = self
                    .state
                    .trading
                    .start_batch_increase(BatchIncreaseRequest {
                        opportunity_id: opportunity.id,
                        target_notional_usdt: target.parse()?,
                        order_notional_usdt: order.parse()?,
                        interval_seconds: interval.parse()?,
                        leverage: leverage.parse()?,
                        spread_guard: false,
                        spread_threshold: None,
                    })
                    .await?;
                self.show_batch_task(&task).await
            }
            ["gox", token, long, short, target, leverage, threshold] => {
                let opportunity = self
                    .find_opportunity(token, Some(long), Some(short))
                    .await?;
                let task = self
                    .state
                    .trading
                    .start_batch_increase(BatchIncreaseRequest {
                        opportunity_id: opportunity.id,
                        target_notional_usdt: target.parse()?,
                        order_notional_usdt: 0.0,
                        interval_seconds: 0.25,
                        leverage: leverage.parse()?,
                        spread_guard: true,
                        spread_threshold: Some(threshold.parse()?),
                    })
                    .await?;
                self.show_batch_task(&task).await
            }
            ["p", id] => {
                let position = self.find_position(id).await?;
                self.show_position(&position).await
            }
            ["ac", id, amount] => {
                let position = self.find_position(id).await?;
                let opportunity = self
                    .find_opportunity(
                        &position.token,
                        Some(&short_exchange(&position.long.exchange)),
                        Some(&short_exchange(&position.short.exchange)),
                    )
                    .await?;
                self.confirm_open(&opportunity, amount.parse()?, position.leverage)
                    .await
            }
            ["rc", id, amount] => {
                let position = self.find_position(id).await?;
                self.send(
                    &format!(
                        "⚠️ 确认减少 <b>{}</b> 每腿 ${}？",
                        html(&position.token),
                        html(amount)
                    ),
                    vec![vec![
                        button("确认减仓", &format!("rx|{id}|{amount}")),
                        button("取消", &format!("p|{id}")),
                    ]],
                )
                .await
            }
            ["rx", id, amount] => {
                self.edit(message_id, "⏳ 正在减少双腿仓位…", vec![])
                    .await?;
                let response = self
                    .state
                    .trading
                    .reduce(
                        id,
                        AdjustPositionRequest {
                            notional_usdt: amount.parse()?,
                        },
                    )
                    .await?;
                self.send(
                    &format!(
                        "✅ {}\n<b>{}</b> 当前每腿约 ${:.2}",
                        html(&response.message),
                        html(&response.position.token),
                        response.position.notional_usdt
                    ),
                    position_keyboard(&response.position),
                )
                .await
            }
            ["brc", id, target, order, interval] => {
                let position = self.find_position(id).await?;
                let (target, close_all) = parse_close_target(target, &position)?;
                self.confirm_batch_reduce(
                    &position,
                    target,
                    close_all,
                    order.parse()?,
                    interval.parse()?,
                )
                .await
            }
            ["brx", id, target, order, interval] => {
                let position = self.find_position(id).await?;
                let (target_notional_usdt, close_all) = parse_close_target(target, &position)?;
                let task = self
                    .state
                    .trading
                    .start_batch_reduce(BatchReduceRequest {
                        position_id: (*id).into(),
                        target_notional_usdt,
                        close_all,
                        order_notional_usdt: order.parse()?,
                        interval_seconds: interval.parse()?,
                        no_loss_guard: false,
                        close_spread_threshold: None,
                        minimum_roi: None,
                    })
                    .await?;
                self.show_batch_task(&task).await
            }
            ["nlx", id, target] => {
                let position = self.find_position(id).await?;
                let (target_notional_usdt, close_all) = parse_close_target(target, &position)?;
                let task = self
                    .state
                    .trading
                    .start_batch_reduce(BatchReduceRequest {
                        position_id: (*id).into(),
                        target_notional_usdt,
                        close_all,
                        order_notional_usdt: 0.0,
                        interval_seconds: 0.25,
                        no_loss_guard: true,
                        close_spread_threshold: None,
                        minimum_roi: None,
                    })
                    .await?;
                self.show_batch_task(&task).await
            }
            ["nlx", id, target, threshold] => {
                let position = self.find_position(id).await?;
                let (target_notional_usdt, close_all) = parse_close_target(target, &position)?;
                let task = self
                    .state
                    .trading
                    .start_batch_reduce(BatchReduceRequest {
                        position_id: (*id).into(),
                        target_notional_usdt,
                        close_all,
                        order_notional_usdt: 0.0,
                        interval_seconds: 0.25,
                        no_loss_guard: true,
                        close_spread_threshold: Some(threshold.parse()?),
                        minimum_roi: None,
                    })
                    .await?;
                self.show_batch_task(&task).await
            }
            ["bcc", id] => {
                self.send(
                    "确认取消这个批量任务？已经成交的批次不会回滚。",
                    vec![vec![
                        button("确认取消", &format!("bcx|{id}")),
                        button("返回任务", &format!("bt|{id}")),
                    ]],
                )
                .await
            }
            ["bcx", id] => {
                let task = self.state.trading.cancel_batch_task(id).await?;
                self.show_batch_task(&task).await
            }
            ["lc", id, leverage] => {
                let position = self.find_position(id).await?;
                self.send(
                    &format!(
                        "确认将 <b>{}</b> 双腿杠杆调整为 {}×？",
                        html(&position.token),
                        html(leverage)
                    ),
                    vec![vec![
                        button("确认调整", &format!("lx|{id}|{leverage}")),
                        button("取消", &format!("p|{id}")),
                    ]],
                )
                .await
            }
            ["lx", id, leverage] => {
                let response = self
                    .state
                    .trading
                    .adjust_leverage(id, leverage.parse()?)
                    .await?;
                self.send(
                    &format!("✅ {}", html(&response.message)),
                    position_keyboard(&response.position),
                )
                .await
            }
            ["cc", id] => {
                let position = self.find_position(id).await?;
                self.send(
                    &format!(
                        "🚨 确认完全平掉 <b>{}</b> 的双腿仓位？此操作不可撤销。",
                        html(&position.token)
                    ),
                    vec![vec![
                        button("确认平仓", &format!("cx|{id}")),
                        button("取消", &format!("p|{id}")),
                    ]],
                )
                .await
            }
            ["cx", id] => {
                self.edit(message_id, "⏳ 正在平掉双腿仓位…", vec![])
                    .await?;
                let response = self.state.trading.close(id).await?;
                self.send(
                    &format!(
                        "✅ {}\n<b>{}</b> 已平仓",
                        html(&response.message),
                        html(&response.position.token)
                    ),
                    vec![vec![button("查看持仓", "pos")]],
                )
                .await
            }
            ["auc", id, threshold, amount, interval] => {
                let position = self.find_position(id).await?;
                self.confirm_auto_close(
                    &position,
                    threshold.parse()?,
                    amount.parse()?,
                    interval.parse()?,
                )
                .await
            }
            ["aux", id, threshold, amount, interval] => {
                let rule = self
                    .state
                    .trading
                    .set_auto_close(id, threshold.parse()?, amount.parse()?, interval.parse()?)
                    .await?;
                self.send(
                    &format!(
                        "✅ 自动平仓已启用\n<b>{}</b> APY 低于 {:.1}% 时触发\n每单 ${:.2} · 间隔 {:.1}s · 直到全部平仓",
                        html(&rule.token),
                        rule.threshold_apy_percent,
                        rule.order_notional_usdt,
                        rule.interval_seconds
                    ),
                    vec![vec![
                        button("查看规则", "al"),
                        button("返回持仓", &format!("p|{}", rule.position_id)),
                    ]],
                )
                .await
            }
            ["ax", id] => {
                let rule = self.state.trading.cancel_auto_close(id).await?;
                self.send(
                    &format!("✅ 已取消 <b>{}</b> 的自动平仓规则", html(&rule.token)),
                    vec![vec![button("查看规则", "al")]],
                )
                .await
            }
            _ => Err(anyhow!("按钮已失效，请重新查询")),
        }
    }

    async fn show_opportunities(&self) -> Result<()> {
        let snapshot = self.state.market.opportunities().await?;
        let mut text = format!(
            "<b>当前资费套利机会</b>\n更新时间：{}\n\n<b>跨所永续 TOP 10</b>\n",
            chrono::Local::now().format("%H:%M:%S")
        );
        let mut keyboard = vec![];
        for (index, opportunity) in snapshot.opportunities.iter().take(10).enumerate() {
            text.push_str(&format!(
                "{}. <b>{}</b>{} {}→{} · APY {:+.1}%（{}h）· 价差 {:+.3}% · 回本 {}\n",
                index + 1,
                html(&opportunity.token.symbol),
                telegram_tags(&opportunity.token.tags),
                html(&opportunity.long.exchange),
                html(&opportunity.short.exchange),
                opportunity.apy * 100.0,
                opportunity.apy_horizon_hours,
                opportunity.spread * 100.0,
                break_even(opportunity.break_even_hours)
            ));
            keyboard.push(vec![button(
                &format!(
                    "{} · {:+.1}%",
                    opportunity.token.symbol,
                    opportunity.apy * 100.0
                ),
                &format!(
                    "o|{}|{}|{}",
                    opportunity.token.symbol,
                    short_exchange(&opportunity.long.exchange),
                    short_exchange(&opportunity.short.exchange)
                ),
            )]);
        }
        text.push_str("\n<b>同所现货–永续 TOP 10</b>\n");
        for (index, opportunity) in snapshot.spot_opportunities.iter().take(10).enumerate() {
            text.push_str(&format!(
                "{}. <b>{}</b>{} {} {}→{} {} · APY {:+.1}% · 价差 {:+.3}% · 回本 {}\n",
                index + 1,
                html(&opportunity.token.symbol),
                telegram_tags(&opportunity.token.tags),
                html(&opportunity.long.exchange),
                html(&opportunity.long.market),
                html(&opportunity.short.exchange),
                html(&opportunity.short.market),
                opportunity.apy * 100.0,
                opportunity.spread * 100.0,
                break_even(opportunity.break_even_hours)
            ));
            keyboard.push(vec![button(
                &format!(
                    "{} Spot/Perp · {:+.1}%",
                    opportunity.token.symbol,
                    opportunity.apy * 100.0
                ),
                &format!(
                    "o|{}|{}|{}",
                    opportunity.token.symbol,
                    short_exchange(&opportunity.long.exchange),
                    short_exchange(&opportunity.short.exchange)
                ),
            )]);
        }
        keyboard.push(vec![button("刷新", "opps"), button("持仓", "pos")]);
        self.send(&text, keyboard).await
    }

    async fn show_opportunity(&self, opportunity: &Opportunity) -> Result<()> {
        let route = format!(
            "{}|{}|{}",
            opportunity.token.symbol,
            short_exchange(&opportunity.long.exchange),
            short_exchange(&opportunity.short.exchange)
        );
        let text = format!(
            "<b>{}/USDT</b>{}\n🟢 LONG {} · {} @ ${:.6} · 24h {}\n   {}\n🔴 SHORT {} · {} @ ${:.6} · 24h {}\n   {}\n\n资金 APY：{:+.2}%{}\n开仓价差：{:+.3}%\n预计回本：{}\n资金净收益：{:+.5}%/小时{}",
            html(&opportunity.token.symbol),
            telegram_tags(&opportunity.token.tags),
            html(&opportunity.long.exchange),
            market_label(&opportunity.long.market),
            opportunity.long.ask,
            compact_usdt(opportunity.long.volume_24h_usdt),
            funding_schedule(&opportunity.long),
            html(&opportunity.short.exchange),
            market_label(&opportunity.short.market),
            opportunity.short.bid,
            compact_usdt(opportunity.short.volume_24h_usdt),
            funding_schedule(&opportunity.short),
            opportunity.apy * 100.0,
            if opportunity.route_type == "cross_perpetual" {
                format!("（最佳平均收益需持有 {} 小时）", opportunity.apy_horizon_hours)
            } else {
                String::new()
            },
            opportunity.spread * 100.0,
            break_even(opportunity.break_even_hours),
            opportunity.funding_per_hour * 100.0,
            if opportunity.borrow_interest_per_hour > 0.0 {
                format!(
                    "\nSpot 借币成本：-{:.5}%/小时",
                    opportunity.borrow_interest_per_hour * 100.0
                )
            } else {
                String::new()
            }
        );
        let keyboard = vec![
            vec![
                button("开仓 $100 · 5×", &format!("oc|{route}|100|5")),
                button("开仓 $500 · 5×", &format!("oc|{route}|500|5")),
            ],
            vec![
                button("开仓 $1000 · 5×", &format!("oc|{route}|1000|5")),
                button("刷新机会", &format!("o|{route}")),
            ],
            vec![button("查看 24h 价差与资费", &format!("oh|{route}"))],
            vec![button("返回列表", "opps")],
        ];
        self.send(&text, keyboard).await
    }

    async fn show_market_history(
        &self,
        token: &str,
        history: &crate::models::OpportunityHistoryResponse,
        back_callback: &str,
    ) -> Result<()> {
        let mut table = String::from("时间   价差%    Long%   Short%\n");
        for point in &history.points {
            let time = chrono::DateTime::from_timestamp_millis(point.timestamp)
                .map(|value| {
                    value
                        .with_timezone(
                            &chrono::FixedOffset::east_opt(8 * 60 * 60).expect("valid timezone"),
                        )
                        .format("%m-%d %H")
                        .to_string()
                })
                .unwrap_or_else(|| "—".into());
            table.push_str(&format!(
                "{} {:+7.3} {:>7} {:>7}\n",
                time,
                point.directional_spread_percent,
                history_rate(point.long_funding_rate),
                history_rate(point.short_funding_rate)
            ));
        }
        let text = format!(
            "<b>{}/USDT · 过去 24 小时</b>\n方向：SHORT − LONG；正值表示 SHORT 价格更高\n资金费率为截至该小时最近一次已知结算费率\n\n<pre>{}</pre>",
            html(token),
            html(table.trim_end())
        );
        let refresh_callback = if let Some(route) = back_callback.strip_prefix("o|") {
            format!("oh|{route}")
        } else if let Some(id) = back_callback.strip_prefix("p|") {
            format!("ph|{id}")
        } else {
            back_callback.into()
        };
        self.send(
            &text,
            vec![vec![
                button("刷新历史", &refresh_callback),
                button("返回", back_callback),
            ]],
        )
        .await
    }

    async fn confirm_open(
        &self,
        opportunity: &Opportunity,
        notional: f64,
        leverage: u8,
    ) -> Result<()> {
        if notional < 10.0 || !(1..=20).contains(&leverage) {
            return Err(anyhow!("金额至少 10 USDT，杠杆范围 1–20×"));
        }
        let route = format!(
            "{}|{}|{}|{}|{}",
            opportunity.token.symbol,
            short_exchange(&opportunity.long.exchange),
            short_exchange(&opportunity.short.exchange),
            notional,
            leverage
        );
        self.send(
            &format!(
                "⚠️ 确认开仓？\n<b>{}</b> · 每腿 ${:.2} · {}×\n🟢 LONG {}\n🔴 SHORT {}",
                html(&opportunity.token.symbol),
                notional,
                leverage,
                html(&opportunity.long.exchange),
                html(&opportunity.short.exchange)
            ),
            vec![vec![
                button("确认开仓", &format!("ox|{route}")),
                button(
                    "取消",
                    &format!(
                        "o|{}|{}|{}",
                        opportunity.token.symbol,
                        short_exchange(&opportunity.long.exchange),
                        short_exchange(&opportunity.short.exchange)
                    ),
                ),
            ]],
        )
        .await
    }

    async fn confirm_batch_open(
        &self,
        opportunity: &Opportunity,
        target: f64,
        order: f64,
        interval: f64,
        leverage: u8,
    ) -> Result<()> {
        validate_batch_settings(target, order, interval)?;
        if !(1..=20).contains(&leverage) {
            return Err(anyhow!("杠杆范围为 1–20×"));
        }
        let route = format!(
            "{}|{}|{}|{}|{}|{}|{}",
            opportunity.token.symbol,
            short_exchange(&opportunity.long.exchange),
            short_exchange(&opportunity.short.exchange),
            target,
            order,
            interval,
            leverage
        );
        self.send(
            &format!(
                "⚠️ 确认批量加仓？\n<b>{}</b> · 每腿目标 ${:.2} · {}×\n每单最多 ${:.2} · 间隔 {:.1}s\n预计 {} 批\n🟢 LONG {}\n🔴 SHORT {}",
                html(&opportunity.token.symbol),
                target,
                leverage,
                order,
                interval,
                (target / order).ceil() as usize,
                html(&opportunity.long.exchange),
                html(&opportunity.short.exchange)
            ),
            vec![vec![
                button("确认启动", &format!("box|{route}")),
                button(
                    "取消",
                    &format!(
                        "o|{}|{}|{}",
                        opportunity.token.symbol,
                        short_exchange(&opportunity.long.exchange),
                        short_exchange(&opportunity.short.exchange)
                    ),
                ),
            ]],
        )
        .await
    }

    async fn confirm_guard_open(
        &self,
        opportunity: &Opportunity,
        target: f64,
        leverage: u8,
        threshold: f64,
    ) -> Result<()> {
        if !(10.0..=1_000_000.0).contains(&target) {
            return Err(anyhow!("目标金额范围为 10–1,000,000 USDT"));
        }
        if !(1..=20).contains(&leverage) {
            return Err(anyhow!("杠杆范围为 1–20×"));
        }
        let route = format!(
            "{}|{}|{}|{}|{}|{}",
            opportunity.token.symbol,
            short_exchange(&opportunity.long.exchange),
            short_exchange(&opportunity.short.exchange),
            target,
            leverage,
            threshold
        );
        self.send(
            &format!(
                "⚠️ 确认保价差限价开仓？\n<b>{}</b> · 每腿目标 ${:.2} · {}×\n仅当累计成交价差满足 {:+.4}% 门槛时提交 IOC 限价单；单腿每次最多 $50，成交差额立即市价补腿，不利价差由后续 10 批分摊追回\n当前参考价差 {:+.4}%",
                html(&opportunity.token.symbol),
                target,
                leverage,
                threshold * 100.0,
                opportunity.spread * 100.0
            ),
            vec![vec![
                button("确认启动", &format!("gox|{route}")),
                button(
                    "取消",
                    &format!(
                        "o|{}|{}|{}",
                        opportunity.token.symbol,
                        short_exchange(&opportunity.long.exchange),
                        short_exchange(&opportunity.short.exchange)
                    ),
                ),
            ]],
        )
        .await
    }

    async fn confirm_batch_reduce(
        &self,
        position: &Position,
        target: f64,
        close_all: bool,
        order: f64,
        interval: f64,
    ) -> Result<()> {
        validate_batch_settings(target, order, interval)?;
        self.send(
            &format!(
                "⚠️ 确认批量减仓？\n<b>{}</b> · 每腿目标 {}\n每单最多 ${:.2} · 间隔 {:.1}s\n预计 {} 批\n当前每腿约 ${:.2}",
                html(&position.token),
                if close_all { "ALL（全平）".into() } else { format!("${target:.2}") },
                order,
                interval,
                (target / order).ceil() as usize,
                position.notional_usdt
            ),
            vec![vec![
                button(
                    "确认启动",
                    &format!("brx|{}|{}|{}|{}", position.id, if close_all { "all".into() } else { target.to_string() }, order, interval),
                ),
                button("取消", &format!("p|{}", position.id)),
            ]],
        )
        .await
    }

    async fn confirm_no_loss_close(
        &self,
        position: &Position,
        target: f64,
        close_all: bool,
        threshold: Option<f64>,
    ) -> Result<()> {
        let maximum = (position.notional_usdt - 10.0).max(0.0);
        if !close_all && !(10.0..=maximum).contains(&target) {
            return Err(anyhow!(
                "目标金额必须在 10 与 {:.2} USDT 之间；需保留至少 10 USDT，全部退出请使用普通平仓",
                maximum
            ));
        }
        self.send(
            &format!(
                "⚠️ 确认启动保价差且不亏限价平仓？\n<b>{}</b> · 每腿目标 {}\n最高平仓价差：{}\n同时满足价差门槛和累计仓位平仓盈亏不为负时下单；不包含资金费和手续费",
                html(&position.token),
                if close_all { "ALL（全平）".into() } else { format!("${target:.2}") },
                threshold.map(|value| format!("{:+.4}%", value * 100.0)).unwrap_or_else(|| "启动时当前价差".into())
            ),
            vec![vec![
                button(
                    "确认启动",
                    &threshold.map_or_else(
                        || format!("nlx|{}|{}", position.id, if close_all { "all".into() } else { target.to_string() }),
                        |value| format!("nlx|{}|{}|{}", position.id, if close_all { "all".into() } else { target.to_string() }, value),
                    ),
                ),
                button("取消", &format!("p|{}", position.id)),
            ]],
        )
        .await
    }

    async fn show_batch_task(&self, task: &BatchIncreaseTask) -> Result<()> {
        let progress = if task.target_notional_usdt > 0.0 {
            (task.completed_notional_usdt / task.target_notional_usdt * 100.0).clamp(0.0, 100.0)
        } else {
            0.0
        };
        let action = if task.no_loss_guard {
            "保不亏平仓"
        } else if task.action == "reduce" {
            "批量减仓"
        } else {
            "批量加仓"
        };
        let batch_description = if task.spread_guard || task.no_loss_guard {
            format!("{} 次动态盘口下单", task.completed_batches)
        } else {
            format!("{} / {}", task.completed_batches, task.total_batches)
        };
        let execution_description = if task.spread_guard {
            "单腿硬顶 $50；差额立即市价补齐，价差欠账由后续 10 批分摊，无人为间隔".to_string()
        } else if task.no_loss_guard {
            "只看双腿仓位盈亏；差额立即市价补齐，价差欠账由后续 10 批分摊".to_string()
        } else {
            format!(
                "单笔 ${:.2} · 间隔 {:.1}s",
                task.order_notional_usdt, task.interval_seconds
            )
        };
        let mut text = format!(
            "<b>{} · {}</b>\n状态：{}\n进度：{:.1}% · ${:.2} / ${:.2}\n批次：{}\n{}",
            action,
            html(&task.token),
            html(batch_status(&task.status)),
            progress,
            task.completed_notional_usdt,
            task.target_notional_usdt,
            batch_description,
            execution_description
        );
        if let Some(leverage) = task.leverage {
            text.push_str(&format!("\n杠杆：{}×", leverage));
        }
        if task.spread_guard {
            text.push_str(&format!(
                "\n保价差限价：门槛 {:+.4}% · 当前 {} · 已等待 {} 次",
                task.spread_threshold.unwrap_or_default() * 100.0,
                task.current_spread
                    .map(|value| format!("{:+.4}%", value * 100.0))
                    .unwrap_or_else(|| "读取中".into()),
                task.spread_wait_count
            ));
            text.push_str(&format!(
                "\n动态要求：{} · 累计成交价差：{}",
                task.effective_spread_threshold
                    .map(|value| format!("{:+.4}%", value * 100.0))
                    .unwrap_or_else(|| "读取中".into()),
                task.cumulative_filled_spread
                    .map(|value| format!("{:+.4}%", value * 100.0))
                    .unwrap_or_else(|| "暂无".into())
            ));
        }
        if task.no_loss_guard {
            text.push_str(&format!(
                "\n保价差且不亏平仓：门槛 {} · 当前 {} · 累计成交 {} · 当前可配对仓位盈亏 {} · 已等待 {} 次",
                task.effective_spread_threshold
                    .map(|value| format!("{:+.4}%", value * 100.0))
                    .unwrap_or_else(|| "读取中".into()),
                task.current_spread
                    .map(|value| format!("{:+.4}%", value * 100.0))
                    .unwrap_or_else(|| "读取中".into()),
                task.cumulative_filled_spread
                    .map(|value| format!("{:+.4}%", value * 100.0))
                    .unwrap_or_else(|| "暂无".into()),
                task.current_close_pnl_usdt
                    .map(|value| format!("{value:+.4} USDT"))
                    .unwrap_or_else(|| "读取中".into()),
                task.spread_wait_count
            ));
        }
        if let Some(error) = task.error.as_deref() {
            text.push_str(&format!("\n\n❌ {}", html(error)));
        }
        if !task.logs.is_empty() {
            text.push_str("\n\n<b>最近成交</b>");
            for log in task.logs.iter().rev().take(6).rev() {
                text.push_str(&format!(
                    "\n#{} {} · {} {} · ${:.2} @ ${:.6}",
                    log.batch,
                    html(&log.exchange),
                    html(&log.side.to_uppercase()),
                    html(&log.token),
                    log.notional_usdt,
                    log.average_price
                ));
            }
        }
        let mut keyboard = vec![vec![button("刷新进度", &format!("bt|{}", task.id))]];
        if matches!(task.status.as_str(), "queued" | "running" | "cancelling") {
            keyboard[0].push(button("取消任务", &format!("bcc|{}", task.id)));
        }
        keyboard.push(vec![button("查看持仓", "pos")]);
        self.send(&text, keyboard).await
    }

    async fn show_positions(&self) -> Result<()> {
        let positions = self.state.trading.positions().await?;
        if positions.is_empty() {
            return self
                .send("当前没有套利仓位。", vec![vec![button("查看机会", "opps")]])
                .await;
        }
        let mut text = String::from("<b>当前套利仓位</b>\n\n");
        let mut keyboard = vec![];
        for position in &positions {
            let market_metrics = position_market_metrics(position);
            text.push_str(&format!(
                "<b>{}</b> · ${:.2} · {}× · 扣费盈亏 {:+.2} · ROI {:+.2}%\n{}\n",
                html(&position.token),
                position.notional_usdt,
                position.leverage,
                position.current_pnl_usdt,
                position.current_roi * 100.0,
                market_metrics
            ));
            keyboard.push(vec![button(
                &format!("管理 {}", position.token),
                &format!("p|{}", position.id),
            )]);
        }
        keyboard.push(vec![button("刷新", "pos"), button("机会", "opps")]);
        self.send(&text, keyboard).await
    }

    async fn show_position(&self, position: &Position) -> Result<()> {
        self.send(
            &format!(
                "<b>{}/USDT</b> · 每腿约 ${:.2} · {}×\n\n🟢 LONG {}: {:.6} @ {:.6}\n🔴 SHORT {}: {:.6} @ {:.6}\n未实现盈亏：{:+.4} USDT\n累计 Funding：{:+.4} USDT\n预计往返手续费：-{:.4} USDT\n扣费盈亏：{:+.4} USDT\n当前 ROI：{:+.3}%（保证金基数 ${:.2}）\n{}",
                html(&position.token),
                position.notional_usdt,
                position.leverage,
                html(&position.long.exchange),
                position.long.quantity,
                position.long.mark_price,
                html(&position.short.exchange),
                position.short.quantity,
                position.short.mark_price,
                position.unrealized_pnl,
                position.funding_earned,
                position.estimated_trading_fees_usdt,
                position.current_pnl_usdt,
                position.current_roi * 100.0,
                position.roi_basis_usdt,
                position_market_metrics(position)
            ),
            position_keyboard(position),
        )
        .await
    }

    async fn show_account(&self) -> Result<()> {
        let account = self.state.trading.account_summary().await?;
        let mut text = format!(
            "<b>账户概览 · {}</b>\n总权益 ${:.2}\n可用 ${:.2}\n未实现 {:+.2}\n已实现 {:+.2}\n\n",
            html(&account.mode),
            account.equity_usdt,
            account.available_usdt,
            account.unrealized_pnl,
            account.realized_pnl
        );
        for exchange in account.exchanges {
            text.push_str(&format!(
                "<b>{}</b>：可用 ${:.2} / 权益 ${:.2}\n",
                html(&exchange.exchange),
                exchange.available_usdt,
                exchange.equity_usdt
            ));
        }
        self.send(
            &text,
            vec![vec![button("刷新", "acct"), button("持仓", "pos")]],
        )
        .await
    }

    async fn show_protection(&self) -> Result<()> {
        let status = self.state.trading.hedge_protection_status().await;
        let mut text = format!(
            "<b>孤腿仓位保护</b>\n状态：{}\n触发条件：仅当一条腿不存在时退出另一条腿\n双腿偏差：不自动调仓\n孤腿退出：每单 ${:.2} / {:.1}s\n保护标的：{}",
            if status.enabled { "运行中" } else { "未启用" },
            status.order_notional_usdt,
            status.interval_seconds,
            if status.protected_tokens.is_empty() {
                "等待识别".into()
            } else {
                status.protected_tokens.join("、")
            }
        );
        for event in status
            .events
            .iter()
            .rev()
            .filter(|event| !event.orders.is_empty() || event.status == "failed")
            .take(5)
        {
            let initial_long = event.initial_long_notional_usdt.unwrap_or(0.0);
            let initial_short = event.initial_short_notional_usdt.unwrap_or(0.0);
            let final_long = event.final_long_notional_usdt.unwrap_or(initial_long);
            let final_short = event.final_short_notional_usdt.unwrap_or(initial_short);
            text.push_str(&format!(
                "\n\n<b>{}</b>：LONG ${:.2} → ${:.2}；SHORT ${:.2} → ${:.2} · {} 笔",
                html(&event.token),
                initial_long,
                final_long,
                initial_short,
                final_short,
                event.orders.len()
            ));
            if event.status == "failed" {
                text.push_str(&format!("\n失败：{}", html(&event.message)));
            }
        }
        self.send(&text, vec![vec![button("刷新", "protect")]])
            .await
    }

    async fn show_spread_strategy(&self) -> Result<()> {
        let Some(trade) = self.state.spread_strategy.status().await else {
            return self
                .send("<b>价差回归策略</b>\n状态：等待价差偏离达到 1%", vec![])
                .await;
        };
        self.send(
            &format!(
                "<b>价差回归策略 · {}</b>\n状态：{}\n入场偏离：{:+.3}%\n入场价差：{:+.3}%\n当前 ROI：{}\n止盈目标：{}\n退出原因：{}{}",
                html(&trade.token),
                html(&trade.status),
                trade.entry_deviation * 100.0,
                trade.entry_spread * 100.0,
                trade.current_roi
                    .map(|value| format!("{:+.3}%", value * 100.0))
                    .unwrap_or_else(|| "—".into()),
                trade.target_roi
                    .map(|value| format!("{:+.3}%", value * 100.0))
                    .unwrap_or_else(|| "—".into()),
                trade.exit_reason.as_deref().map(html).unwrap_or_else(|| "—".into()),
                trade.error
                    .as_deref()
                    .map(|error| format!("\n错误：{}", html(error)))
                    .unwrap_or_default()
            ),
            vec![],
        )
        .await
    }

    async fn confirm_auto_close(
        &self,
        position: &Position,
        threshold: f64,
        order_notional: f64,
        interval: f64,
    ) -> Result<()> {
        if order_notional < 10.0 || interval < 0.5 {
            return Err(anyhow!("单笔金额至少 10 USDT，间隔至少 0.5 秒"));
        }
        self.send(
            &format!(
                "⚠️ 确认启用自动平仓？\n<b>{}</b>\nAPY 低于 {:.1}% 时触发\n每单 ${:.2} · 间隔 {:.1}s · 直到全部平仓",
                html(&position.token),
                threshold,
                order_notional,
                interval
            ),
            vec![vec![
                button(
                    "确认启用",
                    &format!(
                        "aux|{}|{}|{}|{}",
                        position.id, threshold, order_notional, interval
                    ),
                ),
                button("取消", &format!("p|{}", position.id)),
            ]],
        )
        .await
    }

    async fn show_auto_close_rules(&self) -> Result<()> {
        let rules = self.state.trading.auto_close_rules().await;
        let active = rules
            .iter()
            .filter(|rule| matches!(rule.status.as_str(), "armed" | "triggered" | "closing"))
            .collect::<Vec<_>>();
        if active.is_empty() {
            return self.send("当前没有生效中的自动平仓规则。", vec![]).await;
        }
        let mut text = String::from("<b>自动平仓规则</b>\n\n");
        let mut keyboard = vec![];
        for rule in active {
            let current = rule
                .current_apy_percent
                .map(|value| format!("{value:.1}%"))
                .unwrap_or_else(|| "等待检查".into());
            text.push_str(&format!(
                "<b>{}</b> · {}\n当前 APY {} / 阈值 {:.1}%\n每单 ${:.2} · {:.1}s · 已执行 ${:.2}\n\n",
                html(&rule.token),
                html(&rule.status),
                current,
                rule.threshold_apy_percent,
                rule.order_notional_usdt,
                rule.interval_seconds,
                rule.completed_notional_usdt
            ));
            if rule.status == "armed" {
                keyboard.push(vec![button(
                    &format!("取消 {}", rule.token),
                    &format!("ax|{}", rule.id),
                )]);
            }
        }
        keyboard.push(vec![button("刷新", "al"), button("持仓", "pos")]);
        self.send(&text, keyboard).await
    }

    async fn send_help(&self) -> Result<()> {
        self.send(
            &format!(
                "<b>ArbiView Telegram Bot</b>\n\n/opportunities — 查询前 10 个资费套利机会\n/positions — 查询并管理仓位\n/account — 查询账户余额和盈亏\n/protection — 查询孤腿保护状态和事件\n/spread_strategy — 查询价差回归策略状态\n/open TOKEN 金额 [杠杆] — 开仓或加仓；新仓默认 5×，加仓沿用当前杠杆\n/batch_open TOKEN 目标 单笔 间隔 [杠杆] — 市价批量加仓\n/guard_open TOKEN 目标 [杠杆] [门槛%] — 按实时盘口容量保价差限价开仓\n/reduce TOKEN 金额/all — 减仓或全平\n/batch_reduce TOKEN 金额/all 单笔 间隔 — 批量减仓\n/no_loss_close TOKEN 金额/all [最高平仓价差%] — 保价差且不亏限价平仓\n/leverage TOKEN 杠杆 — 调整双腿杠杆\n/close TOKEN — 完全平仓\n/autoclose TOKEN APY [单笔] [间隔] — APY 跌破阈值后批量全平\n/autoclose_list — 查询自动平仓规则\n/autoclose_cancel RULE_ID — 取消自动平仓规则\n/help — 显示帮助\n\n示例：<code>/open DEXE {} 2</code>\n<code>/batch_open DEXE 1000 100 2 3</code>\n<code>/guard_open DEXE 1000 5 -1.5</code>\n<code>/batch_reduce DEXE all 100 2</code>\n<code>/no_loss_close DEXE all -0.5</code>\n所有交易动作均需按钮二次确认。",
                DEFAULT_NOTIONAL
            ),
            vec![
                vec![button("套利机会", "opps"), button("当前持仓", "pos")],
                vec![button("账户概览", "acct")],
            ],
        )
        .await
    }

    async fn find_opportunity(
        &self,
        token: &str,
        long: Option<&str>,
        short: Option<&str>,
    ) -> Result<Opportunity> {
        let snapshot = self.state.market.opportunities().await?;
        snapshot
            .opportunities
            .into_iter()
            .chain(snapshot.spot_opportunities)
            .find(|opportunity| {
                opportunity.token.symbol.eq_ignore_ascii_case(token)
                    && long.is_none_or(|exchange| {
                        short_exchange(&opportunity.long.exchange).eq_ignore_ascii_case(exchange)
                    })
                    && short.is_none_or(|exchange| {
                        short_exchange(&opportunity.short.exchange).eq_ignore_ascii_case(exchange)
                    })
            })
            .ok_or_else(|| anyhow!("该套利机会已不存在，请刷新机会列表"))
    }

    async fn find_position(&self, id: &str) -> Result<Position> {
        self.state
            .trading
            .positions()
            .await?
            .into_iter()
            .find(|position| position.id == id)
            .ok_or_else(|| anyhow!("仓位不存在或已经平仓"))
    }

    async fn find_position_by_token(&self, token: &str) -> Result<Position> {
        self.state
            .trading
            .positions()
            .await?
            .into_iter()
            .find(|position| position.token.eq_ignore_ascii_case(token))
            .ok_or_else(|| anyhow!("找不到 {token} 的套利仓位"))
    }

    async fn send(&self, text: &str, keyboard: Keyboard) -> Result<()> {
        let mut payload = json!({
            "chat_id": self.config.chat_id,
            "text": text,
            "parse_mode": "HTML",
            "reply_markup": {"inline_keyboard": keyboard}
        });
        if let Some(topic_id) = self.config.topic_id {
            payload["message_thread_id"] = json!(topic_id);
        }
        self.call::<Value>("sendMessage", payload).await.map(|_| ())
    }

    async fn edit(&self, message_id: i64, text: &str, keyboard: Keyboard) -> Result<()> {
        self.call::<Value>(
            "editMessageText",
            json!({
                "chat_id": self.config.chat_id,
                "message_id": message_id,
                "text": text,
                "parse_mode": "HTML",
                "reply_markup": {"inline_keyboard": keyboard}
            }),
        )
        .await
        .map(|_| ())
    }

    async fn answer_callback(&self, callback_query_id: &str) -> Result<()> {
        self.call::<Value>(
            "answerCallbackQuery",
            json!({"callback_query_id": callback_query_id}),
        )
        .await
        .map(|_| ())
    }

    async fn call<T>(&self, method: &str, body: Value) -> Result<T>
    where
        T: for<'de> Deserialize<'de>,
    {
        let response = self
            .client
            .post(format!(
                "https://api.telegram.org/bot{}/{}",
                self.config.token, method
            ))
            .json(&body)
            .send()
            .await
            .with_context(|| format!("Telegram {method} request failed"))?
            .error_for_status()
            .with_context(|| format!("Telegram {method} HTTP error"))?
            .json::<ApiResponse<T>>()
            .await
            .with_context(|| format!("invalid Telegram {method} response"))?;
        if !response.ok {
            return Err(anyhow!(
                "Telegram {method}: {}",
                response
                    .description
                    .unwrap_or_else(|| "unknown error".into())
            ));
        }
        response
            .result
            .ok_or_else(|| anyhow!("Telegram {method} returned no result"))
    }
}

fn position_keyboard(position: &Position) -> Keyboard {
    let id = &position.id;
    vec![
        vec![
            button("加仓 $100", &format!("ac|{id}|100")),
            button("加仓 $500", &format!("ac|{id}|500")),
        ],
        vec![button("APY<300% 自动全平", &format!("auc|{id}|300|100|2"))],
        vec![button("查看 24h 价差与资费", &format!("ph|{id}"))],
        vec![
            button("减仓 $100", &format!("rc|{id}|100")),
            button("减仓 $500", &format!("rc|{id}|500")),
        ],
        vec![
            button("杠杆 1×", &format!("lc|{id}|1")),
            button("杠杆 2×", &format!("lc|{id}|2")),
            button("杠杆 3×", &format!("lc|{id}|3")),
        ],
        vec![
            button("完全平仓", &format!("cc|{id}")),
            button("刷新", &format!("p|{id}")),
            button("返回", "pos"),
        ],
    ]
}

fn history_rate(rate: Option<f64>) -> String {
    rate.map(|value| format!("{:+.3}", value * 100.0))
        .unwrap_or_else(|| "—".into())
}

fn market_label(market: &str) -> &'static str {
    if market == "spot" {
        "SPOT"
    } else {
        "PERP"
    }
}

fn funding_schedule(leg: &crate::models::Leg) -> String {
    if leg.market == "spot" {
        return "现货腿 · 无资金费结算".into();
    }
    let settlement = chrono::DateTime::from_timestamp_millis(leg.next_funding_time)
        .map(|value| {
            value
                .with_timezone(&chrono::FixedOffset::east_opt(8 * 60 * 60).expect("valid timezone"))
                .format("%m-%d %H:%M")
                .to_string()
        })
        .unwrap_or_else(|| "—".into());
    format!(
        "资费 {:+.4}% / {}h · 下次 {}",
        leg.rate * 100.0,
        leg.interval_hours,
        settlement
    )
}

fn button(text: &str, callback_data: &str) -> Button {
    debug_assert!(callback_data.len() <= 64);
    Button {
        text: text.into(),
        callback_data: callback_data.into(),
    }
}

fn short_exchange(exchange: &str) -> String {
    exchange.chars().next().unwrap_or('?').to_string()
}

fn parse_f64(value: Option<&str>, name: &str) -> Result<f64> {
    value
        .ok_or_else(|| anyhow!("缺少{name}"))?
        .parse()
        .map_err(|_| anyhow!("{name}格式错误"))
}

fn parse_close_target(value: &str, position: &Position) -> Result<(f64, bool)> {
    if value.eq_ignore_ascii_case("all") {
        Ok((position.notional_usdt, true))
    } else {
        Ok((
            value
                .parse::<f64>()
                .with_context(|| format!("目标金额必须是数字或 all：{value}"))?,
            false,
        ))
    }
}

fn validate_batch_settings(target: f64, order: f64, interval: f64) -> Result<()> {
    if !(10.0..=1_000_000.0).contains(&target) {
        return Err(anyhow!("目标金额范围为 10–1,000,000 USDT"));
    }
    if !(10.0..=target).contains(&order) {
        return Err(anyhow!("单笔金额必须在 10 USDT 与目标金额之间"));
    }
    if !(0.5..=3_600.0).contains(&interval) {
        return Err(anyhow!("批次间隔范围为 0.5–3,600 秒"));
    }
    Ok(())
}

fn batch_status(status: &str) -> &str {
    match status {
        "queued" => "等待执行",
        "running" => "执行中",
        "cancelling" => "正在取消",
        "cancelled" => "已取消",
        "completed" => "已完成",
        "failed" => "失败",
        _ => status,
    }
}

fn position_market_metrics(position: &Position) -> String {
    if let (Some(funding_per_hour), Some(spread), Some(apy)) = (
        position.current_funding_per_hour,
        position.current_spread,
        position.current_apy,
    ) {
        return format!(
            "资费净差 {:+.4}%/h · 价差 {:+.3}% · APY {:+.1}%",
            funding_per_hour * 100.0,
            spread * 100.0,
            apy * 100.0
        );
    }
    "资费净差 / 价差 / APY：实时行情暂不可用".into()
}

fn telegram_tags(tags: &[String]) -> String {
    if tags.is_empty() {
        String::new()
    } else {
        format!(
            " · {}",
            tags.iter()
                .map(|tag| format!("#{}", html(tag)))
                .collect::<Vec<_>>()
                .join(" ")
        )
    }
}

fn compact_usdt(value: f64) -> String {
    if value >= 1_000_000_000.0 {
        format!("${:.2}B", value / 1_000_000_000.0)
    } else if value >= 1_000_000.0 {
        format!("${:.2}M", value / 1_000_000.0)
    } else if value >= 1_000.0 {
        format!("${:.2}K", value / 1_000.0)
    } else {
        format!("${value:.2}")
    }
}

fn break_even(hours: f64) -> String {
    if hours <= 0.0 {
        "立即".into()
    } else if hours < 1.0 {
        format!("{:.0} 分钟", hours * 60.0)
    } else {
        format!("{hours:.1} 小时")
    }
}

fn html(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn escapes_html() {
        assert_eq!(html("A&B<C>"), "A&amp;B&lt;C&gt;");
    }

    #[test]
    fn formats_break_even() {
        assert_eq!(break_even(0.5), "30 分钟");
        assert_eq!(break_even(2.25), "2.2 小时");
    }

    #[test]
    fn callback_payloads_fit_telegram_limit() {
        let position = Position {
            id: "live-SOMELONGTOKENSYMBOL".into(),
            token: "SOMELONGTOKENSYMBOL".into(),
            status: "open".into(),
            opened_at: 0,
            notional_usdt: 100.0,
            leverage: 1,
            long: test_leg("Binance", "long"),
            short: test_leg("Bybit", "short"),
            funding_earned: 0.0,
            unrealized_pnl: 0.0,
            estimated_trading_fees_usdt: 0.0,
            current_pnl_usdt: 0.0,
            current_roi: 0.0,
            roi_basis_usdt: 200.0,
            current_funding_per_hour: None,
            current_spread: None,
            current_apy: None,
        };
        for row in position_keyboard(&position) {
            for button in row {
                assert!(button.callback_data.len() <= 64);
            }
        }
    }

    #[test]
    fn validates_batch_settings() {
        assert!(validate_batch_settings(1_000.0, 100.0, 2.0).is_ok());
        assert!(validate_batch_settings(1_000.0, 2_000.0, 2.0).is_err());
        assert!(validate_batch_settings(1_000.0, 100.0, 0.1).is_err());
    }

    #[test]
    fn parses_all_as_the_full_position_notional() {
        let position = Position {
            id: "live-TEST".into(),
            token: "TEST".into(),
            status: "open".into(),
            opened_at: 0,
            notional_usdt: 1_234.5,
            leverage: 10,
            long: test_leg("Binance", "long"),
            short: test_leg("Bybit", "short"),
            funding_earned: 0.0,
            unrealized_pnl: 0.0,
            estimated_trading_fees_usdt: 0.0,
            current_pnl_usdt: 0.0,
            current_roi: 0.0,
            roi_basis_usdt: 200.0,
            current_funding_per_hour: None,
            current_spread: None,
            current_apy: None,
        };
        assert_eq!(
            parse_close_target("ALL", &position).unwrap(),
            (1_234.5, true)
        );
        assert_eq!(
            parse_close_target("500", &position).unwrap(),
            (500.0, false)
        );
    }

    fn test_leg(exchange: &str, side: &str) -> crate::models::PositionLeg {
        crate::models::PositionLeg {
            exchange: exchange.into(),
            market: "perpetual".into(),
            symbol: "TESTUSDT".into(),
            side: side.into(),
            quantity: 1.0,
            entry_price: 1.0,
            mark_price: 1.0,
            unrealized_pnl: 0.0,
            funding_earned: 0.0,
            funding_rate: 0.0,
            leverage: 1,
        }
    }
}
