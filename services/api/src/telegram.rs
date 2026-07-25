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
    tokio::spawn(async move {
        tracing::info!("Telegram bot enabled with long polling");
        if let Err(error) = bot.run().await {
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
                    {"command": "open", "description": "开仓：TOKEN 金额 [杠杆]"},
                    {"command": "batch_open", "description": "批量开仓：TOKEN 目标 单笔 间隔 [杠杆]"},
                    {"command": "reduce", "description": "减仓：TOKEN 金额"},
                    {"command": "batch_reduce", "description": "批量减仓：TOKEN 目标 单笔 间隔"},
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
                        if let Err(error) = self.handle_update(update).await {
                            tracing::warn!("Telegram update failed: {error:#}");
                        }
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
            "/open" => {
                let token = parts
                    .next()
                    .ok_or_else(|| anyhow!("用法：/open TOKEN 金额 [杠杆]"))?;
                let notional = parse_f64(parts.next(), "金额")?;
                let leverage = parts.next().unwrap_or("1").parse::<u8>()?;
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
                let leverage = parts.next().unwrap_or("1").parse::<u8>()?;
                let opportunity = self.find_opportunity(token, None, None).await?;
                self.confirm_batch_open(&opportunity, target, order, interval, leverage)
                    .await
            }
            "/reduce" => {
                let token = parts
                    .next()
                    .ok_or_else(|| anyhow!("用法：/reduce TOKEN 金额"))?;
                let amount = parse_f64(parts.next(), "金额")?;
                let position = self.find_position_by_token(token).await?;
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
                let target = parse_f64(parts.next(), "目标金额")?;
                let order = parse_f64(parts.next(), "单笔金额")?;
                let interval = parse_f64(parts.next(), "间隔秒数")?;
                let position = self.find_position_by_token(token).await?;
                self.confirm_batch_reduce(&position, target, order, interval)
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
                self.confirm_batch_reduce(
                    &position,
                    target.parse()?,
                    order.parse()?,
                    interval.parse()?,
                )
                .await
            }
            ["brx", id, target, order, interval] => {
                let task = self
                    .state
                    .trading
                    .start_batch_reduce(BatchReduceRequest {
                        position_id: (*id).into(),
                        target_notional_usdt: target.parse()?,
                        order_notional_usdt: order.parse()?,
                        interval_seconds: interval.parse()?,
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
            "<b>当前资费套利机会</b>\n更新时间：{}\n\n",
            chrono::Local::now().format("%H:%M:%S")
        );
        let mut keyboard = vec![];
        for (index, opportunity) in snapshot.opportunities.iter().take(10).enumerate() {
            text.push_str(&format!(
                "{}. <b>{}</b> {}→{} · APY {:+.1}% · 价差 {:+.3}% · 回本 {}\n",
                index + 1,
                html(&opportunity.token.symbol),
                html(&opportunity.long.exchange),
                html(&opportunity.short.exchange),
                opportunity.apy * 100.0,
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
            "<b>{}/USDT</b>\n🟢 LONG {} @ ${:.6}\n🔴 SHORT {} @ ${:.6}\n\n资金 APY：{:+.2}%\n开仓价差：{:+.3}%\n预计回本：{}\n资金净收益：{:+.5}%/小时",
            html(&opportunity.token.symbol),
            html(&opportunity.long.exchange),
            opportunity.long.ask,
            html(&opportunity.short.exchange),
            opportunity.short.bid,
            opportunity.apy * 100.0,
            opportunity.spread * 100.0,
            break_even(opportunity.break_even_hours),
            opportunity.funding_per_hour * 100.0
        );
        let keyboard = vec![
            vec![
                button("开仓 $100", &format!("oc|{route}|100|1")),
                button("开仓 $500", &format!("oc|{route}|500|1")),
            ],
            vec![
                button("开仓 $1000", &format!("oc|{route}|1000|1")),
                button("刷新机会", &format!("o|{route}")),
            ],
            vec![button("返回列表", "opps")],
        ];
        self.send(&text, keyboard).await
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

    async fn confirm_batch_reduce(
        &self,
        position: &Position,
        target: f64,
        order: f64,
        interval: f64,
    ) -> Result<()> {
        validate_batch_settings(target, order, interval)?;
        self.send(
            &format!(
                "⚠️ 确认批量减仓？\n<b>{}</b> · 每腿共减少 ${:.2}\n每单最多 ${:.2} · 间隔 {:.1}s\n预计 {} 批\n当前每腿约 ${:.2}",
                html(&position.token),
                target,
                order,
                interval,
                (target / order).ceil() as usize,
                position.notional_usdt
            ),
            vec![vec![
                button(
                    "确认启动",
                    &format!("brx|{}|{}|{}|{}", position.id, target, order, interval),
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
        let action = if task.action == "reduce" {
            "批量减仓"
        } else {
            "批量加仓"
        };
        let mut text = format!(
            "<b>{} · {}</b>\n状态：{}\n进度：{:.1}% · ${:.2} / ${:.2}\n批次：{} / {}\n单笔 ${:.2} · 间隔 {:.1}s",
            action,
            html(&task.token),
            html(batch_status(&task.status)),
            progress,
            task.completed_notional_usdt,
            task.target_notional_usdt,
            task.completed_batches,
            task.total_batches,
            task.order_notional_usdt,
            task.interval_seconds
        );
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
            text.push_str(&format!(
                "<b>{}</b> · ${:.2} · {}× · 未实现 {:+.2}\n",
                html(&position.token),
                position.notional_usdt,
                position.leverage,
                position.unrealized_pnl
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
                "<b>{}/USDT</b> · 每腿约 ${:.2} · {}×\n\n🟢 LONG {}: {:.6} @ {:.6}\n🔴 SHORT {}: {:.6} @ {:.6}\n未实现盈亏：{:+.4} USDT\n累计 Funding：{:+.4} USDT",
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
                position.funding_earned
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
                "<b>ArbiView Telegram Bot</b>\n\n/opportunities — 查询前 10 个资费套利机会\n/positions — 查询并管理仓位\n/account — 查询账户余额与盈亏\n/open TOKEN 金额 [杠杆] — 开仓或加仓\n/batch_open TOKEN 目标 单笔 间隔 [杠杆] — 批量加仓\n/reduce TOKEN 金额 — 减仓\n/batch_reduce TOKEN 目标 单笔 间隔 — 批量减仓\n/leverage TOKEN 杠杆 — 调整双腿杠杆\n/close TOKEN — 完全平仓\n/autoclose TOKEN APY [单笔] [间隔] — APY 跌破阈值后批量全平\n/autoclose_list — 查询自动平仓规则\n/autoclose_cancel RULE_ID — 取消规则\n/help — 显示帮助\n\n示例：<code>/open DEXE {} 2</code>\n<code>/batch_open DEXE 1000 100 2 3</code>\n<code>/batch_reduce DEXE 1000 100 2</code>\n所有交易动作均需按钮二次确认。",
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

    fn test_leg(exchange: &str, side: &str) -> crate::models::PositionLeg {
        crate::models::PositionLeg {
            exchange: exchange.into(),
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
