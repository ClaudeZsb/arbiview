use anyhow::{bail, Result};

#[derive(Clone)]
pub struct ExchangeCredentials {
    pub api_key: String,
    pub api_secret: String,
}

#[derive(Clone)]
pub struct TelegramConfig {
    pub token: String,
    pub chat_id: i64,
    pub topic_id: Option<i64>,
    pub authorized_users: Vec<i64>,
}

#[derive(Clone, PartialEq)]
pub enum TradingMode {
    Paper,
    Live,
}

#[derive(Clone)]
pub struct Config {
    pub port: u16,
    pub cmc_api_key: String,
    pub binance: Option<ExchangeCredentials>,
    pub bybit: Option<ExchangeCredentials>,
    pub trading_mode: TradingMode,
    pub web_origin: String,
    pub max_slippage_bps: u32,
    pub position_tolerance_usdt: f64,
    pub telegram: Option<TelegramConfig>,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        let trading_mode = match std::env::var("ARBIVIEW_TRADING_MODE")
            .unwrap_or_else(|_| "paper".into())
            .as_str()
        {
            "paper" => TradingMode::Paper,
            "live" => TradingMode::Live,
            other => bail!("ARBIVIEW_TRADING_MODE must be paper or live, got {other}"),
        };
        let binance = credentials("BINANCE");
        let bybit = credentials("BYBIT");
        if trading_mode == TradingMode::Live && (binance.is_none() || bybit.is_none()) {
            bail!("live mode requires BINANCE_API_KEY/SECRET and BYBIT_API_KEY/SECRET");
        }
        Ok(Self {
            port: std::env::var("PORT")
                .ok()
                .and_then(|x| x.parse().ok())
                .unwrap_or(8080),
            cmc_api_key: std::env::var("CMC_API_KEY").unwrap_or_default(),
            binance,
            bybit,
            trading_mode,
            web_origin: std::env::var("WEB_ORIGIN")
                .unwrap_or_else(|_| "http://localhost:3000".into()),
            max_slippage_bps: std::env::var("MAX_SLIPPAGE_BPS")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(50),
            position_tolerance_usdt: std::env::var("POSITION_TOLERANCE_USDT")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(10.0),
            telegram: telegram_config()?,
        })
    }
}

fn credentials(prefix: &str) -> Option<ExchangeCredentials> {
    Some(ExchangeCredentials {
        api_key: std::env::var(format!("{prefix}_API_KEY")).ok()?,
        api_secret: std::env::var(format!("{prefix}_API_SECRET")).ok()?,
    })
}

fn telegram_config() -> Result<Option<TelegramConfig>> {
    let enabled = std::env::var("TELEGRAM_ENABLED")
        .unwrap_or_else(|_| "false".into())
        .parse::<bool>()
        .map_err(|_| anyhow::anyhow!("TELEGRAM_ENABLED must be true or false"))?;
    if !enabled {
        return Ok(None);
    }
    let token = std::env::var("TELEGRAM_BOT_TOKEN")
        .map_err(|_| anyhow::anyhow!("TELEGRAM_BOT_TOKEN is required when Telegram is enabled"))?;
    let chat_id = std::env::var("TELEGRAM_CHAT_ID")
        .map_err(|_| anyhow::anyhow!("TELEGRAM_CHAT_ID is required when Telegram is enabled"))?
        .parse()
        .map_err(|_| anyhow::anyhow!("TELEGRAM_CHAT_ID must be an integer"))?;
    let topic_id = std::env::var("TELEGRAM_TOPIC_ID")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .map(|value| {
            value
                .parse()
                .map_err(|_| anyhow::anyhow!("TELEGRAM_TOPIC_ID must be an integer"))
        })
        .transpose()?;
    let authorized_users = std::env::var("TELEGRAM_AUTHORIZED_USERS")
        .unwrap_or_default()
        .split(',')
        .filter(|value| !value.trim().is_empty())
        .map(|value| {
            value
                .trim()
                .parse()
                .map_err(|_| anyhow::anyhow!("TELEGRAM_AUTHORIZED_USERS must contain integers"))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Some(TelegramConfig {
        token,
        chat_id,
        topic_id,
        authorized_users,
    }))
}
