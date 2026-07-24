use anyhow::{bail, Result};

#[derive(Clone)]
pub struct ExchangeCredentials {
    pub api_key: String,
    pub api_secret: String,
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
        })
    }
}

fn credentials(prefix: &str) -> Option<ExchangeCredentials> {
    Some(ExchangeCredentials {
        api_key: std::env::var(format!("{prefix}_API_KEY")).ok()?,
        api_secret: std::env::var(format!("{prefix}_API_SECRET")).ok()?,
    })
}
