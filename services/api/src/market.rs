use crate::{config::Config, models::*};
use anyhow::{anyhow, Context, Result};
use reqwest::Client;
use serde_json::Value;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::{Mutex, RwLock};

const BINANCE_FEE: f64 = 0.0005;
const BYBIT_FEE: f64 = 0.00055;
const YEAR_HOURS: f64 = 365.0 * 24.0;
type QuoteCache = Option<(Instant, Vec<PositionQuote>)>;

#[derive(Clone)]
pub struct MarketService {
    client: Client,
    config: Config,
    cache: Arc<RwLock<Option<(Instant, OpportunitiesResponse)>>>,
    scan_lock: Arc<Mutex<()>>,
    quote_cache: Arc<RwLock<QuoteCache>>,
    quote_lock: Arc<Mutex<()>>,
}

impl MarketService {
    pub fn new(config: Config) -> Result<Self> {
        let client = Client::builder()
            .timeout(Duration::from_secs(15))
            .user_agent("ArbiView/0.1")
            .build()?;
        Ok(Self {
            client,
            config,
            cache: Arc::new(RwLock::new(None)),
            scan_lock: Arc::new(Mutex::new(())),
            quote_cache: Arc::new(RwLock::new(None)),
            quote_lock: Arc::new(Mutex::new(())),
        })
    }

    pub async fn opportunities(&self) -> Result<OpportunitiesResponse> {
        if let Some((at, value)) = self.cache.read().await.as_ref() {
            // Keep the cache slightly shorter than the UI's 20-second polling
            // interval so each scheduled scan receives a fresh snapshot.
            if at.elapsed() < Duration::from_secs(15) {
                return Ok(value.clone());
            }
        }
        let _scan_guard = self.scan_lock.lock().await;
        if let Some((at, value)) = self.cache.read().await.as_ref() {
            if at.elapsed() < Duration::from_secs(15) {
                return Ok(value.clone());
            }
        }
        let (tokens, binance, bybit) = tokio::try_join!(
            self.top_tokens(),
            self.binance_markets(),
            self.bybit_markets()
        )?;
        let allowed: HashMap<String, Token> = tokens
            .iter()
            .cloned()
            .map(|x| (x.symbol.clone(), x))
            .collect();
        let b_map: HashMap<String, Leg> =
            binance.into_iter().map(|x| (x.base.clone(), x)).collect();
        let y_map: HashMap<String, Leg> = bybit.into_iter().map(|x| (x.base.clone(), x)).collect();
        let mut opportunities = vec![];
        let mut spread_opportunities = vec![];
        let mut matched = HashSet::new();
        for (symbol, token) in &allowed {
            if let (Some(b), Some(y)) = (b_map.get(symbol), y_map.get(symbol)) {
                matched.insert(symbol);
                if let Some(x) = make_opportunity(token, b, y) {
                    opportunities.push(x);
                }
                if let Some(x) = make_opportunity(token, y, b) {
                    opportunities.push(x);
                }
                let mut spread_candidates = [
                    make_spread_opportunity(token, b, y),
                    make_spread_opportunity(token, y, b),
                ]
                .into_iter()
                .flatten()
                .collect::<Vec<_>>();
                spread_candidates.sort_by(|a, b| b.spread.total_cmp(&a.spread));
                if let Some(best) = spread_candidates.into_iter().next() {
                    spread_opportunities.push(best);
                }
            }
        }
        opportunities.sort_by(|a, b| b.apy.total_cmp(&a.apy));
        spread_opportunities.sort_by(|a, b| b.spread.total_cmp(&a.spread));
        let result = OpportunitiesResponse {
            opportunities,
            spread_opportunities,
            updated_at: chrono::Utc::now().timestamp_millis(),
            universe_size: tokens.len(),
            matched_pairs: matched.len(),
            assumptions: FeeAssumptions {
                binance_taker_fee: BINANCE_FEE,
                bybit_taker_fee: BYBIT_FEE,
            },
        };
        *self.cache.write().await = Some((Instant::now(), result.clone()));
        Ok(result)
    }

    pub async fn funding_rates(&self) -> Result<HashMap<String, f64>> {
        Ok(self
            .position_quotes()
            .await?
            .into_iter()
            .map(|quote| {
                (
                    format!("{}:{}", quote.exchange, quote.symbol),
                    quote.funding_rate,
                )
            })
            .collect())
    }

    pub async fn position_quotes(&self) -> Result<Vec<PositionQuote>> {
        if let Some((at, quotes)) = self.quote_cache.read().await.as_ref() {
            if at.elapsed() < Duration::from_millis(1500) {
                return Ok(quotes.clone());
            }
        }
        let _quote_guard = self.quote_lock.lock().await;
        if let Some((at, quotes)) = self.quote_cache.read().await.as_ref() {
            if at.elapsed() < Duration::from_millis(1500) {
                return Ok(quotes.clone());
            }
        }
        let (binance_marks, binance_book, bybit): (Value, Value, Value) = tokio::try_join!(
            self.get_json("https://fapi.binance.com/fapi/v1/premiumIndex".into()),
            self.get_json("https://fapi.binance.com/fapi/v1/ticker/bookTicker".into()),
            self.get_json("https://api.bybit.com/v5/market/tickers?category=linear".into())
        )?;
        let binance_funding = binance_marks
            .as_array()
            .unwrap_or(&vec![])
            .iter()
            .filter_map(|item| {
                Some((
                    item["symbol"].as_str()?.to_string(),
                    (
                        parse(&item["markPrice"])?,
                        parse(&item["lastFundingRate"]).unwrap_or(0.0),
                    ),
                ))
            })
            .collect::<HashMap<_, _>>();
        let mut quotes = Vec::new();
        for item in binance_book.as_array().unwrap_or(&vec![]) {
            if let (Some(symbol), Some(bid_price), Some(ask_price)) = (
                item["symbol"].as_str(),
                parse(&item["bidPrice"]),
                parse(&item["askPrice"]),
            ) {
                let (mark_price, funding_rate) =
                    binance_funding.get(symbol).copied().unwrap_or((0.0, 0.0));
                quotes.push(PositionQuote {
                    exchange: "Binance".into(),
                    symbol: symbol.into(),
                    mark_price,
                    bid_price,
                    ask_price,
                    funding_rate,
                });
            }
        }
        for item in bybit["result"]["list"].as_array().unwrap_or(&vec![]) {
            if let (Some(symbol), Some(mark_price), Some(bid_price), Some(ask_price)) = (
                item["symbol"].as_str(),
                parse(&item["markPrice"]),
                parse(&item["bid1Price"]),
                parse(&item["ask1Price"]),
            ) {
                quotes.push(PositionQuote {
                    exchange: "Bybit".into(),
                    symbol: symbol.into(),
                    mark_price,
                    bid_price,
                    ask_price,
                    funding_rate: parse(&item["fundingRate"]).unwrap_or(0.0),
                });
            }
        }
        *self.quote_cache.write().await = Some((Instant::now(), quotes.clone()));
        Ok(quotes)
    }

    pub async fn trading_legs(&self) -> Result<Vec<Leg>> {
        let (binance, bybit) = tokio::try_join!(self.binance_markets(), self.bybit_markets())?;
        Ok(binance.into_iter().chain(bybit).collect())
    }

    async fn top_tokens(&self) -> Result<Vec<Token>> {
        let base = if self.config.cmc_api_key.is_empty() {
            "https://pro-api.coinmarketcap.com/public-api/v3/cryptocurrency/listings/latest"
        } else {
            "https://pro-api.coinmarketcap.com/v3/cryptocurrency/listings/latest"
        };
        let mut request = self.client.get(base).query(&[
            ("start", "1"),
            ("limit", "200"),
            ("sort", "market_cap"),
            ("sort_dir", "desc"),
            ("convert", "USD"),
        ]);
        if !self.config.cmc_api_key.is_empty() {
            request = request.header("X-CMC_PRO_API_KEY", &self.config.cmc_api_key);
        }
        let response = request
            .send()
            .await
            .context("CoinMarketCap request failed")?
            .error_for_status()
            .context("CoinMarketCap returned an error")?
            .json::<CmcResponse>()
            .await
            .context("invalid CoinMarketCap response")?;
        Ok(response
            .data
            .into_iter()
            .map(|x| Token {
                symbol: x.symbol.to_uppercase(),
                name: x.name,
                rank: x.cmc_rank,
            })
            .collect())
    }

    async fn binance_markets(&self) -> Result<Vec<Leg>> {
        let base = "https://fapi.binance.com";
        let (premium, book, exchange, funding): (Value, Value, Value, Value) = tokio::try_join!(
            self.get_json(format!("{base}/fapi/v1/premiumIndex")),
            self.get_json(format!("{base}/fapi/v1/ticker/bookTicker")),
            self.get_json(format!("{base}/fapi/v1/exchangeInfo")),
            self.get_json(format!("{base}/fapi/v1/fundingInfo")),
        )?;
        let empty = Vec::new();
        let books: HashMap<&str, &Value> = book
            .as_array()
            .unwrap_or(&empty)
            .iter()
            .filter_map(|x| Some((x["symbol"].as_str()?, x)))
            .collect();
        let intervals: HashMap<&str, f64> = funding
            .as_array()
            .unwrap_or(&empty)
            .iter()
            .filter_map(|x| Some((x["symbol"].as_str()?, x["fundingIntervalHours"].as_f64()?)))
            .collect();
        let mut meta = HashMap::new();
        for item in exchange["symbols"].as_array().unwrap_or(&empty) {
            if item["quoteAsset"] == "USDT"
                && item["contractType"] == "PERPETUAL"
                && item["status"] == "TRADING"
            {
                let step = item["filters"]
                    .as_array()
                    .and_then(|f| f.iter().find(|v| v["filterType"] == "LOT_SIZE"))
                    .and_then(|v| v["stepSize"].as_str())
                    .and_then(|x| x.parse().ok())
                    .unwrap_or(0.001);
                if let (Some(symbol), Some(base)) =
                    (item["symbol"].as_str(), item["baseAsset"].as_str())
                {
                    meta.insert(symbol, (base, step));
                }
            }
        }
        Ok(premium
            .as_array()
            .unwrap_or(&vec![])
            .iter()
            .filter_map(|x| {
                let symbol = x["symbol"].as_str()?;
                let (base, step) = *meta.get(symbol)?;
                let b = books.get(symbol)?;
                Some(Leg {
                    exchange: "Binance".into(),
                    base: base.into(),
                    symbol: symbol.into(),
                    bid: parse(&b["bidPrice"])?,
                    ask: parse(&b["askPrice"])?,
                    mark: parse(&x["markPrice"])?,
                    rate: parse(&x["lastFundingRate"]).unwrap_or(0.0),
                    interval_hours: intervals.get(symbol).copied().unwrap_or(8.0),
                    next_funding_time: x["nextFundingTime"].as_i64()?,
                    qty_step: step,
                })
            })
            .collect())
    }

    async fn bybit_markets(&self) -> Result<Vec<Leg>> {
        let (tickers, instruments): (Value, Value) = tokio::try_join!(
            self.get_json("https://api.bybit.com/v5/market/tickers?category=linear".into()),
            self.get_json(
                "https://api.bybit.com/v5/market/instruments-info?category=linear&limit=1000"
                    .into()
            ),
        )?;
        let empty = Vec::new();
        let mut meta = HashMap::new();
        for x in instruments["result"]["list"].as_array().unwrap_or(&empty) {
            if x["quoteCoin"] == "USDT"
                && x["contractType"] == "LinearPerpetual"
                && x["status"] == "Trading"
            {
                if let (Some(symbol), Some(base)) = (x["symbol"].as_str(), x["baseCoin"].as_str()) {
                    let interval = parse(&x["fundingInterval"]).unwrap_or(480.0) / 60.0;
                    let step = x["lotSizeFilter"]["qtyStep"]
                        .as_str()
                        .and_then(|v| v.parse().ok())
                        .unwrap_or(0.001);
                    meta.insert(symbol, (base, interval, step));
                }
            }
        }
        Ok(tickers["result"]["list"]
            .as_array()
            .unwrap_or(&vec![])
            .iter()
            .filter_map(|x| {
                let symbol = x["symbol"].as_str()?;
                let (base, interval, step) = *meta.get(symbol)?;
                Some(Leg {
                    exchange: "Bybit".into(),
                    base: base.into(),
                    symbol: symbol.into(),
                    bid: parse(&x["bid1Price"])?,
                    ask: parse(&x["ask1Price"])?,
                    mark: parse(&x["markPrice"])?,
                    rate: parse(&x["fundingRate"]).unwrap_or(0.0),
                    interval_hours: interval,
                    next_funding_time: x["nextFundingTime"].as_str()?.parse().ok()?,
                    qty_step: step,
                })
            })
            .collect())
    }

    async fn get_json(&self, url: String) -> Result<Value> {
        self.client
            .get(&url)
            .send()
            .await
            .with_context(|| format!("{} request failed", host(&url)))?
            .error_for_status()
            .with_context(|| format!("{} returned an error", host(&url)))?
            .json()
            .await
            .map_err(|e| anyhow!("invalid {} response: {e}", host(&url)))
    }
}

fn make_opportunity(token: &Token, long: &Leg, short: &Leg) -> Option<Opportunity> {
    let funding_per_hour = short.rate / short.interval_hours - long.rate / long.interval_hours;
    if funding_per_hour <= 0.0 {
        return None;
    }
    Some(build_opportunity(token, long, short, funding_per_hour))
}

fn make_spread_opportunity(token: &Token, long: &Leg, short: &Leg) -> Option<Opportunity> {
    let spread = (short.bid - long.ask) / long.ask;
    if spread <= 0.005 {
        return None;
    }
    let funding_per_hour = short.rate / short.interval_hours - long.rate / long.interval_hours;
    Some(build_opportunity(token, long, short, funding_per_hour))
}

fn build_opportunity(token: &Token, long: &Leg, short: &Leg, funding_per_hour: f64) -> Opportunity {
    let spread = (short.bid - long.ask) / long.ask;
    let fee = |exchange: &str| {
        if exchange == "Binance" {
            BINANCE_FEE
        } else {
            BYBIT_FEE
        }
    };
    let fees = 2.0 * (fee(&long.exchange) + fee(&short.exchange));
    Opportunity {
        id: format!("{}-{}-{}", token.symbol, long.exchange, short.exchange),
        token: token.clone(),
        long: long.clone(),
        short: short.clone(),
        funding_per_hour,
        apy: funding_per_hour * YEAR_HOURS,
        spread,
        fees,
        break_even_hours: if funding_per_hour > 0.0 {
            (fees - spread).max(0.0) / funding_per_hour
        } else {
            0.0
        },
    }
}

fn parse(value: &Value) -> Option<f64> {
    value
        .as_str()
        .and_then(|x| x.parse().ok())
        .or_else(|| value.as_f64())
}

fn host(url: &str) -> &str {
    url.split('/').nth(2).unwrap_or("upstream")
}
