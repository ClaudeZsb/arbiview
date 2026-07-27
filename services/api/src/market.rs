use crate::{config::Config, models::*};
use anyhow::{anyhow, bail, Context, Result};
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
type VolumeCache = Option<(Instant, HashMap<String, f64>)>;
type SpreadAverageCache = HashMap<String, (Instant, f64, f64)>;

fn latest_rate_at(rates: &[(i64, f64)], timestamp: i64) -> Option<f64> {
    rates
        .iter()
        .rev()
        .find(|(rate_timestamp, _)| *rate_timestamp <= timestamp)
        .map(|(_, rate)| *rate)
}

fn current_hour_start_millis() -> i64 {
    let now = chrono::Utc::now().timestamp_millis();
    now.div_euclid(60 * 60 * 1_000) * 60 * 60 * 1_000
}

#[derive(Clone)]
pub struct MarketService {
    client: Client,
    config: Config,
    cache: Arc<RwLock<Option<(Instant, OpportunitiesResponse)>>>,
    scan_lock: Arc<Mutex<()>>,
    quote_cache: Arc<RwLock<QuoteCache>>,
    quote_lock: Arc<Mutex<()>>,
    binance_volume_cache: Arc<RwLock<VolumeCache>>,
    binance_volume_lock: Arc<Mutex<()>>,
    spread_average_cache: Arc<RwLock<SpreadAverageCache>>,
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
            binance_volume_cache: Arc::new(RwLock::new(None)),
            binance_volume_lock: Arc::new(Mutex::new(())),
            spread_average_cache: Arc::new(RwLock::new(HashMap::new())),
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
        let (tokens_result, markets_result) = tokio::join!(self.top_tokens(), async {
            tokio::try_join!(self.binance_markets(), self.bybit_markets())
        });
        let (binance, bybit) = markets_result?;
        let cmc = match tokens_result {
            Ok(tokens) => tokens
                .into_iter()
                .map(|token| (token.symbol.clone(), token))
                .collect::<HashMap<_, _>>(),
            Err(error) => {
                tracing::warn!("CMC tags unavailable; scanning all exchange contracts: {error:#}");
                HashMap::new()
            }
        };
        let b_map: HashMap<String, Leg> =
            binance.into_iter().map(|x| (x.base.clone(), x)).collect();
        let y_map: HashMap<String, Leg> = bybit.into_iter().map(|x| (x.base.clone(), x)).collect();
        let mut opportunities = vec![];
        let mut spread_opportunities = vec![];
        let mut matched = HashSet::new();
        for (symbol, b) in &b_map {
            if let Some(y) = y_map.get(symbol) {
                if is_tradefi(b) != is_tradefi(y) {
                    tracing::warn!(
                        symbol,
                        "skipping cross-exchange symbol with incompatible asset classes"
                    );
                    continue;
                }
                matched.insert(symbol);
                let token = classify_token(symbol, cmc.get(symbol), b, y);
                if let Some(x) = make_opportunity(&token, b, y) {
                    opportunities.push(x);
                }
                if let Some(x) = make_opportunity(&token, y, b) {
                    opportunities.push(x);
                }
                let mut spread_candidates = [
                    make_spread_opportunity(&token, b, y),
                    make_spread_opportunity(&token, y, b),
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
        opportunities.truncate(10);
        let mut average_tasks = tokio::task::JoinSet::new();
        for (index, opportunity) in opportunities.iter().enumerate() {
            let service = self.clone();
            let symbol = opportunity.long.symbol.clone();
            average_tasks.spawn(async move {
                (
                    index,
                    symbol.clone(),
                    service.hourly_spread_averages(&symbol).await,
                )
            });
        }
        while let Some(result) = average_tasks.join_next().await {
            match result {
                Ok((index, _, Ok((binance_long, bybit_long)))) => {
                    let opportunity = &mut opportunities[index];
                    opportunity.average_spread_24h = if opportunity.long.exchange == "Binance" {
                        binance_long
                    } else {
                        bybit_long
                    };
                    opportunity.spread_vs_average =
                        opportunity.spread - opportunity.average_spread_24h;
                    opportunity.break_even_hours = break_even_hours(
                        opportunity.funding_per_hour,
                        opportunity.fees,
                        opportunity.spread_vs_average,
                    );
                }
                Ok((_, symbol, Err(error))) => {
                    tracing::warn!(symbol, "24h hourly spread average unavailable: {error:#}");
                }
                Err(error) => tracing::warn!("24h spread-average task failed: {error}"),
            }
        }
        spread_opportunities.sort_by(|a, b| b.spread.total_cmp(&a.spread));
        let result = OpportunitiesResponse {
            opportunities,
            spread_opportunities,
            updated_at: chrono::Utc::now().timestamp_millis(),
            universe_size: b_map.len() + y_map.len() - matched.len(),
            matched_pairs: matched.len(),
            assumptions: FeeAssumptions {
                binance_taker_fee: BINANCE_FEE,
                bybit_taker_fee: BYBIT_FEE,
            },
        };
        *self.cache.write().await = Some((Instant::now(), result.clone()));
        Ok(result)
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
        let (binance_marks, binance_book, binance_last, bybit): (Value, Value, Value, Value) = tokio::try_join!(
            self.get_json("https://fapi.binance.com/fapi/v1/premiumIndex".into()),
            self.get_json("https://fapi.binance.com/fapi/v1/ticker/bookTicker".into()),
            self.get_json("https://fapi.binance.com/fapi/v1/ticker/price".into()),
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
        let binance_last = binance_last
            .as_array()
            .unwrap_or(&vec![])
            .iter()
            .filter_map(|item| Some((item["symbol"].as_str()?.to_string(), parse(&item["price"])?)))
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
                let last_price = binance_last.get(symbol).copied().unwrap_or(mark_price);
                let (reference_price, uses_last_price) =
                    strategy_reference_price(mark_price, last_price);
                quotes.push(PositionQuote {
                    exchange: "Binance".into(),
                    symbol: symbol.into(),
                    mark_price,
                    last_price,
                    reference_price,
                    uses_last_price,
                    bid_price,
                    ask_price,
                    funding_rate,
                });
            }
        }
        for item in bybit["result"]["list"].as_array().unwrap_or(&vec![]) {
            if let (
                Some(symbol),
                Some(mark_price),
                Some(last_price),
                Some(bid_price),
                Some(ask_price),
            ) = (
                item["symbol"].as_str(),
                parse(&item["markPrice"]),
                parse(&item["lastPrice"]),
                parse(&item["bid1Price"]),
                parse(&item["ask1Price"]),
            ) {
                let (reference_price, uses_last_price) =
                    strategy_reference_price(mark_price, last_price);
                quotes.push(PositionQuote {
                    exchange: "Bybit".into(),
                    symbol: symbol.into(),
                    mark_price,
                    last_price,
                    reference_price,
                    uses_last_price,
                    bid_price,
                    ask_price,
                    funding_rate: parse(&item["fundingRate"]).unwrap_or(0.0),
                });
            }
        }
        *self.quote_cache.write().await = Some((Instant::now(), quotes.clone()));
        Ok(quotes)
    }

    pub async fn spread_history(&self, symbol: &str) -> Result<SpreadHistoryResponse> {
        let symbol = symbol.trim().to_ascii_uppercase();
        if !symbol.ends_with("USDT") || !symbol.chars().all(|ch| ch.is_ascii_alphanumeric()) {
            bail!("symbol must be an alphanumeric USDT perpetual symbol");
        }
        let now = chrono::Utc::now().timestamp_millis();
        let start = now - 48 * 60 * 60 * 1000;
        let (binance, bybit): (Value, Value) = tokio::try_join!(
            self.get_json(format!(
                "https://fapi.binance.com/fapi/v1/klines?symbol={symbol}&interval=1h&limit=25"
            )),
            self.get_json(format!(
                "https://api.bybit.com/v5/market/kline?category=linear&symbol={symbol}&interval=60&limit=25"
            ))
        )?;
        let (binance_funding_result, bybit_funding_result) = tokio::join!(
            self.get_json(format!(
                "https://fapi.binance.com/fapi/v1/fundingRate?symbol={symbol}&startTime={start}&limit=1000"
            )),
            self.get_json(format!(
                "https://api.bybit.com/v5/market/funding/history?category=linear&symbol={symbol}&startTime={start}&endTime={now}&limit=200"
            )),
        );
        let binance_funding = binance_funding_result.unwrap_or_else(|error| {
            tracing::warn!(symbol, "Binance funding history unavailable: {error:#}");
            Value::Null
        });
        let bybit_funding = bybit_funding_result.unwrap_or_else(|error| {
            tracing::warn!(symbol, "Bybit funding history unavailable: {error:#}");
            Value::Null
        });
        let binance_closes = binance
            .as_array()
            .unwrap_or(&vec![])
            .iter()
            .filter_map(|candle| Some((candle[0].as_i64()?, parse(&candle[4])?)))
            .collect::<HashMap<_, _>>();
        let bybit_closes = bybit["result"]["list"]
            .as_array()
            .unwrap_or(&vec![])
            .iter()
            .filter_map(|candle| {
                Some((candle[0].as_str()?.parse::<i64>().ok()?, parse(&candle[4])?))
            })
            .collect::<HashMap<_, _>>();
        let mut binance_rates = binance_funding
            .as_array()
            .unwrap_or(&vec![])
            .iter()
            .filter_map(|item| Some((item["fundingTime"].as_i64()?, parse(&item["fundingRate"])?)))
            .collect::<Vec<_>>();
        let mut bybit_rates = bybit_funding["result"]["list"]
            .as_array()
            .unwrap_or(&vec![])
            .iter()
            .filter_map(|item| {
                Some((
                    item["fundingRateTimestamp"].as_str()?.parse::<i64>().ok()?,
                    parse(&item["fundingRate"])?,
                ))
            })
            .collect::<Vec<_>>();
        binance_rates.sort_by_key(|(timestamp, _)| *timestamp);
        bybit_rates.sort_by_key(|(timestamp, _)| *timestamp);
        let mut points = binance_closes
            .into_iter()
            .filter_map(|(timestamp, binance_close)| {
                let bybit_close = *bybit_closes.get(&timestamp)?;
                (binance_close > 0.0).then_some(SpreadHistoryPoint {
                    timestamp,
                    binance_close,
                    bybit_close,
                    spread_percent: (bybit_close - binance_close) / binance_close * 100.0,
                    binance_funding_rate: latest_rate_at(&binance_rates, timestamp),
                    bybit_funding_rate: latest_rate_at(&bybit_rates, timestamp),
                })
            })
            .collect::<Vec<_>>();
        points.sort_by_key(|point| point.timestamp);
        points.retain(|point| point.timestamp < current_hour_start_millis());
        if points.len() > 24 {
            points.drain(..points.len() - 24);
        }
        Ok(SpreadHistoryResponse {
            symbol,
            formula: "(Bybit - Binance) / Binance × 100%".into(),
            funding_note: "每小时展示截至该时刻最近一次已知结算费率；并非每小时发生结算".into(),
            points,
        })
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
                rank: Some(x.cmc_rank),
                tags: vec!["cmc200".into()],
            })
            .collect())
    }

    async fn binance_markets(&self) -> Result<Vec<Leg>> {
        let base = "https://fapi.binance.com";
        let volumes = match self.binance_volumes().await {
            Ok(volumes) => volumes,
            Err(error) => {
                tracing::warn!("Binance 24h volume unavailable: {error:#}");
                self.binance_volume_cache
                    .read()
                    .await
                    .as_ref()
                    .map(|(_, volumes)| volumes.clone())
                    .unwrap_or_default()
            }
        };
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
                && matches!(
                    item["contractType"].as_str(),
                    Some("PERPETUAL" | "TRADIFI_PERPETUAL")
                )
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
                    let is_tradefi = item["contractType"] == "TRADIFI_PERPETUAL"
                        || item["underlyingSubType"]
                            .as_array()
                            .is_some_and(|values| values.iter().any(|value| value == "TradFi"));
                    meta.insert(symbol, (base, step, is_tradefi));
                }
            }
        }
        Ok(premium
            .as_array()
            .unwrap_or(&vec![])
            .iter()
            .filter_map(|x| {
                let symbol = x["symbol"].as_str()?;
                let (base, step, is_tradefi) = *meta.get(symbol)?;
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
                    tags: if is_tradefi {
                        vec!["tradefi".into()]
                    } else {
                        vec![]
                    },
                    volume_24h_usdt: volumes.get(symbol).copied().unwrap_or(0.0),
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
                    let is_tradefi =
                        matches!(x["symbolType"].as_str(), Some("stock" | "commodity"));
                    meta.insert(symbol, (base, interval, step, is_tradefi));
                }
            }
        }
        Ok(tickers["result"]["list"]
            .as_array()
            .unwrap_or(&vec![])
            .iter()
            .filter_map(|x| {
                let symbol = x["symbol"].as_str()?;
                let (base, interval, step, is_tradefi) = *meta.get(symbol)?;
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
                    tags: if is_tradefi {
                        vec!["tradefi".into()]
                    } else {
                        vec![]
                    },
                    volume_24h_usdt: parse(&x["turnover24h"]).unwrap_or(0.0),
                })
            })
            .collect())
    }

    async fn hourly_spread_averages(&self, symbol: &str) -> Result<(f64, f64)> {
        if let Some((at, binance_long, bybit_long)) =
            self.spread_average_cache.read().await.get(symbol)
        {
            if at.elapsed() < Duration::from_secs(60 * 60) {
                return Ok((*binance_long, *bybit_long));
            }
        }
        let (binance, bybit): (Value, Value) = tokio::try_join!(
            self.get_json(format!(
                "https://fapi.binance.com/fapi/v1/klines?symbol={symbol}&interval=1h&limit=25"
            )),
            self.get_json(format!(
                "https://api.bybit.com/v5/market/kline?category=linear&symbol={symbol}&interval=60&limit=25"
            ))
        )?;
        let binance_closes = binance
            .as_array()
            .unwrap_or(&vec![])
            .iter()
            .filter_map(|candle| Some((candle[0].as_i64()?, parse(&candle[4])?)))
            .collect::<HashMap<_, _>>();
        let mut aligned = bybit["result"]["list"]
            .as_array()
            .unwrap_or(&vec![])
            .iter()
            .filter_map(|candle| {
                let timestamp = candle[0].as_str()?.parse::<i64>().ok()?;
                let bybit_close = parse(&candle[4])?;
                let binance_close = *binance_closes.get(&timestamp)?;
                (binance_close > 0.0 && bybit_close > 0.0).then_some((
                    timestamp,
                    (bybit_close - binance_close) / binance_close,
                    (binance_close - bybit_close) / bybit_close,
                ))
            })
            .collect::<Vec<_>>();
        aligned.sort_by_key(|point| point.0);
        aligned.retain(|point| point.0 < current_hour_start_millis());
        if aligned.len() > 24 {
            aligned.drain(..aligned.len() - 24);
        }
        if aligned.is_empty() {
            bail!("no aligned Binance/Bybit hourly candles");
        }
        let count = aligned.len() as f64;
        let binance_long = aligned.iter().map(|point| point.1).sum::<f64>() / count;
        let bybit_long = aligned.iter().map(|point| point.2).sum::<f64>() / count;
        self.spread_average_cache.write().await.insert(
            symbol.to_string(),
            (Instant::now(), binance_long, bybit_long),
        );
        Ok((binance_long, bybit_long))
    }

    async fn binance_volumes(&self) -> Result<HashMap<String, f64>> {
        if let Some((at, volumes)) = self.binance_volume_cache.read().await.as_ref() {
            if at.elapsed() < Duration::from_secs(300) {
                return Ok(volumes.clone());
            }
        }
        let _guard = self.binance_volume_lock.lock().await;
        if let Some((at, volumes)) = self.binance_volume_cache.read().await.as_ref() {
            if at.elapsed() < Duration::from_secs(300) {
                return Ok(volumes.clone());
            }
        }
        let response = self
            .get_json("https://fapi.binance.com/fapi/v1/ticker/24hr".into())
            .await?;
        let volumes = response
            .as_array()
            .unwrap_or(&vec![])
            .iter()
            .filter_map(|item| {
                Some((
                    item["symbol"].as_str()?.to_string(),
                    parse(&item["quoteVolume"])?,
                ))
            })
            .collect::<HashMap<_, _>>();
        *self.binance_volume_cache.write().await = Some((Instant::now(), volumes.clone()));
        Ok(volumes)
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

fn strategy_reference_price(mark_price: f64, last_price: f64) -> (f64, bool) {
    let uses_last_price = last_price > 0.0 && (mark_price - last_price).abs() / last_price > 0.001;
    if uses_last_price {
        (last_price, true)
    } else {
        (mark_price, false)
    }
}

fn make_opportunity(token: &Token, long: &Leg, short: &Leg) -> Option<Opportunity> {
    let funding_per_hour = short.rate / short.interval_hours - long.rate / long.interval_hours;
    if funding_per_hour <= 0.0 {
        return None;
    }
    Some(build_opportunity(token, long, short, funding_per_hour))
}

fn classify_token(symbol: &str, cmc: Option<&Token>, binance: &Leg, bybit: &Leg) -> Token {
    let mut tags = Vec::new();
    let tradefi = is_tradefi(binance) && is_tradefi(bybit);
    if tradefi {
        tags.push("tradefi".into());
    } else if cmc.is_some() {
        tags.push("cmc200".into());
    }
    Token {
        symbol: symbol.into(),
        name: if tradefi {
            symbol.into()
        } else {
            cmc.map(|token| token.name.clone())
                .unwrap_or_else(|| symbol.into())
        },
        rank: if tradefi {
            None
        } else {
            cmc.and_then(|token| token.rank)
        },
        tags,
    }
}

fn is_tradefi(leg: &Leg) -> bool {
    leg.tags.iter().any(|tag| tag == "tradefi")
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
    let average_spread_24h = spread;
    let spread_vs_average = 0.0;
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
        average_spread_24h,
        spread_vs_average,
        fees,
        break_even_hours: break_even_hours(funding_per_hour, fees, spread_vs_average),
    }
}

fn break_even_hours(funding_per_hour: f64, fees: f64, spread_vs_average: f64) -> f64 {
    if funding_per_hour > 0.0 {
        (fees - spread_vs_average).max(0.0) / funding_per_hour
    } else {
        0.0
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_cmc_and_tradefi_independently() {
        let cmc = Token {
            symbol: "BTC".into(),
            name: "Bitcoin".into(),
            rank: Some(1),
            tags: vec!["cmc200".into()],
        };
        let crypto = test_leg("Binance", vec![]);
        let bybit_crypto = test_leg("Bybit", vec![]);
        let btc = classify_token("BTC", Some(&cmc), &crypto, &bybit_crypto);
        assert_eq!(btc.tags, vec!["cmc200"]);
        assert_eq!(btc.rank, Some(1));

        let stock = test_leg("Binance", vec!["tradefi".into()]);
        let bybit_stock = test_leg("Bybit", vec!["tradefi".into()]);
        let micron = classify_token("MU", None, &stock, &bybit_stock);
        assert_eq!(micron.tags, vec!["tradefi"]);
        assert_eq!(micron.rank, None);
    }

    #[test]
    fn strategy_price_falls_back_to_last_above_point_one_percent() {
        assert_eq!(strategy_reference_price(100.1, 100.0), (100.1, false));
        assert_eq!(strategy_reference_price(100.1001, 100.0), (100.0, true));
        assert_eq!(strategy_reference_price(99.8999, 100.0), (100.0, true));
    }

    #[test]
    fn break_even_uses_spread_deviation_from_24h_average() {
        assert_eq!(break_even_hours(0.001, 0.0021, 0.01), 0.0);
        assert!((break_even_hours(0.001, 0.0021, -0.01) - 12.1).abs() < 1e-9);
    }

    fn test_leg(exchange: &str, tags: Vec<String>) -> Leg {
        Leg {
            exchange: exchange.into(),
            base: "TEST".into(),
            symbol: "TESTUSDT".into(),
            bid: 1.0,
            ask: 1.0,
            mark: 1.0,
            rate: 0.0,
            interval_hours: 8.0,
            next_funding_time: 0,
            qty_step: 0.01,
            tags,
            volume_24h_usdt: 1_000_000.0,
        }
    }
}
