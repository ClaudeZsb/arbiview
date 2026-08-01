use crate::{config::Config, models::*};
use anyhow::{anyhow, bail, Context, Result};
use futures_util::{SinkExt, StreamExt};
use hmac::{Hmac, Mac};
use reqwest::Client;
use serde_json::Value;
use sha2::Sha256;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::{Mutex, RwLock, Semaphore};
use tokio_tungstenite::{connect_async, tungstenite::Message};

const BINANCE_FEE: f64 = 0.0005;
const BYBIT_FEE: f64 = 0.00055;
const YEAR_HOURS: f64 = 365.0 * 24.0;
const SPREAD_AVERAGE_HALF_LIFE_HOURS: f64 = 6.0;
const FUNDING_AVERAGE_HALF_LIFE_HOURS: f64 = SPREAD_AVERAGE_HALF_LIFE_HOURS;
const SPREAD_OPPORTUNITY_ABSOLUTE_THRESHOLD: f64 = 0.005;
const SPREAD_OPPORTUNITY_DEVIATION_THRESHOLD: f64 = 0.005;
const SPREAD_HISTORY_CONCURRENCY: usize = 8;
type QuoteCache = Option<(Instant, Vec<PositionQuote>)>;
type VolumeCache = Option<(Instant, HashMap<String, f64>)>;
type SpreadAverageCache = HashMap<String, (Instant, f64)>;
type FundingAverageCache = HashMap<String, (Instant, f64)>;
type BorrowRateCache = HashMap<String, (Instant, f64)>;
type BorrowAvailabilityCache = HashMap<String, (Instant, bool)>;
type RouteLegCache = HashMap<String, Leg>;
type LiveQuoteCache = HashMap<String, LiveQuote>;

#[derive(Clone, Copy)]
struct LiveQuote {
    at: Instant,
    bid: f64,
    ask: f64,
    bid_quantity: f64,
    ask_quantity: f64,
}

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
    funding_average_cache: Arc<RwLock<FundingAverageCache>>,
    borrow_rate_cache: Arc<RwLock<BorrowRateCache>>,
    borrow_availability_cache: Arc<RwLock<BorrowAvailabilityCache>>,
    route_leg_cache: Arc<RwLock<RouteLegCache>>,
    live_quote_cache: Arc<RwLock<LiveQuoteCache>>,
    live_quote_streams: Arc<Mutex<HashSet<String>>>,
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
            funding_average_cache: Arc::new(RwLock::new(HashMap::new())),
            borrow_rate_cache: Arc::new(RwLock::new(HashMap::new())),
            borrow_availability_cache: Arc::new(RwLock::new(HashMap::new())),
            route_leg_cache: Arc::new(RwLock::new(HashMap::new())),
            live_quote_cache: Arc::new(RwLock::new(HashMap::new())),
            live_quote_streams: Arc::new(Mutex::new(HashSet::new())),
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
        let (tokens_result, perpetual_result, binance_spot_result, bybit_spot_result) = tokio::join!(
            self.top_tokens(),
            async { tokio::try_join!(self.binance_markets(), self.bybit_markets()) },
            self.binance_spot_markets(),
            self.bybit_spot_markets()
        );
        let (binance, bybit) = perpetual_result?;
        let binance_spot = binance_spot_result.unwrap_or_else(|error| {
            tracing::warn!("Binance spot markets unavailable: {error:#}");
            Vec::new()
        });
        let bybit_spot = bybit_spot_result.unwrap_or_else(|error| {
            tracing::warn!("Bybit spot markets unavailable: {error:#}");
            Vec::new()
        });
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
        let b_spot_map: HashMap<String, Leg> = binance_spot
            .into_iter()
            .map(|x| (x.base.clone(), x))
            .collect();
        let y_spot_map: HashMap<String, Leg> = bybit_spot
            .into_iter()
            .map(|x| (x.base.clone(), x))
            .collect();
        let route_legs = b_map
            .values()
            .chain(y_map.values())
            .chain(b_spot_map.values())
            .chain(y_spot_map.values())
            .map(|leg| {
                (
                    route_leg_key(&leg.exchange, &leg.market, &leg.symbol),
                    leg.clone(),
                )
            })
            .collect();
        *self.route_leg_cache.write().await = route_legs;
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
                if let Some(x) = make_cross_candidate(&token, b, y) {
                    opportunities.push(x);
                }
                if let Some(spread_opportunity) = make_spread_opportunity(&token, b, y) {
                    spread_opportunities.push(spread_opportunity);
                }
            }
            if let Some(spot) = b_spot_map.get(symbol) {
                let token = classify_token(symbol, cmc.get(symbol), b, b);
                if let Some(opportunity) = make_spot_short_perpetual_long(&token, b, spot) {
                    opportunities.push(opportunity);
                }
                if let Some(opportunity) = make_spot_long_perpetual_short(&token, b, spot) {
                    opportunities.push(opportunity);
                }
            }
        }
        for (symbol, perpetual) in &y_map {
            if let Some(spot) = y_spot_map.get(symbol) {
                let token = classify_token(symbol, cmc.get(symbol), perpetual, perpetual);
                if let Some(opportunity) = make_spot_short_perpetual_long(&token, perpetual, spot) {
                    opportunities.push(opportunity);
                }
                if let Some(opportunity) = make_spot_long_perpetual_short(&token, perpetual, spot) {
                    opportunities.push(opportunity);
                }
            }
        }
        let mut borrow_tasks = tokio::task::JoinSet::new();
        for (index, opportunity) in opportunities.iter().enumerate() {
            if opportunity.short.market == "spot" {
                let service = self.clone();
                let exchange = opportunity.short.exchange.clone();
                let asset = opportunity.short.base.clone();
                borrow_tasks.spawn(async move {
                    (index, service.spot_borrow_rate(&exchange, &asset).await)
                });
            }
        }
        let mut unavailable_borrow_rates = HashSet::new();
        while let Some(result) = borrow_tasks.join_next().await {
            match result {
                Ok((index, Ok(rate))) => {
                    let opportunity = &mut opportunities[index];
                    opportunity.borrow_interest_per_hour = rate;
                    opportunity.funding_per_hour -= rate;
                    opportunity.apy = opportunity.funding_per_hour * YEAR_HOURS;
                }
                Ok((index, Err(error))) => {
                    tracing::warn!(
                        id = opportunities[index].id,
                        "spot borrow rate unavailable; excluding route: {error:#}"
                    );
                    unavailable_borrow_rates.insert(index);
                }
                Err(error) => tracing::warn!("spot borrow-rate task failed: {error}"),
            }
        }
        opportunities = opportunities
            .into_iter()
            .enumerate()
            .filter_map(|(index, opportunity)| {
                (!unavailable_borrow_rates.contains(&index) && opportunity.funding_per_hour > 0.0)
                    .then_some(opportunity)
            })
            .collect();
        let (mut cross_opportunities, mut spot_candidates): (Vec<_>, Vec<_>) = opportunities
            .into_iter()
            .partition(|opportunity| opportunity.route_type == "cross_perpetual");
        self.enrich_cross_funding_projections(&mut cross_opportunities)
            .await;
        cross_opportunities.retain(|opportunity| opportunity.funding_per_hour > 0.0);
        cross_opportunities.sort_by(|a, b| b.apy.total_cmp(&a.apy));
        cross_opportunities.truncate(10);
        spot_candidates.sort_by(|a, b| b.apy.total_cmp(&a.apy));
        let mut spot_opportunities = Vec::with_capacity(10);
        for opportunity in spot_candidates {
            if opportunity.short.exchange == "Binance" && opportunity.short.market == "spot" {
                match self.binance_borrow_available(&opportunity.short.base).await {
                    Ok(true) => {}
                    Ok(false) => continue,
                    Err(error) => {
                        tracing::warn!(
                            id = opportunity.id,
                            "Binance borrow availability unavailable; excluding route: {error:#}"
                        );
                        continue;
                    }
                }
            }
            spot_opportunities.push(opportunity);
            if spot_opportunities.len() == 10 {
                break;
            }
        }
        self.enrich_opportunity_averages(&mut cross_opportunities)
            .await;
        self.enrich_opportunity_averages(&mut spot_opportunities)
            .await;
        self.enrich_opportunity_averages(&mut spread_opportunities)
            .await;
        spread_opportunities.retain(|opportunity| {
            opportunity.spread_vs_average > SPREAD_OPPORTUNITY_DEVIATION_THRESHOLD
        });
        spread_opportunities.sort_by(|a, b| b.spread_vs_average.total_cmp(&a.spread_vs_average));
        let result = OpportunitiesResponse {
            opportunities: cross_opportunities,
            spot_opportunities,
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

    pub async fn position_market_metrics(&self, position: &Position) -> Result<(f64, f64, f64)> {
        self.opportunities().await?;
        let (long, short) = {
            let legs = self.route_leg_cache.read().await;
            let long = legs
                .get(&route_leg_key(
                    &position.long.exchange,
                    &position.long.market,
                    &position.long.symbol,
                ))
                .cloned()
                .ok_or_else(|| anyhow!("long-leg market data is unavailable"))?;
            let short = legs
                .get(&route_leg_key(
                    &position.short.exchange,
                    &position.short.market,
                    &position.short.symbol,
                ))
                .cloned()
                .ok_or_else(|| anyhow!("short-leg market data is unavailable"))?;
            (long, short)
        };
        let funding_per_hour = match (long.market.as_str(), short.market.as_str()) {
            ("perpetual", "perpetual") => {
                let (long_average, short_average) = tokio::try_join!(
                    self.weighted_funding_average(&long),
                    self.weighted_funding_average(&short)
                )?;
                cross_funding_projection(
                    &long,
                    &short,
                    chrono::Utc::now().timestamp_millis(),
                    long_average,
                    short_average,
                )
                .map(|(average, _)| average)
                .unwrap_or(0.0)
            }
            ("perpetual", "spot") => {
                let borrow_rate = self.spot_borrow_rate(&short.exchange, &short.base).await?;
                -long.rate / long.interval_hours - borrow_rate
            }
            ("spot", "perpetual") => short.rate / short.interval_hours,
            _ => bail!("unsupported held-route market combination"),
        };
        if long.ask <= 0.0 {
            bail!("long-leg ask price is unavailable");
        }
        let spread = (short.bid - long.ask) / long.ask;
        Ok((funding_per_hour, spread, funding_per_hour * YEAR_HOURS))
    }

    pub async fn refresh_opportunity_quotes(
        &self,
        opportunity: &Opportunity,
    ) -> Result<Opportunity> {
        let (long, short) = tokio::try_join!(
            self.refresh_leg_quote(&opportunity.long),
            self.refresh_leg_quote(&opportunity.short)
        )?;
        if long.ask <= 0.0 || short.bid <= 0.0 {
            bail!("executable bid/ask quote is unavailable");
        }
        let mut refreshed = opportunity.clone();
        refreshed.long = long;
        refreshed.short = short;
        refreshed.spread = (refreshed.short.bid - refreshed.long.ask) / refreshed.long.ask;
        Ok(refreshed)
    }

    pub async fn refresh_leg_quote(&self, leg: &Leg) -> Result<Leg> {
        let key = route_leg_key(&leg.exchange, &leg.market, &leg.symbol);
        if let Some(quote) = self.fresh_live_quote(&key).await {
            return Ok(apply_live_quote(leg, quote));
        }
        self.ensure_live_quote_stream(leg).await;
        // A newly-created stream normally publishes immediately. Waiting here
        // keeps the first protected order off REST without delaying later loops.
        for _ in 0..10 {
            tokio::time::sleep(Duration::from_millis(100)).await;
            if let Some(quote) = self.fresh_live_quote(&key).await {
                return Ok(apply_live_quote(leg, quote));
            }
        }
        tracing::warn!(
            exchange = leg.exchange,
            market = leg.market,
            symbol = leg.symbol,
            "websocket quote is not ready; using one REST bootstrap quote"
        );
        let refreshed = self.refresh_leg_quote_rest(leg).await?;
        self.live_quote_cache.write().await.insert(
            key,
            LiveQuote {
                at: Instant::now(),
                bid: refreshed.bid,
                ask: refreshed.ask,
                bid_quantity: refreshed.bid_quantity,
                ask_quantity: refreshed.ask_quantity,
            },
        );
        Ok(refreshed)
    }

    async fn fresh_live_quote(&self, key: &str) -> Option<LiveQuote> {
        self.live_quote_cache
            .read()
            .await
            .get(key)
            .copied()
            .filter(|quote| quote.at.elapsed() <= Duration::from_secs(5))
    }

    async fn ensure_live_quote_stream(&self, leg: &Leg) {
        let key = route_leg_key(&leg.exchange, &leg.market, &leg.symbol);
        let mut streams = self.live_quote_streams.lock().await;
        if !streams.insert(key.clone()) {
            return;
        }
        let leg = leg.clone();
        let cache = self.live_quote_cache.clone();
        tokio::spawn(async move {
            live_quote_stream_loop(leg, key, cache).await;
        });
    }

    async fn refresh_leg_quote_rest(&self, leg: &Leg) -> Result<Leg> {
        let value: Value = match (leg.exchange.as_str(), leg.market.as_str()) {
            ("Binance", "perpetual") => {
                self.get_json(format!(
                    "https://fapi.binance.com/fapi/v1/ticker/bookTicker?symbol={}",
                    leg.symbol
                ))
                .await?
            }
            ("Binance", "spot") => {
                self.get_json(format!(
                    "https://api.binance.com/api/v3/ticker/bookTicker?symbol={}",
                    leg.symbol
                ))
                .await?
            }
            ("Bybit", "perpetual") => {
                self.get_json(format!(
                    "https://api.bybit.com/v5/market/tickers?category=linear&symbol={}",
                    leg.symbol
                ))
                .await?
            }
            ("Bybit", "spot") => {
                self.get_json(format!(
                    "https://api.bybit.com/v5/market/tickers?category=spot&symbol={}",
                    leg.symbol
                ))
                .await?
            }
            _ => bail!("unsupported executable quote route"),
        };
        let (bid, ask) = if leg.exchange == "Binance" {
            (parse(&value["bidPrice"]), parse(&value["askPrice"]))
        } else {
            let ticker = value["result"]["list"]
                .as_array()
                .and_then(|items| items.first())
                .ok_or_else(|| anyhow!("Bybit executable quote is unavailable"))?;
            (parse(&ticker["bid1Price"]), parse(&ticker["ask1Price"]))
        };
        let mut refreshed = leg.clone();
        refreshed.bid = bid.ok_or_else(|| anyhow!("executable bid quote is unavailable"))?;
        refreshed.ask = ask.ok_or_else(|| anyhow!("executable ask quote is unavailable"))?;
        if leg.exchange == "Binance" {
            refreshed.bid_quantity = parse(&value["bidQty"]).unwrap_or(0.0);
            refreshed.ask_quantity = parse(&value["askQty"]).unwrap_or(0.0);
        } else if let Some(ticker) = value["result"]["list"]
            .as_array()
            .and_then(|items| items.first())
        {
            refreshed.bid_quantity = parse(&ticker["bid1Size"]).unwrap_or(0.0);
            refreshed.ask_quantity = parse(&ticker["ask1Size"]).unwrap_or(0.0);
        }
        Ok(refreshed)
    }

    async fn enrich_opportunity_averages(&self, opportunities: &mut [Opportunity]) {
        let mut average_tasks = tokio::task::JoinSet::new();
        let permits = Arc::new(Semaphore::new(SPREAD_HISTORY_CONCURRENCY));
        for (index, opportunity) in opportunities.iter().enumerate() {
            let service = self.clone();
            let permits = permits.clone();
            let long = opportunity.long.clone();
            let short = opportunity.short.clone();
            let symbol = opportunity.long.symbol.clone();
            average_tasks.spawn(async move {
                let _permit = permits
                    .acquire_owned()
                    .await
                    .expect("spread-history semaphore remains open");
                (
                    index,
                    symbol.clone(),
                    service.hourly_spread_averages(&long, &short).await,
                )
            });
        }
        while let Some(result) = average_tasks.join_next().await {
            match result {
                Ok((index, _, Ok(average))) => {
                    let opportunity = &mut opportunities[index];
                    opportunity.average_spread_24h = average;
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
    }

    async fn enrich_cross_funding_projections(&self, opportunities: &mut [Opportunity]) {
        let mut tasks = tokio::task::JoinSet::new();
        let permits = Arc::new(Semaphore::new(SPREAD_HISTORY_CONCURRENCY));
        for opportunity in opportunities.iter_mut() {
            opportunity.funding_per_hour = 0.0;
            opportunity.apy = 0.0;
        }
        for (index, opportunity) in opportunities.iter().enumerate() {
            let service = self.clone();
            let permits = permits.clone();
            let long = opportunity.long.clone();
            let short = opportunity.short.clone();
            tasks.spawn(async move {
                let _permit = permits
                    .acquire_owned()
                    .await
                    .expect("funding-history semaphore remains open");
                let averages = tokio::try_join!(
                    service.weighted_funding_average(&long),
                    service.weighted_funding_average(&short)
                );
                (index, long, short, averages)
            });
        }
        while let Some(result) = tasks.join_next().await {
            match result {
                Ok((index, long, short, Ok((long_average, short_average)))) => {
                    let opportunity = &mut opportunities[index];
                    if let Some((average_per_hour, horizon_hours)) = cross_funding_projection(
                        &long,
                        &short,
                        chrono::Utc::now().timestamp_millis(),
                        long_average,
                        short_average,
                    ) {
                        opportunity.funding_per_hour = average_per_hour;
                        opportunity.apy = average_per_hour * YEAR_HOURS;
                        opportunity.apy_horizon_hours = horizon_hours;
                    } else {
                        opportunity.funding_per_hour = 0.0;
                        opportunity.apy = 0.0;
                    }
                }
                Ok((index, _, _, Err(error))) => {
                    opportunities[index].funding_per_hour = 0.0;
                    opportunities[index].apy = 0.0;
                    tracing::warn!("cross-exchange funding projection unavailable: {error:#}");
                }
                Err(error) => tracing::warn!("funding-projection task failed: {error}"),
            }
        }
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
        // History charts include the current in-progress hour as a live snapshot.
        // The weighted baseline below intentionally continues to use only closed hours.
        points.retain(|point| point.timestamp <= current_hour_start_millis());
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

    pub async fn opportunity_history(
        &self,
        opportunity_id: &str,
    ) -> Result<OpportunityHistoryResponse> {
        let snapshot = self.opportunities().await?;
        let opportunity = snapshot
            .opportunities
            .into_iter()
            .chain(snapshot.spot_opportunities)
            .find(|item| item.id == opportunity_id)
            .ok_or_else(|| anyhow!("opportunity is no longer available"))?;
        self.history_for_legs(opportunity.id, &opportunity.long, &opportunity.short)
            .await
    }

    pub async fn position_history(
        &self,
        position: &Position,
    ) -> Result<OpportunityHistoryResponse> {
        self.opportunities().await?;
        let (long, short) = {
            let legs = self.route_leg_cache.read().await;
            let long = legs
                .get(&route_leg_key(
                    &position.long.exchange,
                    &position.long.market,
                    &position.long.symbol,
                ))
                .cloned()
                .ok_or_else(|| anyhow!("long-leg market data is unavailable"))?;
            let short = legs
                .get(&route_leg_key(
                    &position.short.exchange,
                    &position.short.market,
                    &position.short.symbol,
                ))
                .cloned()
                .ok_or_else(|| anyhow!("short-leg market data is unavailable"))?;
            (long, short)
        };
        self.history_for_legs(position.id.clone(), &long, &short)
            .await
    }

    async fn history_for_legs(
        &self,
        id: String,
        long: &Leg,
        short: &Leg,
    ) -> Result<OpportunityHistoryResponse> {
        let (long_closes, short_closes, long_rates_result, short_rates_result) = tokio::join!(
            self.hourly_closes(long),
            self.hourly_closes(short),
            self.funding_history(long),
            self.funding_history(short),
        );
        let long_closes = long_closes?;
        let short_closes = short_closes?;
        let long_rates = long_rates_result.unwrap_or_else(|error| {
            tracing::warn!(%id, "long-leg funding history unavailable: {error:#}");
            Vec::new()
        });
        let short_rates = short_rates_result.unwrap_or_else(|error| {
            tracing::warn!(%id, "short-leg funding history unavailable: {error:#}");
            Vec::new()
        });
        let mut points = long_closes
            .into_iter()
            .filter_map(|(timestamp, long_close)| {
                let short_close = *short_closes.get(&timestamp)?;
                (long_close > 0.0 && short_close > 0.0).then_some(OpportunityHistoryPoint {
                    timestamp,
                    long_close,
                    short_close,
                    directional_spread_percent: (short_close - long_close) / long_close * 100.0,
                    long_funding_rate: latest_rate_at(&long_rates, timestamp),
                    short_funding_rate: latest_rate_at(&short_rates, timestamp),
                })
            })
            .collect::<Vec<_>>();
        points.sort_by_key(|point| point.timestamp);
        points.retain(|point| point.timestamp <= current_hour_start_millis());
        if points.len() > 24 {
            points.drain(..points.len() - 24);
        }
        Ok(OpportunityHistoryResponse {
            opportunity_id: id,
            funding_note: "每小时展示截至该时刻永续腿最近一次已知结算费率；现货腿没有资金费率"
                .into(),
            points,
        })
    }

    pub async fn trading_legs(&self) -> Result<Vec<Leg>> {
        let (binance, bybit, binance_spot) = tokio::try_join!(
            self.binance_markets(),
            self.bybit_markets(),
            self.binance_spot_markets()
        )?;
        Ok(binance
            .into_iter()
            .chain(bybit)
            .chain(binance_spot)
            .collect())
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
                    market: "perpetual".into(),
                    base: base.into(),
                    symbol: symbol.into(),
                    bid: parse(&b["bidPrice"])?,
                    ask: parse(&b["askPrice"])?,
                    bid_quantity: parse(&b["bidQty"]).unwrap_or(0.0),
                    ask_quantity: parse(&b["askQty"]).unwrap_or(0.0),
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
                    market: "perpetual".into(),
                    base: base.into(),
                    symbol: symbol.into(),
                    bid: parse(&x["bid1Price"])?,
                    ask: parse(&x["ask1Price"])?,
                    bid_quantity: parse(&x["bid1Size"]).unwrap_or(0.0),
                    ask_quantity: parse(&x["ask1Size"]).unwrap_or(0.0),
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

    async fn binance_spot_markets(&self) -> Result<Vec<Leg>> {
        let (exchange, book, tickers): (Value, Value, Value) = tokio::try_join!(
            self.get_json("https://api.binance.com/api/v3/exchangeInfo".into()),
            self.get_json("https://api.binance.com/api/v3/ticker/bookTicker".into()),
            self.get_json("https://api.binance.com/api/v3/ticker/24hr?type=MINI".into()),
        )?;
        let empty = Vec::new();
        let books = book
            .as_array()
            .unwrap_or(&empty)
            .iter()
            .filter_map(|x| {
                Some((
                    x["symbol"].as_str()?.to_string(),
                    (
                        parse(&x["bidPrice"])?,
                        parse(&x["askPrice"])?,
                        parse(&x["bidQty"]).unwrap_or(0.0),
                        parse(&x["askQty"]).unwrap_or(0.0),
                    ),
                ))
            })
            .collect::<HashMap<_, _>>();
        let volumes = tickers
            .as_array()
            .unwrap_or(&empty)
            .iter()
            .filter_map(|x| {
                Some((
                    x["symbol"].as_str()?.to_string(),
                    (
                        parse(&x["quoteVolume"]).unwrap_or(0.0),
                        parse(&x["lastPrice"]).unwrap_or(0.0),
                    ),
                ))
            })
            .collect::<HashMap<_, _>>();
        Ok(exchange["symbols"]
            .as_array()
            .unwrap_or(&empty)
            .iter()
            .filter_map(|x| {
                if x["quoteAsset"] != "USDT"
                    || x["status"] != "TRADING"
                    || !x["isMarginTradingAllowed"].as_bool().unwrap_or(false)
                {
                    return None;
                }
                let symbol = x["symbol"].as_str()?;
                let base = x["baseAsset"].as_str()?;
                let (bid, ask, bid_quantity, ask_quantity) = *books.get(symbol)?;
                let (volume, last) = volumes.get(symbol).copied().unwrap_or_default();
                let step = x["filters"]
                    .as_array()
                    .and_then(|filters| {
                        filters
                            .iter()
                            .find(|filter| filter["filterType"] == "LOT_SIZE")
                    })
                    .and_then(|filter| filter["stepSize"].as_str())
                    .and_then(|value| value.parse().ok())
                    .unwrap_or(0.001);
                Some(Leg {
                    exchange: "Binance".into(),
                    market: "spot".into(),
                    base: base.into(),
                    symbol: symbol.into(),
                    bid,
                    ask,
                    bid_quantity,
                    ask_quantity,
                    mark: if last > 0.0 { last } else { (bid + ask) / 2.0 },
                    rate: 0.0,
                    interval_hours: 0.0,
                    next_funding_time: 0,
                    qty_step: step,
                    tags: vec![],
                    volume_24h_usdt: volume,
                })
            })
            .collect())
    }

    async fn bybit_spot_markets(&self) -> Result<Vec<Leg>> {
        let (instruments, tickers): (Value, Value) = tokio::try_join!(
            self.get_json(
                "https://api.bybit.com/v5/market/instruments-info?category=spot&limit=1000".into()
            ),
            self.get_json("https://api.bybit.com/v5/market/tickers?category=spot".into()),
        )?;
        let empty = Vec::new();
        let ticker_map = tickers["result"]["list"]
            .as_array()
            .unwrap_or(&empty)
            .iter()
            .filter_map(|x| Some((x["symbol"].as_str()?.to_string(), x)))
            .collect::<HashMap<_, _>>();
        Ok(instruments["result"]["list"]
            .as_array()
            .unwrap_or(&empty)
            .iter()
            .filter_map(|x| {
                if x["quoteCoin"] != "USDT"
                    || x["status"] != "Trading"
                    || matches!(x["marginTrading"].as_str(), None | Some("" | "none"))
                {
                    return None;
                }
                let symbol = x["symbol"].as_str()?;
                let ticker = *ticker_map.get(symbol)?;
                let bid = parse(&ticker["bid1Price"])?;
                let ask = parse(&ticker["ask1Price"])?;
                let bid_quantity = parse(&ticker["bid1Size"]).unwrap_or(0.0);
                let ask_quantity = parse(&ticker["ask1Size"]).unwrap_or(0.0);
                let last = parse(&ticker["lastPrice"]).unwrap_or((bid + ask) / 2.0);
                let step = x["lotSizeFilter"]["basePrecision"]
                    .as_str()
                    .and_then(|value| value.parse().ok())
                    .unwrap_or(0.001);
                Some(Leg {
                    exchange: "Bybit".into(),
                    market: "spot".into(),
                    base: x["baseCoin"].as_str()?.into(),
                    symbol: symbol.into(),
                    bid,
                    ask,
                    bid_quantity,
                    ask_quantity,
                    mark: last,
                    rate: 0.0,
                    interval_hours: 0.0,
                    next_funding_time: 0,
                    qty_step: step,
                    tags: vec![],
                    volume_24h_usdt: parse(&ticker["turnover24h"]).unwrap_or(0.0),
                })
            })
            .collect())
    }

    async fn hourly_spread_averages(&self, long: &Leg, short: &Leg) -> Result<f64> {
        let key = format!(
            "{}:{}:{}|{}:{}:{}",
            long.exchange, long.market, long.symbol, short.exchange, short.market, short.symbol
        );
        if let Some((at, average)) = self.spread_average_cache.read().await.get(&key) {
            if at.elapsed() < Duration::from_secs(5 * 60) {
                return Ok(*average);
            }
        }
        let (long_closes, short_closes) =
            tokio::try_join!(self.hourly_closes(long), self.hourly_closes(short))?;
        let mut aligned = long_closes
            .iter()
            .filter_map(|(timestamp, long_close)| {
                let short_close = short_closes.get(timestamp)?;
                (*long_close > 0.0 && *short_close > 0.0)
                    .then_some((*timestamp, (*short_close - *long_close) / *long_close))
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
        let average = time_weighted_spread_average(&aligned, SPREAD_AVERAGE_HALF_LIFE_HOURS)
            .ok_or_else(|| anyhow!("weighted spread average is unavailable"))?;
        self.spread_average_cache
            .write()
            .await
            .insert(key, (Instant::now(), average));
        Ok(average)
    }

    async fn hourly_closes(&self, leg: &Leg) -> Result<HashMap<i64, f64>> {
        let url = match (leg.exchange.as_str(), leg.market.as_str()) {
            ("Binance", "perpetual") => format!("https://fapi.binance.com/fapi/v1/klines?symbol={}&interval=1h&limit=25", leg.symbol),
            ("Binance", "spot") => format!("https://api.binance.com/api/v3/klines?symbol={}&interval=1h&limit=25", leg.symbol),
            ("Bybit", "perpetual") => format!("https://api.bybit.com/v5/market/kline?category=linear&symbol={}&interval=60&limit=25", leg.symbol),
            ("Bybit", "spot") => format!("https://api.bybit.com/v5/market/kline?category=spot&symbol={}&interval=60&limit=25", leg.symbol),
            _ => bail!("unsupported market history route"),
        };
        let response = self.get_json(url).await?;
        let candles = if leg.exchange == "Binance" {
            response.as_array()
        } else {
            response["result"]["list"].as_array()
        };
        let empty = Vec::new();
        Ok(candles
            .unwrap_or(&empty)
            .iter()
            .filter_map(|candle| {
                let timestamp = candle[0]
                    .as_i64()
                    .or_else(|| candle[0].as_str().and_then(|value| value.parse().ok()))?;
                Some((timestamp, parse(&candle[4])?))
            })
            .collect())
    }

    async fn funding_history(&self, leg: &Leg) -> Result<Vec<(i64, f64)>> {
        if leg.market != "perpetual" {
            return Ok(Vec::new());
        }
        let now = chrono::Utc::now().timestamp_millis();
        let start = now - 48 * 60 * 60 * 1000;
        let response = match leg.exchange.as_str() {
            "Binance" => {
                self.get_json(format!(
                    "https://fapi.binance.com/fapi/v1/fundingRate?symbol={}&startTime={start}&limit=1000",
                    leg.symbol
                ))
                .await?
            }
            "Bybit" => {
                self.get_json(format!(
                    "https://api.bybit.com/v5/market/funding/history?category=linear&symbol={}&startTime={start}&endTime={now}&limit=200",
                    leg.symbol
                ))
                .await?
            }
            _ => bail!("unsupported funding-history exchange"),
        };
        let empty = Vec::new();
        let mut rates = if leg.exchange == "Binance" {
            response
                .as_array()
                .unwrap_or(&empty)
                .iter()
                .filter_map(|item| {
                    Some((item["fundingTime"].as_i64()?, parse(&item["fundingRate"])?))
                })
                .collect::<Vec<_>>()
        } else {
            response["result"]["list"]
                .as_array()
                .unwrap_or(&empty)
                .iter()
                .filter_map(|item| {
                    Some((
                        item["fundingRateTimestamp"].as_str()?.parse().ok()?,
                        parse(&item["fundingRate"])?,
                    ))
                })
                .collect::<Vec<_>>()
        };
        rates.sort_by_key(|(timestamp, _)| *timestamp);
        Ok(rates)
    }

    async fn weighted_funding_average(&self, leg: &Leg) -> Result<f64> {
        let key = format!("{}:{}", leg.exchange, leg.symbol);
        if let Some((at, average)) = self.funding_average_cache.read().await.get(&key) {
            // Historical settlement data only changes at funding boundaries; avoid
            // refetching every instrument on each 20-second opportunity scan.
            if at.elapsed() < Duration::from_secs(60 * 60) {
                return Ok(*average);
            }
        }
        let now = chrono::Utc::now().timestamp_millis();
        let cutoff = now - 24 * 60 * 60 * 1_000;
        let points = self
            .funding_history(leg)
            .await?
            .into_iter()
            .filter(|(timestamp, _)| *timestamp >= cutoff && *timestamp <= now)
            .collect::<Vec<_>>();
        let average =
            time_weighted_average(&points, FUNDING_AVERAGE_HALF_LIFE_HOURS).unwrap_or(leg.rate);
        self.funding_average_cache
            .write()
            .await
            .insert(key, (Instant::now(), average));
        Ok(average)
    }

    async fn spot_borrow_rate(&self, exchange: &str, asset: &str) -> Result<f64> {
        let key = format!("{exchange}:{asset}");
        if let Some((at, rate)) = self.borrow_rate_cache.read().await.get(&key) {
            if at.elapsed() < Duration::from_secs(60 * 60) {
                return Ok(*rate);
            }
        }
        let rate = match exchange {
            "Binance" => {
                let credentials = self
                    .config
                    .binance
                    .as_ref()
                    .ok_or_else(|| anyhow!("Binance credentials missing"))?;
                let query = format!(
                    "asset={asset}&timestamp={}&recvWindow=10000",
                    chrono::Utc::now().timestamp_millis()
                );
                let signature = hmac_sign(&credentials.api_secret, &query);
                let url = format!(
                    "https://api.binance.com/sapi/v1/margin/interestRateHistory?{query}&signature={signature}"
                );
                let value = self
                    .client
                    .get(url)
                    .header("X-MBX-APIKEY", &credentials.api_key)
                    .send()
                    .await?
                    .error_for_status()?
                    .json::<Value>()
                    .await?;
                let daily = value
                    .as_array()
                    .and_then(|rates| rates.first())
                    .and_then(|item| parse(&item["dailyInterestRate"]))
                    .ok_or_else(|| anyhow!("Binance returned no borrow rate for {asset}"))?;
                daily / 24.0
            }
            "Bybit" => {
                let credentials = self
                    .config
                    .bybit
                    .as_ref()
                    .ok_or_else(|| anyhow!("Bybit credentials missing"))?;
                let timestamp = chrono::Utc::now().timestamp_millis().to_string();
                let recv_window = "5000";
                let query = format!("currency={asset}");
                let signature = hmac_sign(
                    &credentials.api_secret,
                    &format!("{timestamp}{}{recv_window}{query}", credentials.api_key),
                );
                let value = self
                    .client
                    .get(format!(
                        "https://api.bybit.com/v5/spot-margin-trade/interest-rate-history?{query}"
                    ))
                    .header("X-BAPI-API-KEY", &credentials.api_key)
                    .header("X-BAPI-SIGN", signature)
                    .header("X-BAPI-TIMESTAMP", timestamp)
                    .header("X-BAPI-RECV-WINDOW", recv_window)
                    .send()
                    .await?
                    .error_for_status()?
                    .json::<Value>()
                    .await?;
                if value["retCode"].as_i64().unwrap_or(-1) != 0 {
                    bail!(
                        "Bybit borrow-rate request rejected: {}",
                        value["retMsg"].as_str().unwrap_or("unknown error")
                    );
                }
                let rate = value["result"]["list"]
                    .as_array()
                    .and_then(|rates| rates.first())
                    .and_then(|item| parse(&item["hourlyBorrowRate"]))
                    .ok_or_else(|| anyhow!("Bybit returned no borrow rate for {asset}"))?;
                rate
            }
            _ => bail!("unsupported spot borrow-rate exchange"),
        };
        self.borrow_rate_cache
            .write()
            .await
            .insert(key, (Instant::now(), rate));
        Ok(rate)
    }

    async fn binance_borrow_available(&self, asset: &str) -> Result<bool> {
        if let Some((at, available)) = self.borrow_availability_cache.read().await.get(asset) {
            if at.elapsed() < Duration::from_secs(5 * 60) {
                return Ok(*available);
            }
        }
        let credentials = self
            .config
            .binance
            .as_ref()
            .ok_or_else(|| anyhow!("Binance credentials missing"))?;
        let query = format!(
            "asset={asset}&timestamp={}&recvWindow=10000",
            chrono::Utc::now().timestamp_millis()
        );
        let signature = hmac_sign(&credentials.api_secret, &query);
        let response = self
            .client
            .get(format!(
                "https://api.binance.com/sapi/v1/margin/maxBorrowable?{query}&signature={signature}"
            ))
            .header("X-MBX-APIKEY", &credentials.api_key)
            .send()
            .await?;
        let available = if response.status().is_success() {
            parse(&response.json::<Value>().await?["amount"]).unwrap_or(0.0) > 0.0
        } else {
            false
        };
        self.borrow_availability_cache
            .write()
            .await
            .insert(asset.into(), (Instant::now(), available));
        Ok(available)
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

fn apply_live_quote(leg: &Leg, quote: LiveQuote) -> Leg {
    let mut refreshed = leg.clone();
    refreshed.bid = quote.bid;
    refreshed.ask = quote.ask;
    refreshed.bid_quantity = quote.bid_quantity;
    refreshed.ask_quantity = quote.ask_quantity;
    refreshed
}

async fn live_quote_stream_loop(leg: Leg, key: String, cache: Arc<RwLock<LiveQuoteCache>>) {
    let mut retry_seconds = 1u64;
    loop {
        match run_live_quote_stream(&leg, &key, &cache).await {
            Ok(()) => tracing::warn!(
                exchange = leg.exchange,
                market = leg.market,
                symbol = leg.symbol,
                "quote websocket closed"
            ),
            Err(error) => tracing::warn!(
                exchange = leg.exchange,
                market = leg.market,
                symbol = leg.symbol,
                "quote websocket failed: {error:#}"
            ),
        }
        tokio::time::sleep(Duration::from_secs(retry_seconds)).await;
        retry_seconds = (retry_seconds * 2).min(30);
    }
}

async fn run_live_quote_stream(
    leg: &Leg,
    key: &str,
    cache: &Arc<RwLock<LiveQuoteCache>>,
) -> Result<()> {
    let (url, subscription) = match (leg.exchange.as_str(), leg.market.as_str()) {
        ("Binance", "perpetual") => (
            format!(
                "wss://fstream.binance.com/ws/{}@bookTicker",
                leg.symbol.to_ascii_lowercase()
            ),
            None,
        ),
        ("Binance", "spot") => (
            format!(
                "wss://stream.binance.com:9443/ws/{}@bookTicker",
                leg.symbol.to_ascii_lowercase()
            ),
            None,
        ),
        ("Bybit", "perpetual") => (
            "wss://stream.bybit.com/v5/public/linear".to_string(),
            Some(serde_json::json!({
                "op": "subscribe",
                "args": [format!("orderbook.1.{}", leg.symbol)]
            })),
        ),
        ("Bybit", "spot") => (
            "wss://stream.bybit.com/v5/public/spot".to_string(),
            Some(serde_json::json!({
                "op": "subscribe",
                "args": [format!("orderbook.1.{}", leg.symbol)]
            })),
        ),
        _ => bail!("unsupported websocket quote route"),
    };
    let (mut socket, _) = tokio::time::timeout(Duration::from_secs(10), connect_async(&url))
        .await
        .with_context(|| format!("connect {url} timed out"))?
        .with_context(|| format!("connect {url}"))?;
    if let Some(subscription) = subscription {
        socket
            .send(Message::Text(subscription.to_string().into()))
            .await?;
    }
    tracing::info!(
        exchange = leg.exchange,
        market = leg.market,
        symbol = leg.symbol,
        "quote websocket connected"
    );
    let mut heartbeat = tokio::time::interval(Duration::from_secs(20));
    heartbeat.tick().await;
    loop {
        tokio::select! {
            _ = heartbeat.tick() => {
                if leg.exchange == "Bybit" {
                    socket
                        .send(Message::Text(r#"{"op":"ping"}"#.into()))
                        .await?;
                }
            }
            message = socket.next() => {
                let message = message.ok_or_else(|| anyhow!("websocket stream ended"))??;
                match message {
                    Message::Text(text) => {
                        let value: Value = match serde_json::from_str(text.as_ref()) {
                            Ok(value) => value,
                            Err(error) => {
                                tracing::debug!("invalid quote websocket payload: {error}");
                                continue;
                            }
                        };
                        let quote = if leg.exchange == "Binance" {
                            live_quote_from_binance(&value)
                        } else {
                            live_quote_from_bybit(&value)
                        };
                        if let Some(quote) = quote {
                            cache.write().await.insert(key.to_string(), quote);
                        }
                    }
                    Message::Ping(payload) => socket.send(Message::Pong(payload)).await?,
                    Message::Close(frame) => bail!("websocket closed: {frame:?}"),
                    _ => {}
                }
            }
        }
    }
}

fn live_quote_from_binance(value: &Value) -> Option<LiveQuote> {
    Some(LiveQuote {
        at: Instant::now(),
        bid: parse(&value["b"])?,
        ask: parse(&value["a"])?,
        bid_quantity: parse(&value["B"]).unwrap_or(0.0),
        ask_quantity: parse(&value["A"]).unwrap_or(0.0),
    })
}

fn live_quote_from_bybit(value: &Value) -> Option<LiveQuote> {
    let data = value.get("data")?;
    if let (Some(bid), Some(ask)) = (
        data["b"].as_array().and_then(|levels| levels.first()),
        data["a"].as_array().and_then(|levels| levels.first()),
    ) {
        return Some(LiveQuote {
            at: Instant::now(),
            bid: bid.as_array().and_then(|level| parse(level.first()?))?,
            ask: ask.as_array().and_then(|level| parse(level.first()?))?,
            bid_quantity: bid
                .as_array()
                .and_then(|level| level.get(1))
                .and_then(parse)
                .unwrap_or(0.0),
            ask_quantity: ask
                .as_array()
                .and_then(|level| level.get(1))
                .and_then(parse)
                .unwrap_or(0.0),
        });
    }
    Some(LiveQuote {
        at: Instant::now(),
        bid: parse(&data["bid1Price"])?,
        ask: parse(&data["ask1Price"])?,
        bid_quantity: parse(&data["bid1Size"]).unwrap_or(0.0),
        ask_quantity: parse(&data["ask1Size"]).unwrap_or(0.0),
    })
}

fn time_weighted_spread_average(points: &[(i64, f64)], half_life_hours: f64) -> Option<f64> {
    time_weighted_average(points, half_life_hours)
}

fn time_weighted_average(points: &[(i64, f64)], half_life_hours: f64) -> Option<f64> {
    if points.is_empty() || !half_life_hours.is_finite() || half_life_hours <= 0.0 {
        return None;
    }
    let latest_timestamp = points.iter().map(|point| point.0).max()?;
    let (weighted_sum, weight_sum) = points.iter().fold(
        (0.0, 0.0),
        |(weighted_sum, weight_sum), (timestamp, spread)| {
            let age_hours = (latest_timestamp - *timestamp).max(0) as f64 / (60.0 * 60.0 * 1_000.0);
            let weight = 2.0_f64.powf(-age_hours / half_life_hours);
            (weighted_sum + spread * weight, weight_sum + weight)
        },
    );
    (weight_sum > f64::EPSILON).then_some(weighted_sum / weight_sum)
}

fn strategy_reference_price(mark_price: f64, last_price: f64) -> (f64, bool) {
    let uses_last_price = last_price > 0.0 && (mark_price - last_price).abs() / last_price > 0.001;
    if uses_last_price {
        (last_price, true)
    } else {
        (mark_price, false)
    }
}

fn make_cross_candidate(token: &Token, first: &Leg, second: &Leg) -> Option<Opportunity> {
    make_cross_candidate_at(token, first, second, chrono::Utc::now().timestamp_millis())
}

fn make_cross_candidate_at(
    token: &Token,
    first: &Leg,
    second: &Leg,
    now: i64,
) -> Option<Opportunity> {
    const HOUR_MILLIS: i64 = 60 * 60 * 1_000;
    let current_hour = now.div_euclid(HOUR_MILLIS) * HOUR_MILLIS + HOUR_MILLIS;
    let first_rate = projected_rate_at(first, current_hour, 0.0);
    let second_rate = projected_rate_at(second, current_hour, 0.0);
    let (long, short, current_hour_return) = if second_rate > first_rate {
        (first, second, second_rate - first_rate)
    } else if first_rate > second_rate {
        (second, first, first_rate - second_rate)
    } else {
        return None;
    };
    Some(build_opportunity(token, long, short, current_hour_return))
}

fn settles_at(leg: &Leg, settlement_at: i64) -> bool {
    const HOUR_MILLIS: i64 = 60 * 60 * 1_000;
    const SETTLEMENT_TOLERANCE_MILLIS: i64 = 60 * 1_000;
    if leg.market != "perpetual"
        || leg.next_funding_time <= 0
        || settlement_at < leg.next_funding_time - SETTLEMENT_TOLERANCE_MILLIS
    {
        return false;
    }
    let interval_millis = (leg.interval_hours.round() as i64).max(1) * HOUR_MILLIS;
    let offset = settlement_at - leg.next_funding_time;
    offset.rem_euclid(interval_millis) <= SETTLEMENT_TOLERANCE_MILLIS
        || interval_millis - offset.rem_euclid(interval_millis) <= SETTLEMENT_TOLERANCE_MILLIS
}

fn projected_rate_at(leg: &Leg, settlement_at: i64, weighted_average: f64) -> f64 {
    if !settles_at(leg, settlement_at) {
        0.0
    } else if (settlement_at - leg.next_funding_time).abs() <= 60 * 1_000 {
        leg.rate
    } else {
        weighted_average
    }
}

fn cross_funding_projection(
    long: &Leg,
    short: &Leg,
    now_millis: i64,
    long_weighted_average: f64,
    short_weighted_average: f64,
) -> Option<(f64, u8)> {
    const HOUR_MILLIS: i64 = 60 * 60 * 1_000;
    const EIGHT_HOURS_MILLIS: i64 = 8 * HOUR_MILLIS;
    let first_hour = now_millis.div_euclid(HOUR_MILLIS) * HOUR_MILLIS + HOUR_MILLIS;
    let end = now_millis.div_euclid(EIGHT_HOURS_MILLIS) * EIGHT_HOURS_MILLIS + EIGHT_HOURS_MILLIS;
    let mut cumulative = 0.0;
    let mut best: Option<(f64, u8)> = None;
    for (index, settlement_at) in (first_hour..=end).step_by(HOUR_MILLIS as usize).enumerate() {
        cumulative += projected_rate_at(short, settlement_at, short_weighted_average)
            - projected_rate_at(long, settlement_at, long_weighted_average);
        let elapsed_hours = (index + 1) as u8;
        let average_per_hour = cumulative / f64::from(elapsed_hours);
        if best.is_none_or(|(best_average, _)| average_per_hour > best_average) {
            best = Some((average_per_hour, elapsed_hours));
        }
    }
    best.filter(|(average, _)| *average > 0.0)
}

fn make_spot_short_perpetual_long(
    token: &Token,
    perpetual: &Leg,
    spot: &Leg,
) -> Option<Opportunity> {
    if perpetual.rate >= 0.0 || perpetual.interval_hours <= 0.0 {
        return None;
    }
    let mut spot = spot.clone();
    spot.next_funding_time = perpetual.next_funding_time;
    Some(build_opportunity(
        token,
        perpetual,
        &spot,
        -perpetual.rate / perpetual.interval_hours,
    ))
}

fn make_spot_long_perpetual_short(
    token: &Token,
    perpetual: &Leg,
    spot: &Leg,
) -> Option<Opportunity> {
    if perpetual.rate <= 0.0 || perpetual.interval_hours <= 0.0 {
        return None;
    }
    let mut spot = spot.clone();
    spot.next_funding_time = perpetual.next_funding_time;
    Some(build_opportunity(
        token,
        &spot,
        perpetual,
        perpetual.rate / perpetual.interval_hours,
    ))
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

fn make_spread_opportunity(token: &Token, first: &Leg, second: &Leg) -> Option<Opportunity> {
    let first_long_spread = (second.bid - first.ask) / first.ask;
    let second_long_spread = (first.bid - second.ask) / second.ask;
    let (long, short, spread) = if first_long_spread > second_long_spread {
        (first, second, first_long_spread)
    } else {
        (second, first, second_long_spread)
    };
    if spread <= SPREAD_OPPORTUNITY_ABSOLUTE_THRESHOLD {
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
        id: format!(
            "{}-{}-{}-{}-{}",
            token.symbol, long.exchange, long.market, short.exchange, short.market
        ),
        token: token.clone(),
        long: long.clone(),
        short: short.clone(),
        funding_per_hour,
        borrow_interest_per_hour: 0.0,
        apy: funding_per_hour * YEAR_HOURS,
        apy_horizon_hours: 1,
        spread,
        average_spread_24h,
        spread_vs_average,
        fees,
        break_even_hours: break_even_hours(funding_per_hour, fees, spread_vs_average),
        route_type: if long.market == "perpetual" && short.market == "perpetual" {
            "cross_perpetual".into()
        } else {
            "spot_perpetual".into()
        },
        execution_supported: (long.market == "perpetual" && short.market == "perpetual")
            || (long.exchange == "Binance"
                && long.market == "perpetual"
                && short.exchange == "Binance"
                && short.market == "spot"),
    }
}

fn break_even_hours(funding_per_hour: f64, fees: f64, spread_vs_average: f64) -> f64 {
    if funding_per_hour > 0.0 {
        (fees - spread_vs_average).max(0.0) / funding_per_hour
    } else {
        0.0
    }
}

fn route_leg_key(exchange: &str, market: &str, symbol: &str) -> String {
    format!("{exchange}:{market}:{symbol}")
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

fn hmac_sign(secret: &str, message: &str) -> String {
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).expect("HMAC accepts any key");
    mac.update(message.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_binance_book_ticker_websocket_quote() {
        let quote = live_quote_from_binance(&serde_json::json!({
            "b": "2.662", "B": "14.81", "a": "2.664", "A": "144.48"
        }))
        .expect("Binance quote");
        assert_eq!(quote.bid, 2.662);
        assert_eq!(quote.ask, 2.664);
        assert_eq!(quote.bid_quantity, 14.81);
        assert_eq!(quote.ask_quantity, 144.48);
    }

    #[test]
    fn parses_bybit_ticker_websocket_quote() {
        let quote = live_quote_from_bybit(&serde_json::json!({
            "data": {
                "bid1Price": "2.660", "bid1Size": "45.2",
                "ask1Price": "2.665", "ask1Size": "4.4"
            }
        }))
        .expect("Bybit quote");
        assert_eq!(quote.bid, 2.660);
        assert_eq!(quote.ask, 2.665);
        assert_eq!(quote.bid_quantity, 45.2);
        assert_eq!(quote.ask_quantity, 4.4);
    }

    #[test]
    fn parses_bybit_orderbook_websocket_quote() {
        let quote = live_quote_from_bybit(&serde_json::json!({
            "data": {
                "b": [["2.660", "45.2"]],
                "a": [["2.665", "4.4"]]
            }
        }))
        .expect("Bybit order book");
        assert_eq!(quote.bid, 2.660);
        assert_eq!(quote.ask, 2.665);
        assert_eq!(quote.bid_quantity, 45.2);
        assert_eq!(quote.ask_quantity, 4.4);
    }

    #[test]
    fn weighted_spread_average_prioritizes_recent_hours() {
        let hour = 60 * 60 * 1_000;
        let points = vec![
            (0, 0.10),
            (18 * hour, 0.0),
            (23 * hour, 0.0),
            (24 * hour, 0.0),
        ];
        let simple_average = points.iter().map(|point| point.1).sum::<f64>() / points.len() as f64;
        let weighted = time_weighted_spread_average(&points, 6.0).expect("weighted spread average");
        assert!(weighted < simple_average / 2.0);
    }

    #[test]
    fn weighted_spread_average_tracks_recent_spike_more_than_old_values() {
        let hour = 60 * 60 * 1_000;
        let points = vec![
            (0, 0.0),
            (18 * hour, 0.0),
            (23 * hour, 0.0),
            (24 * hour, 0.04),
        ];
        let simple_average = points.iter().map(|point| point.1).sum::<f64>() / points.len() as f64;
        let weighted = time_weighted_spread_average(&points, 6.0).expect("weighted spread average");
        assert!(weighted > simple_average);
    }

    #[tokio::test]
    #[ignore = "requires direct access to exchange websocket endpoints"]
    async fn exchange_websocket_quotes_are_live() {
        let cache = Arc::new(RwLock::new(HashMap::new()));
        let routes = [
            ("Binance", "perpetual"),
            ("Binance", "spot"),
            ("Bybit", "perpetual"),
            ("Bybit", "spot"),
        ];
        for (exchange, market) in routes {
            let mut leg = test_leg(exchange, vec![]);
            leg.market = market.into();
            leg.base = "BTC".into();
            leg.symbol = "BTCUSDT".into();
            let key = route_leg_key(exchange, market, &leg.symbol);
            let cache = cache.clone();
            tokio::spawn(async move {
                let _ = run_live_quote_stream(&leg, &key, &cache).await;
            });
        }
        let result = tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                if cache.read().await.len() == routes.len() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await;
        if result.is_err() {
            let connected = cache.read().await.keys().cloned().collect::<Vec<_>>();
            panic!("all four websocket quote routes should publish; received {connected:?}");
        }
        for quote in cache.read().await.values() {
            assert!(quote.bid > 0.0);
            assert!(quote.ask >= quote.bid);
        }
    }

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

    #[test]
    fn binance_spot_short_perpetual_long_is_execution_eligible() {
        let token = Token {
            symbol: "TEST".into(),
            name: "Test".into(),
            rank: None,
            tags: vec![],
        };
        let mut perpetual = test_leg("Binance", vec![]);
        perpetual.rate = -0.008;
        let mut spot = test_leg("Binance", vec![]);
        spot.market = "spot".into();
        spot.bid = 1.01;
        let opportunity =
            make_spot_short_perpetual_long(&token, &perpetual, &spot).expect("opportunity");
        assert!((opportunity.funding_per_hour - 0.001).abs() < 1e-12);
        assert_eq!(opportunity.route_type, "spot_perpetual");
        assert!(opportunity.execution_supported);
        assert_eq!(opportunity.long.market, "perpetual");
        assert_eq!(opportunity.short.market, "spot");

        perpetual.rate = 0.001;
        assert!(make_spot_short_perpetual_long(&token, &perpetual, &spot).is_none());
    }

    fn test_leg(exchange: &str, tags: Vec<String>) -> Leg {
        Leg {
            exchange: exchange.into(),
            market: "perpetual".into(),
            base: "TEST".into(),
            symbol: "TESTUSDT".into(),
            bid: 1.0,
            ask: 1.0,
            bid_quantity: 100.0,
            ask_quantity: 100.0,
            mark: 1.0,
            rate: 0.0,
            interval_hours: 8.0,
            next_funding_time: 0,
            qty_step: 0.01,
            tags,
            volume_24h_usdt: 1_000_000.0,
        }
    }

    #[test]
    fn spread_arbitrage_has_one_direction_from_low_price_to_high_price() {
        let token = Token {
            symbol: "TEST".into(),
            name: "TEST".into(),
            rank: None,
            tags: vec![],
        };
        let mut binance = test_leg("Binance", vec![]);
        binance.bid = 99.9;
        binance.ask = 100.0;
        let mut bybit = test_leg("Bybit", vec![]);
        bybit.bid = 101.0;
        bybit.ask = 101.1;

        let opportunity =
            make_spread_opportunity(&token, &binance, &bybit).expect("price dislocation");
        assert_eq!(opportunity.long.exchange, "Binance");
        assert_eq!(opportunity.short.exchange, "Bybit");
        assert!((opportunity.spread - 0.01).abs() < 1e-12);

        binance.bid = 102.0;
        binance.ask = 102.1;
        bybit.bid = 100.0;
        bybit.ask = 100.1;
        let reverse =
            make_spread_opportunity(&token, &binance, &bybit).expect("reverse dislocation");
        assert_eq!(reverse.long.exchange, "Bybit");
        assert_eq!(reverse.short.exchange, "Binance");
    }

    #[test]
    fn spread_arbitrage_prefilter_requires_more_than_half_percent_absolute_gap() {
        let token = Token {
            symbol: "TEST".into(),
            name: "TEST".into(),
            rank: None,
            tags: vec![],
        };
        let mut binance = test_leg("Binance", vec![]);
        binance.ask = 100.0;
        let mut bybit = test_leg("Bybit", vec![]);
        bybit.bid = 100.5;
        assert!(make_spread_opportunity(&token, &binance, &bybit).is_none());

        bybit.bid = 100.51;
        assert!(make_spread_opportunity(&token, &binance, &bybit).is_some());
    }

    #[test]
    fn cross_funding_projection_uses_live_rate_for_current_cycle_and_weighted_rate_later() {
        let now = 9 * 60 * 60 * 1_000 + 30 * 60 * 1_000;
        let token = Token {
            symbol: "TEST".into(),
            name: "TEST".into(),
            rank: None,
            tags: vec![],
        };
        let mut long = test_leg("Binance", vec![]);
        long.rate = 0.001;
        long.interval_hours = 4.0;
        long.next_funding_time = 12 * 60 * 60 * 1_000;
        let mut short = test_leg("Bybit", vec![]);
        short.rate = 0.002;
        short.interval_hours = 1.0;
        short.next_funding_time = 10 * 60 * 60 * 1_000;

        let candidate =
            make_cross_candidate_at(&token, &long, &short, now).expect("funding direction");
        assert_eq!(candidate.long.exchange, "Binance");
        assert_eq!(candidate.short.exchange, "Bybit");
        let (average, hours) = cross_funding_projection(&long, &short, now, 0.002, 0.001)
            .expect("profitable projection");
        assert_eq!(hours, 1);
        assert!((average - 0.002).abs() < 1e-12);
        assert_eq!(
            projected_rate_at(&short, 10 * 60 * 60 * 1_000, 0.001),
            0.002
        );
        assert_eq!(
            projected_rate_at(&short, 11 * 60 * 60 * 1_000, 0.001),
            0.001
        );
        assert_eq!(projected_rate_at(&long, 11 * 60 * 60 * 1_000, 0.002), 0.0);
    }

    #[test]
    fn cross_funding_projection_selects_maximum_average_and_horizon() {
        let now = 8 * 60 * 60 * 1_000 + 30 * 60 * 1_000;
        let mut long = test_leg("Binance", vec![]);
        long.rate = 0.008;
        long.interval_hours = 8.0;
        long.next_funding_time = 16 * 60 * 60 * 1_000;
        let mut short = test_leg("Bybit", vec![]);
        short.rate = 0.001;
        short.interval_hours = 1.0;
        short.next_funding_time = 9 * 60 * 60 * 1_000;

        let (average, hours) = cross_funding_projection(&long, &short, now, 0.008, 0.002)
            .expect("profitable projection");
        assert_eq!(hours, 7);
        assert!((average - (0.001 + 6.0 * 0.002) / 7.0).abs() < 1e-12);
    }
}
