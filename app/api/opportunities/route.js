import { NextResponse } from "next/server";

export const revalidate = 0;

const BINANCE_FEE = 0.0005;
const BYBIT_FEE = 0.00055;
const YEAR_HOURS = 365 * 24;

async function getJson(url, options = {}) {
  const host = new URL(url).hostname;
  try {
    const response = await fetch(url, {
      ...options,
      headers: { Accept: "application/json", ...(options.headers || {}) },
      signal: AbortSignal.timeout(12000)
    });
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    return response.json();
  } catch (error) {
    throw new Error(`${host}: ${error.message}`);
  }
}

async function getTopTokens() {
  const key = process.env.CMC_API_KEY;
  const base = key
    ? "https://pro-api.coinmarketcap.com/v3/cryptocurrency/listings/latest"
    : "https://pro-api.coinmarketcap.com/public-api/v3/cryptocurrency/listings/latest";
  const url = `${base}?start=1&limit=200&sort=market_cap&sort_dir=desc&convert=USD`;
  const headers = key ? { "X-CMC_PRO_API_KEY": key } : {};
  const json = await getJson(url, { headers });
  return (json.data || []).map((coin) => ({
    symbol: coin.symbol.toUpperCase(),
    name: coin.name,
    rank: coin.cmc_rank
  }));
}

async function getBinance() {
  const [premium, book, exchange, fundingInfo] = await Promise.all([
    getJson("https://fapi.binance.com/fapi/v1/premiumIndex"),
    getJson("https://fapi.binance.com/fapi/v1/ticker/bookTicker"),
    getJson("https://fapi.binance.com/fapi/v1/exchangeInfo"),
    getJson("https://fapi.binance.com/fapi/v1/fundingInfo").catch(() => [])
  ]);
  const books = new Map(book.map((x) => [x.symbol, x]));
  const symbols = new Map(
    exchange.symbols
      .filter((x) => x.quoteAsset === "USDT" && x.contractType === "PERPETUAL" && x.status === "TRADING")
      .map((x) => [x.symbol, x.baseAsset])
  );
  const intervals = new Map(fundingInfo.map((x) => [x.symbol, Number(x.fundingIntervalHours)]));

  return premium.flatMap((x) => {
    const base = symbols.get(x.symbol);
    const b = books.get(x.symbol);
    if (!base || !b || !Number(b.bidPrice) || !Number(b.askPrice)) return [];
    return [{
      exchange: "Binance",
      base,
      symbol: x.symbol,
      bid: Number(b.bidPrice),
      ask: Number(b.askPrice),
      mark: Number(x.markPrice),
      rate: Number(x.lastFundingRate),
      intervalHours: intervals.get(x.symbol) || 8,
      nextFundingTime: Number(x.nextFundingTime)
    }];
  });
}

async function getBybit() {
  const [tickers, instruments] = await Promise.all([
    getJson("https://api.bybit.com/v5/market/tickers?category=linear"),
    getJson("https://api.bybit.com/v5/market/instruments-info?category=linear&limit=1000")
  ]);
  const meta = new Map(
    (instruments.result?.list || [])
      .filter((x) => x.quoteCoin === "USDT" && x.contractType === "LinearPerpetual" && x.status === "Trading")
      .map((x) => [x.symbol, x])
  );
  return (tickers.result?.list || []).flatMap((x) => {
    const m = meta.get(x.symbol);
    if (!m || !Number(x.bid1Price) || !Number(x.ask1Price)) return [];
    return [{
      exchange: "Bybit",
      base: m.baseCoin,
      symbol: x.symbol,
      bid: Number(x.bid1Price),
      ask: Number(x.ask1Price),
      mark: Number(x.markPrice),
      rate: Number(x.fundingRate),
      intervalHours: Number(m.fundingInterval) / 60 || 8,
      nextFundingTime: Number(x.nextFundingTime)
    }];
  });
}

function makeOpportunity(token, long, short) {
  const fundingPerHour = short.rate / short.intervalHours - long.rate / long.intervalHours;
  if (fundingPerHour <= 0) return null;

  // Executable entry spread: buy long at ask, sell short at bid.
  const spread = (short.bid - long.ask) / long.ask;
  const fees = 2 * (
    (long.exchange === "Binance" ? BINANCE_FEE : BYBIT_FEE) +
    (short.exchange === "Binance" ? BINANCE_FEE : BYBIT_FEE)
  );
  const recoveryCost = Math.max(0, fees - spread);

  return {
    id: `${token.symbol}-${long.exchange}-${short.exchange}`,
    token,
    long,
    short,
    fundingPerHour,
    apy: fundingPerHour * YEAR_HOURS,
    spread,
    fees,
    breakEvenHours: recoveryCost / fundingPerHour
  };
}

export async function GET() {
  try {
    const [tokens, binance, bybit] = await Promise.all([
      getTopTokens(),
      getBinance(),
      getBybit()
    ]);
    const allowed = new Map(tokens.map((t) => [t.symbol, t]));
    const bMap = new Map(binance.map((x) => [x.base, x]));
    const yMap = new Map(bybit.map((x) => [x.base, x]));
    const opportunities = [];

    for (const [symbol, token] of allowed) {
      const b = bMap.get(symbol);
      const y = yMap.get(symbol);
      if (!b || !y) continue;
      const a = makeOpportunity(token, b, y);
      const c = makeOpportunity(token, y, b);
      if (a) opportunities.push(a);
      if (c) opportunities.push(c);
    }

    opportunities.sort((a, b) => b.apy - a.apy);
    return NextResponse.json({
      opportunities,
      updatedAt: Date.now(),
      universeSize: tokens.length,
      matchedPairs: [...allowed.keys()].filter((s) => bMap.has(s) && yMap.has(s)).length,
      assumptions: { binanceTakerFee: BINANCE_FEE, bybitTakerFee: BYBIT_FEE }
    }, { headers: { "Cache-Control": "public, s-maxage=20, stale-while-revalidate=40" } });
  } catch (error) {
    return NextResponse.json(
      { error: "暂时无法聚合交易所行情", detail: error.message },
      { status: 502, headers: { "Cache-Control": "no-store" } }
    );
  }
}
