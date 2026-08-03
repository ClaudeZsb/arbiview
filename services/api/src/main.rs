mod account_stream;
mod config;
mod market;
mod models;
mod spread_strategy;
mod telegram;
mod trading;

use axum::{
    extract::{Path, State},
    http::{HeaderValue, Method, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use config::Config;
use market::MarketService;
use models::{
    AdjustLeverageRequest, AdjustPositionRequest, BatchIncreaseRequest, BatchReduceRequest,
    OpenTradeRequest, SetAutoCloseRequest,
};
use serde_json::json;
use spread_strategy::SpreadStrategyService;
use std::sync::Arc;
use tower_http::{cors::CorsLayer, trace::TraceLayer};
use trading::TradingService;

#[derive(Clone)]
struct AppState {
    market: MarketService,
    trading: TradingService,
    spread_strategy: SpreadStrategyService,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();
    dotenvy::from_filename(".env.local").ok();
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("arbiview_api=info".parse()?),
        )
        .init();
    let config = Config::from_env()?;
    let market = MarketService::new(config.clone())?;
    let trading = TradingService::new(config.clone(), market.clone())?;
    let spread_strategy = SpreadStrategyService::new(
        market.clone(),
        trading.clone(),
        config.spread_strategy_state_path.clone(),
        config.spread_strategy_enabled,
        config.telegram.clone().map(telegram::TelegramNotifier::new),
    )?;
    let state = Arc::new(AppState {
        market,
        trading,
        spread_strategy,
    });
    state.trading.spawn_auto_close_monitor();
    state.trading.spawn_hedge_protection_monitor();
    state.trading.spawn_account_streams();
    state.spread_strategy.spawn();
    if let Some(telegram) = config.telegram.clone() {
        telegram::spawn(telegram, state.clone());
    }
    let cors = CorsLayer::new()
        .allow_origin(config.web_origin.parse::<HeaderValue>()?)
        .allow_methods([Method::GET, Method::POST])
        .allow_headers(tower_http::cors::Any);
    let app = Router::new()
        .route("/health", get(|| async { Json(json!({"status":"ok"})) }))
        .route("/api/opportunities", get(opportunities))
        .route("/api/position-quotes", get(position_quotes))
        .route("/api/spread-history/:symbol", get(spread_history))
        .route(
            "/api/opportunity-history/:opportunity_id",
            get(opportunity_history),
        )
        .route("/api/account/summary", get(account_summary))
        .route("/api/account/stream-status", get(account_stream_status))
        .route("/api/account/hedge-protection", get(hedge_protection))
        .route("/api/spread-strategy", get(spread_strategy_status))
        .route("/api/auto-close", get(auto_close_rules))
        .route("/api/auto-close/:id/cancel", post(cancel_auto_close))
        .route("/api/positions", get(positions))
        .route("/api/trades/open", post(open_trade))
        .route("/api/trades/batch-increase", post(start_batch_increase))
        .route("/api/trades/batch-reduce", post(start_batch_reduce))
        .route("/api/trades/batch-tasks", get(batch_tasks))
        .route("/api/trades/batch-increase/:id", get(batch_increase))
        .route(
            "/api/trades/batch-increase/:id/cancel",
            post(cancel_batch_increase),
        )
        .route("/api/positions/:id/close", post(close_position))
        .route("/api/positions/:id/reduce", post(reduce_position))
        .route("/api/positions/:id/leverage", post(adjust_leverage))
        .route("/api/positions/:id/auto-close", post(set_auto_close))
        .layer(cors)
        .layer(TraceLayer::new_for_http())
        .with_state(state);
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", config.port)).await?;
    tracing::info!(
        "ArbiView API listening on http://{}",
        listener.local_addr()?
    );
    axum::serve(listener, app).await?;
    Ok(())
}

async fn opportunities(State(state): State<Arc<AppState>>) -> Result<impl IntoResponse, ApiError> {
    Ok(Json(state.market.opportunities().await?))
}

async fn position_quotes(
    State(state): State<Arc<AppState>>,
) -> Result<impl IntoResponse, ApiError> {
    Ok(Json(state.market.position_quotes().await?))
}

async fn account_stream_status(
    State(state): State<Arc<AppState>>,
) -> Result<impl IntoResponse, ApiError> {
    Ok(Json(state.trading.account_stream_status().await))
}

async fn spread_strategy_status(
    State(state): State<Arc<AppState>>,
) -> Result<impl IntoResponse, ApiError> {
    Ok(Json(state.spread_strategy.status().await))
}

async fn spread_history(
    State(state): State<Arc<AppState>>,
    Path(symbol): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    Ok(Json(state.market.spread_history(&symbol).await?))
}

async fn opportunity_history(
    State(state): State<Arc<AppState>>,
    Path(opportunity_id): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    Ok(Json(
        state.market.opportunity_history(&opportunity_id).await?,
    ))
}

async fn account_summary(
    State(state): State<Arc<AppState>>,
) -> Result<impl IntoResponse, ApiError> {
    Ok(Json(state.trading.account_summary().await?))
}

async fn hedge_protection(
    State(state): State<Arc<AppState>>,
) -> Result<impl IntoResponse, ApiError> {
    Ok(Json(state.trading.hedge_protection_status().await))
}

async fn auto_close_rules(
    State(state): State<Arc<AppState>>,
) -> Result<impl IntoResponse, ApiError> {
    Ok(Json(state.trading.auto_close_rules().await))
}

async fn set_auto_close(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(request): Json<SetAutoCloseRequest>,
) -> Result<impl IntoResponse, ApiError> {
    Ok(Json(
        state
            .trading
            .set_auto_close(
                &id,
                request.threshold_apy_percent,
                request.order_notional_usdt,
                request.interval_seconds,
            )
            .await?,
    ))
}

async fn cancel_auto_close(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    Ok(Json(state.trading.cancel_auto_close(&id).await?))
}

async fn positions(State(state): State<Arc<AppState>>) -> Result<impl IntoResponse, ApiError> {
    Ok(Json(state.trading.positions().await?))
}

async fn open_trade(
    State(state): State<Arc<AppState>>,
    Json(request): Json<OpenTradeRequest>,
) -> Result<impl IntoResponse, ApiError> {
    Ok(Json(state.trading.open(request).await?))
}

async fn start_batch_increase(
    State(state): State<Arc<AppState>>,
    Json(request): Json<BatchIncreaseRequest>,
) -> Result<impl IntoResponse, ApiError> {
    Ok(Json(state.trading.start_batch_increase(request).await?))
}

async fn start_batch_reduce(
    State(state): State<Arc<AppState>>,
    Json(request): Json<BatchReduceRequest>,
) -> Result<impl IntoResponse, ApiError> {
    Ok(Json(state.trading.start_batch_reduce(request).await?))
}

async fn batch_increase(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    Ok(Json(state.trading.batch_task(&id).await?))
}

async fn batch_tasks(State(state): State<Arc<AppState>>) -> Result<impl IntoResponse, ApiError> {
    Ok(Json(state.trading.batch_tasks().await))
}

async fn cancel_batch_increase(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    Ok(Json(state.trading.cancel_batch_task(&id).await?))
}

async fn close_position(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    Ok(Json(state.trading.close(&id).await?))
}

async fn reduce_position(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(request): Json<AdjustPositionRequest>,
) -> Result<impl IntoResponse, ApiError> {
    Ok(Json(state.trading.reduce(&id, request).await?))
}

async fn adjust_leverage(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(request): Json<AdjustLeverageRequest>,
) -> Result<impl IntoResponse, ApiError> {
    Ok(Json(
        state.trading.adjust_leverage(&id, request.leverage).await?,
    ))
}

struct ApiError(anyhow::Error);

impl<E: Into<anyhow::Error>> From<E> for ApiError {
    fn from(value: E) -> Self {
        Self(value.into())
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        tracing::error!("{:#}", self.0);
        let message = self.0.to_string();
        let status = if message.contains("NAKED_EXPOSURE") {
            StatusCode::BAD_GATEWAY
        } else {
            StatusCode::BAD_REQUEST
        };
        (
            status,
            Json(json!({
                "error": message
            })),
        )
            .into_response()
    }
}
