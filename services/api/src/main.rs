mod config;
mod market;
mod models;
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
use models::{AdjustLeverageRequest, AdjustPositionRequest, OpenTradeRequest};
use serde_json::json;
use std::sync::Arc;
use tower_http::{cors::CorsLayer, trace::TraceLayer};
use trading::TradingService;

#[derive(Clone)]
struct AppState {
    market: MarketService,
    trading: TradingService,
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
    let state = Arc::new(AppState { market, trading });
    let cors = CorsLayer::new()
        .allow_origin(config.web_origin.parse::<HeaderValue>()?)
        .allow_methods([Method::GET, Method::POST])
        .allow_headers(tower_http::cors::Any);
    let app = Router::new()
        .route("/health", get(|| async { Json(json!({"status":"ok"})) }))
        .route("/api/opportunities", get(opportunities))
        .route("/api/account/summary", get(account_summary))
        .route("/api/positions", get(positions))
        .route("/api/trades/open", post(open_trade))
        .route("/api/positions/:id/close", post(close_position))
        .route("/api/positions/:id/reduce", post(reduce_position))
        .route("/api/positions/:id/leverage", post(adjust_leverage))
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

async fn account_summary(
    State(state): State<Arc<AppState>>,
) -> Result<impl IntoResponse, ApiError> {
    Ok(Json(state.trading.account_summary().await?))
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
