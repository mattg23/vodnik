use std::{
    env,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use axum::{Router, extract::DefaultBodyLimit, routing::get};
use opendal::Operator;
use tower_http::trace::{DefaultMakeSpan, TraceLayer};
use tracing::level_filters::LevelFilter;
use tracing_subscriber::{EnvFilter, prelude::*};

use crate::{
    hot::HotSet,
    meta::{block::BlockMetaStore, store::SqlMetaStore},
};

use vodnik_core::{VODNIK_ASCII, VODNIK_ASCII_REV};

mod api;
mod crud;
mod hot;
mod ingest;
mod meta;
mod persistence;
mod query;

#[derive(Clone, Debug)]
struct AppState {
    pub meta_store: SqlMetaStore,
    pub block_meta: BlockMetaStore,
    pub storage: Operator,
    pub hot: Arc<HotSet>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("{VODNIK_ASCII}");

    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .with(
            EnvFilter::builder()
                .with_default_directive(LevelFilter::INFO.into())
                .with_env_var("VODNIK_LOG")
                .from_env_lossy(),
        )
        .init();

    let db_url =
        env::var("DATABASE_URL").unwrap_or_else(|_| "sqlite://db.sqlite?mode=rwc".to_string());

    let db = meta::store::create(&db_url).await?;

    let store = SqlMetaStore::new(db.clone());
    let block_store = BlockMetaStore::new(db);

    let mut builder = opendal::services::Fs::default();
    builder = builder.root("/tmp/vodnik_test");

    let op = Operator::new(builder)
        .unwrap()
        .layer(opendal::layers::LoggingLayer::default())
        .finish();

    let state = AppState {
        meta_store: store,
        storage: op,
        block_meta: block_store,
        hot: Arc::new(HotSet::new()),
    };

    let app = Router::new()
        .route("/health", get(health))
        .merge(api::routes())
        .layer(DefaultBodyLimit::max(500 * 1024 * 1024))
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(DefaultMakeSpan::new().include_headers(false)),
        )
        .with_state(state);

    let port = 8123;

    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{port}"))
        .await
        .unwrap();
    axum::serve(listener, app).await?;
    Ok(())
}

static CNT: AtomicUsize = AtomicUsize::new(0);

async fn health() -> &'static str {
    if CNT.fetch_add(1, Ordering::Relaxed) % 2 == 0 {
        VODNIK_ASCII
    } else {
        VODNIK_ASCII_REV
    }
}
