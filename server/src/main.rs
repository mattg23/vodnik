use std::{
    env,
    num::NonZero,
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Instant,
};

use axum::{
    Router,
    body::Body,
    extract::DefaultBodyLimit,
    http::{HeaderValue, Request},
    middleware::Next,
    response::Response,
    routing::get,
};
use opendal::Operator;
use tower_http::trace::{DefaultMakeSpan, TraceLayer};
use tracing::{debug, info, level_filters::LevelFilter, warn};
use tracing_subscriber::{EnvFilter, prelude::*};

use crate::{
    hot::HotSet,
    meta::{block::BlockMetaStore, store::SqlMetaStore},
    wal::{Wal, WalConfig},
};

use vodnik_core::{VODNIK_ASCII, VODNIK_ASCII_REV, wal::WalSync};

mod api;
mod crud;
mod hot;
mod ingest;
mod meta;
mod persistence;
mod query;
mod wal;

#[derive(Clone, Debug)]
struct AppState {
    pub meta_store: SqlMetaStore,
    pub block_meta: BlockMetaStore,
    pub storage: Operator,
    pub hot: Arc<HotSet>,
    pub wal: Arc<Mutex<Wal>>,
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

    let wal_dir = PathBuf::from("/tmp/vodnik_test/wal");

    let wal_config = WalConfig {
        dir: wal_dir.clone(),
        max_file_size: 128 * 1024 * 1024,
        sync_mode: WalSync::FixedTimeMillis(NonZero::new(500).unwrap()),
        //sync_mode: WalSync::Immediate,
    };

    let wal_ptr = Arc::new(Mutex::new(Wal::new(wal_config.clone())?));
    let state = AppState {
        meta_store: store,
        storage: op,
        block_meta: block_store,
        hot: Arc::new(HotSet::new()),
        wal: wal_ptr.clone(),
    };

    // recovery
    info!("starting WAL recovery...");
    let to_recover = wal::find_wal_to_recover(wal_dir.clone())?;
    let len = to_recover.len();
    wal::replay(to_recover, &state).await?;
    wal::force_flush(&state).await?;
    info!("recovered {len} WAL entries.");
    wal::cleanup_wal_files(wal_dir)?;
    info!("recovery completed.");

    let port = 8123;

    let stop_timer = Arc::new(AtomicBool::new(false));

    let handle = if let WalSync::FixedTimeMillis(ms) = wal_config.sync_mode {
        let stop_timer_ref = stop_timer.clone();
        Some(wal::create_background_flush_task(
            ms.get() as u64,
            wal_ptr,
            stop_timer_ref,
        ))
    } else {
        None
    };

    let app = Router::new()
        .route("/health", get(health))
        .merge(api::routes())
        .layer(axum::middleware::from_fn(timing_middleware))
        .layer(DefaultBodyLimit::max(500 * 1024 * 1024))
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(DefaultMakeSpan::new().include_headers(false)),
        )
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{port}"))
        .await
        .unwrap();

    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            tokio::signal::ctrl_c()
                .await
                .expect("failed to install CTRL+C handler");
        })
        .await?;

    if let Some(handle) = handle {
        stop_timer.store(true, Ordering::Release);
        info!("Waiting for flush timer to exit");
        _ = handle.await;
    }

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

async fn timing_middleware(req: Request<Body>, next: Next) -> Response {
    let start = Instant::now();
    let mut response = next.run(req).await;
    let elapsed_ms = start.elapsed().as_millis();

    dbg!(elapsed_ms);

    if let Ok(value) = HeaderValue::from_str(&elapsed_ms.to_string()) {
        response.headers_mut().insert("x-vodnik-service-ms", value);
    }

    response
}
