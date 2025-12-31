use std::{
    env,
    sync::{Arc, Mutex},
};

use axum::{Router, routing::get};
use opendal::Operator;
use tower_http::{
    LatencyUnit,
    trace::{DefaultMakeSpan, DefaultOnFailure, DefaultOnRequest, DefaultOnResponse, TraceLayer},
};
use tracing::Level;
//use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use tracing_subscriber::{EnvFilter, fmt, prelude::*};

use crate::{hot::HotSet, meta::store::SqlMetaStore};

mod api;
mod crud;
mod helpers;
mod hot;
mod ingest;
mod meta;

pub const VODNIK_ASCII: &str = r#"
         ~~~~~~~
     ~~~  VODNÍK  ~~~
         ~~~~~~~
           ___
         .(o o).
     _oOO--(_)--OOo_
        /  /~~~\  \
       |  | ~~~ |  |
        \  \____/  /
         '--.___.--'
"#;

#[derive(Clone, Debug)]
struct AppState {
    pub meta_store: SqlMetaStore,
    pub storage: Operator,
    pub hot: Arc<HotSet>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("{VODNIK_ASCII}");

    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .with(EnvFilter::from_env("VODNIK_LOG"))
        .init();

    let db_url =
        env::var("DATABASE_URL").unwrap_or_else(|_| "sqlite://db.sqlite?mode=rwc".to_string());
    let store = SqlMetaStore::create(&db_url).await?;

    let mut builder = opendal::services::Fs::default();
    builder = builder.root("/tmp/vodnik_test");

    let op = Operator::new(builder)
        .unwrap()
        .layer(opendal::layers::LoggingLayer::default())
        .finish();

    let state = AppState {
        meta_store: store,
        storage: op,
        hot: Arc::new(HotSet::new()),
    };

    let app = Router::new()
        .route("/health", get(health))
        .merge(api::routes())
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

async fn health() -> &'static str {
    VODNIK_ASCII
}
