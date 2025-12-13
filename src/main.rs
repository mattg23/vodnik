use std::{env, num::NonZero};

use tracing::info;

use crate::meta::{
    BlockLength, BlockNumber, Label, MetaStore, SeriesId, SeriesMeta, StorageType, TimeResolution,
    store::SqliteMetaStore,
};

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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("{VODNIK_ASCII}");
    tracing_subscriber::fmt::init();

    info!("The goblin says 'Hello'");
    let db_url =
        env::var("DATABASE_URL").unwrap_or_else(|_| "sqlite://db.sqlite?mode=rwc".to_string());
    let store = SqliteMetaStore::create(&db_url).await?;

    let series = SeriesMeta {
        id: SeriesId(NonZero::new(1).unwrap()),
        name: "temp".to_string(),
        storage_type: StorageType::Int64,
        block_length: BlockLength(NonZero::new(1).unwrap()),
        block_resolution: TimeResolution::Hour,
        first_block: BlockNumber(NonZero::new(1).unwrap()),
        last_block: BlockNumber(NonZero::new(1).unwrap()),
        labels: vec![Label {
            name: "location".to_string(),
            value: "Paris".to_string(),
        }],
    };

    store.create(&series).await?;

    let count = store.get_all().await?.len();
    info!("{count} series in database");

    Ok(())
}
