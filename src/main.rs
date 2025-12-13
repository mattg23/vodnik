use tracing::info;

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

    Ok(())
}
