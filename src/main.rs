use std::path::PathBuf;

use anyhow::{Result, anyhow};
use clap::Parser;
use tokio::sync::mpsc;
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};
use vixen_indexer::{DEFAULT_CHANNEL_CAPACITY, StreamEvent, load_config, run_with_sender};

#[derive(Parser, Debug)]
#[command(
    name = "vixen-indexer",
    version,
    about = "Minimal block subscriber using Yellowstone Vixen"
)]
struct Cli {
    #[arg(short, long, value_name = "PATH", default_value = "Vixen.toml")]
    config: PathBuf,

    #[arg(long, default_value = "info")]
    log_level: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    init_tracing(&cli.log_level)?;

    let config = load_config(&cli.config)?;
    let (sender, mut receiver) = mpsc::channel(DEFAULT_CHANNEL_CAPACITY);

    println!("Starting block subscriber with config: {:?}", cli.config);

    let printer = tokio::spawn(async move {
        while let Some(event) = receiver.recv().await {
            print_event(&event);
        }
    });

    if let Err(err) = run_with_sender(config, sender).await {
        eprintln!("Runtime terminated with error: {err}");
    }

    let _ = printer.await;
    Ok(())
}

fn print_event(event: &StreamEvent) {
    match event {
        StreamEvent::Block { event } => {
            println!(
                "[BLOCK] slot={} transaction_count={}",
                event.slot, event.transaction_count
            );
        }
        StreamEvent::BlockMeta { event } => {
            println!(
                "[BLOCK_META] slot={} hash={} executed_txs={} entries={} parent_slot={}",
                event.slot, event.blockhash, event.executed_transaction_count, 
                event.entries_count, event.parent_slot
            );
        }
    }
}

fn init_tracing(level: &str) -> Result<()> {
    let filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new(level))
        .unwrap_or_else(|_| EnvFilter::new("info"));

    tracing_subscriber::registry()
        .with(filter)
        .with(tracing_subscriber::fmt::layer())
        .try_init()
        .map_err(|err| anyhow!("failed to initialize tracing: {err}"))
}

