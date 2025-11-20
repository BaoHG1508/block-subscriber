use std::path::PathBuf;

use anyhow::{Context, Result, anyhow};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use yellowstone_vixen::{
    Runtime,
    config::VixenConfig,
    handler::{Handler, HandlerResult, Pipeline},
    vixen_core::{BlockMetaUpdate, BlockUpdate, ParseResult, Parser as VixenParser},
};
use yellowstone_vixen_core::Prefilter;
use yellowstone_vixen_yellowstone_fumarole_source::{FumaroleConfig, YellowstoneFumaroleSource};

pub const DEFAULT_CHANNEL_CAPACITY: usize = 1024;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct AppConfig {
    #[serde(flatten)]
    pub vixen: VixenConfig<FumaroleConfig>,
}

#[derive(Debug, Clone, Serialize)]
pub struct BlockEvent {
    pub slot: u64,
    pub transaction_count: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct BlockMetaEvent {
    pub slot: u64,
    pub blockhash: String,
    pub executed_transaction_count: u64,
    pub entries_count: u64,
    pub parent_slot: u64,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum StreamEvent {
    Block { event: BlockEvent },
    BlockMeta { event: BlockMetaEvent },
}

pub async fn run_with_sender(config: AppConfig, sender: mpsc::Sender<StreamEvent>) -> Result<()> {
    let AppConfig { vixen } = config;

    let mut builder = Runtime::<YellowstoneFumaroleSource>::builder();

    // Add block subscription (full blocks with transactions)
    let block_parser = BlockPassthroughParser;
    let block_handler = BlockForwarder::new(sender.clone());
    builder = builder.block(Pipeline::new(block_parser, [block_handler]));

    // Add block meta subscription (block metadata)
    let block_meta_parser = BlockMetaPassthroughParser;
    let block_meta_handler = BlockMetaForwarder::new(sender.clone());
    builder = builder.block_meta(Pipeline::new(block_meta_parser, [block_meta_handler]));

    eprintln!("[run_with_sender] building runtime...");
    let runtime = builder.build(vixen);
    eprintln!("[run_with_sender] runtime built, starting async run...");

    let run_result = runtime.try_run_async().await;

    match &run_result {
        Ok(_) => eprintln!("[run_with_sender] runtime completed successfully (unexpected)"),
        Err(e) => eprintln!("[run_with_sender] runtime error: {e}"),
    }

    run_result.map_err(|e| {
        eprintln!("[run_with_sender] runtime terminated with error: {e}");
        anyhow!("runtime terminated: {e}")
    })
}

#[derive(Debug, Clone, Copy)]
struct BlockPassthroughParser;

impl VixenParser for BlockPassthroughParser {
    type Input = BlockUpdate;
    type Output = BlockUpdate;

    fn id(&self) -> std::borrow::Cow<'static, str> {
        "block::passthrough".into()
    }

    fn prefilter(&self) -> Prefilter {
        // Subscribe to full blocks with transactions
        yellowstone_vixen_core::Prefilter::builder()
            .block_include_transactions()
            .build()
            .unwrap()
    }

    fn parse(
        &self,
        update: &Self::Input,
    ) -> impl std::future::Future<Output = ParseResult<Self::Output>> + Send {
        let cloned = update.clone();
        async move { Ok(cloned) }
    }
}

#[derive(Debug, Clone)]
struct BlockForwarder {
    sender: mpsc::Sender<StreamEvent>,
}

impl BlockForwarder {
    fn new(sender: mpsc::Sender<StreamEvent>) -> Self {
        Self { sender }
    }
}

impl Handler<BlockUpdate> for BlockForwarder {
    fn handle(&self, update: &BlockUpdate) -> impl std::future::Future<Output = HandlerResult<()>> + Send {
        let sender = self.sender.clone();
        let slot = update.slot;
        let tx_count = update.transactions.len();
        async move {
            let event = StreamEvent::Block {
                event: BlockEvent {
                    slot,
                    transaction_count: tx_count,
                },
            };
            
            sender
                .send(event)
                .await
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct BlockMetaPassthroughParser;

impl VixenParser for BlockMetaPassthroughParser {
    type Input = BlockMetaUpdate;
    type Output = BlockMetaUpdate;

    fn id(&self) -> std::borrow::Cow<'static, str> {
        "block_meta::passthrough".into()
    }

    fn prefilter(&self) -> Prefilter {
        // Subscribe to block metadata
        yellowstone_vixen_core::Prefilter::builder()
            .block_metas()
            .build()
            .unwrap()
    }

    fn parse(
        &self,
        update: &Self::Input,
    ) -> impl std::future::Future<Output = ParseResult<Self::Output>> + Send {
        let cloned = update.clone();
        async move { Ok(cloned) }
    }
}

#[derive(Debug, Clone)]
struct BlockMetaForwarder {
    sender: mpsc::Sender<StreamEvent>,
}

impl BlockMetaForwarder {
    fn new(sender: mpsc::Sender<StreamEvent>) -> Self {
        Self { sender }
    }
}

impl Handler<BlockMetaUpdate> for BlockMetaForwarder {
    fn handle(&self, update: &BlockMetaUpdate) -> impl std::future::Future<Output = HandlerResult<()>> + Send {
        let sender = self.sender.clone();
        let slot = update.slot;
        let blockhash = update.blockhash.clone();
        let executed_tx_count = update.executed_transaction_count;
        let entries_count = update.entries_count;
        let parent_slot = update.parent_slot;
        async move {
            let event = StreamEvent::BlockMeta {
                event: BlockMetaEvent {
                    slot,
                    blockhash,
                    executed_transaction_count: executed_tx_count,
                    entries_count,
                    parent_slot,
                },
            };
            
            sender
                .send(event)
                .await
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
        }
    }
}


pub fn load_config(path: &PathBuf) -> Result<AppConfig> {
    let raw = std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    let cfg = toml::from_str(&raw).with_context(|| format!("parsing {}", path.display()))?;
    Ok(cfg)
}

pub fn parse_config_str(raw: &str) -> Result<AppConfig> {
    let cfg = toml::from_str(raw).context("parsing Vixen TOML config")?;
    Ok(cfg)
}
