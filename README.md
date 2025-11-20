# Stream Core

A minimal Rust library for subscribing to Solana blocks and block metadata using [Yellowstone Vixen](https://github.com/rpcpool/yellowstone-vixen) and [Fumarole](https://github.com/rpcpool/fumarole).

## Features

- **Block Subscription**: Subscribe to full blocks with transactions
- **Block Metadata Subscription**: Subscribe to lightweight block metadata
- **Minimal Dependencies**: Focused on block streaming without unnecessary complexity
- **Async/Await**: Built on Tokio for high-performance async streaming

## Requirements

- Rust 1.70+ (edition 2021)
- A Solana RPC endpoint with Fumarole support
- An x-token for authentication (if required by your provider)

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
stream-core = { path = "stream-core" }
```

Or from git (when published):

```toml
[dependencies]
stream-core = { git = "https://github.com/yourusername/stream-core" }
```

## Configuration

Create a `Vixen.toml` configuration file:

```toml
[source]
endpoint = "https://your-solana-rpc-endpoint.com"
x-token = "your-x-token-here"
subscriber-name = "block-subscriber"

[buffer]
sources-channel-size = 100
```

### Configuration Options

- `endpoint`: Your Solana RPC endpoint URL (must support Fumarole)
- `x-token`: Authentication token (if required by your provider)
- `subscriber-name`: Unique identifier for this subscriber instance
- `sources-channel-size`: Buffer size for incoming updates (default: 100)

## Usage

### As a Library

```rust
use stream_core::{load_config, run_with_sender, StreamEvent, DEFAULT_CHANNEL_CAPACITY};
use tokio::sync::mpsc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = load_config(&"Vixen.toml".into())?;
    let (sender, mut receiver) = mpsc::channel(DEFAULT_CHANNEL_CAPACITY);

    // Spawn a task to handle events
    tokio::spawn(async move {
        while let Some(event) = receiver.recv().await {
            match event {
                StreamEvent::Block { event } => {
                    println!("Block: slot={}, txs={}", event.slot, event.transaction_count);
                }
                StreamEvent::BlockMeta { event } => {
                    println!("BlockMeta: slot={}, hash={}", event.slot, event.blockhash);
                }
            }
        }
    });

    // Run the subscriber
    run_with_sender(config, sender).await?;
    Ok(())
}
```

### As a Binary

Run the included binary:

```bash
cargo run -- --config Vixen.toml
```

Or with custom log level:

```bash
cargo run -- --config Vixen.toml --log-level debug
```

## Event Types

### Block Event

Full block updates with transaction data:

```rust
StreamEvent::Block {
    event: BlockEvent {
        slot: u64,
        transaction_count: usize,
    }
}
```

### Block Meta Event

Lightweight block metadata:

```rust
StreamEvent::BlockMeta {
    event: BlockMetaEvent {
        slot: u64,
        blockhash: String,
        executed_transaction_count: u64,
        entries_count: u64,
        parent_slot: u64,
    }
}
```

## Architecture

The library uses two parallel pipelines:

1. **Block Pipeline**: Subscribes to full blocks with transactions using `block_include_transactions()` prefilter
2. **Block Meta Pipeline**: Subscribes to block metadata using `block_metas()` prefilter

Both pipelines run concurrently and forward events through a shared channel.

## Dependencies

- `yellowstone-vixen`: Core Vixen runtime
- `yellowstone-vixen-yellowstone-fumarole-source`: Fumarole source implementation
- `yellowstone-vixen-core`: Core types and prefilters
- `tokio`: Async runtime
- `serde`: Serialization
- `anyhow`: Error handling

## Example Output

```
[BLOCK] slot=123456789 transaction_count=42
[BLOCK_META] slot=123456789 hash=ABC123... executed_txs=42 entries=100 parent_slot=123456788
[BLOCK] slot=123456790 transaction_count=38
[BLOCK_META] slot=123456790 hash=DEF456... executed_txs=38 entries=95 parent_slot=123456789
```
