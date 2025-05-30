# Custom Solana TPU Client

A custom implementation of the Solana TPU (Transaction Processing Unit) client that provides enhanced functionality for sending transactions directly to the current leader's TPU port over UDP.

## Features

- Direct TPU communication for faster transaction processing
- Separate TPU clients for buy and sell operations
- Async transaction submission
- Batch transaction submission
- Automatic client reinitialization on failures
- Configurable fanout slots for leader tracking

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
custom-solana-tpu-client = "0.2.0"
```

## Usage

```rust
use {
    custom_solana_tpu_client::tpu_client::{TpuClient, TpuClientConfig},
    solana_rpc_client::rpc_client::RpcClient,
    solana_udp_client::{UdpConfig, UdpConnectionManager, UdpPool},
    std::sync::Arc,
};

// Create RPC client
let rpc_client = Arc::new(RpcClient::new("https://api.mainnet-beta.solana.com".to_string()));

// Set up connection manager
let connection_manager = UdpConnectionManager::default();

// Configure TPU client
let config = TpuClientConfig { fanout_slots: 20 };

// Create TPU client
let tpu_client = TpuClient::<UdpPool, UdpConnectionManager, UdpConfig>::new(
    "tpu_client_example",
    rpc_client.clone(),
    "", // websocket_url
    config,
    connection_manager,
).expect("Failed to create TpuClient");

// Get leader information
let leader_info = tpu_client.get_leader_info(100);
```

## Examples

Check out the [examples](examples/) directory for more usage examples:

- `check_leaders.rs`: Demonstrates how to get and display leader information
- More examples coming soon...

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.