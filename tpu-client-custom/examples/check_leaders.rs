use {
    solana_rpc_client::rpc_client::RpcClient,
    custom_solana_tpu_client::tpu_client::{TpuClient, TpuClientConfig},
    solana_udp_client::{UdpConfig, UdpConnectionManager, UdpPool},
    std::sync::Arc,
    env_logger,
    std::time::Instant,
};

fn main() {
    env_logger::init();
    let rpc_url = "https://api.mainnet-beta.solana.com";
    let rpc_client = Arc::new(RpcClient::new(rpc_url.to_string()));

    // Set up the connection manager for UDP
    let connection_manager = UdpConnectionManager::default();

    // Set up the TpuClient config
    let config = TpuClientConfig { fanout_slots: 20 };

    // Create the blocking TpuClient
    let tpu_client = TpuClient::<UdpPool, UdpConnectionManager, UdpConfig>::new(
        "tpu_client_example",
        rpc_client.clone(),
        "", // websocket_url
        config,
        connection_manager,
    ).expect("Failed to create TpuClient");

    // Get and print leader info
    let start_time = Instant::now();
    let leader_info = tpu_client.get_leader_info(100);
    let end_time = Instant::now();

    println!("Start time: {:?}", start_time);
    println!("End time: {:?}", end_time);
    println!("Elapsed: {:?}", end_time.duration_since(start_time));

    for (i, (slot, leader, tpu_addr)) in leader_info.iter().enumerate() {
        println!(
            "Count: {}, Slot: {}, Leader: {} TPU: {}",
            i + 1,
            slot,
            leader,
            tpu_addr
        );
    }
}