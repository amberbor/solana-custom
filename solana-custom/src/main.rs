use solana_client::nonblocking::rpc_client::RpcClient;
use std::sync::Arc;
use tokio::runtime::Runtime;

fn main() {
    let rpc_url = "https://api.mainnet-beta.solana.com";
    let rt = Runtime::new().expect("Failed to create Tokio runtime");
    let rpc = Arc::new(RpcClient::new(rpc_url.to_string()));
    rt.block_on(async {
        match rpc.get_leader_schedule(None).await {
            Ok(Some(schedule)) => {
                let num_unique_leaders = schedule.keys().count();
                let num_slot_assignments = schedule.values().map(|v| v.len()).sum::<usize>();
                println!("[DEBUG] Number of unique leader pubkeys in current epoch: {}", num_unique_leaders);
                println!("[DEBUG] Total slot assignments in current epoch: {}", num_slot_assignments);
            },
            Ok(None) => {
                println!("[DEBUG] No leader schedule returned by RPC");
            },
            Err(e) => {
                println!("[DEBUG] Error fetching leader schedule: {}", e);
            }
        }
    });
} 