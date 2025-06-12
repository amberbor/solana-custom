use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use bincode::deserialize;
use chrono::Utc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use once_cell::sync::OnceCell;

use tokio::runtime::Builder as RuntimeBuilder;
use solana_quic_client::{QuicPool, QuicConnectionManager, QuicConfig};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_tpu_client::nonblocking::tpu_client::TpuClient;
use solana_client::tpu_client::TpuClientConfig;
use solana_transaction_status::{UiTransactionEncoding, EncodedConfirmedTransactionWithStatusMeta};
use solana_sdk::{transaction::Transaction, signature::Signature};
use solana_client::rpc_response::RpcLeaderSchedule;
use solana_sdk::pubkey::Pubkey;
use std::str::FromStr;
use solana_connection_cache::connection_cache::{NewConnectionConfig, ConnectionCache};
use tokio::runtime::Runtime;

// Static globals for RPC and TPU client
static RPC_CLIENT: OnceCell<Arc<RpcClient>> = OnceCell::new();
static TPU_CLIENT: Mutex<Option<Arc<TpuClient<QuicPool, QuicConnectionManager, QuicConfig>>>> = Mutex::new(None);
static FAIL_COUNT: AtomicUsize = AtomicUsize::new(0);

// Global variables for RPC and WS URLs
static RPC_URL: OnceCell<String> = OnceCell::new();
static WS_URL: OnceCell<String> = OnceCell::new();

static GLOBAL_RUNTIME: OnceCell<Runtime> = OnceCell::new();

fn get_or_init_runtime() -> &'static Runtime {
    GLOBAL_RUNTIME.get_or_init(|| {
        Runtime::new().expect("Failed to create Tokio runtime")
    })
}

// Async helper to fetch leader info
async fn get_leader_info_async(
    _rpc_client: &RpcClient,
    fanout_slots: Option<u64>,
) -> Result<Vec<Pubkey>, Box<dyn std::error::Error>> {
    let tpu_client = TPU_CLIENT.lock().unwrap().as_ref().cloned()
        .ok_or_else(|| "TPU client not initialized")?;
    let leader_info = tpu_client.get_leader_info(fanout_slots).await;
    Ok(leader_info.into_iter().collect())
}

// Async helper to fetch leader info with slots and TPU address
async fn get_leader_info_slot_async(
    _rpc_client: &RpcClient,
    fanout_slots: u64,
) -> Result<Vec<(u64, Pubkey, SocketAddr)>, Box<dyn std::error::Error>> {
    let tpu_client = TPU_CLIENT.lock().unwrap().as_ref().cloned()
        .ok_or_else(|| "TPU client not initialized")?;
    let leader_info = tpu_client.get_leader_info_slot(fanout_slots).await;
    Ok(leader_info)
}

async fn return_leader_info(
    fanout_slots: Option<u64>
) -> Result<String, Box<dyn std::error::Error>> {
    let rpc_client = RPC_CLIENT.get().ok_or_else(|| "RPC_CLIENT not initialized")?.clone();
    let leader_info = get_leader_info_async(&rpc_client, fanout_slots).await?;
    let leader_strings: Vec<String> = leader_info.into_iter().map(|pubkey| pubkey.to_string()).collect();
    let leader_info_str = serde_json::to_string(&leader_strings)?;
    Ok(leader_info_str)
}

async fn return_leader_info_slots(
    fanout_slots: Option<u64>
) -> Result<String, Box<dyn std::error::Error>> {
    let rpc_client = RPC_CLIENT.get().ok_or_else(|| "RPC_CLIENT not initialized")?.clone();
    let leader_info = get_leader_info_slot_async(&rpc_client, fanout_slots.unwrap_or(1)).await?;
    let leader_info_json: Vec<serde_json::Value> = leader_info.into_iter()
        .map(|(slot, pubkey, tpu_addr)| {
            serde_json::json!({
                "slot": slot,
                "leader": pubkey.to_string(),
                "tpu_addr": tpu_addr.to_string(),
            })
        })
        .collect();
    let leader_info_str = serde_json::to_string(&leader_info_json)?;
    Ok(leader_info_str)
}

/// Initialize RPC and TPU client
async fn init_tpu_clients(
    rpc_url: &str,
    ws_url: &str,
    fanout_slots: Option<u64>,
) -> Result<(), Box<dyn std::error::Error>> {
    // Set the global URLs if not already set
    let _ = RPC_URL.set(rpc_url.to_string());
    let _ = WS_URL.set(ws_url.to_string());

    let rpc = Arc::new(RpcClient::new(rpc_url.to_string()));
    print_total_leader_schedule(&rpc).await?;
    let _ = RPC_CLIENT.set(rpc.clone());

    let cfg = TpuClientConfig {
        fanout_slots: fanout_slots.unwrap_or(1),
    };
    let rt = get_or_init_runtime();
    let quic_config = QuicConfig::new()?;
    let connection_manager = QuicConnectionManager::new_with_connection_config(quic_config);
    let connection_pool_size = 8;
    let connection_cache = Arc::new(
        ConnectionCache::new(
            "tpu_client",
            connection_manager,
            connection_pool_size,
        )?
    );
    let client = TpuClient::new_with_connection_cache(
        rpc.clone(),
        ws_url,
        cfg,
        connection_cache,
    ).await?;
    let client = Arc::new(client);
    let mut tpu_client_guard = TPU_CLIENT.lock().unwrap();
    *tpu_client_guard = Some(client.clone());

    println!(
        "[{}][init] RPC+TPU fanout_slots={} initialized",
        Utc::now().to_rfc3339(),
        fanout_slots.unwrap_or(2)
    );

    Ok(())
}

/// Internal async confirmation helper
async fn wait_for_confirmation_internal(
    rpc: Arc<RpcClient>,
    signature: Signature,
    poll_time: usize,
    wait_ms: u64,
) -> Result<bool, Box<dyn std::error::Error>> {
    for _ in 1..=poll_time {
        let res: Result<EncodedConfirmedTransactionWithStatusMeta, _> =
            rpc.get_transaction(&signature, UiTransactionEncoding::JsonParsed).await;
        match res {
            Ok(txm) => {
                if let Some(meta) = txm.transaction.meta {
                    return Ok(meta.err.is_none());
                }
            }
            Err(_) => (),
        }
        tokio::time::sleep(Duration::from_millis(wait_ms)).await;
    }
    Ok(false)
}

async fn wait_for_confirmation(
    signature: &str,
    poll_time: usize,
    wait_ms: u64,
) -> Result<bool, Box<dyn std::error::Error>> {
    if RPC_CLIENT.get().is_none() {
        let rpc_url = RPC_URL.get().ok_or_else(|| "RPC_URL not initialized")?;
        let ws_url = WS_URL.get().ok_or_else(|| "WS_URL not initialized")?;
        init_tpu_clients(rpc_url, ws_url, Some(2)).await?;
    }
    let sig = signature.parse::<Signature>()?;
    let rpc = RPC_CLIENT.get().unwrap().clone();
    wait_for_confirmation_internal(rpc, sig, poll_time, wait_ms).await
}

async fn send_transaction_async(
    raw_tx: Vec<u8>,
    max_retries: usize,
    fanout_slots: Option<u64>,
) -> Result<String, Box<dyn std::error::Error>> {
    let client = TPU_CLIENT
        .lock()
        .unwrap()
        .as_ref()
        .ok_or_else(|| "TPU client not initialized")?
        .clone();

    let tx: Transaction = deserialize(&raw_tx)?;
    let sig = tx
        .signatures
        .get(0)
        .cloned()
        .ok_or_else(|| "No signature found in transaction")?;
    
    for _ in 1..=max_retries {
        let sent = client.send_transaction(&tx, fanout_slots).await;
        if sent {
            return Ok(sig.to_string());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Err("Failed to send transaction after max retries".into())
}

async fn send_transaction_batch_async(
    raw_txs: Vec<Vec<u8>>,
    max_retries: usize,
    fanout_slots: Option<u64>,
) -> Result<String, Box<dyn std::error::Error>> {
    let client = TPU_CLIENT
        .lock()
        .unwrap()
        .as_ref()
        .ok_or_else(|| "TPU client not initialized")?
        .clone();

    let transactions: Vec<Transaction> = raw_txs
        .into_iter()
        .map(|raw_tx| deserialize(&raw_tx))
        .collect::<Result<Vec<_>, _>>()?;
    
    let wire_transactions: Vec<Vec<u8>> = transactions
        .iter()
        .map(|tx| bincode::serialize(tx))
        .collect::<Result<_, _>>()?;
    
    let signatures: Vec<String> = transactions
        .iter()
        .map(|tx| tx.signatures[0].to_string())
        .collect();

    for _ in 1..=max_retries {
        let result = client.try_send_wire_transaction_batch(wire_transactions.clone(), fanout_slots).await;
        if result.is_ok() {
            let signatures_json = serde_json::to_string(&signatures)?;
            return Ok(signatures_json);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Err("Failed to send transactions after max retries".into())
}

async fn print_total_leader_schedule(rpc: &Arc<RpcClient>) -> Result<(), Box<dyn std::error::Error>> {
    match rpc.get_leader_schedule(None).await {
        Ok(Some(schedule)) => {
            let unique_leaders: std::collections::HashSet<_> = schedule.values().flat_map(|v| v.iter()).collect();
            println!("[DEBUG] Total unique leaders in current epoch: {}", unique_leaders.len());
        },
        Ok(None) => {
            println!("[DEBUG] No leader schedule returned by RPC");
        },
        Err(e) => {
            println!("[DEBUG] Error fetching leader schedule: {}", e);
        }
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Example usage
    let rpc_url = "https://api.mainnet-beta.solana.com";
    let ws_url = "wss://api.mainnet-beta.solana.com";
    
    // Initialize clients
    init_tpu_clients(rpc_url, ws_url, Some(2)).await?;
    
    // Example: Get leader info
    let leader_info = return_leader_info(Some(2)).await?;
    println!("Leader info: {}", leader_info);
    
    // Example: Get leader info with slots
    let leader_info_slots = return_leader_info_slots(Some(2)).await?;
    println!("Leader info with slots: {}", leader_info_slots);
    
    Ok(())
} 