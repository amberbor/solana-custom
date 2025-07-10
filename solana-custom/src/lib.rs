use pyo3::prelude::*;
use pyo3::exceptions::PyRuntimeError;
use pyo3::PyErr;
use pyo3_async_runtimes::tokio;
use pyo3::wrap_pyfunction;
use serde_json;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use bincode::deserialize;
use chrono::Utc;
use std::sync::atomic::{AtomicU64, Ordering, AtomicBool};
use std::sync::Mutex;
use once_cell::sync::OnceCell;

use ::tokio::runtime::Runtime;
use ::tokio::time;
use solana_quic_client::{QuicPool, QuicConnectionManager, QuicConfig};
use solana_connection_cache::connection_cache::NewConnectionConfig;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_tpu_client::nonblocking::tpu_client::TpuClient;
use solana_client::tpu_client::TpuClientConfig;
use solana_transaction_status::{UiTransactionEncoding, EncodedConfirmedTransactionWithStatusMeta};
use solana_sdk::{transaction::Transaction, signature::Signature};
use solana_sdk::pubkey::Pubkey;

// Just the basics - no complex monitoring
static RPC_CLIENT: OnceCell<Arc<RpcClient>> = OnceCell::new();
static TPU_CLIENT: Mutex<Option<Arc<TpuClient<QuicPool, QuicConnectionManager, QuicConfig>>>> = Mutex::new(None);
static RPC_URL: OnceCell<String> = OnceCell::new();
static WS_URL: OnceCell<String> = OnceCell::new();
static FANOUT_SLOTS: AtomicU64 = AtomicU64::new(2);
static GLOBAL_RUNTIME: OnceCell<Runtime> = OnceCell::new();
static REINIT_IN_PROGRESS: AtomicBool = AtomicBool::new(false);
static LAST_LEADER_SLOT: AtomicU64 = AtomicU64::new(0);
static REINIT_COUNT: AtomicU64 = AtomicU64::new(0);

fn get_runtime() -> &'static Runtime {
    GLOBAL_RUNTIME.get_or_init(|| Runtime::new().expect("Failed to create runtime"))
}

// Simple background monitor - just checks for stale slots and reinits
fn start_background_monitor(rpc: Arc<RpcClient>, fanout_slots: u64) {
    get_runtime().spawn(async move {
        println!("[{}] 🔍 Background TPU monitor started - checking every 5 seconds", Utc::now().to_rfc3339());
        let mut check_count = 0;
        
        loop {
            time::sleep(Duration::from_secs(5)).await;
            check_count += 1;
            
            // Show summary every 12 checks (1 minute)
            if check_count % 12 == 0 {
                let total_reinits = REINIT_COUNT.load(Ordering::SeqCst);
                println!("[{}] 📊 Status: {} health checks completed, {} reinits total", 
                    Utc::now().to_rfc3339(), check_count, total_reinits);
            }
                        
            // Get leader slots with timeout protection
            let tpu_client = {
                TPU_CLIENT.lock().unwrap().as_ref().cloned()
            }; // Guard is dropped here
            
            if let Some(tpu_client) = tpu_client {
                // Add timeout to prevent hanging
                let leader_info_result = time::timeout(
                    Duration::from_secs(10), 
                    tpu_client.get_leader_info_slot(fanout_slots)
                ).await;
                
                match leader_info_result {
                    Ok(leader_info) => {
                        if !leader_info.is_empty() {
                            let latest_slot = leader_info.iter().map(|(slot, _, _)| *slot).max().unwrap_or(0);
                            let previous_slot = LAST_LEADER_SLOT.load(Ordering::SeqCst);
                            
                            // if previous_slot == 0 {
                            //     // println!("TPU healthy - First check, slot: {}", latest_slot);
                            // } else if latest_slot > previous_slot {
                            //     // println!("TPU healthy - New slots: {} -> {} (+{})", previous_slot, latest_slot, latest_slot - previous_slot);
                            if latest_slot <= previous_slot {
                                // Determine the reason for reinit
                                let reason = if latest_slot == previous_slot {
                                    "STALE (same slot)"
                                } else {
                                    "BACKWARDS (slot decreased)"
                                };
                                
                                let current_count = REINIT_COUNT.load(Ordering::SeqCst);
                                println!("⚠️  {} detected: {} -> {} (triggering reinit #{}) ", 
                                    reason, previous_slot, latest_slot, current_count + 1);
                                
                                // Simple reinit - fire and forget
                                if REINIT_IN_PROGRESS.compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst).is_ok() {
                                    let reason_owned = reason.to_string();
                                    get_runtime().spawn(async move {
                                        let reinit_number = REINIT_COUNT.fetch_add(1, Ordering::SeqCst) + 1;
                                        print!("[{}] 🔧 Reinit #{} starting (reason: {})... ", 
                                            Utc::now().to_rfc3339(), reinit_number, reason_owned);
                                        
                                        match simple_reinit().await {
                                            Ok(_) => println!("✅ Reinit #{} completed successfully", reinit_number),
                                            Err(e) => println!("❌ Reinit #{} failed: {}", reinit_number, e),
                                        }
                                        REINIT_IN_PROGRESS.store(false, Ordering::SeqCst);
                                    });
                                } else {
                                    println!("(reinit #{} already in progress)", current_count + 1);
                                }
                            }
                            
                            LAST_LEADER_SLOT.store(latest_slot, Ordering::SeqCst);
                        } else {
                            println!("⚠️  Empty leader info received");
                        }
                    }
                    Err(_) => {
                        let current_count = REINIT_COUNT.load(Ordering::SeqCst);
                        println!("❌ TPU call timed out (10s) - network issues (clearing client for reinit #{})", current_count + 1);
                        
                        // Clear TPU client to force fresh connection on next check
                        let mut guard = TPU_CLIENT.lock().unwrap();
                        *guard = None;
                        drop(guard);
                        
                        // Increment counter for timeout-triggered reinit
                        REINIT_COUNT.fetch_add(1, Ordering::SeqCst);
                    }
                }
            } else {
                println!("❌ No TPU client available");
            }
        }
    });
}

// Simple reinit function
async fn simple_reinit() -> Result<(), String> {
    let ws_url = WS_URL.get().ok_or("No WS URL")?;
    let fanout_slots = FANOUT_SLOTS.load(Ordering::SeqCst);
    let rpc = RPC_CLIENT.get().unwrap().clone();
    
    let cfg = TpuClientConfig { fanout_slots };
    let quic_config = QuicConfig::new().unwrap();
    let connection_manager = QuicConnectionManager::new_with_connection_config(quic_config);
    let connection_cache = Arc::new(
        solana_connection_cache::connection_cache::ConnectionCache::new("tpu_client", connection_manager, 8).unwrap()
    );
    
    let new_client = TpuClient::new_with_connection_cache(rpc, ws_url, cfg, connection_cache).await
        .map_err(|e| format!("TPU reinit failed: {}", e))?;
    
    let mut guard = TPU_CLIENT.lock().unwrap();
    *guard = Some(Arc::new(new_client));
    Ok(())
}

// Simple non-blocking reinit - just fire and forget
#[pyfunction]
fn reinit_tpu_client() -> PyResult<()> {
    // Skip if already reinitializing  
    if REINIT_IN_PROGRESS.compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst).is_err() {
        println!("[{}] Reinit already in progress, skipping", Utc::now().to_rfc3339());
        return Ok(());
    }
    
    println!("[{}] Starting TPU reinit (non-blocking)...", Utc::now().to_rfc3339());
    
    get_runtime().spawn(async move {
        let result = async {
            let ws_url = WS_URL.get().ok_or("WS_URL not set")?;
            let fanout_slots = FANOUT_SLOTS.load(Ordering::SeqCst);
            
            let rpc = RPC_CLIENT.get().unwrap().clone();
            let cfg = TpuClientConfig { fanout_slots };
            let quic_config = QuicConfig::new().unwrap();
            let connection_manager = QuicConnectionManager::new_with_connection_config(quic_config);
            let connection_cache = Arc::new(
                solana_connection_cache::connection_cache::ConnectionCache::new(
                    "tpu_client", connection_manager, 8
                ).unwrap()
            );
            
            let new_client = TpuClient::new_with_connection_cache(rpc, ws_url, cfg, connection_cache)
                .await.map_err(|e| format!("TPU reinit failed: {}", e))?;
            
            // Replace client
            let mut guard = TPU_CLIENT.lock().unwrap();
            *guard = Some(Arc::new(new_client));
            Ok::<_, String>(())
        };
        
        // 10 second timeout, then move on
        match time::timeout(Duration::from_secs(10), result).await {
            Ok(Ok(_)) => println!("[{}] TPU reinit completed", Utc::now().to_rfc3339()),
            Ok(Err(e)) => eprintln!("[{}] TPU reinit failed: {}", Utc::now().to_rfc3339(), e),
            Err(_) => eprintln!("[{}] TPU reinit timed out", Utc::now().to_rfc3339()),
        }
        
        REINIT_IN_PROGRESS.store(false, Ordering::SeqCst);
    });
    
    Ok(())
}

// Async helper to fetch leader info
async fn get_leader_info_async(
    _rpc_client: &RpcClient,
    fanout_slots: Option<u64>,
) -> Result<Vec<Pubkey>, PyErr> {
    let tpu_client = TPU_CLIENT.lock().unwrap().as_ref().cloned().ok_or_else(|| PyRuntimeError::new_err("TPU client not initialized"))?;
    let leader_info = tpu_client.get_leader_info(fanout_slots).await;
    Ok(leader_info.into_iter().collect())
}

// Async helper to fetch leader info with slots and TPU address
async fn get_leader_info_slot_async(
    _rpc_client: &RpcClient,
    fanout_slots: u64,
) -> Result<Vec<(u64, Pubkey, SocketAddr)>, PyErr> {
    let tpu_client = TPU_CLIENT.lock().unwrap().as_ref().cloned().ok_or_else(|| PyRuntimeError::new_err("TPU client not initialized"))?;
    let leader_info = tpu_client.get_leader_info_slot(fanout_slots).await;
    Ok(leader_info)
}

#[pyfunction]
fn return_leader_info(
    py: Python,
    fanout_slots: Option<u64>
) -> PyResult<Bound<PyAny>> {
    let rpc_client = RPC_CLIENT.get().ok_or_else(|| PyRuntimeError::new_err("RPC_CLIENT not initialized"))?.clone();
    tokio::future_into_py(py, async move {
        let leader_info = get_leader_info_async(&rpc_client, fanout_slots).await?;
        let leader_strings: Vec<String> = leader_info.into_iter().map(|pubkey| pubkey.to_string()).collect();
        let leader_info_str = serde_json::to_string(&leader_strings)
            .map_err(|e| PyRuntimeError::new_err(format!("JSON serialization error: {}", e)))?;
        Ok(leader_info_str)
    })
}

#[pyfunction]
fn return_leader_info_slots(
    py: Python,
    fanout_slots: Option<u64>
) -> PyResult<Bound<PyAny>> {
    let rpc_client = RPC_CLIENT.get().ok_or_else(|| PyRuntimeError::new_err("RPC_CLIENT not initialized"))?.clone();
    tokio::future_into_py(py, async move {
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
        let leader_info_str = serde_json::to_string(&leader_info_json)
            .map_err(|e| PyRuntimeError::new_err(format!("JSON serialization error: {}", e)))?;
        Ok(leader_info_str)
    })
}

/// Initialize RPC and TPU client
#[pyfunction]
fn init_tpu_clients(
    rpc_url: &str,
    ws_url: &str,
    fanout_slots: Option<u64>,
) -> PyResult<()> {
    // Set the global URLs if not already set
    let _ = RPC_URL.set(rpc_url.to_string());
    let _ = WS_URL.set(ws_url.to_string());
    let fanout_val = fanout_slots.unwrap_or(2);
    FANOUT_SLOTS.store(fanout_val, Ordering::SeqCst);

    let rpc = Arc::new(RpcClient::new(rpc_url.to_string()));
    let _ = RPC_CLIENT.set(rpc.clone());

    let cfg = TpuClientConfig { fanout_slots: fanout_val };
    let rt = get_runtime();
    let quic_config = QuicConfig::new().unwrap();
    let connection_manager = QuicConnectionManager::new_with_connection_config(quic_config);
    let connection_pool_size = 8;
    let connection_cache = Arc::new(
        solana_connection_cache::connection_cache::ConnectionCache::new(
            "tpu_client",
            connection_manager,
            connection_pool_size,
        ).unwrap()
    );
    
    let client = rt.block_on(async {
        TpuClient::new_with_connection_cache(rpc.clone(), ws_url, cfg, connection_cache).await
    }).map_err(|e| PyRuntimeError::new_err(format!("TPU init error: {}", e)))?;
    
    let mut tpu_client_guard = TPU_CLIENT.lock().unwrap();
    *tpu_client_guard = Some(Arc::new(client));
    drop(tpu_client_guard);

    println!("[{}] TPU client initialized with fanout_slots={}", Utc::now().to_rfc3339(), fanout_val);
    
    // Start background monitor
    start_background_monitor(rpc.clone(), fanout_val);
    
    Ok(())
}

/// Internal async confirmation helper
async fn wait_for_confirmation_internal(
    rpc: Arc<RpcClient>,
    signature: Signature,
    poll_time: usize,
    wait_ms: u64,
) -> Result<bool, PyErr> {
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
        time::sleep(Duration::from_millis(wait_ms)).await;
    }
    Ok(false)
}

/// Blocking Python function for polling confirmation
#[pyfunction]
fn wait_for_confirmation(
    signature: &str,
    poll_time: usize,
    wait_ms: u64,
) -> PyResult<bool> {
    if RPC_CLIENT.get().is_none() {
        let rpc_url = RPC_URL.get().ok_or_else(|| PyRuntimeError::new_err("RPC_URL not initialized"))?;
        let ws_url = WS_URL.get().ok_or_else(|| PyRuntimeError::new_err("WS_URL not initialized"))?;
        init_tpu_clients(rpc_url, ws_url, Some(2))?;
    }
    let sig = signature
        .parse::<Signature>()
        .map_err(|e| PyRuntimeError::new_err(format!("Invalid signature: {}", e)))?;
    let rpc = RPC_CLIENT.get().unwrap().clone();

    let rt = Runtime::new()
        .map_err(|e| PyRuntimeError::new_err(format!("Runtime error: {}", e)))?;
    rt.block_on(wait_for_confirmation_internal(rpc, sig, poll_time, wait_ms))
        .map_err(|e| e)
}

#[pyfunction]
fn send_transaction_async(
    py: Python,
    raw_tx: Vec<u8>,
    max_retries: usize,
    fanout_slots: Option<u64>,
) -> PyResult<Bound<PyAny>> {
    let client = TPU_CLIENT
        .lock()
        .unwrap()
        .as_ref()
        .ok_or_else(|| PyRuntimeError::new_err("TPU client not initialized"))?
        .clone();

    tokio::future_into_py(py, async move {
        let tx: Transaction = deserialize(&raw_tx)
            .map_err(|e| PyRuntimeError::new_err(format!("deserialize error: {}", e)))?;
        let sig = tx
            .signatures
            .get(0)
            .cloned()
            .ok_or_else(|| PyRuntimeError::new_err("No signature found in transaction"))?;
        for _ in 1..=max_retries {
            let sent = client.send_transaction(&tx, fanout_slots).await;
            if sent {
                return Ok(sig.to_string());
            }
            time::sleep(Duration::from_millis(100)).await;
        }
        Err(PyRuntimeError::new_err("Failed to send transaction after max retries"))
    })
}

#[pyfunction]
fn send_transaction_batch_async(
    py: Python,
    raw_txs: Vec<Vec<u8>>,
    max_retries: usize,
    fanout_slots: Option<u64>,
) -> PyResult<Bound<PyAny>> {
    let client = TPU_CLIENT
        .lock()
        .unwrap()
        .as_ref()
        .ok_or_else(|| PyRuntimeError::new_err("TPU client not initialized"))?
        .clone();

    tokio::future_into_py(py, async move {
        let transactions: Vec<Transaction> = raw_txs
            .into_iter()
            .map(|raw_tx| deserialize::<Transaction>(&raw_tx))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| PyRuntimeError::new_err(format!("deserialize error: {}", e)))?;

        let wire_transactions: Vec<Vec<u8>> = transactions
            .iter()
            .map(|tx| bincode::serialize(tx).map_err(|e| PyRuntimeError::new_err(format!("serialize error: {}", e))))
            .collect::<Result<_, _>>()?;
        let signatures: Vec<String> = transactions
            .iter()
            .map(|tx| tx.signatures[0].to_string())
            .collect();
        for _ in 1..=max_retries {
            let result = client.try_send_wire_transaction_batch(wire_transactions.clone(), fanout_slots).await;
            if result.is_ok() {
                let signatures_json = serde_json::to_string(&signatures)
                    .map_err(|e| PyRuntimeError::new_err(format!("JSON serialization error: {}", e)))?;
                return Ok(signatures_json);
            }
            time::sleep(Duration::from_millis(100)).await;
        }
        Err(PyRuntimeError::new_err("Failed to send transactions after max retries"))
    })
}

#[pymodule]
fn solana_custom(_py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(init_tpu_clients, m)?)?;
    m.add_function(wrap_pyfunction!(wait_for_confirmation, m)?)?;
    m.add_function(wrap_pyfunction!(send_transaction_async, m)?)?;
    m.add_function(wrap_pyfunction!(return_leader_info, m)?)?;
    m.add_function(wrap_pyfunction!(return_leader_info_slots, m)?)?;
    m.add_function(wrap_pyfunction!(send_transaction_batch_async, m)?)?;
    m.add_function(wrap_pyfunction!(reinit_tpu_client, m)?)?; // Still available for manual control
    Ok(())
}