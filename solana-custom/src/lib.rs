use pyo3::prelude::*;
use pyo3::exceptions::PyRuntimeError;
use pyo3::PyErr;
use pyo3_async_runtimes::tokio;
use pyo3::wrap_pyfunction;
use pyo3::types::PyString;
use serde_json;
use std::net::SocketAddr;

use once_cell::sync::OnceCell;
use std::sync::Arc;
use std::time::Duration;
use bincode::deserialize;
use chrono::Utc;
use std::sync::atomic::{AtomicUsize, Ordering, AtomicU64};
use std::sync::Mutex;

use ::tokio::runtime::Builder as RuntimeBuilder;
use ::tokio::runtime::Runtime;
use ::tokio::time;
use ::tokio::task::JoinHandle;
use solana_quic_client::{QuicPool, QuicConnectionManager, QuicConfig};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_tpu_client::nonblocking::tpu_client::TpuClient;
use solana_client::tpu_client::TpuClientConfig;
use solana_transaction_status::{UiTransactionEncoding, EncodedConfirmedTransactionWithStatusMeta};
use solana_sdk::{transaction::Transaction, signature::Signature};
use solana_sdk::pubkey::Pubkey;
use solana_connection_cache::connection_cache::NewConnectionConfig;
use std::collections::HashSet;

// Static globals for RPC and TPU client
static RPC_CLIENT: OnceCell<Arc<RpcClient>> = OnceCell::new();
static TPU_CLIENT: Mutex<Option<Arc<TpuClient<QuicPool, QuicConnectionManager, QuicConfig>>>> = Mutex::new(None);
static FAIL_COUNT: AtomicUsize = AtomicUsize::new(0);

// Global variables for RPC and WS URLs
static RPC_URL: OnceCell<String> = OnceCell::new();
static WS_URL: OnceCell<String> = OnceCell::new();
static FANOUT_SLOTS: AtomicU64 = AtomicU64::new(2); // Default value

static GLOBAL_RUNTIME: OnceCell<Runtime> = OnceCell::new();

// Global variables for slot tracking
static CURRENT_SLOT: AtomicU64 = AtomicU64::new(0);
static SLOT_UPDATE_TASK: OnceCell<JoinHandle<()>> = OnceCell::new();
static TPU_HEALTH_CHECK_TASK: OnceCell<JoinHandle<()>> = OnceCell::new();

fn get_or_init_runtime() -> &'static Runtime {
    GLOBAL_RUNTIME.get_or_init(|| {
        Runtime::new().expect("Failed to create Tokio runtime")
    })
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

    let cfg = TpuClientConfig {
        fanout_slots: fanout_val,
    };
    let rt = get_or_init_runtime();
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
        TpuClient::new_with_connection_cache(
            rpc.clone(),
            ws_url,
            cfg,
            connection_cache,
        ).await
    }).map_err(|e| PyRuntimeError::new_err(format!("TPU init error: {}", e)))?;
    let client = Arc::new(client);
    let mut tpu_client_guard = TPU_CLIENT.lock().unwrap();
    *tpu_client_guard = Some(client.clone());

    println!(
        "[{}][init] RPC+TPU fanout_slots={} initialized",
        Utc::now().to_rfc3339(),
        fanout_val
    );

    // Creating connection cache before for all leaders
    

    // pyo3_asyncio::tokio::get_runtime().spawn({
    //     let client = client.clone();
    //     async move {
    //         loop {
    //             let leader_info = client.get_leader_info_slot(2).await;
    //             for (_slot, _leader_pubkey, tpu_addr) in leader_info {
    //                 println!("[DEBUG] Getting connection for slot: {}, leader: {}, TPU address: {}", _slot, _leader_pubkey, tpu_addr);

                    
    //                 client.connection_cache().get_nonblocking_connection(&tpu_addr);
    //             }
    //             tokio::time::sleep(Duration::from_secs(10)).await;
    //         }
    //     }
    // });

    // Start slot tracking
    start_slot_tracking_task(rpc);
    start_tpu_health_check_task();

    Ok(())
}

async fn reinit_tpu_client() -> Result<(), PyErr> {
    let rpc_url = RPC_URL.get().ok_or_else(|| PyRuntimeError::new_err("RPC_URL not initialized"))?.clone();
    let ws_url = WS_URL.get().ok_or_else(|| PyRuntimeError::new_err("WS_URL not initialized"))?.clone();
    let fanout_slots = FANOUT_SLOTS.load(Ordering::SeqCst);

    let rpc = match RPC_CLIENT.get() {
        Some(client) => client.clone(),
        None => {
             Arc::new(RpcClient::new(rpc_url.clone()))
        }
    };

    let cfg = TpuClientConfig {
        fanout_slots,
    };

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
    
    let reinit_fut = TpuClient::new_with_connection_cache(rpc.clone(), &ws_url, cfg, connection_cache);

    // Add a 15-second timeout to the re-initialization process itself.
    match time::timeout(Duration::from_secs(15), reinit_fut).await {
        Ok(Ok(client)) => { // Re-initialization succeeded within the time limit.
            let client_arc = Arc::new(client);
            let mut tpu_client_guard = TPU_CLIENT.lock().unwrap();
            *tpu_client_guard = Some(client_arc);
            println!(
                "[{}][reinit] RPC+TPU with fanout_slots={} re-initialized successfully.",
                Utc::now().to_rfc3339(),
                fanout_slots
            );
            Ok(())
        }
        Ok(Err(e)) => { // Re-initialization failed, but didn't time out.
            let err_msg = format!("Failed to re-initialize TPU client: {}", e);
            eprintln!("[{}] {}", Utc::now().to_rfc3339(), err_msg);
            Err(PyRuntimeError::new_err(err_msg))
        }
        Err(_) => { // Re-initialization timed out.
            let err_msg = "TPU client re-initialization timed out after 15 seconds.".to_string();
            eprintln!("[{}] {}", Utc::now().to_rfc3339(), err_msg);
            Err(PyRuntimeError::new_err(err_msg))
        }
    }
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

fn print_total_leader_schedule(rpc: &Arc<RpcClient>) {
    let rt = get_or_init_runtime();
    match rt.block_on(async { rpc.get_leader_schedule(None).await }) {
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
}

/// Get the current slot value
#[pyfunction]
fn get_current_slot() -> PyResult<u64> {
    Ok(CURRENT_SLOT.load(Ordering::SeqCst))
}

/// Start background slot tracking task
fn start_slot_tracking_task(rpc: Arc<RpcClient>) {
    if SLOT_UPDATE_TASK.get().is_none() {
        let rpc_clone = rpc.clone();
        let rt = get_or_init_runtime();
        let handle = rt.spawn(async move {
            loop {
                match rpc_clone.get_slot().await {
                    Ok(slot) => {
                        CURRENT_SLOT.store(slot, Ordering::SeqCst);
                    }
                    Err(e) => {
                        eprintln!("[{}] Error fetching slot: {}", Utc::now().to_rfc3339(), e);
                    }
                }
                time::sleep(Duration::from_millis(500)).await;
            }
        });
        let _ = SLOT_UPDATE_TASK.set(handle);
    }
}

fn start_tpu_health_check_task() {
    if TPU_HEALTH_CHECK_TASK.get().is_none() {
        let rt = get_or_init_runtime();
        let handle = rt.spawn(async move {
            loop {
                time::sleep(Duration::from_secs(10)).await;
                
                let tpu_client_opt = TPU_CLIENT.lock().unwrap().clone();
                let mut needs_reinit = false;
                const STALE_SLOT_THRESHOLD: u64 = 20; 

                if let Some(tpu_client) = tpu_client_opt {
                    let rpc_slot = CURRENT_SLOT.load(Ordering::SeqCst);
                    let tpu_slot = tpu_client.tpu_client_slot();

                    if rpc_slot > tpu_slot.saturating_add(STALE_SLOT_THRESHOLD) {
                        eprintln!(
                            "[{}] Health Check Failed: TPU client slot ({}) is stale compared to RPC slot ({}).",
                            Utc::now().to_rfc3339(), tpu_slot, rpc_slot
                        );
                        needs_reinit = true;
                    } else {
                        println!("[{}] Health Check Passed: TPU client is responsive and current.", Utc::now().to_rfc3339());
                    }
                } else {
                    eprintln!("[{}] Health Check Failed: TPU client is not initialized.", Utc::now().to_rfc3339());
                    needs_reinit = true;
                }

                if needs_reinit {
                    eprintln!("[{}] Attempting to auto-recover TPU client...", Utc::now().to_rfc3339());
                    if let Err(e) = reinit_tpu_client().await {
                        eprintln!("[{}] Auto-recovery failed: {}", Utc::now().to_rfc3339(), e);
                    }
                }
            }
        });
        let _ = TPU_HEALTH_CHECK_TASK.set(handle);
    }
}

#[pyfunction]
fn test_clear_tpu_client() -> PyResult<()> {
    let mut tpu_client_guard = TPU_CLIENT.lock().unwrap();
    *tpu_client_guard = None;
    println!("[{}] TPU client manually cleared for testing.", Utc::now().to_rfc3339());
    Ok(())
}

#[pymodule]
fn solana_custom(_py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(init_tpu_clients, m)?)?;
    m.add_function(wrap_pyfunction!(wait_for_confirmation, m)?)?;
    m.add_function(wrap_pyfunction!(send_transaction_async, m)?)?;
    m.add_function(wrap_pyfunction!(return_leader_info, m)?)?;
    m.add_function(wrap_pyfunction!(return_leader_info_slots, m)?)?;
    m.add_function(wrap_pyfunction!(send_transaction_batch_async, m)?)?;
    m.add_function(wrap_pyfunction!(get_current_slot, m)?)?;
    m.add_function(wrap_pyfunction!(test_clear_tpu_client, m)?)?;
    Ok(())
}