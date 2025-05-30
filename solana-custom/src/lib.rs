use pyo3::prelude::*;
use pyo3::exceptions::PyRuntimeError;
use pyo3::PyErr;
use pyo3_asyncio::tokio as pyo3_tokio;
use pyo3::wrap_pyfunction;

use once_cell::sync::OnceCell;
use std::sync::Arc;
use std::time::Duration;
use bincode::deserialize;
use chrono::Utc;
use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::runtime::Builder as RuntimeBuilder;
use solana_quic_client::{QuicPool, QuicConnectionManager, QuicConfig};
use solana_client::{
    client_error::{ClientError, ClientErrorKind},
    rpc_client::RpcClient,
    tpu_client::{TpuClient, TpuClientConfig},
    rpc_request::RpcError,
};
use solana_transaction_status::{UiTransactionEncoding, EncodedConfirmedTransactionWithStatusMeta};
use solana_sdk::{transaction::Transaction, signature::Signature};

// Static globals for RPC and two TPU clients (buy and sell)
static RPC_CLIENT: OnceCell<Arc<RpcClient>> = OnceCell::new();
static TPU_CLIENT_BUY: OnceCell<Arc<TpuClient<QuicPool, QuicConnectionManager, QuicConfig>>> = OnceCell::new();
static TPU_CLIENT_SELL: OnceCell<Arc<TpuClient<QuicPool, QuicConnectionManager, QuicConfig>>> = OnceCell::new();
static BUY_FAIL_COUNT: AtomicUsize = AtomicUsize::new(0);
static SELL_FAIL_COUNT: AtomicUsize = AtomicUsize::new(0);

/// Blocking Python function for polling confirmation
// #[pyfunction]
// fn return_slot_sockets(
//     py: Python,
//     fanout_slots: u64
// ) -> PyResult<PyObject> {
//     let tpu_service = TPU_CLIENT_BUY
//         .get()
//         .ok_or_else(|| PyRuntimeError::new_err("TPU_CLIENT_BUY not initialized"))?
//         .clone();

//     let sockets = tpu_service.tpu_client.leader_tpu_service(fanout_slots);
//     // 3. Convert Vec<SocketAddr> → Vec<String>
//     let socket_strs: Vec<String> = sockets
//         .into_iter()
//         .map(|addr| addr.to_string())
//         .collect();

//     // 4. Build a Python list of those strings and return
//     let py_list = PyList::new(py, &socket_strs);
//     Ok(py_list.to_object(py))
// }


/// Initialize RPC and both TPU clients (buy/sell)
#[pyfunction]
fn init_tpu_clients(
    rpc_url: &str,
    ws_url: &str,
    buy_slots: Option<u64>,
    sell_slots: Option<u64>,
) -> PyResult<()> {
    let rpc = Arc::new(RpcClient::new(rpc_url.to_string()));
    RPC_CLIENT
        .set(rpc.clone())
        .map_err(|_| PyRuntimeError::new_err("RPC already initialized"))?;

    let buy_slots_val = buy_slots.unwrap_or(2);
    let mut buy_cfg = TpuClientConfig::default();
    buy_cfg.fanout_slots = buy_slots_val;
    let buy_client = TpuClient::new(rpc.clone(), ws_url, buy_cfg)
        .map_err(|e| PyRuntimeError::new_err(format!("TPU buy init error: {}", e)))?;
    TPU_CLIENT_BUY
        .set(Arc::new(buy_client))
        .map_err(|_| PyRuntimeError::new_err("TPU buy already initialized"))?;

    let sell_slots_val = sell_slots.unwrap_or(2);
    let mut sell_cfg = TpuClientConfig::default();
    sell_cfg.fanout_slots = sell_slots_val;
    let sell_client = TpuClient::new(rpc.clone(), ws_url, sell_cfg)
        .map_err(|e| PyRuntimeError::new_err(format!("TPU sell init error: {}", e)))?;
    TPU_CLIENT_SELL
        .set(Arc::new(sell_client))
        .map_err(|_| PyRuntimeError::new_err("TPU sell already initialized"))?;

    println!(
        "[{}][init] RPC+TPU buy={} slots sell={} slots initialized",
        Utc::now().to_rfc3339(),
        buy_slots_val,
        sell_slots_val
    );
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
        let res: Result<EncodedConfirmedTransactionWithStatusMeta, ClientError> =
            tokio::task::spawn_blocking({
                let rpc2 = rpc.clone();
                let sig2 = signature.clone();
                move || rpc2.get_transaction(&sig2, UiTransactionEncoding::JsonParsed)
            })
            .await
            .map_err(|e| PyRuntimeError::new_err(format!("join error: {}", e)))?;

        match res {
            Ok(txm) => {
                if let Some(meta) = txm.transaction.meta {
                    return Ok(meta.err.is_none());
                }
            }
            Err(err) => match err.kind() {
                ClientErrorKind::RpcError(RpcError::RpcResponseError { .. }) => return Ok(false),
                _ => (),
            },
        }
        tokio::time::sleep(Duration::from_millis(wait_ms)).await;
    }
    Ok(false)
}

/// Blocking Python function for polling confirmation
#[pyfunction]
fn wait_for_confirmation(
    signature: &str,
    rpc_url: &str,
    ws_url: &str,
    poll_time: usize,
    wait_ms: u64,
) -> PyResult<bool> {
    if RPC_CLIENT.get().is_none() {
        init_tpu_clients(rpc_url, ws_url, Some(2), Some(2))?;
    }
    let sig = signature
        .parse::<Signature>()
        .map_err(|e| PyRuntimeError::new_err(format!("Invalid signature: {}", e)))?;
    let rpc = RPC_CLIENT.get().unwrap().clone();

    let rt = RuntimeBuilder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| PyRuntimeError::new_err(format!("Runtime error: {}", e)))?;
    rt.block_on(wait_for_confirmation_internal(rpc, sig, poll_time, wait_ms))
        .map_err(|e| e)
}

#[pyfunction]
fn send_transaction_async<'p>(
    py: Python<'p>,
    raw_tx: Vec<u8>,
    action: String,
    max_retries: usize,
    rpc_url: String,
    ws_url: String,
    buy_slots: Option<u64>,
    sell_slots: Option<u64>,
) -> PyResult<&'p PyAny> {
    // pick the right global client and fail counter
    let (client_cell, fail_counter) = match action.as_str() {
        "buy" => (&TPU_CLIENT_BUY, &BUY_FAIL_COUNT),
        "sell" => (&TPU_CLIENT_SELL, &SELL_FAIL_COUNT),
        _ => return Err(PyRuntimeError::new_err("Invalid action; must be 'buy' or 'sell'")),
    };
    let client = client_cell
        .get()
        .ok_or_else(|| PyRuntimeError::new_err(format!("TPU client '{}' not initialized", action)))?
        .clone();

    pyo3_tokio::future_into_py(py, async move {
        // deserialize once
        let tx: Transaction = deserialize(&raw_tx)
            .map_err(|e| PyRuntimeError::new_err(format!("deserialize error: {}", e)))?;
        let sig = tx
            .signatures
            .get(0)
            .cloned()
            .ok_or_else(|| PyRuntimeError::new_err("No signature found in transaction"))?;

        for attempt in 1..=max_retries {
            // send on a blocking thread
            let sent = tokio::task::spawn_blocking({
                let tx = tx.clone();
                let client = client.clone();
                move || client.send_transaction(&tx)
            })
            .await
            .map_err(|e| PyRuntimeError::new_err(format!("join error: {}", e)))?;

            if sent {
                // success: return the signature string
                return Ok(sig.to_string());
            }

            // failure path: increment and check counter
            let fails = fail_counter.fetch_add(1, Ordering::SeqCst) + 1;
            eprintln!(
                "[{}] {} attempt {}/{} failed",
                Utc::now().to_rfc3339(),
                action,
                attempt,
                max_retries
            );

            if fails >= max_retries {
                // you've hit your threshold: reset counter and re-init client
                fail_counter.store(0, Ordering::SeqCst);
                eprintln!(
                    "[{}] Re-initializing TPU '{}' client after {} total consecutive failures",
                    Utc::now().to_rfc3339(),
                    action,
                    fails
                );
                // reconstruct the client (you’ll need your RPC/WS URLs here)
                init_tpu_clients(&rpc_url, &ws_url, buy_slots, sell_slots)?;
            }
        }

        // all retries exhausted
        Err(PyRuntimeError::new_err(format!(
            "send_transaction failed after {} attempts",
            max_retries
        )))
    })
}


/// Async send + confirm; action = "buy" or "sell"
#[pyfunction]
fn send_transaction_confirmed<'p>(
    py: Python<'p>,
    raw_tx: Vec<u8>,
    action: &str,
    max_retries: usize,
    poll_time: usize,
    wait_ms: u64,
) -> PyResult<&'p PyAny> {
    let client_cell = match action {
        "buy" => &TPU_CLIENT_BUY,
        "sell" => &TPU_CLIENT_SELL,
        _ => return Err(PyRuntimeError::new_err("Invalid action; must be 'buy' or 'sell'")),
    };
    let client = client_cell
        .get()
        .ok_or_else(|| PyRuntimeError::new_err(format!("TPU client '{}' not initialized", action)))?
        .clone();
    let rpc = RPC_CLIENT
        .get()
        .ok_or_else(|| PyRuntimeError::new_err("RPC not initialized"))?
        .clone();

    pyo3_tokio::future_into_py(py, async move {
        let tx: Transaction = deserialize(&raw_tx)
            .map_err(|e| PyRuntimeError::new_err(format!("deserialize error: {}", e)))?;
        let sig = tx
            .signatures
            .get(0)
            .cloned()
            .ok_or_else(|| PyRuntimeError::new_err("No signature"))?;
        for _ in 0..max_retries {
            let sent = tokio::task::spawn_blocking({
                let c = client.clone();
                let t = tx.clone();
                move || c.send_transaction(&t)
            })
            .await
            .map_err(|e| PyRuntimeError::new_err(format!("join error: {}", e)))?;
            if sent && wait_for_confirmation_internal(rpc.clone(), sig.clone(), poll_time, wait_ms).await? {
                return Ok(sig.to_string());
            }
        }
        Err(PyRuntimeError::new_err("Transaction not confirmed after retries"))
    })
}

#[pymodule]
fn solana_tpu_client(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(init_tpu_clients, m)?)?;
    m.add_function(wrap_pyfunction!(wait_for_confirmation, m)?)?;
    m.add_function(wrap_pyfunction!(send_transaction_async, m)?)?;
    m.add_function(wrap_pyfunction!(send_transaction_confirmed, m)?)?;
    Ok(())
}