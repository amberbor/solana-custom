use pyo3::prelude::*;
use pyo3::exceptions::PyRuntimeError;
use pyo3::PyErr;
use pyo3_asyncio::tokio as pyo3_tokio;
use pyo3::wrap_pyfunction;
use pyo3::types::PyString;
use serde_json;

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

// Static globals for RPC and TPU client
static RPC_CLIENT: OnceCell<Arc<RpcClient>> = OnceCell::new();
static TPU_CLIENT: OnceCell<Arc<TpuClient<QuicPool, QuicConnectionManager, QuicConfig>>> = OnceCell::new();
static FAIL_COUNT: AtomicUsize = AtomicUsize::new(0);

#[pyfunction]
fn return_leader_info(
    py: Python,
    fanout_slots: u64
) -> PyResult<PyObject> {
    let tpu_service = TPU_CLIENT
        .get()
        .ok_or_else(|| PyRuntimeError::new_err("TPU_CLIENT not initialized"))?
        .clone();

    let leader_info = tpu_service.get_leader_info(fanout_slots);
    // Convert HashSet<Pubkey> to Vec<String> for proper JSON serialization
    let leader_strings: Vec<String> = leader_info.into_iter()
        .map(|pubkey| pubkey.to_string())
        .collect();
    
    let leader_info_str = serde_json::to_string(&leader_strings)
        .map_err(|e| PyRuntimeError::new_err(format!("JSON serialization error: {}", e)))?;
    let py_str = PyString::new(py, &leader_info_str);
    Ok(py_str.to_object(py))
}

/// Initialize RPC and TPU client
#[pyfunction]
fn init_tpu_clients(
    rpc_url: &str,
    ws_url: &str,
    fanout_slots: Option<u64>,
) -> PyResult<()> {
    let rpc = Arc::new(RpcClient::new(rpc_url.to_string()));
    RPC_CLIENT
        .set(rpc.clone())
        .map_err(|_| PyRuntimeError::new_err("RPC already initialized"))?;

    let slots_val = fanout_slots.unwrap_or(2);
    let mut cfg = TpuClientConfig::default();
    cfg.fanout_slots = slots_val;
    let client = TpuClient::new(rpc.clone(), ws_url, cfg)
        .map_err(|e| PyRuntimeError::new_err(format!("TPU init error: {}", e)))?;
    TPU_CLIENT
        .set(Arc::new(client))
        .map_err(|_| PyRuntimeError::new_err("TPU already initialized"))?;

    println!(
        "[{}][init] RPC+TPU fanout_slots={} initialized",
        Utc::now().to_rfc3339(),
        slots_val
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
        init_tpu_clients(rpc_url, ws_url, Some(2))?;
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
    max_retries: usize,
    rpc_url: String,
    ws_url: String,
    fanout_slots: Option<u64>,
) -> PyResult<&'p PyAny> {
    let client = TPU_CLIENT
        .get()
        .ok_or_else(|| PyRuntimeError::new_err("TPU client not initialized"))?
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
            let fails = FAIL_COUNT.fetch_add(1, Ordering::SeqCst) + 1;
            eprintln!(
                "[{}] attempt {}/{} failed",
                Utc::now().to_rfc3339(),
                attempt,
                max_retries
            );

            if fails >= max_retries {
                // you've hit your threshold: reset counter and re-init client
                FAIL_COUNT.store(0, Ordering::SeqCst);
                eprintln!(
                    "[{}] Re-initializing TPU client after {} total consecutive failures",
                    Utc::now().to_rfc3339(),
                    fails
                );
                // reconstruct the client
                init_tpu_clients(&rpc_url, &ws_url, fanout_slots)?;
            }
        }

        // all retries exhausted
        Err(PyRuntimeError::new_err(format!(
            "send_transaction failed after {} attempts",
            max_retries
        )))
    })
}

#[pyfunction]
fn send_transaction_batch_async<'p>(
    py: Python<'p>,
    raw_txs: Vec<Vec<u8>>,
    max_retries: usize,
    rpc_url: String,
    ws_url: String,
    fanout_slots: Option<u64>,
) -> PyResult<&'p PyAny> {
    let client = TPU_CLIENT
        .get()
        .ok_or_else(|| PyRuntimeError::new_err("TPU client not initialized"))?
        .clone();

    pyo3_tokio::future_into_py(py, async move {
        // deserialize all transactions
        let transactions: Vec<Transaction> = raw_txs
            .into_iter()
            .map(|raw_tx| deserialize(&raw_tx)
                .map_err(|e| PyRuntimeError::new_err(format!("deserialize error: {}", e))))
            .collect::<Result<Vec<_>, _>>()?;

        // Get all signatures for tracking
        let signatures: Vec<String> = transactions
            .iter()
            .map(|tx| tx.signatures[0].to_string())
            .collect();

        // Get fanout slots from client config
        let fanout_slots_val = fanout_slots.unwrap_or(client.fanout_slots);

        for attempt in 1..=max_retries {
            // send batch on a blocking thread
            let result = tokio::task::spawn_blocking({
                let txs = transactions.clone();
                let client = client.clone();
                let slots = fanout_slots_val;
                move || client.try_send_transaction_batch(&txs, slots)
            })
            .await
            .map_err(|e| PyRuntimeError::new_err(format!("join error: {}", e)))?;

            if result.is_ok() {
                // success: return the signatures as a JSON array
                let signatures_json = serde_json::to_string(&signatures)
                    .map_err(|e| PyRuntimeError::new_err(format!("JSON serialization error: {}", e)))?;
                return Ok(signatures_json);
            }

            // failure path: increment and check counter
            let fails = FAIL_COUNT.fetch_add(1, Ordering::SeqCst) + 1;
            eprintln!(
                "[{}] batch attempt {}/{} failed",
                Utc::now().to_rfc3339(),
                attempt,
                max_retries
            );

            if fails >= max_retries {
                // you've hit your threshold: reset counter and re-init client
                FAIL_COUNT.store(0, Ordering::SeqCst);
                eprintln!(
                    "[{}] Re-initializing TPU client after {} total consecutive failures",
                    Utc::now().to_rfc3339(),
                    fails
                );
                // reconstruct the client
                init_tpu_clients(&rpc_url, &ws_url, fanout_slots)?;
            }
        }

        // all retries exhausted
        Err(PyRuntimeError::new_err(format!(
            "send_transaction_batch failed after {} attempts",
            max_retries
        )))
    })
}

#[pymodule]
fn solana_custom(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(init_tpu_clients, m)?)?;
    m.add_function(wrap_pyfunction!(wait_for_confirmation, m)?)?;
    m.add_function(wrap_pyfunction!(send_transaction_async, m)?)?;
    m.add_function(wrap_pyfunction!(return_leader_info, m)?)?;
    m.add_function(wrap_pyfunction!(send_transaction_batch_async, m)?)?;
    Ok(())
}