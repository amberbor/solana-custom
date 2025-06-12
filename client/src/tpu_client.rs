use {
    crate::connection_cache::ConnectionCache,
    solana_connection_cache::connection_cache::{
        ConnectionCache as BackendConnectionCache, ConnectionManager, ConnectionPool,
        NewConnectionConfig,
    },
    solana_quic_client::{QuicConfig, QuicConnectionManager, QuicPool},
    solana_rpc_client::rpc_client::RpcClient,
    solana_sdk::{
        message::Message,
        signers::Signers,
        transaction::{Transaction, TransactionError},
        transport::Result as TransportResult,
    },
    solana_tpu_client::tpu_client::{Result, TpuClient as BackendTpuClient},
    solana_udp_client::{UdpConfig, UdpConnectionManager, UdpPool},
    std::sync::Arc,
    std::collections::HashSet,
    std::net::SocketAddr,
    solana_sdk::pubkey::Pubkey,
};
pub use {
    crate::nonblocking::tpu_client::TpuSenderError,
    solana_tpu_client::tpu_client::{TpuClientConfig, DEFAULT_FANOUT_SLOTS, MAX_FANOUT_SLOTS},
};

pub type QuicTpuClient = TpuClient<QuicPool, QuicConnectionManager, QuicConfig>;

pub enum TpuClientWrapper {
    Quic(TpuClient<QuicPool, QuicConnectionManager, QuicConfig>),
    Udp(TpuClient<UdpPool, UdpConnectionManager, UdpConfig>),
}

/// Client which sends transactions directly to the current leader's TPU port over UDP.
/// The client uses RPC to determine the current leader and fetch node contact info
/// This is just a thin wrapper over the "BackendTpuClient", use that directly for more efficiency.
pub struct TpuClient<
    P, // ConnectionPool
    M, // ConnectionManager
    C, // NewConnectionConfig
> {
    tpu_client: BackendTpuClient<P, M, C>,
}

impl<P, M, C> TpuClient<P, M, C>
where
    P: ConnectionPool<NewConnectionConfig = C>,
    M: ConnectionManager<ConnectionPool = P, NewConnectionConfig = C>,
    C: NewConnectionConfig,
{
    /// Serialize and send transaction to the current and upcoming leader TPUs according to fanout
    /// size
    pub fn send_transaction(&self, transaction: &Transaction, fanout_slots: Option<u64>) -> bool {
        self.tpu_client.send_transaction(transaction, fanout_slots)
    }

    /// Send a wire transaction to the current and upcoming leader TPUs according to fanout size
    pub fn send_wire_transaction(&self, wire_transaction: Vec<u8>, fanout_slots: Option<u64>) -> bool {
        self.tpu_client.send_wire_transaction(wire_transaction, fanout_slots)
    }

    /// Serialize and send transaction to the current and upcoming leader TPUs according to fanout
    /// size
    /// Returns the last error if all sends fail
    pub fn try_send_transaction(&self, transaction: &Transaction, fanout_slots: Option<u64>) -> TransportResult<()> {
        self.tpu_client.try_send_transaction(transaction, fanout_slots)
    }

    /// Serialize and send a batch of transactions to the current and upcoming leader TPUs according
    /// to fanout size
    /// Returns the last error if all sends fail
    pub fn try_send_transaction_batch(&self, transactions: &[Transaction], fanout_slots: Option<u64>) -> TransportResult<()> {
        self.tpu_client.try_send_transaction_batch(transactions, fanout_slots)
    }

    /// Send a wire transaction to the current and upcoming leader TPUs according to fanout size
    /// Returns the last error if all sends fail
    pub fn try_send_wire_transaction(&self, wire_transaction: Vec<u8>, fanout_slots: Option<u64>) -> TransportResult<()> {
        self.tpu_client.try_send_wire_transaction(wire_transaction, fanout_slots)
    }

    pub async fn get_leader_info(&self, fanout_slots: Option<u64>) -> HashSet<Pubkey> {
        self.tpu_client.get_leader_info(fanout_slots).await
    }

    pub async fn get_leader_info_slot(&self, fanout_slots: u64) -> Vec<(u64, Pubkey, SocketAddr)> {
        self.tpu_client.get_leader_info_slot(fanout_slots).await
    }
}

impl TpuClient<QuicPool, QuicConnectionManager, QuicConfig> {
    /// Create a new client that disconnects when dropped
    pub fn new(
        rpc_client: Arc<RpcClient>,
        websocket_url: &str,
        config: TpuClientConfig,
    ) -> Result<Self> {
        let connection_cache = match ConnectionCache::new("connection_cache_tpu_client") {
            ConnectionCache::Quic(cache) => cache,
            ConnectionCache::Udp(_) => {
                return Err(TpuSenderError::Custom(String::from(
                    "Invalid default connection cache",
                )))
            }
        };
        Self::new_with_connection_cache(rpc_client, websocket_url, config, connection_cache)
    }
}

impl<P, M, C> TpuClient<P, M, C>
where
    P: ConnectionPool<NewConnectionConfig = C>,
    M: ConnectionManager<ConnectionPool = P, NewConnectionConfig = C>,
    C: NewConnectionConfig,
{
    /// Create a new client that disconnects when dropped
    pub fn new_with_connection_cache(
        rpc_client: Arc<RpcClient>,
        websocket_url: &str,
        config: TpuClientConfig,
        connection_cache: Arc<BackendConnectionCache<P, M, C>>,
    ) -> Result<Self> {
        Ok(Self {
            tpu_client: BackendTpuClient::new_with_connection_cache(
                rpc_client,
                websocket_url,
                config,
                connection_cache,
            )?,
        })
    }

    pub fn send_and_confirm_messages_with_spinner<T: Signers + ?Sized>(
        &self,
        messages: &[Message],
        signers: &T,
    ) -> Result<Vec<Option<TransactionError>>> {
        self.tpu_client
            .send_and_confirm_messages_with_spinner(messages, signers)
    }

    pub fn rpc_client(&self) -> &RpcClient {
        self.tpu_client.rpc_client()
    }
}
