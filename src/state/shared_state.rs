use std::{
    collections::HashMap,
    fmt::Display,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    }, time::Duration,
};

use dashmap::{DashMap, DashSet};
use log::{error, info};
use pft::{
    config::AtomicConfig,
    consensus::{
        ConsensusNode,
        app::AppEngine,
        batch_proposal::{MsgAckChanWithTag, TxWithAckChanTag},
    },
    crypto::{AtomicKeyStore, KeyStore},
    proto::{client::ProtoClientReply, execution::ProtoTransaction},
    rpc::{
        MessageRef, SenderType,
        client::{Client, PinnedClient}
    },
    utils::{
        AtomicStruct,
        channel::{Sender, make_channel},
    },
};
use rand::Rng as _;
use serde::{Deserialize, Serialize};
use tokio::{sync::Mutex, task::JoinSet};

use crate::state::ClientId;
use prost::Message;

/// No checkpointing needed here.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct DummyState;

impl Display for DummyState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DummyState")
    }
}

#[derive(Clone)]
enum CounterOp {
    Increment,
    Reset,
}

/// Implements an increment-and-reset-only counter.
/// Only ops allowed: increment by 1, and reset to 0.
/// Returns incremented value on increment, and 0 on reset.
/// No byz_commit phase in the transaction.
/// Unlogged requests return the number of committed but not audited operations for the client.
/// For full correctness, we must check if the leader's execution result matches every replica's.
/// Currently Pirateship doesn't support this.
pub struct CounterStore {
    audited_counters: HashMap<ClientId, usize>,
    committed_ops: HashMap<
        ClientId,
        Vec<(
            u64,   /* block n */
            usize, /* tx n */
            usize, /* op n */
            CounterOp,
        )>,
    >,

    ci: u64,
    bci: u64,
}

impl CounterStore {
    /// Iterates over the ops in reverse order, checks if there is a reset op.
    /// If there is a reset op, returns true, so that caller doesn't need to add the audited value to the result.
    /// If there is no reset op, returns false, returns the addition of all increment ops,
    /// and then the caller must add the audited value as well.
    fn get_value(
        ops: &[(
            u64,   /* block n */
            usize, /* tx n */
            usize, /* op n */
            CounterOp,
        )],
    ) -> (usize, bool) {
        let mut val = 0;
        let mut guarded_by_reset = false;
        for (_, _, _, op) in ops.iter().rev() {
            match op {
                CounterOp::Increment => {
                    val += 1;
                }
                CounterOp::Reset => {
                    guarded_by_reset = true;
                }
            }
            if guarded_by_reset {
                break;
            }
        }
        (val, guarded_by_reset)
    }
    fn get_committed_value(&self, client_id: &ClientId) -> usize {
        let Some(committed_ops) = self.committed_ops.get(client_id) else {
            return 0;
        };
        let (mut val, guarded_by_reset) = Self::get_value(committed_ops);

        if !guarded_by_reset {
            val += self.audited_counters.get(client_id).unwrap_or(&0);
        }
        val
    }
}

impl AppEngine for CounterStore {
    type State = DummyState;

    fn new(_config: pft::config::AtomicConfig) -> Self {
        Self {
            audited_counters: HashMap::new(),
            committed_ops: HashMap::new(),
            ci: 0,
            bci: 0,
        }
    }

    fn handle_crash_commit(
        &mut self,
        blocks: Vec<pft::crypto::CachedBlock>,
    ) -> Vec<Vec<pft::proto::execution::ProtoTransactionResult>> {
        let mut all_results = Vec::new();
        for block in blocks {
            self.ci = std::cmp::max(self.ci, block.block.n);
            let mut results = Vec::new();
            for (tx_n, tx) in block.block.tx_list.iter().enumerate() {
                let Some(on_crash_commit) = &tx.on_crash_commit else {
                    results.push(pft::proto::execution::ProtoTransactionResult { result: vec![] });
                    continue;
                };
                let mut op_results = Vec::new();
                for (op_n, op) in on_crash_commit.ops.iter().enumerate() {
                    if op.operands.len() != 1 {
                        op_results.push(pft::proto::execution::ProtoTransactionOpResult {
                            success: false,
                            values: vec![],
                        });
                        continue;
                    }
                    let operand = &op.operands[0];
                    let client_id: ClientId = String::from_utf8(operand.clone()).unwrap();
                    let entry = self.committed_ops.entry(client_id.clone()).or_insert(Vec::new());
                    match op.op_type() {
                        pft::proto::execution::ProtoTransactionOpType::Increment => {
                            entry.push((
                                block.block.n, tx_n, op_n,
                                CounterOp::Increment,
                            ));
                        }
                        pft::proto::execution::ProtoTransactionOpType::Write => {
                            entry.push((
                                block.block.n, tx_n, op_n,
                                CounterOp::Reset,
                            ));
                        }
                        _ => { /* If you send a read request here, you get the value back, without interfering with the counter. */}
                    }
                    let result_val = self.get_committed_value(&client_id);
                    op_results.push(pft::proto::execution::ProtoTransactionOpResult {
                        success: true,
                        values: vec![result_val.to_be_bytes().to_vec()],
                    });
                }
                results.push(pft::proto::execution::ProtoTransactionResult { result: op_results });
            }

            all_results.push(results);
        }
        all_results
    }

    fn handle_byz_commit(
        &mut self,
        blocks: Vec<pft::crypto::CachedBlock>,
    ) -> Vec<Vec<pft::proto::client::ProtoByzResponse>> {
        let mut all_results = Vec::new();
        for block in &blocks {
            self.bci = std::cmp::max(self.bci, block.block.n);

            let mut block_result = Vec::new();

            // There will be no byz_commit phase in the transaction.
            for (tx_n, _) in block.block.tx_list.iter().enumerate() {
                let byz_result = pft::proto::client::ProtoByzResponse {
                    block_n: block.block.n,
                    tx_n: tx_n as u64,
                    client_tag: 0,
                };

                block_result.push(byz_result);
            }

            all_results.push(block_result);
        }

        // Move audited entries from committed_ops and merge the results with audited_counters.
        let curr_val = self.get_committed_value(&String::from("client5"));
        for (client_id, committed_ops) in self.committed_ops.iter_mut() {
            let ops = committed_ops
                .iter()
                .filter(|(block_n, _, _, _)| *block_n <= self.bci)
                .cloned()
                .collect::<Vec<_>>();
            let (mut val, guarded_by_reset) = Self::get_value(&ops);
            if !guarded_by_reset {
                val += self.audited_counters.get(client_id).unwrap_or(&0);
            }
            self.audited_counters.insert(client_id.clone(), val);
            // Remove committed ops
            committed_ops.retain(|(block_n, _, _, _)| *block_n > self.bci);
        }
        let val_after_change = self.get_committed_value(&String::from("client5"));
        assert!(val_after_change == curr_val);

        

        all_results
    }

    fn handle_rollback(&mut self, new_last_block: u64) {
        for (_, committed_ops) in self.committed_ops.iter_mut() {
            committed_ops.retain(|(block_n, _, _, _)| *block_n <= new_last_block);
        }
    }

    // Returns the number of committed but not audited operations for the client.
    fn handle_unlogged_request(
        &mut self,
        request: pft::proto::execution::ProtoTransaction,
    ) -> pft::proto::execution::ProtoTransactionResult {
        let Some(on_receive) = &request.on_receive else {
            return pft::proto::execution::ProtoTransactionResult { result: Vec::new() };
        };
        let mut result = Vec::new();
        // let _empty_vec = Vec::new();
        for op in on_receive.ops.iter() {
            // let client_id: ClientId = String::from_utf8(op.operands[0].clone()).unwrap();
            // let val = self
            //     .committed_ops
            //     .get(&client_id)
            //     .unwrap_or(&_empty_vec)
            //     .len();
            result.push(pft::proto::execution::ProtoTransactionOpResult {
                success: true,
                values: vec![self.bci.to_be_bytes().to_vec()],
            });
        }
        pft::proto::execution::ProtoTransactionResult { result: result }
    }

    fn get_current_state(&self) -> Self::State {
        DummyState
    }
}

type AtomicString = AtomicStruct<String>;

pub struct SharedState {
    consensus_node: Arc<Mutex<ConsensusNode<CounterStore>>>,
    consensus_client_store: DashMap<usize, Arc<Mutex<PinnedClient>>>,
    consensus_self_tx: Sender<TxWithAckChanTag>,
    name: String,
    tag: AtomicU64,
    alleged_leader: AtomicString,

    pub last_seen_values: DashMap<ClientId, usize>,
    pub last_seen_block_n: DashMap<ClientId, u64>,

}

const CLIENT_SUB_ID_REMOTE: u64 = 42;
const CLIENT_SUB_ID_SELF: u64 = 43;

impl SharedState {
    pub fn new(config: pft::config::Config) -> Self {
        let (tx, rx) = make_channel(config.rpc_config.channel_depth as usize);
        let consensus_node = ConsensusNode::mew(config.clone(), tx.clone(), rx);
        let name = config.net_config.name.clone();
        let alleged_leader =
            AtomicString::new(config.consensus_config.node_list.first().unwrap().clone());

        let key_store = KeyStore::new(
            &config.rpc_config.allowed_keylist_path,
            &config.rpc_config.signing_priv_key_path,
            // &String::from("configs/client1_signing_privkey.pem"),
        );
        let key_store = AtomicKeyStore::new(key_store);
        let mut _config = config.clone();
        _config.net_config.name = name.clone() + "_client1";

        let config = AtomicConfig::new(_config);
        // Name is nodeN. Find n
        let n = name.split("node").nth(1).unwrap().parse::<usize>().unwrap();

        let consensus_client_store = DashMap::new();

        for i in 0..100usize {

            let consensus_client =
                Client::new_atomic(config.clone(), key_store.clone(), false, n as u64 * CLIENT_SUB_ID_REMOTE + i as u64).into();
            consensus_client_store.insert(i, Arc::new(Mutex::new(consensus_client)));
        }
        Self {
            consensus_node: Arc::new(Mutex::new(consensus_node)),
            consensus_client_store,
            consensus_self_tx: tx,
            name,
            tag: AtomicU64::new(1),
            alleged_leader,
            last_seen_values: DashMap::new(),
            last_seen_block_n: DashMap::new(),
        }
    }

    pub async fn init(&'static self) -> JoinSet<()> {
        let mut consensus_node = self.consensus_node.lock().await;
        let mut handle = consensus_node.run().await;

        if self.name == **self.alleged_leader.get() {

            handle.spawn(async move {
                info!("Waiting 5s for view to stabilize");
                tokio::time::sleep(Duration::from_secs(5)).await;
                info!("Submitting background noise");
                loop {
                    let transaction = pft::proto::execution::ProtoTransaction {
                        on_crash_commit: Some(pft::proto::execution::ProtoTransactionPhase {
                            ops: vec![pft::proto::execution::ProtoTransactionOp {
                                op_type: pft::proto::execution::ProtoTransactionOpType::Increment as i32,
                                operands: vec!["dummy".as_bytes().to_vec()],
                            }],
                        }),
                        on_receive: None,
                        on_byzantine_commit: None,
                        is_reconfiguration: false,
                        is_2pc: false,
                    };
                    self.execute_local(transaction, true).await;

                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
    
            });
        }


        handle
    }

    /// Asks own state, how many ops are committed but not audited for the client.
    pub async fn get_local_unaudited_ops(&self, client_id: ClientId) -> usize {
        let transaction = pft::proto::execution::ProtoTransaction {
            on_receive: Some(pft::proto::execution::ProtoTransactionPhase {
                ops: vec![pft::proto::execution::ProtoTransactionOp {
                    op_type: pft::proto::execution::ProtoTransactionOpType::Read as i32,
                    operands: vec![client_id.as_bytes().to_vec()],
                }],
            }),
            on_crash_commit: None,
            on_byzantine_commit: None,
            is_reconfiguration: false,
            is_2pc: false,
        };

        self.execute_local(transaction, true).await
    }

    /// Asks the leader the current counter value for the client.
    pub async fn get_remote_unaudited_ops(&self, client_id: ClientId) -> usize {
        let transaction = pft::proto::execution::ProtoTransaction {
            on_receive: Some(pft::proto::execution::ProtoTransactionPhase {
                ops: vec![pft::proto::execution::ProtoTransactionOp {
                    op_type: pft::proto::execution::ProtoTransactionOpType::Read as i32,
                    operands: vec![client_id.as_bytes().to_vec()],
                }],
            }),
            on_crash_commit: None,
            on_byzantine_commit: None,
            is_reconfiguration: false,
            is_2pc: false,
        };

        self.execute_remote(transaction).await.0
    }

    pub async fn add_fetch(
        &self,
        client_id: ClientId,
    ) -> (usize /* counter value */, u64 /* block n */) {
        let transaction = pft::proto::execution::ProtoTransaction {
            on_crash_commit: Some(pft::proto::execution::ProtoTransactionPhase {
                ops: vec![pft::proto::execution::ProtoTransactionOp {
                    op_type: pft::proto::execution::ProtoTransactionOpType::Increment as i32,
                    operands: vec![client_id.as_bytes().to_vec()],
                }],
            }),
            on_receive: None,
            on_byzantine_commit: None,
            is_reconfiguration: false,
            is_2pc: false,
        };

        self.execute_remote(transaction).await
    }

    pub async fn reset(
        &self,
        client_id: ClientId,
    ) -> (usize /* counter value */, u64 /* block n */) {
        let transaction = pft::proto::execution::ProtoTransaction {
            on_crash_commit: Some(pft::proto::execution::ProtoTransactionPhase {
                ops: vec![pft::proto::execution::ProtoTransactionOp {
                    op_type: pft::proto::execution::ProtoTransactionOpType::Write as i32,
                    operands: vec![client_id.as_bytes().to_vec()],
                }],
            }),
            on_receive: None,
            on_byzantine_commit: None,
            is_reconfiguration: false,
            is_2pc: false,
        };

        self.execute_remote(transaction).await
    }

    pub async fn probe_for_audit(&self, block_n: u64) {
        let probe_transaction = pft::proto::execution::ProtoTransaction {
            on_receive: Some(pft::proto::execution::ProtoTransactionPhase {
                ops: vec![pft::proto::execution::ProtoTransactionOp {
                    op_type: pft::proto::execution::ProtoTransactionOpType::Probe as i32,
                    operands: vec![block_n.to_be_bytes().to_vec()],
                }],
            }),
            on_crash_commit: None,
            on_byzantine_commit: None,
            is_reconfiguration: false,
            is_2pc: false,
        };

        self.execute_local(probe_transaction, false).await;
    }

    async fn execute_remote(
        &self,
        transaction: pft::proto::execution::ProtoTransaction,
    ) -> (usize /* counter value */, u64 /* block n */) {
        loop {
            if let Some((val, block_n)) = self.execute_remote_once(transaction.clone()).await {
                return (val, block_n);
            }
        }
    }

    async fn execute_remote_once(
        &self,
        transaction: pft::proto::execution::ProtoTransaction,
    ) -> Option<(usize /* counter value */, u64 /* block n */)> {
        // return Some((0, 0));
        // Choose a random client from the store.
        let client_idx = rand::thread_rng().gen_range(0..self.consensus_client_store.len());
        let client = self.consensus_client_store.get(&client_idx).unwrap().clone();
        let client = client.lock().await;
        let leader = self.alleged_leader.get();
        let origin = self.name.clone() + "_client1";
        let client_tag = self.tag.fetch_add(1, Ordering::SeqCst);

        let client_request = pft::proto::rpc::ProtoPayload {
            message: Some(pft::proto::rpc::proto_payload::Message::ClientRequest(
                pft::proto::client::ProtoClientRequest {
                    tx: Some(transaction),
                    origin,
                    sig: vec![0u8; 1],
                    client_tag,
                },
            )),
        };

        let payload = client_request.encode_to_vec();
        let sz = payload.len();

        let Ok(reply) = PinnedClient::send_and_await_reply(
            &client,
            &leader,
            MessageRef(&payload, sz, &pft::rpc::SenderType::Anon),
        )
        .await
        else {
            return None;
        };

        let reply = reply.as_ref();

        let reply = ProtoClientReply::decode(&reply.0.as_slice()[0..reply.1]).unwrap();
        match reply.reply {
            Some(pft::proto::client::proto_client_reply::Reply::Receipt(receipt)) => Some((
                usize::from_be_bytes(
                    receipt.results.unwrap().result[0].values[0]
                        .as_slice()
                        .try_into()
                        .unwrap(),
                ),
                receipt.block_n,
            )),
            _ => None,
        }
    }

    async fn execute_local(
        &self,
        transaction: pft::proto::execution::ProtoTransaction,
        await_reply: bool,
    ) -> usize {
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        let sender = SenderType::Auth(self.name.clone() + "_client1", CLIENT_SUB_ID_SELF);
        let tag = self.tag.fetch_add(1, Ordering::SeqCst);
        let ack_chan: MsgAckChanWithTag = (tx, tag, sender);
        let msg: TxWithAckChanTag = (Some(transaction), ack_chan);

        self.consensus_self_tx.send(msg).await.unwrap();

        
        if await_reply {
            let result = rx.recv().await.unwrap().0;
            let result = result.as_ref();
            // Response is guaranteed here.
            let reply = ProtoClientReply::decode(&result.0.as_slice()[0..result.1]).unwrap();

            match reply.reply {
                Some(pft::proto::client::proto_client_reply::Reply::Receipt(receipt)) => {
                    usize::from_be_bytes(
                        receipt.results.unwrap().result[0].values[0]
                            .as_slice()
                            .try_into()
                            .unwrap(),
                    )
                }
                _ => {
                    unreachable!();
                }
            }
        } else {
            0
        }
    }
}
