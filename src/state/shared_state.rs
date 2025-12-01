use std::{collections::HashMap, fmt::Display, sync::Arc};

use pft::consensus::{ConsensusNode, app::AppEngine};
use serde::{Deserialize, Serialize};
use tokio::{sync::Mutex, task::JoinSet};

use crate::state::ClientId;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct DummyState;

impl Display for DummyState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DummyState")
    }
}

/// Implements an increment-and-reset-only counter.
/// Only ops allowed: increment by 1, and reset to 0.
/// Returns incremented value on increment, and 0 on reset.
/// Only unlogged ops allowed: How many committed but not audited ops are there for a given client?
pub struct CounterStore {
    audited_counters: HashMap<ClientId, usize>,
    committed_ops: HashMap<ClientId, Vec<(u64 /* block n */, u64 /* tx n */)>>,
    max_oprf_eval_attempts: usize,
}

impl AppEngine for CounterStore {
    type State = DummyState;
    
    fn new(config: pft::config::AtomicConfig) -> Self {
        let max_oprf_eval_attempts = config.get().app_config.app_specific.get("max_oprf_eval_attempts")
            .expect("max_oprf_eval_attempts not found in config")
            .as_u64()
            .expect("max_oprf_eval_attempts must be a number") as usize;
        Self {
            audited_counters: HashMap::new(),
            committed_ops: HashMap::new(),
            max_oprf_eval_attempts,
        }
    }
    
    fn handle_crash_commit(&mut self, blocks: Vec<pft::crypto::CachedBlock>) -> Vec<Vec<pft::proto::execution::ProtoTransactionResult>> {
        todo!()
    }
    
    fn handle_byz_commit(&mut self, blocks: Vec<pft::crypto::CachedBlock>) -> Vec<Vec<pft::proto::client::ProtoByzResponse>> {
        todo!()
    }
    
    fn handle_rollback(&mut self, new_last_block: u64) {
        todo!()
    }
    
    fn handle_unlogged_request(&mut self, request: pft::proto::execution::ProtoTransaction) -> pft::proto::execution::ProtoTransactionResult {
        let Some(on_receive) = request.on_receive else {
            return pft::proto::execution::ProtoTransactionResult {
                result: vec![],
            }
        };
        let op_results = on_receive.ops.iter().map(|op| {
            match op.op_type() {
                pft::proto::execution::ProtoTransactionOpType::Read => {
                    if op.operands.len() != 1 {
                        pft::proto::execution::ProtoTransactionOpResult {
                            success: false,
                            values: vec![],
                        }
                    } else {
                        let operand = &op.operands[0];
                        let client_id: ClientId = String::from_utf8(operand.clone()).unwrap();
                        if let Some(audited_counter) = self.audited_counters.get(&client_id) {
                            pft::proto::execution::ProtoTransactionOpResult {
                                success: true,
                                values: vec![audited_counter.to_be_bytes().to_vec()],
                            }
                        } else {
                            pft::proto::execution::ProtoTransactionOpResult {
                                success: true,
                                values: vec![0_usize.to_be_bytes().to_vec()],
                            }
                        }
                    }
                },
                _ => pft::proto::execution::ProtoTransactionOpResult {
                    success: false,
                    values: vec![],
                }
            }
        }).collect::<Vec<_>>();
        pft::proto::execution::ProtoTransactionResult {
            result: op_results,
        }
    }
    
    fn get_current_state(&self) -> Self::State {
        DummyState
    }
}

pub struct SharedState {
    consensus_node: Arc<Mutex<ConsensusNode<CounterStore>>>,
}

impl SharedState {
    pub fn new(config: pft::config::Config) -> Self {
        Self {
            consensus_node: Arc::new(Mutex::new(ConsensusNode::new(config))),
        }
    }

    pub async fn init(&self) -> JoinSet<()> {
        let mut consensus_node = self.consensus_node.lock().await;
        consensus_node.run().await
    }
}