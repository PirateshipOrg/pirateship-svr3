/// Request/response marshalling.
pub mod marshal;

use log::{debug, error, warn};
use marshal::ClientRequest;
use std::sync::Arc;

use axum::{Json, extract::{self, State}};
use serde_json::{Value, json};
use voprf::VoprfServerEvaluateResult;

use crate::{errors::Svr3Error, handlers::marshal::{EvaluateResponse, RefreshResponse}, state::{ClientId, ServerState}};


/// For Get /.
/// Returns the number of clients active.
pub async fn root_handler(State(state): State<Arc<ServerState>>) -> Result<Json<Value>, Svr3Error> {
    let num_clients = state.private_state.len();
    Ok(Json(json!({
        "num_clients": num_clients,
    })))
}

/// For Post /refresh.
/// Refreshes the OPRF server for a client.
pub async fn refresh_handler(State(state): State<Arc<ServerState>>, extract::Json(request): Json<ClientRequest>) -> Result<Json<RefreshResponse>, Svr3Error> {
    let ClientRequest { client_id, blinded_element } = request;

    // Step 1: Reset counter to 0.
    state.shared_state.reset(client_id.clone()).await;

    // Step 2: Refresh the OPRF server.
    let (evaluate_result, public_key) = state.private_state.refresh_client(client_id, &blinded_element)?;
    let VoprfServerEvaluateResult { message, proof } = evaluate_result;
    Ok(Json(RefreshResponse {
        evaluate_result: message,
        proof: proof,
        public_key: public_key,
    }))
}

fn check_for_throttle(val: usize, bci: usize, last_seen_value: usize, last_seen_block_n: usize, t: usize, max_oprf_eval_attempts: usize) -> bool {
    if val + t + 1 > max_oprf_eval_attempts {
        debug!("Type 1");
        // Too close to the limit.
        return true;
    }

    // if last_seen_value > 0 {
    //     if (val - 1) / (t + 1) <= (last_seen_value - 1) / (t + 1) {
    //         error!("Type 2: {} {} {}", val, last_seen_value, t + 1);
    //         // Too quick.
    //         return true;
    //     }
    // }


    // Threshold must be set to (N - u) out of N servers.
    // m sets of (N - u) servers intersect in at least N - mu servers.
    // To protect against rollback attacks, N - mu > r + 1,
    // since at least 1 unrolled back server must see both ops.
    // This is only satisfied if m = 1.
    // So, even if there are 2 unaudited ops, a correct server must throttle for audit.

    // if unaudited_ops > t + 1 {
    //     error!("Type 3: {} {}", unaudited_ops, t + 1);
    //     // Too many unaudited ops.
    //     return true;
    // }

    if last_seen_block_n > 0 {
        if last_seen_block_n > bci {
            debug!("Type 2: {} {}", last_seen_block_n, bci);
            return true;
        }
    }

    // No throttle.
    false
}



/// For Post /evaluate.
/// Evaluates the OPRF for a client.
pub async fn evaluate_handler(State(state): State<Arc<ServerState>>, extract::Json(request): Json<ClientRequest>) -> Result<Json<EvaluateResponse>, Svr3Error> {
    let ClientRequest { client_id, blinded_element } = request;
    // Step 1: Increment counter.
    let (mut val, block_n) = state.shared_state.add_fetch(client_id.clone()).await;
    if val > state.max_oprf_eval_attempts {
        return Err(Svr3Error::UsageExceeded);
    }
    // Step 2: Find locally how many unaudited ops are there for this client.
    let bci = state.shared_state.get_local_unaudited_ops(client_id.clone()).await;
    let last_seen_value = match state.shared_state.last_seen_values.get(&client_id) {
        Some(value) => *value,
        None => 0,
    };
    let last_seen_block_n = match state.shared_state.last_seen_block_n.get(&client_id) {
        Some(block_n) => *block_n,
        None => 0,
    };

    #[cfg(feature = "throttle")]
    if check_for_throttle(val, bci, last_seen_value, last_seen_block_n as usize, state.threshold, state.max_oprf_eval_attempts) {
        throttle_for_audit(&state, client_id.clone(), block_n).await;
    }
    
    state.shared_state.last_seen_values.insert(client_id.clone(), val);
    state.shared_state.last_seen_block_n.insert(client_id.clone(), block_n);
    if val > state.max_oprf_eval_attempts {
        return Err(Svr3Error::UsageExceeded);
    }

    // Step 3: Evaluate the OPRF.
    let evaluate_result = state.private_state.blind_evaluate(client_id, &blinded_element)?;
    let VoprfServerEvaluateResult { message, proof } = evaluate_result;

    Ok(Json(EvaluateResponse {
        evaluate_result: message,
        proof: proof,
    }))
}

async fn throttle_for_audit(state: &Arc<ServerState>, client_id: ClientId, block_n: u64) {
    warn!("Throttling {client_id}");
    // Step 1: Probe for audit.
    state.shared_state.probe_for_audit(block_n).await;

}