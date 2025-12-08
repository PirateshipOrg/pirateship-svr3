/// Request/response marshalling.
pub mod marshal;

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

/// Threshold must be set to (N - u) out of N servers.
/// m sets of (N - u) servers intersect in at least N - mu servers.
/// To protect against rollback attacks, N - mu > r + 1,
/// since at least 1 unrolled back server must see both ops.
/// This is only satisfied if m = 1.
/// So, even if there are 2 unaudited ops, a correct server must throttle for audit.
const MAX_UNAUDITED_OPS: usize = 2;


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
    let unaudited_ops = state.shared_state.get_local_unaudited_ops(client_id.clone()).await;

    if unaudited_ops > MAX_UNAUDITED_OPS || val + unaudited_ops > state.max_oprf_eval_attempts {
        val = throttle_for_audit(&state, client_id.clone(), block_n).await;
    }

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

async fn throttle_for_audit(state: &Arc<ServerState>, client_id: ClientId, block_n: u64) -> usize {
    // Step 1: Probe for audit.
    state.shared_state.probe_for_audit(block_n).await;


    // Step 2: Recalculate the final counter value.
    let counter_value = state.shared_state.get_remote_unaudited_ops(client_id.clone()).await;
    counter_value
}