/// Request/response marshalling.
pub mod marshal;

use marshal::ClientRequest;
use std::sync::Arc;

use axum::{Json, extract::{self, State}};
use serde_json::{Value, json};
use voprf::VoprfServerEvaluateResult;

use crate::{errors::Svr3Error, state::ServerState};


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
pub async fn refresh_handler(State(state): State<Arc<ServerState>>, extract::Json(request): Json<ClientRequest>) -> Result<Json<Value>, Svr3Error> {
    // TODO: Reset counter to 0.

    let ClientRequest { client_id, blinded_element } = request;
    let (evaluate_result, public_key) = state.private_state.refresh_client(client_id, &blinded_element)?;
    let VoprfServerEvaluateResult { message, proof } = evaluate_result;
    Ok(Json(json!({
        "evaluate_result": message,
        "proof": proof,
        "public_key": public_key,
    })))
}

/// For Post /evaluate.
/// Evaluates the OPRF for a client.
pub async fn evaluate_handler(State(state): State<Arc<ServerState>>, extract::Json(request): Json<ClientRequest>) -> Result<Json<Value>, Svr3Error> {
    // TODO: Increment counter.
    let ClientRequest { client_id, blinded_element } = request;
    let evaluate_result = state.private_state.blind_evaluate(client_id, &blinded_element)?;
    let VoprfServerEvaluateResult { message, proof } = evaluate_result;
    Ok(Json(json!({
        "evaluate_result": message,
        "proof": proof,
    })))
}