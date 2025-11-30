use std::sync::Arc;

use axum::{Json, extract::State};
use serde_json::{Value, json};

use crate::state::ServerState;

pub async fn root_handler(State(_state): State<Arc<ServerState>>) -> Json<Value> {
    Json(json!({
        "message": "Hello, world!"
    }))
}