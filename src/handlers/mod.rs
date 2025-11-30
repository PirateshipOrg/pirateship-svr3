use std::sync::Arc;

use axum::{Json, extract::State};
use serde_json::{Value, json};

use crate::{errors::Svr3Error, state::ServerState};

pub async fn root_handler(State(_state): State<Arc<ServerState>>) -> Result<Json<Value>, Svr3Error> {
    Err(Svr3Error::UsageExceeded)
}