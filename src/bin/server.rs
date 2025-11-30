use std::sync::Arc;

use axum::{Router, routing::get};
use log::info;
use pirateship_svr3::{handlers::root_handler, log_config::default_log4rs_config, state::ServerState};
use tokio::net::TcpListener;


fn get_app<T>(state: Arc<ServerState>) -> Router<T> {
    Router::new()
        .route("/", get(root_handler))
        .with_state(state)
}


#[tokio::main]
async fn main() {
    let _ = log4rs::init_config(default_log4rs_config()).unwrap();
    let state = Arc::new(ServerState::new());

    info!("Initializing state...");
    // Initialize the state. Wait until it's ready.
    state.init().await;
    info!("State initialized");

    let app = get_app(state);

    let addr = "0.0.0.0:3000";
    info!("Server listening on {}", addr);
    let listener = TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}