use std::sync::{Arc, OnceLock};

use axum::{Router, routing::{get, post}};
use log::info;
use pirateship_svr3::{handlers::{evaluate_handler, refresh_handler, root_handler}, log_config::default_log4rs_config, state::ServerState};
use tokio::net::TcpListener;


fn get_app<T>(state: &Arc<ServerState>) -> Router<T> {
    Router::new()
        .route("/", get(root_handler))
        .route("/refresh", post(refresh_handler))
        .route("/evaluate", post(evaluate_handler))
        .with_state(state.clone())
}

static STATE: OnceLock<Arc<ServerState>> = OnceLock::new();

#[tokio::main]
async fn main() {
    let _ = log4rs::init_config(default_log4rs_config()).unwrap();
    // Load the config.
    // Calling system: ./pirateship-svr3 path/to/config.json
    let config_path = std::env::args().nth(1)
        .expect(format!("Usage: {} <config_path>", std::env::args().nth(0).unwrap()).as_str());
    let config = pft::config::Config::deserialize(&std::fs::read_to_string(config_path).expect("Failed to read config"));

    let rest_api_addr = config.app_config.app_specific.get("rest_api_addr")
        .expect("rest_api_addr not found in config")
        .as_str()
        .expect("rest_api_addr must be a string")
        .to_string();

    let state = STATE.get_or_init(|| Arc::new(ServerState::new(config)));


    info!("Initializing state...");
    // Initialize the state. Wait until it's ready.
    let handles = state.init().await;
    info!("State initialized");

    let app = get_app(state);

    info!("Server listening on {}", rest_api_addr);
    let listener = TcpListener::bind(rest_api_addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();

    handles.join_all().await;
}