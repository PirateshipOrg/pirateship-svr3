mod shared_state;
mod private_state;

use shared_state::SharedState;
use private_state::PrivateState;
use tokio::task::JoinSet;

pub type ClientId = String;

pub struct ServerState {
    pub shared_state: SharedState,
    pub private_state: PrivateState,
    pub max_oprf_eval_attempts: usize,
    pub threshold: usize,
}

impl ServerState {
    pub fn new(config: pft::config::Config) -> Self {
        let max_oprf_eval_attempts = config.app_config.app_specific.get("max_oprf_eval_attempts")
            .expect("max_oprf_eval_attempts not found in config")
            .as_u64()
            .expect("max_oprf_eval_attempts must be a number") as usize;

        let threshold = config.app_config.app_specific.get("server_threshold")
            .expect("threshold not found in config")
            .as_u64()
            .expect("threshold must be a number") as usize;

        Self {
            shared_state: SharedState::new(config),
            private_state: PrivateState::new(),
            max_oprf_eval_attempts,
            threshold,
        }
    }

    pub async fn init(&'static self) -> JoinSet<()> {
        self.private_state.init().await;
        self.shared_state.init().await
    }
}