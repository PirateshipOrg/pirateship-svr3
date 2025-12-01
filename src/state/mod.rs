mod shared_state;
mod private_state;

use shared_state::SharedState;
use private_state::PrivateState;
use tokio::task::JoinSet;

pub type ClientId = String;

pub struct ServerState {
    pub shared_state: SharedState,
    pub private_state: PrivateState,
}

impl ServerState {
    pub fn new(config: pft::config::Config) -> Self {
        Self {
            shared_state: SharedState::new(config),
            private_state: PrivateState::new(),
        }
    }

    pub async fn init(&self) -> JoinSet<()> {
        self.private_state.init().await;
        self.shared_state.init().await
    }
}