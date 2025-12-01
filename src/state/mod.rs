mod shared_state;
mod private_state;

use shared_state::SharedState;
use private_state::PrivateState;

pub type ClientId = String;

pub struct ServerState {
    pub shared_state: SharedState,
    pub private_state: PrivateState,
}

impl ServerState {
    pub fn new() -> Self {
        Self {
            shared_state: SharedState::new(),
            private_state: PrivateState::new(),
        }
    }

    pub async fn init(&self) {
        self.shared_state.init().await;
        self.private_state.init().await;
    }
}