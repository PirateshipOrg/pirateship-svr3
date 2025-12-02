use curve25519_dalek::RistrettoPoint;
use dashmap::DashMap;
use rand::rngs::OsRng;
use voprf::{BlindedElement, Ristretto255, VoprfServer, VoprfServerEvaluateResult};

use crate::{errors::Svr3Error, state::ClientId};

/// Private state includes the VoprfServer objects for each client.
/// Private state blindly refreshes Oprf servers and evaluates the Oprf inputs for each client.
/// Usage count restriction is NOT enforced here.
/// All operations must be thread-safe.
pub struct PrivateState {
    voprf_servers: DashMap<ClientId, VoprfServer<Ristretto255>>,
}

impl PrivateState {
    pub fn new() -> Self {        
        Self {
            voprf_servers: DashMap::new(),
        }
    }

    pub async fn init(&self) {
        // Currently, this is a stub.
    }

    pub fn len(&self) -> usize {
        self.voprf_servers.len()
    }

    pub fn refresh_client(&self, client_id: ClientId, blinded_element: &BlindedElement<Ristretto255>) -> Result<(VoprfServerEvaluateResult<Ristretto255>, RistrettoPoint), Svr3Error> {
        let mut rng = OsRng;

        let oprf_server = VoprfServer::<Ristretto255>::new(&mut rng).unwrap();

        let public_key = oprf_server.get_public_key().clone();
        self.voprf_servers.insert(client_id.clone(), oprf_server);

        let evaluate_result = self.blind_evaluate(client_id, blinded_element)?;

        Ok((evaluate_result, public_key))
    }

    pub fn blind_evaluate(&self, client_id: ClientId, blinded_element: &BlindedElement<Ristretto255>) -> Result<VoprfServerEvaluateResult<Ristretto255>, Svr3Error> {
        let oprf_server = self.voprf_servers.get_mut(&client_id).ok_or(Svr3Error::KeyNotFound { client_id })?;
        
        let mut rng = OsRng;

        let evaluate_result = oprf_server.blind_evaluate(&mut rng, blinded_element);
        Ok(evaluate_result)
    }
}