use std::collections::HashMap;

use itertools::Itertools;
use rand::{RngCore as _, rngs::OsRng, seq::IteratorRandom as _};
use curve25519_dalek::ristretto::RistrettoPoint;
use sha2::{Digest as _, Sha512};
use voprf::{BlindedElement, Ristretto255, VoprfClient, VoprfClientBlindResult, VoprfServer, VoprfServerEvaluateResult};

#[derive(Debug, thiserror::Error)]
enum Svr3Error {
    #[error("usage count exceeded")]
    UsageExceeded,

    #[error("key not found for client {client_id}")]
    KeyNotFound { client_id: usize },

    #[error("shamir secret sharing error: {0}")]
    ShamirSecretSharingError(shamirsecretsharing::SSSError),

    #[error("oprf client error: {0}")]
    OprfClientError(voprf::Error),

    #[error("server {server_id} not found")]
    ServerNotFound { server_id: usize },

    #[error("invalid shares")]
    InvalidShares,

    #[error("commitment not found")]
    CommitmentNotFound,

    #[error("invalid commitment")]
    InvalidCommitment,
}

fn __hash(parts: &[&[u8]]) -> Vec<u8> {
    let mut hasher = Sha512::new();
    for part in parts {
        hasher.update(part);
    }
    hasher.finalize().to_vec()
}

fn __xor_bytes(a: &[u8], b: &[u8]) -> Vec<u8> {
    assert_eq!(a.len(), b.len());
    let mut result = vec![0; a.len()];

    for i in 0..a.len() {
        result[i] = a[i] ^ b[i];
    }
    result
}

const MAX_USES: usize = 4;

struct Client {
    id: usize,
    server_threshold: usize,
    server_count: usize, // Server ids are in the range [0, server_count)
    commitment: Option<Vec<u8>>,
    masked_shares: HashMap<usize /* server id */, Vec<u8> /* to change */>,
    server_pks: HashMap<usize /* server id */, RistrettoPoint>,

    server_handles: HashMap<usize /* server id */, Box<Server>>,
}

impl Client {
    fn new(id: usize, server_threshold: usize, server_count: usize, server_handles: HashMap<usize /* server id */, Box<Server>>) -> Self {
        assert!(server_threshold + 1 <= server_count);
        assert!(server_count <= 255); // Limitation of the shamir secret sharing library
        
        Self {
            id,
            server_threshold,
            server_count,
            commitment: None,
            masked_shares: HashMap::new(),
            server_pks: HashMap::new(),
            server_handles,
        }
    }

    fn store(&mut self, password: &[u8], client_secret: &[u8]) -> Result<(), Svr3Error> {
        let r = __hash(&[client_secret]);

        let t = self.server_threshold;
        let n = self.server_count;

        let shares = shamirsecretsharing::create_shares(client_secret, n as u8, (t + 1) as u8).map_err(Svr3Error::ShamirSecretSharingError)?;

        let mut rng = OsRng;
        for (i, share) in shares.iter().enumerate() {

            let oprf_input = __hash(&[password, i.to_be_bytes().as_ref()]);
            let VoprfClientBlindResult { state, message: blinded_element } =
                VoprfClient::<Ristretto255>::blind(&oprf_input, &mut rng).map_err(Svr3Error::OprfClientError)?;

            let server_handle = self.server_handles.get_mut(&i).ok_or(Svr3Error::ServerNotFound { server_id: i })?;
            let (evaluate_result, server_pk) = server_handle.refresh_client(self.id, &blinded_element)?;

            let mask = state.finalize(&oprf_input, &evaluate_result.message, &evaluate_result.proof, server_pk.clone())
                .map_err(Svr3Error::OprfClientError)?
                .to_vec();

            let padded_mask = if mask.len() < share.len() {
                let mut padded_mask = vec![0; share.len()];
                for i in 0..share.len() {
                    padded_mask[i] = mask[i % mask.len()];
                }
                padded_mask
            } else if mask.len() > share.len() {
                let mut padded_mask = mask.clone();
                padded_mask.truncate(share.len());
                padded_mask
            } else {
                mask
            };

            let masked_share = __xor_bytes(&share, &padded_mask);

            self.masked_shares.insert(i, masked_share);

            self.server_pks.insert(i, server_pk);
        }

        let serialized_shares = self.masked_shares.iter()
            .sorted()
            .map(|(i, share)| {
                let mut serialized = i.to_be_bytes().to_vec();
                serialized.extend_from_slice(share);
                serialized
            })
            .fold(Vec::new(), |mut acc, share| {
                acc.extend_from_slice(&share);
                acc
            });

        let commitment = __hash(&[password, &serialized_shares, &r]);
        self.commitment = Some(commitment);


        Ok(())
    
    }

    fn restore(&mut self, password: &[u8]) -> Result<Vec<u8>, Svr3Error> {
        let mut rng = OsRng;
        let target_server_ids = (0..self.server_count).choose_multiple(&mut rng, self.server_threshold + 1);

        let mut retrieved_shares = HashMap::new();
        for server_id in target_server_ids {
            let server_handle = self.server_handles.get_mut(&server_id).ok_or(Svr3Error::ServerNotFound { server_id })?;

            let oprf_input = __hash(&[password, server_id.to_be_bytes().as_ref()]);
            let VoprfClientBlindResult { state, message: blinded_element } =
                VoprfClient::<Ristretto255>::blind(&oprf_input, &mut rng).map_err(Svr3Error::OprfClientError)?;

            let VoprfServerEvaluateResult { message, proof } = server_handle.blind_evaluate(self.id, &blinded_element)?;

            let mask = state.finalize(&oprf_input, &message, &proof,
                self.server_pks.get(&server_id).ok_or(Svr3Error::ServerNotFound { server_id })?.clone())
                .map_err(Svr3Error::OprfClientError)?
                .to_vec();

            let masked_share = self.masked_shares.get(&server_id).ok_or(Svr3Error::ServerNotFound { server_id })?.clone();

            let padded_mask = if mask.len() < masked_share.len() {
                let mut padded_mask = vec![0; masked_share.len()];
                for i in 0..masked_share.len() {
                    padded_mask[i] = mask[i % mask.len()];
                }
                padded_mask
            } else if mask.len() > masked_share.len() {
                let mut padded_mask = mask.clone();
                padded_mask.truncate(masked_share.len());
                padded_mask
            } else {
                mask
            };

            let unmasked_share = __xor_bytes(&masked_share, &padded_mask);
            retrieved_shares.insert(server_id, unmasked_share);
        }

        let secret = shamirsecretsharing::combine_shares(&retrieved_shares.values().cloned().collect::<Vec<_>>())
            .map_err(Svr3Error::ShamirSecretSharingError)?
            .ok_or(Svr3Error::InvalidShares)?;

        let r = __hash(&[&secret]);
        let serialized_shares = self.masked_shares.iter()
            .sorted()
            .map(|(i, share)| {
                let mut serialized = i.to_be_bytes().to_vec();
                serialized.extend_from_slice(share);
                serialized
            })
            .fold(Vec::new(), |mut acc, share| {
                acc.extend_from_slice(&share);
                acc
            });
        let computed_commitment = __hash(&[password, &serialized_shares, &r]);

        if &computed_commitment != self.commitment.as_ref().ok_or(Svr3Error::CommitmentNotFound)? {
            return Err(Svr3Error::InvalidCommitment);
        }

        self.store(password, &secret)?;

        Ok(secret)

    }
}

struct Server {
    _id: usize,
    max_uses: usize,

    usage_count: HashMap<usize /* client id */, usize /* usage count */>,
    oprf_servers: HashMap<usize /* client id */, VoprfServer<Ristretto255>>,
}

impl Server {
    fn new(id: usize, max_uses: usize) -> Self {
        Self {
            _id: id,
            max_uses,
            usage_count: HashMap::new(),
            oprf_servers: HashMap::new(),
        }
    }

    fn refresh_client(&mut self, client_id: usize, blinded_element: &BlindedElement<Ristretto255>) -> Result<(VoprfServerEvaluateResult<Ristretto255>, RistrettoPoint), Svr3Error> {
        let mut rng = OsRng;

        let oprf_server = VoprfServer::<Ristretto255>::new(&mut rng).unwrap();

        let public_key = oprf_server.get_public_key().clone();
        self.oprf_servers.insert(client_id, oprf_server);
        self.usage_count.insert(client_id, 0);

        let evaluate_result = self.blind_evaluate(client_id, blinded_element)?;
        self.usage_count.insert(client_id, 0);

        Ok((evaluate_result, public_key))
    }

    fn blind_evaluate(&mut self, client_id: usize, blinded_element: &BlindedElement<Ristretto255>) -> Result<VoprfServerEvaluateResult<Ristretto255>, Svr3Error> {
        let oprf_server = self.oprf_servers.get_mut(&client_id).ok_or(Svr3Error::KeyNotFound { client_id })?;
        let usage_count = self.usage_count.get_mut(&client_id).ok_or(Svr3Error::KeyNotFound { client_id })?;
        if *usage_count >= self.max_uses {
            return Err(Svr3Error::UsageExceeded);
        }
        *usage_count += 1;


        let mut rng = OsRng;

        let evaluate_result = oprf_server.blind_evaluate(&mut rng, blinded_element);
        Ok(evaluate_result)
    }
}





fn __test_client_server(n: usize, t: usize, __log: bool) {
    // Make 5 servers
    let mut servers = HashMap::new();
    for i in 0..n {
        servers.insert(i, Box::new(Server::new(i, MAX_USES)));
    }
    let mut client = Client::new(0, t, n, servers);

    let mut rand_aes_key = [0; 64];
    OsRng.fill_bytes(&mut rand_aes_key);

    client.store(b"password", &rand_aes_key).unwrap();
    let max_pin_attempts = n * MAX_USES / (t + 1);

    if __log {
        println!("Max pin attempts: {}", max_pin_attempts);
    }

    for _ in 0..(MAX_USES - 1) {
        client.restore(b"wrong_password").expect_err("Should have failed to restore key");
    }

    let restored_aes_key = client.restore(b"password").expect("Failed to restore key");
    assert_eq!(restored_aes_key, rand_aes_key);


    for _ in 0..max_pin_attempts {
        client.restore(b"wrong_password").expect_err("Should have failed to restore key");
    }

    client.restore(b"password").expect_err("Should have failed to restore key, even with correct password");
}

const NUM_TESTS: usize = 100;

fn main() {
    for i in 0..NUM_TESTS {
        let n = 7;
        let t = 5;
        __test_client_server(n, t, i == 0);
    }
    println!("All tests passed");
}