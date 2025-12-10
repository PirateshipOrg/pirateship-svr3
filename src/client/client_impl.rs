//! Main client implementation for key recovery.
//! 
//! This module implements the core client logic for storing and restoring secrets
//! using the SVR3 protocol. It follows the same logic as the benchmark test but
//! uses HTTP clients to communicate with remote servers.

use std::collections::HashMap;

use curve25519_dalek::ristretto::RistrettoPoint;
use itertools::Itertools;
use rand::{rngs::OsRng, seq::IteratorRandom as _};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha512};
use voprf::{Ristretto255, VoprfClient, VoprfClientBlindResult};

use crate::errors::{SSSErrorWrapper, Svr3Error, VoprfErrorWrapper};
use crate::state::ClientId;

use super::config::ClientConfig;
use crate::handlers::marshal::{EvaluateResponse, RefreshResponse};
use super::http_client::ServerHttpClient;

/// Helper function to hash multiple byte slices together.
/// 
/// # Arguments
/// 
/// * `parts` - Slice of byte slices to hash
/// 
/// # Returns
/// 
/// Returns the SHA-512 hash of the concatenated inputs.
fn hash(parts: &[&[u8]]) -> Vec<u8> {
    let mut hasher = Sha512::new();
    for part in parts {
        hasher.update(part);
    }
    hasher.finalize().to_vec()
}

/// Helper function to XOR two byte slices.
/// 
/// # Arguments
/// 
/// * `a` - First byte slice
/// * `b` - Second byte slice (must be same length as `a`)
/// 
/// # Returns
/// 
/// Returns the XOR of the two byte slices.
/// 
/// # Panics
/// 
/// Panics if the two slices have different lengths.
fn xor_bytes(a: &[u8], b: &[u8]) -> Vec<u8> {
    assert_eq!(a.len(), b.len());
    let mut result = vec![0; a.len()];

    for i in 0..a.len() {
        result[i] = a[i] ^ b[i];
    }
    result
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RecoveryClientShareStore {
    pub masked_shares: HashMap<usize, Vec<u8>>,
    pub server_pks: HashMap<usize, RistrettoPoint>,
    pub commitment: Option<Vec<u8>>,
}

impl From<&mut RecoveryClient> for RecoveryClientShareStore {
    fn from(client: &mut RecoveryClient) -> Self {
        Self {
            masked_shares: client.masked_shares.clone(),
            server_pks: client.server_pks.clone(),
            commitment: client.commitment.clone(),
        }
    }
}


/// Client for storing and recovering secrets using the SVR3 protocol.
/// 
/// This client implements the SVR3 protocol for secure value recovery:
/// 1. Secrets are split using Shamir Secret Sharing
/// 2. Each share is masked using an OPRF (Oblivious Pseudorandom Function)
/// 3. Shares are distributed across multiple servers
/// 4. Recovery requires threshold+1 servers and the correct password
pub struct RecoveryClient {
    /// Client identifier
    client_id: ClientId,
    
    /// Threshold for secret sharing (minimum number of servers needed)
    server_threshold: usize,
    
    /// Total number of servers
    server_count: usize,
    
    /// Commitment hash for verifying the stored secret
    commitment: Option<Vec<u8>>,
    
    /// Masked shares stored per server (server_id -> masked_share)
    masked_shares: HashMap<usize, Vec<u8>>,
    
    /// Public keys of OPRF servers (server_id -> public_key)
    server_pks: HashMap<usize, RistrettoPoint>,
    
    /// HTTP clients for each server (server_id -> http_client)
    server_clients: HashMap<usize, ServerHttpClient>,

    /// Share store file path
    share_store_file_path: String,
}

impl RecoveryClient {
    /// Creates a new recovery client from a configuration.
    /// 
    /// # Arguments
    /// 
    /// * `config` - Client configuration containing server URLs and parameters
    /// 
    /// # Returns
    /// 
    /// Returns a new `RecoveryClient` instance.
    pub fn new(config: ClientConfig) -> Self {
        let mut server_clients = HashMap::new();
        for (i, url) in config.server_urls.iter().enumerate() {
            server_clients.insert(i, ServerHttpClient::new(url.clone()));
        }

        let share_store = if let Ok(share_store_json) = std::fs::read_to_string(&config.share_store_file_path) {
            serde_json::from_str(&share_store_json).unwrap()
        } else {
            RecoveryClientShareStore {
                masked_shares: HashMap::new(),
                server_pks: HashMap::new(),
                commitment: None,
            }
        };
        
        Self {
            client_id: config.client_id,
            server_threshold: config.server_threshold,
            server_count: config.server_count,
            commitment: share_store.commitment,
            masked_shares: share_store.masked_shares,
            server_pks: share_store.server_pks,
            server_clients,
            share_store_file_path: config.share_store_file_path,
        }
    }
    
    /// Creates a new recovery client from a JSON configuration string.
    /// 
    /// This is the main entry point for creating a client from a `pft::config::ClientConfig`
    /// JSON string.
    /// 
    /// # Arguments
    /// 
    /// * `config_json` - JSON string containing the client configuration
    /// 
    /// # Returns
    /// 
    /// Returns a new `RecoveryClient` instance or an error if parsing fails.
    pub fn from_json(config_json: &str) -> Result<Self, serde_json::Error> {
        let config = ClientConfig::from_json(config_json)?;
        Ok(Self::new(config))
    }
    
    /// Stores a secret using the SVR3 protocol.
    /// 
    /// This method:
    /// 1. Splits the secret into shares using Shamir Secret Sharing
    /// 2. For each share, performs OPRF blinding and gets the mask from the server
    /// 3. Masks each share with the OPRF output
    /// 4. Stores the masked shares and creates a commitment
    /// 
    /// # Arguments
    /// 
    /// * `password` - Password used for OPRF operations
    /// * `client_secret` - The secret to store
    /// 
    /// # Returns
    /// 
    /// Returns `Ok(())` if storage succeeds, or an error otherwise.
    pub async fn store(&mut self, password: &[u8], client_secret: &[u8]) -> Result<(), Svr3Error> {
        // Compute r = hash(client_secret) for commitment
        let r = hash(&[client_secret]);

        let t = self.server_threshold;
        let n = self.server_count;

        // Split the secret into shares using Shamir Secret Sharing
        let shares = shamirsecretsharing::create_shares(client_secret, n as u8, (t + 1) as u8)
            .map_err(|e| Svr3Error::ShamirSecretSharingError(SSSErrorWrapper::from(e)))?;

        let mut rng = OsRng;
        
        // Process each share
        for (i, share) in shares.iter().enumerate() {
            // Compute OPRF input: hash(password || server_id)
            let oprf_input = hash(&[password, i.to_be_bytes().as_ref()]);
            
            // Blind the OPRF input
            let VoprfClientBlindResult { state, message: blinded_element } =
                VoprfClient::<Ristretto255>::blind(&oprf_input, &mut rng)
                    .map_err(|e| Svr3Error::OprfClientError(VoprfErrorWrapper::from(e)))?;

            // Call the server's /refresh endpoint to get a new OPRF server instance
            let server_client = self.server_clients
                .get(&i)
                .ok_or(Svr3Error::ServerNotFound { server_id: i })?;
            
            let RefreshResponse {
                evaluate_result,
                proof,
                public_key,
            } = server_client.refresh(self.client_id.clone(), &blinded_element).await?;
            
            // Finalize the OPRF to get the mask
            let mask = state.finalize(&oprf_input, &evaluate_result, &proof, public_key.clone())
                .map_err(|e| Svr3Error::OprfClientError(VoprfErrorWrapper::from(e)))?
                .to_vec();
            
            // Pad or truncate mask to match share length
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

            // Mask the share
            let masked_share = xor_bytes(&share, &padded_mask);

            // Store the masked share
            self.masked_shares.insert(i, masked_share);

            // Store the server's public key
            self.server_pks.insert(i, public_key);
        }

        // Create commitment: hash(password || serialized_shares || r)
        let serialized_shares = self.masked_shares
            .iter()
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

        let commitment = hash(&[password, &serialized_shares, &r]);
        self.commitment = Some(commitment);

        let _path = self.share_store_file_path.clone();
        let share_store = RecoveryClientShareStore::from(self);
        let share_store_json = serde_json::to_string(&share_store).unwrap();
        std::fs::write(_path, share_store_json).unwrap();

        Ok(())
    }
    
    /// Restores a secret using the SVR3 protocol.
    /// 
    /// This method:
    /// 1. Selects threshold+1 servers at random
    /// 2. For each server, performs OPRF blinding and gets the mask
    /// 3. Unmasks the shares
    /// 4. Combines the shares to recover the secret
    /// 5. Verifies the commitment
    /// 6. Re-stores the secret (refresh operation)
    /// 
    /// # Arguments
    /// 
    /// * `password` - Password used for OPRF operations
    /// 
    /// # Returns
    /// 
    /// Returns the recovered secret if successful, or an error otherwise.
    pub async fn restore(&mut self, password: &[u8]) -> Result<Vec<u8>, Svr3Error> {
        let mut rng = OsRng;
        
        // Select threshold+1 servers at random
        let target_server_ids = (0..self.server_count)
            .choose_multiple(&mut rng, self.server_threshold + 1);

        let mut retrieved_shares = HashMap::new();
        
        // Retrieve shares from each selected server
        for server_id in target_server_ids {
            log::info!("Restoring from server {} {:?}", server_id, self.server_clients.get(&server_id).unwrap().base_url);
            let server_client = self.server_clients
                .get(&server_id)
                .ok_or(Svr3Error::ServerNotFound { server_id })?;

            // Compute OPRF input: hash(password || server_id)
            let oprf_input = hash(&[password, server_id.to_be_bytes().as_ref()]);
            
            // Blind the OPRF input
            let VoprfClientBlindResult { state, message: blinded_element } =
                VoprfClient::<Ristretto255>::blind(&oprf_input, &mut rng)
                    .map_err(|e| Svr3Error::OprfClientError(VoprfErrorWrapper::from(e)))?;

            // Call the server's /evaluate endpoint
            let EvaluateResponse {
                evaluate_result,
                proof,
            } = server_client.evaluate(self.client_id.clone(), &blinded_element).await?;
            
            // Get the server's public key
            let server_pk = self.server_pks.get(&server_id)
                .ok_or(Svr3Error::ServerNotFound { server_id })?.clone();
            
            // Finalize the OPRF to get the mask
            let mask = state.finalize(&oprf_input, &evaluate_result, &proof, server_pk)
                .map_err(|e| Svr3Error::OprfClientError(VoprfErrorWrapper::from(e)))?
                .to_vec();
            
            // Get the masked share
            let masked_share = self.masked_shares
                .get(&server_id)
                .ok_or(Svr3Error::ServerNotFound { server_id })?
                .clone();
            
            // Pad or truncate mask to match masked_share length
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

            // Unmask the share
            let unmasked_share = xor_bytes(&masked_share, &padded_mask);
            retrieved_shares.insert(server_id, unmasked_share);
        }

        // Combine shares to recover the secret
        let secret = shamirsecretsharing::combine_shares(
            &retrieved_shares.values().cloned().collect::<Vec<_>>()
        )
        .map_err(|e| Svr3Error::ShamirSecretSharingError(SSSErrorWrapper::from(e)))?
        .ok_or(Svr3Error::InvalidShares)?;

        // Verify commitment
        let r = hash(&[&secret]);
        let serialized_shares = self.masked_shares
            .iter()
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
        let computed_commitment = hash(&[password, &serialized_shares, &r]);

        if &computed_commitment != self.commitment.as_ref().ok_or(Svr3Error::CommitmentNotFound)? {
            return Err(Svr3Error::InvalidCommitment);
        }

        // Re-store the secret (refresh operation)
        self.store(password, &secret).await?;

        Ok(secret)
    }
}