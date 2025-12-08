//! HTTP client for communicating with SVR3 servers.
//! 
//! This module provides an HTTP client wrapper that handles REST API calls
//! to the SVR3 servers for OPRF operations.

use serde_json::Value;
use voprf::{BlindedElement, Ristretto255};


use crate::errors::Svr3Error;
use crate::handlers::marshal::{ClientRequest, EvaluateResponse, RefreshResponse};
use crate::state::ClientId;


/// HTTP client for communicating with a single SVR3 server.
#[derive(Debug)]
pub struct ServerHttpClient {
    /// Base URL of the server (e.g., "http://localhost:8080")
    pub base_url: String,
    
    /// HTTP client instance
    http_client: reqwest::Client,
}

impl ServerHttpClient {
    /// Creates a new HTTP client for a server.
    /// 
    /// # Arguments
    /// 
    /// * `base_url` - Base URL of the server
    pub fn new(base_url: String) -> Self {
        Self {
            base_url,
            http_client: reqwest::Client::new(),
        }
    }
    
    /// Calls the /refresh endpoint to refresh the OPRF server for a client.
    /// 
    /// This endpoint resets the usage counter and creates a new OPRF server instance.
    /// 
    /// # Arguments
    /// 
    /// * `client_id` - Identifier of the client
    /// * `blinded_element` - The blinded OPRF input element
    /// 
    /// # Returns
    /// 
    /// Returns the refresh response containing the evaluated result, proof, and public key.
    pub async fn refresh(
        &self,
        client_id: ClientId,
        blinded_element: &BlindedElement<Ristretto255>,
    ) -> Result<RefreshResponse, Svr3Error> {
        let request = ClientRequest {
            client_id,
            blinded_element: blinded_element.clone(),
        };
        
        let url = format!("{}/refresh", self.base_url);
        let response = self
            .http_client
            .post(&url)
            .json(&request)
            .send()
            .await
            .map_err(|e| {
                // Map HTTP client errors to appropriate Svr3Error
                Svr3Error::InternalServerError {
                    server_addr: format!("{}: {}", self.base_url, e),
                }
            })?;
        
        if !response.status().is_success() {
            // Try to parse error response from server
            let status = response.status();
            let error_json: Value = response.json().await
                .map_err(|_| {
                    // If we can't parse the error, return a generic error
                    Svr3Error::InternalServerError {
                        server_addr: format!("{}: HTTP {}", self.base_url, status),
                    }
                })?;
            
            // Try to extract the error type from the response
            // Server returns: { "error": {...}, "message": "..." }
            if let Some(error_obj) = error_json.get("error") {
                // Try to parse common error types from the error object
                if let Some(error_type) = error_obj.get("UsageExceeded") {
                    if error_type.is_null() {
                        return Err(Svr3Error::UsageExceeded);
                    }
                }
                if let Some(client_id_val) = error_obj.get("KeyNotFound") {
                    if let Some(client_id) = client_id_val.get("client_id") {
                        if let Some(client_id_str) = client_id.as_str() {
                            return Err(Svr3Error::KeyNotFound {
                                client_id: client_id_str.to_string(),
                            });
                        }
                    }
                }
                if let Some(server_id_val) = error_obj.get("ServerNotFound") {
                    if let Some(server_id) = server_id_val.get("server_id") {
                        if let Some(server_id_num) = server_id.as_u64() {
                            return Err(Svr3Error::ServerNotFound {
                                server_id: server_id_num as usize,
                            });
                        }
                    }
                }
                if let Some(_) = error_obj.get("InvalidShares") {
                    return Err(Svr3Error::InvalidShares);
                }
                if let Some(_) = error_obj.get("CommitmentNotFound") {
                    return Err(Svr3Error::CommitmentNotFound);
                }
                if let Some(_) = error_obj.get("InvalidCommitment") {
                    return Err(Svr3Error::InvalidCommitment);
                }
            }
            
            // Fallback to generic error if we can't parse it
            return Err(Svr3Error::InternalServerError {
                server_addr: format!("{}: HTTP {} - {}", self.base_url, status, error_json),
            });
        }
        // info!("Refresh response: {:?}", response.json::<Value>().await.unwrap());
        let refresh_response: RefreshResponse = response
            .json()
            .await
            .map_err(|e| {
                Svr3Error::InternalServerError {
                    server_addr: format!("{}: Failed to parse response - {}", self.base_url, e),
                }
            })?;
        
        Ok(refresh_response)
    }
    
    /// Calls the /evaluate endpoint to evaluate the OPRF for a client.
    /// 
    /// This endpoint increments the usage counter and evaluates the blinded element.
    /// 
    /// # Arguments
    /// 
    /// * `client_id` - Identifier of the client
    /// * `blinded_element` - The blinded OPRF input element
    /// 
    /// # Returns
    /// 
    /// Returns the evaluate response containing the evaluated result and proof.
    pub async fn evaluate(
        &self,
        client_id: ClientId,
        blinded_element: &BlindedElement<Ristretto255>,
    ) -> Result<EvaluateResponse, Svr3Error> {
        let request = ClientRequest {
            client_id,
            blinded_element: blinded_element.clone(),
        };
        
        let url = format!("{}/evaluate", self.base_url);
        let response = self
            .http_client
            .post(&url)
            .json(&request)
            .send()
            .await
            .map_err(|e| {
                // Map HTTP client errors to appropriate Svr3Error
                Svr3Error::InternalServerError {
                    server_addr: format!("{}: {}", self.base_url, e),
                }
            })?;
        
        if !response.status().is_success() {
            // Try to parse error response from server
            let status = response.status();
            let error_json: Value = response.json().await
                .map_err(|_| {
                    // If we can't parse the error, return a generic error
                    Svr3Error::InternalServerError {
                        server_addr: format!("{}: HTTP {}", self.base_url, status),
                    }
                })?;
            
            // Try to extract the error type from the response
            // Server returns: { "error": {...}, "message": "..." }
            if let Some(error_obj) = error_json.get("error") {
                // Try to parse common error types from the error object
                if let Some(error_type) = error_obj.get("UsageExceeded") {
                    if error_type.is_null() {
                        return Err(Svr3Error::UsageExceeded);
                    }
                }
                if let Some(client_id_val) = error_obj.get("KeyNotFound") {
                    if let Some(client_id) = client_id_val.get("client_id") {
                        if let Some(client_id_str) = client_id.as_str() {
                            return Err(Svr3Error::KeyNotFound {
                                client_id: client_id_str.to_string(),
                            });
                        }
                    }
                }
                if let Some(server_id_val) = error_obj.get("ServerNotFound") {
                    if let Some(server_id) = server_id_val.get("server_id") {
                        if let Some(server_id_num) = server_id.as_u64() {
                            return Err(Svr3Error::ServerNotFound {
                                server_id: server_id_num as usize,
                            });
                        }
                    }
                }
                if let Some(_) = error_obj.get("InvalidShares") {
                    return Err(Svr3Error::InvalidShares);
                }
                if let Some(_) = error_obj.get("CommitmentNotFound") {
                    return Err(Svr3Error::CommitmentNotFound);
                }
                if let Some(_) = error_obj.get("InvalidCommitment") {
                    return Err(Svr3Error::InvalidCommitment);
                }
            }
            
            // Fallback to generic error if we can't parse it
            return Err(Svr3Error::InternalServerError {
                server_addr: format!("{}: HTTP {} - {}", self.base_url, status, error_json),
            });
        }
        
        let evaluate_response: EvaluateResponse = response
            .json()
            .await
            .map_err(|e| {
                Svr3Error::InternalServerError {
                    server_addr: format!("{}: Failed to parse response - {}", self.base_url, e),
                }
            })?;
        
        Ok(evaluate_response)
    }
}

