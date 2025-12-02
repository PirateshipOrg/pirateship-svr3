//! HTTP client for communicating with SVR3 servers.
//! 
//! This module provides an HTTP client wrapper that handles REST API calls
//! to the SVR3 servers for OPRF operations.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use base64::prelude::*;
use voprf::{BlindedElement, EvaluationElement, Proof, Ristretto255};
use curve25519_dalek::ristretto::RistrettoPoint;

use crate::errors::Svr3Error;
use crate::handlers::marshal::ClientRequest;
use crate::state::ClientId;

/// Response from the /refresh endpoint.
#[derive(Debug, Clone)]
pub struct RefreshResponse {
    /// The evaluated OPRF result (evaluated element)
    pub evaluate_result: EvaluationElement<Ristretto255>,
    
    /// Proof of correct evaluation
    pub proof: Proof<Ristretto255>,
    
    /// Public key of the OPRF server
    pub public_key: RistrettoPoint,
}

/// Response from the /evaluate endpoint.
#[derive(Debug, Clone)]
pub struct EvaluateResponse {
    /// The evaluated OPRF result (evaluated element)
    pub evaluate_result: EvaluationElement<Ristretto255>,
    
    /// Proof of correct evaluation
    pub proof: Proof<Ristretto255>,
}

// Custom serialization/deserialization for RefreshResponse
// The server sends these as JSON with base64-encoded binary data
impl Serialize for RefreshResponse {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeMap;
        let mut map = serializer.serialize_map(Some(3))?;
        
        // Serialize evaluate_result as base64
        let eval_ser = self.evaluate_result.serialize();
        let eval_base64 = base64::prelude::BASE64_STANDARD.encode(eval_ser);
        map.serialize_entry("evaluate_result", &eval_base64)?;
        
        // Serialize proof as base64
        let proof_ser = self.proof.serialize();
        let proof_base64 = base64::prelude::BASE64_STANDARD.encode(proof_ser);
        map.serialize_entry("proof", &proof_base64)?;
        
        // Serialize public_key as base64 (RistrettoPoint uses compressed representation)
        let pk_bytes = self.public_key.compress().to_bytes();
        let pk_base64 = base64::prelude::BASE64_STANDARD.encode(pk_bytes);
        map.serialize_entry("public_key", &pk_base64)?;
        
        map.end()
    }
}

impl<'de> Deserialize<'de> for RefreshResponse {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::{self, MapAccess, Visitor};
        use std::fmt;

        struct RefreshResponseVisitor;

        impl<'de> Visitor<'de> for RefreshResponseVisitor {
            type Value = RefreshResponse;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a RefreshResponse object")
            }

            fn visit_map<M>(self, mut map: M) -> Result<Self::Value, M::Error>
            where
                M: MapAccess<'de>,
            {
                let mut evaluate_result = None;
                let mut proof = None;
                let mut public_key = None;

                while let Some(key) = map.next_key::<String>()? {
                    match key.as_str() {
                        "evaluate_result" => {
                            if evaluate_result.is_some() {
                                return Err(de::Error::duplicate_field("evaluate_result"));
                            }
                            let base64_str: String = map.next_value()?;
                            let bytes = base64::prelude::BASE64_STANDARD
                                .decode(base64_str.as_bytes())
                                .map_err(de::Error::custom)?;
                            let eval_elem = EvaluationElement::<Ristretto255>::deserialize(&bytes)
                                .map_err(de::Error::custom)?;
                            evaluate_result = Some(eval_elem);
                        }
                        "proof" => {
                            if proof.is_some() {
                                return Err(de::Error::duplicate_field("proof"));
                            }
                            let base64_str: String = map.next_value()?;
                            let bytes = base64::prelude::BASE64_STANDARD
                                .decode(base64_str.as_bytes())
                                .map_err(de::Error::custom)?;
                            let proof_val = Proof::<Ristretto255>::deserialize(&bytes)
                                .map_err(de::Error::custom)?;
                            proof = Some(proof_val);
                        }
                        "public_key" => {
                            if public_key.is_some() {
                                return Err(de::Error::duplicate_field("public_key"));
                            }
                            let base64_str: String = map.next_value()?;
                            let bytes = base64::prelude::BASE64_STANDARD
                                .decode(base64_str.as_bytes())
                                .map_err(de::Error::custom)?;
                            if bytes.len() != 32 {
                                return Err(de::Error::custom("Invalid RistrettoPoint length"));
                            }
                            let mut pk_bytes = [0u8; 32];
                            pk_bytes.copy_from_slice(&bytes);
                            let compressed = curve25519_dalek::ristretto::CompressedRistretto(pk_bytes);
                            let pk = compressed.decompress()
                                .ok_or_else(|| de::Error::custom("Invalid RistrettoPoint"))?;
                            public_key = Some(pk);
                        }
                        _ => {
                            let _ = map.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }

                let evaluate_result = evaluate_result.ok_or_else(|| de::Error::missing_field("evaluate_result"))?;
                let proof = proof.ok_or_else(|| de::Error::missing_field("proof"))?;
                let public_key = public_key.ok_or_else(|| de::Error::missing_field("public_key"))?;

                Ok(RefreshResponse {
                    evaluate_result,
                    proof,
                    public_key,
                })
            }
        }

        deserializer.deserialize_map(RefreshResponseVisitor)
    }
}

// Custom serialization/deserialization for EvaluateResponse
impl Serialize for EvaluateResponse {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeMap;
        let mut map = serializer.serialize_map(Some(2))?;
        
        // Serialize evaluate_result as base64
        let eval_ser = self.evaluate_result.serialize();
        let eval_base64 = base64::prelude::BASE64_STANDARD.encode(eval_ser);
        map.serialize_entry("evaluate_result", &eval_base64)?;
        
        // Serialize proof as base64
        let proof_ser = self.proof.serialize();
        let proof_base64 = base64::prelude::BASE64_STANDARD.encode(proof_ser);
        map.serialize_entry("proof", &proof_base64)?;
        
        map.end()
    }
}

impl<'de> Deserialize<'de> for EvaluateResponse {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::{self, MapAccess, Visitor};
        use std::fmt;

        struct EvaluateResponseVisitor;

        impl<'de> Visitor<'de> for EvaluateResponseVisitor {
            type Value = EvaluateResponse;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("an EvaluateResponse object")
            }

            fn visit_map<M>(self, mut map: M) -> Result<Self::Value, M::Error>
            where
                M: MapAccess<'de>,
            {
                let mut evaluate_result = None;
                let mut proof = None;

                while let Some(key) = map.next_key::<String>()? {
                    match key.as_str() {
                        "evaluate_result" => {
                            if evaluate_result.is_some() {
                                return Err(de::Error::duplicate_field("evaluate_result"));
                            }
                            let base64_str: String = map.next_value()?;
                            let bytes = base64::prelude::BASE64_STANDARD
                                .decode(base64_str.as_bytes())
                                .map_err(de::Error::custom)?;
                            let eval_elem = EvaluationElement::<Ristretto255>::deserialize(&bytes)
                                .map_err(de::Error::custom)?;
                            evaluate_result = Some(eval_elem);
                        }
                        "proof" => {
                            if proof.is_some() {
                                return Err(de::Error::duplicate_field("proof"));
                            }
                            let base64_str: String = map.next_value()?;
                            let bytes = base64::prelude::BASE64_STANDARD
                                .decode(base64_str.as_bytes())
                                .map_err(de::Error::custom)?;
                            let proof_val = Proof::<Ristretto255>::deserialize(&bytes)
                                .map_err(de::Error::custom)?;
                            proof = Some(proof_val);
                        }
                        _ => {
                            let _ = map.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }

                let evaluate_result = evaluate_result.ok_or_else(|| de::Error::missing_field("evaluate_result"))?;
                let proof = proof.ok_or_else(|| de::Error::missing_field("proof"))?;

                Ok(EvaluateResponse {
                    evaluate_result,
                    proof,
                })
            }
        }

        deserializer.deserialize_map(EvaluateResponseVisitor)
    }
}

/// HTTP client for communicating with a single SVR3 server.
pub struct ServerHttpClient {
    /// Base URL of the server (e.g., "http://localhost:8080")
    base_url: String,
    
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

