//! Client configuration types.
//! 
//! This module defines the configuration structure for the recovery client.
//! The client expects a `pft::config::ClientConfig` as JSON input.

use serde::{Deserialize, Serialize};

/// Client configuration for the recovery client.
/// 
/// This is a wrapper around `pft::config::ClientConfig` that can be deserialized
/// from JSON. The actual `pft::config::ClientConfig` structure should be provided
/// as JSON when initializing the client.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientConfig {
    /// Client identifier
    pub client_id: String,
    
    /// Threshold for secret sharing (minimum number of servers needed to recover)
    pub server_threshold: usize,
    
    /// Total number of servers
    pub server_count: usize,
    
    /// List of server base URLs (e.g., ["http://server1:8080", "http://server2:8080"])
    pub server_urls: Vec<String>,

    /// Share store file path
    pub share_store_file_path: String,
}

impl ClientConfig {
    /// Creates a new client configuration.
    /// 
    /// # Arguments
    /// 
    /// * `client_id` - Unique identifier for this client
    /// * `server_threshold` - Minimum number of servers needed (threshold + 1)
    /// * `server_count` - Total number of servers
    /// * `server_urls` - Base URLs for each server
    /// 
    /// # Panics
    /// 
    /// Panics if `server_threshold + 1 > server_count` or if `server_count > 255`
    /// (limitation of the shamir secret sharing library).
    pub fn new(
        client_id: String,
        server_threshold: usize,
        server_count: usize,
        server_urls: Vec<String>,
        share_store_file_path: String,
    ) -> Self {
        assert!(server_threshold + 1 <= server_count);
        assert!(server_count <= 255);
        assert_eq!(server_urls.len(), server_count);
        
        Self {
            client_id,
            server_threshold,
            server_count,
            server_urls,
            share_store_file_path,
        }
    }
    
    /// Deserializes a `pft::config::ClientConfig` from JSON.
    /// 
    /// This function expects the JSON to contain the client configuration
    /// in a format compatible with `pft::config::ClientConfig`.
    /// 
    /// # Arguments
    /// 
    /// * `json` - JSON string containing the client configuration
    /// 
    /// # Returns
    /// 
    /// Returns a `ClientConfig` if deserialization succeeds.
    pub fn from_json(json: &str) -> Result<Self, serde_json::Error> {
        // For now, we'll deserialize directly. In a real implementation,
        // this would parse pft::config::ClientConfig and extract the relevant fields.
        serde_json::from_str(json)
    }
}

