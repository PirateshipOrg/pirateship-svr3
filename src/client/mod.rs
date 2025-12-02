//! Client library for key recovery using SVR3 protocol.
//! 
//! This module provides a client implementation that can store and restore secrets
//! using the SVR3 (Secure Value Recovery) protocol. The client communicates with
//! multiple servers via REST APIs to perform threshold secret sharing operations.

mod client_impl;
mod http_client;
mod config;

pub use client_impl::RecoveryClient;
pub use config::ClientConfig;

