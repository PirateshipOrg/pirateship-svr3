// Home for the state used by the server.
// Contains both node-local state and state shared using consensus.
pub mod state;

// Home for REST handlers.
pub mod handlers;

// Client library for key recovery.
pub mod client;


// Log4rs configuration (ported from Pirateship)
pub mod log_config {
    use std::env;
    
    use log::LevelFilter;
    use log4rs::{append::console::ConsoleAppender, config::{Appender, Root}, encode::pattern::PatternEncoder, Config};
    
    pub fn default_log4rs_config() -> Config {
        let level = {
            let lvar = env::var("LOG_LEVEL");
            
            let lvl = match lvar.unwrap_or(String::from("info")).as_str() {
                "info" => LevelFilter::Info,
                "warn" => LevelFilter::Warn,
                "debug" => LevelFilter::Debug,
                "error" => LevelFilter::Error,
                "off" => LevelFilter::Off,
                "trace" => LevelFilter::Trace,
                _ => LevelFilter::Info
            };
    
            lvl
    
        };
        let stdout = ConsoleAppender::builder()
            .encoder(Box::new(PatternEncoder::new(
                "{h([{l}][{M}][{d}])} {m}{n}"     // [INFO][module][timestamp] message
            )))
            .build();
    
        Config::builder()
            .appender(Appender::builder().build("stdout", Box::new(stdout)))
            .build(Root::builder().appender("stdout").build(level))
            .unwrap()
    }

}


// Error types common to the entire application.

pub mod errors {
    use std::fmt::Display;

    use axum::{Json, http::StatusCode, response::IntoResponse};
    use serde_json::json;

    use crate::state::ClientId;

    #[derive(Debug)]
    pub struct SSSErrorWrapper {
        pub inner: shamirsecretsharing::SSSError,
    }

    impl Display for SSSErrorWrapper {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{}", self.inner)
        }
    }

    impl From<shamirsecretsharing::SSSError> for SSSErrorWrapper {
        fn from(inner: shamirsecretsharing::SSSError) -> Self {
            Self { inner }
        }
    }

    #[derive(Debug)]
    pub struct VoprfErrorWrapper {
        pub inner: voprf::Error,
    }

    impl serde::Serialize for SSSErrorWrapper {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            serializer.serialize_str(self.inner.to_string().as_str())
        }
    }

    impl Display for VoprfErrorWrapper {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{}", self.inner)
        }
    }

    impl From<voprf::Error> for VoprfErrorWrapper {
        fn from(inner: voprf::Error) -> Self {
            Self { inner }
        }
    }

    impl serde::Serialize for VoprfErrorWrapper {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            serializer.serialize_str(self.inner.to_string().as_str())
        }
    }

    #[derive(Debug, thiserror::Error, serde::Serialize)]
    pub enum Svr3Error {
        #[error("usage count exceeded")]
        UsageExceeded,

        #[error("key not found for client {client_id}")]
        KeyNotFound { client_id: ClientId },

        #[error("shamir secret sharing error: {0}")]
        ShamirSecretSharingError(SSSErrorWrapper),

        #[error("oprf client error: {0}")]
        OprfClientError(VoprfErrorWrapper),

        #[error("server {server_id} not found")]
        ServerNotFound { server_id: usize },

        #[error("internal server error: {server_addr}")]
        InternalServerError { server_addr: String },

        #[error("invalid shares")]
        InvalidShares,

        #[error("commitment not found")]
        CommitmentNotFound,

        #[error("invalid commitment")]
        InvalidCommitment,
    }

    impl IntoResponse for Svr3Error {
        fn into_response(self) -> axum::response::Response {
            let status = match self {
                Svr3Error::UsageExceeded => StatusCode::TOO_MANY_REQUESTS,
                Svr3Error::KeyNotFound { .. } => StatusCode::NOT_FOUND,
                Svr3Error::ServerNotFound { .. } => StatusCode::NOT_FOUND,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            };


            let error_message = serde_json::to_value(self.to_string()).unwrap();
            let error_type = serde_json::to_value(self).unwrap();
            let body = Json(json!({
                "error": error_type,
                "message": error_message,
            }));
            (status, body).into_response()
        }
    }
}