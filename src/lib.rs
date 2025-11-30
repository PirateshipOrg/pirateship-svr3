// Home for the state used by the server.
// Contains both node-local state and state shared using consensus.
pub mod state;

// Home for REST handlers.
pub mod handlers;


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
        KeyNotFound { client_id: usize },

        #[error("shamir secret sharing error: {0}")]
        ShamirSecretSharingError(SSSErrorWrapper),

        #[error("oprf client error: {0}")]
        OprfClientError(VoprfErrorWrapper),

        #[error("server {server_id} not found")]
        ServerNotFound { server_id: usize },

        #[error("invalid shares")]
        InvalidShares,

        #[error("commitment not found")]
        CommitmentNotFound,

        #[error("invalid commitment")]
        InvalidCommitment,
    }
}