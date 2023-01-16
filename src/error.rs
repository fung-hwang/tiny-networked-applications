use thiserror::Error;

/// This type represents all possible errors that can occur when executing commands.
#[derive(Error, Debug)]
pub enum Error {
    #[error("IO")]
    IO(#[from] std::io::Error),
    #[error("Serde_json")]
    SerdeJson(#[from] serde_json::Error),
    #[error("Key not found")]
    KeyNotFound,
    #[error("Unexpected Command")]
    UnexpectedCommand,
    #[error("redb")]
    Redb(#[from] redb::Error),
}

/// Alias for a Result with the error type kvs::Error
pub type Result<T> = std::result::Result<T, crate::Error>;
