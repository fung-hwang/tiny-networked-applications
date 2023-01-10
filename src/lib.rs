//! A on-disk key-value store.

pub use error::{Error, Result};
pub use kv::{KvStore, KvsEngine};

mod error;
mod kv;
