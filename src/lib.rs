//! A key-value store

pub use error::{Error, Result};
pub use kv::KvStore;

mod error;
mod kv;
