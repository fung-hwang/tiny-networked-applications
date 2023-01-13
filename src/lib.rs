//! A on-disk key-value store.

pub use error::{Error, Result};

pub use engines::mykvs::KvStore;
pub use engines::KvsEngine;

mod engines;
mod error;
