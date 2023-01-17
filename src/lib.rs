//! A on-disk key-value store.

pub use error::{Error, Result};

pub use engines::kvstore::*;
pub use engines::redb::*;
pub use engines::KvsEngine;

mod engines;
mod error;
