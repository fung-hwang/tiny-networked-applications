use crate::Result;

pub mod kvstore;
pub mod redb;
pub mod sled;

pub trait KvsEngine {
    fn open(path: impl AsRef<std::path::Path>) -> Result<Self>
    where
        Self: Sized;

    fn set(&mut self, key: String, value: String) -> Result<()>;

    fn get(&mut self, key: String) -> Result<Option<String>>;

    fn remove(&mut self, key: String) -> Result<()>;
}
