use crate::Result;

pub mod mykvs;
pub mod sled;

pub trait KvsEngine {
    fn set(&mut self, key: String, value: String) -> Result<()>;

    fn get(&mut self, key: String) -> Result<Option<String>>;

    fn remove(&mut self, key: String) -> Result<()>;
}
