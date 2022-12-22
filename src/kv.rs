use std::collections::HashMap;
use std::path::PathBuf;

/// The `KvStore` stores string key/value pairs in memory.
///
/// Key/value pairs are stored in a [`HashMap`] in memory and not persisted to disk.
///
/// # Example
///
/// ```rust
/// use kvs::KvStore;
///
/// let mut store = KvStore::new();
/// store.set("key".to_string(), "value".to_string());
/// let val = store.get("key".to_string());
/// assert_eq!(val, Some("value".to_string()));
/// store.remove("key".to_string());
/// ```
pub struct KvStore {
    index: HashMap<String, String>,
}

pub type Result<T> = anyhow::Result<T>;

impl KvStore {
    /// Creates an empty KvStore.
    ///
    /// # Example
    ///
    /// ```rust
    /// use kvs::KvStore;
    /// let mut store = KvStore::new();
    /// ```
    pub fn new() -> Self {
        KvStore {
            index: HashMap::new(),
        }
    }

    /// Open the KvStore at a given path.
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        // if file exists, open it, then rebuild index
        // if not, create
        panic!();
    }

    /// Set the value of a string key to a string.
    ///
    /// If the map did have this key present, the value is updated.
    ///
    /// # Example
    ///
    /// ```rust
    /// use kvs::KvStore;
    ///
    /// let mut store = KvStore::new();
    /// store.set("key".to_string(), "value".to_string());
    /// ```
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        // self.map.insert(key, value);
        panic!();
    }

    /// Get the string value of a given string key.
    ///
    /// Returns [`None`] if the given key does not exist.
    ///
    /// # Example
    ///
    /// ```rust
    /// use kvs::KvStore;
    ///
    /// let mut store = KvStore::new();
    /// store.set("key".to_string(), "value".to_string());
    /// let val = store.get("key".to_string());
    /// assert_eq!(val, Some("value".to_string()));
    /// assert_eq!(store.get("invalid_Key".to_string()), None);
    /// ```
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        // self.map.get(&key).cloned()
        panic!();
    }

    /// Remove a given key.
    ///
    /// # Example
    ///
    /// ```rust
    /// use kvs::KvStore;
    ///
    /// let mut store = KvStore::new();
    /// store.set("key".to_string(), "value".to_string());
    /// store.remove("key".to_string());
    /// ```
    pub fn remove(&mut self, key: String) -> Result<()> {
        // self.map.remove(&key);
        panic!();
    }
}

impl Default for KvStore {
    /// Creates an empty KvStore.
    fn default() -> Self {
        Self::new()
    }
}
