use std::collections::HashMap;
use std::fs::File;
use std::io::SeekFrom;
use std::io::{BufReader, BufWriter};
use std::io::{Read, Write};
use std::path::PathBuf;

use anyhow::Ok;

pub type Result<T> = anyhow::Result<T>;

struct BufReaderWithPos<T: Seek + Read> {
    buf_reader: BufReader<T>,
    pos: usize,
}

impl<T: Seek + Read> BufReaderWithPos<T> {
    fn new(inner: T) -> Self {
        let pos = inner.seek(SeekFrom::Current(0));
        Self {
            buf_reader: BufReader::new(inner),
            pos,
        }
    }
}

impl<T: Seek + Read> Read for BufReaderWithPos<T> {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        let n = self.buf_reader.read(buf)?;
        self.pos += n;
        Ok(n)
    }
}

struct BufWriterWithPos<T: Seek + Write> {
    buf_writer: BufWriter<T>,
    pos: usize,
}

impl<T: Seek + Write> BufWriterWithPos<T> {
    fn new(inner: T) -> Self {
        let pos = inner.seek(SeekFrom::Current(0));
        Self {
            buf_writer: BufWriter::new(inner),
            pos,
        }
    }
}

impl<T: Seek + Write> Write for BufWriterWithPos<T> {
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        let n = self.buf_writer.write(buf)?;
        self.pos += n;
        Ok(n)
    }

    fn flush(&mut self) -> Result<()> {
        self.buf_writer.flush()
    }
}

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
    path: PathBuf, // file name
    reader: BufReaderWithPos,
    writer: BufWriterWithPos,
}

impl KvStore {
    /// Open the KvStore at a given path.
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let path: PathBuf = path.into();
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
        // write log to file, store key->offset in index
        let command = Command::Set { key, value };
        serde_json::to_writer(self.writer, &command)?;
        self.writer.flush()?;
        //TODO

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
        // search k/v in index ,load form log file
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
        // write log to file, store key->offset in index
        let command = Command::Remove { key };
        serde_json::to_writer(self.writer, &command)?;
        self.writer.flush()?;
        panic!();
    }
}

#[derive(Serialize, Deserialize, Debug)]
enum Command {
    Set { key: String, value: String },
    Remove { key: String },
}
