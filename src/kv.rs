use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::File;
use std::io;
use std::io::{BufReader, BufWriter};
use std::io::{Read, Write};
use std::io::{Seek, SeekFrom};
use std::path::PathBuf;
use std::result;

pub type Result<T> = anyhow::Result<T>;

struct BufReaderWithPos<T: Seek + Read> {
    buf_reader: BufReader<T>,
    pos: u64,
}

impl<T: Seek + Read> BufReaderWithPos<T> {
    fn new(mut inner: T) -> Self {
        let pos = inner.seek(SeekFrom::Current(0)).unwrap(); // Initial cursor
        Self {
            buf_reader: BufReader::new(inner),
            pos,
        }
    }
}

impl<T: Seek + Read> Read for BufReaderWithPos<T> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let n = self.buf_reader.read(buf)?;
        self.pos += n as u64;
        result::Result::Ok(n)
    }
}

impl<T: Seek + Read> Seek for BufReaderWithPos<T> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        match pos {
            SeekFrom::Start(n) => {
                self.pos = n;
                result::Result::Ok(n)
            }
            _ => panic!(),
        }
    }
}

struct BufWriterWithPos<T: Seek + Write> {
    buf_writer: BufWriter<T>,
    pos: u64,
}

impl<T: Seek + Write> BufWriterWithPos<T> {
    fn new(mut inner: T) -> Self {
        let pos = inner.seek(SeekFrom::Current(0)).unwrap();
        Self {
            buf_writer: BufWriter::new(inner),
            pos,
        }
    }
}

impl<T: Seek + Write> Write for BufWriterWithPos<T> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let n = self.buf_writer.write(buf)?;
        self.pos += n as u64;
        result::Result::Ok(n)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.buf_writer.flush()
    }
}

#[derive(Serialize, Deserialize, Debug)]
enum Command {
    Set { key: String, value: String },
    Remove { key: String },
}

// Command position in data file
// used in index
struct CommandPos {
    file_id: u64,
    pos: u64,
    len: u64,
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
    index: HashMap<String, CommandPos>, // A map of keys to log pointers
    path: PathBuf,                      // KvStore dir
    readers: HashMap<u64, BufReaderWithPos<File>>, // Map: file_id -> reader
    writer: BufWriterWithPos<File>,     // Writer of active data file
    cur_file_id: u64,                   // Active data file
}

impl KvStore {
    /// Open the KvStore at a given path.
    pub fn open(path: impl Into<PathBuf>) -> anyhow::Result<KvStore> {
        let path: PathBuf = path.into();
        // if path exists, open kvstore, then rebuild index
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
    pub fn set(&mut self, key: String, value: String) -> anyhow::Result<()> {
        let old_writer_pos = self.writer.pos;

        // Write log to file, store key->offset in index
        let command = Command::Set { key, value };
        serde_json::to_writer(&mut self.writer, &command)?;
        self.writer.flush()?;

        // Insert new entry in index
        if let Command::Set { key, .. } = command {
            self.index.insert(
                key,
                CommandPos {
                    file_id: self.cur_file_id,
                    pos: old_writer_pos,
                    len: self.writer.pos - old_writer_pos,
                },
            );
        }

        anyhow::Ok(())
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
    pub fn get(&mut self, key: String) -> anyhow::Result<Option<String>> {
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
    pub fn remove(&mut self, key: String) -> anyhow::Result<()> {
        if !self.index.contains_key(&key) {
            println!("Key not found");
            std::process::exit(1);
        } else {
            let old_writer_pos = self.writer.pos;

            // Write log to file, store key->offset in index
            let command = Command::Remove { key };
            serde_json::to_writer(&mut self.writer, &command)?;
            self.writer.flush()?;

            // Insert new entry in index
            if let Command::Set { key, .. } = command {
                self.index.insert(
                    key,
                    CommandPos {
                        file_id: self.cur_file_id,
                        pos: old_writer_pos,
                        len: self.writer.pos - old_writer_pos,
                    },
                );
            }

            anyhow::Ok(())
        }
    }
}
