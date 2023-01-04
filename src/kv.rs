use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::ffi::OsStr;
use std::fs::{self, create_dir_all, File};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::result;

struct BufReaderWithPos<T: Seek + Read> {
    buf_reader: BufReader<T>,
    pos: u64, // TODO: necessary?
}

impl<T: Seek + Read> BufReaderWithPos<T> {
    fn new(mut inner: T) -> Self {
        let pos = inner.seek(SeekFrom::Current(0)).unwrap();
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
        let n = self.buf_reader.seek(pos)?;
        result::Result::Ok(n)
    }
}

struct BufWriterWithPos<T: Seek + Write> {
    buf_writer: BufWriter<T>,
    pos: u64,
}

impl<T: Seek + Write> BufWriterWithPos<T> {
    fn new(mut inner: T) -> Self {
        let pos = inner.seek(SeekFrom::Current(0)).unwrap(); // Initial cursor
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

// Command position in data file, which is used in index.
struct CommandPos {
    file_id: u64,
    pos: u64,
}

/// Returns sorted file_ids in the given directory.
fn sorted_file_list<P: AsRef<Path>>(path: P) -> Result<Vec<u64>> {
    let mut file_list: Vec<u64> = fs::read_dir(&path)?
        .flat_map(|res| -> Result<_> { Ok(res?.path()) })
        .filter(|path| path.is_file() && path.extension() == Some("log".as_ref()))
        .flat_map(|path| {
            path.file_name()
                .and_then(OsStr::to_str)
                .map(|s| s.trim_end_matches(".log"))
                .map(str::parse::<u64>)
        })
        .flatten()
        .collect();
    file_list.sort_unstable();
    Ok(file_list)
}

/// PathBuf = path + file_id.log
fn log_path<P: AsRef<Path>>(path: P, file_id: u64) -> PathBuf {
    path.as_ref().join(format!("{}.log", file_id))
}

/// Create a new data file with given file_id and add the reader to the readers map.
///
/// Returns the writer to the log.
fn new_data_file<P: AsRef<Path>>(
    path: P,
    file_id: u64,
    readers: &mut HashMap<u64, BufReaderWithPos<File>>,
) -> Result<BufWriterWithPos<File>> {
    let path = log_path(&path, file_id);
    let writer = BufWriterWithPos::new(
        File::options()
            .create(true)
            .write(true)
            .append(true)
            .open(&path)?,
    );
    readers.insert(file_id, BufReaderWithPos::new(File::open(&path)?));
    Ok(writer)
}

/// Rebuild index.
///
/// Load all data files and store key/command position pairs in the index.
fn load_index(
    file_id: u64,
    reader: &mut BufReaderWithPos<File>,
    index: &mut HashMap<String, CommandPos>,
) -> Result<()> {
    let mut pos = reader.seek(SeekFrom::Start(0))?;
    let mut stream = serde_json::Deserializer::from_reader(reader).into_iter::<Command>();

    while let Some(cmd) = stream.next() {
        let new_pos = stream.byte_offset() as u64;
        match cmd? {
            Command::Set { key, .. } => {
                index.insert(key, CommandPos { file_id, pos });
            }
            Command::Remove { key } => {
                index.remove(&key);
            }
        }
        pos = new_pos;
    }
    Ok(())
}

/// The `KvStore` stores string key/value pairs on disk.
///
/// Key/value pairs are persisted to disk in data files.
///
/// It uses a simplification of the storage algorithm used by bitcask.
///
/// # Example
///
/// ```rust
/// use kvs::KvStore;
/// use std::env::current_dir;
///
/// let mut store = KvStore::open(current_dir().unwrap()).unwrap();
/// store.set("key".to_string(), "value".to_string()).unwrap();
/// let val = store.get("key".to_string()).unwrap();
/// assert_eq!(val, Some("value".to_string()));
/// store.remove("key".to_string()).unwrap();
/// ```
pub struct KvStore {
    index: HashMap<String, CommandPos>, // A map of keys to log pointers
    readers: HashMap<u64, BufReaderWithPos<File>>, // A map of file_id to reader
    writer: BufWriterWithPos<File>,     // Writer of active data file
    active_file_id: u64,                // Active data file
}

impl KvStore {
    /// Open the KvStore at a given path.
    ///
    /// # Errors
    ///
    /// It propagates I/O or serialization errors during writing the log.
    ///
    /// # Example
    ///
    /// ```rust
    /// use kvs::KvStore;
    /// use std::env::current_dir;
    ///
    /// let mut store = KvStore::open(current_dir().unwrap()).unwrap();
    /// ```
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let path: PathBuf = path.into();
        create_dir_all(&path).unwrap();

        let mut readers = HashMap::new();
        let mut index = HashMap::new();

        let file_list = sorted_file_list(&path)?;

        for &file_id in &file_list {
            let mut reader = BufReaderWithPos::new(File::open(log_path(&path, file_id))?);
            // rebuild index
            load_index(file_id, &mut reader, &mut index).unwrap();

            readers.insert(file_id, reader);
        }

        // Create new log file(active data file) and its writer
        let active_file_id = (file_list.len() + 1) as u64;
        let writer = new_data_file(&path, active_file_id, &mut readers)?;

        Ok(KvStore {
            index,
            readers,
            writer,
            active_file_id,
        })
    }

    /// Inserts a key-value pair into the kvstore.
    ///
    /// If the map did have this key present, the value will be updated.
    ///
    /// # Errors
    ///
    /// It propagates I/O or serialization errors during writing the log.
    ///
    /// # Example
    ///
    /// ```rust
    /// use kvs::KvStore;
    /// use std::env::current_dir;
    ///
    /// let mut store = KvStore::open(current_dir().unwrap()).unwrap();
    /// store.set("key".to_string(), "value".to_string()).unwrap();
    /// ```
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let entry_pos = self.writer.pos;

        // Write log to file, store key->offset in index
        let command = Command::Set { key, value };
        serde_json::to_writer(&mut self.writer, &command)?;
        self.writer.flush()?;

        // Insert new entry in index
        if let Command::Set { key, .. } = command {
            self.index.insert(
                key,
                CommandPos {
                    file_id: self.active_file_id,
                    pos: entry_pos,
                },
            );
        }

        Ok(())
    }

    /// Get the string value of a given string key.
    ///
    /// Returns `OK(None)` if the given key does not exist.
    ///
    /// # Errors
    ///
    /// It returns `KvsError::UnexpectedCommand` if the given command type unexpected.
    ///
    /// It propagates I/O or serialization errors during writing the log.
    ///
    /// # Example
    ///
    /// ```rust
    /// use kvs::KvStore;
    /// use std::env::current_dir;
    ///
    /// let mut store = KvStore::open(current_dir().unwrap()).unwrap();
    /// store.set("key".to_string(), "value".to_string()).unwrap();
    /// let val = store.get("key".to_string()).unwrap();
    /// assert_eq!(val, Some("value".to_string()));
    /// ```
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        // Find given key in index, and load command from data file
        if let Some(CommandPos { file_id, pos }) = self.index.get(&key) {
            let reader = self.readers.get_mut(&file_id).unwrap();
            reader.seek(SeekFrom::Start(*pos))?;
            let mut a = serde_json::Deserializer::from_reader(reader);
            let cmd = Command::deserialize(&mut a)?;
            if let Command::Set { value, .. } = cmd {
                Ok(Some(value))
            } else {
                Err(Error::UnexpectedCommand)
            }
        } else {
            Ok(None)
        }
    }

    /// Remove a given key.
    ///
    ///  # Errors
    ///
    /// It returns `KvsError::KeyNotFound` if the given key is not found.
    ///
    /// It propagates I/O or serialization errors during writing the log.
    ///
    /// # Example
    ///
    /// ```rust
    /// use kvs::KvStore;
    /// use std::env::current_dir;
    ///
    /// let mut store = KvStore::open(current_dir().unwrap()).unwrap();
    /// store.set("key".to_string(), "value".to_string()).unwrap();
    /// store.remove("key".to_string()).unwrap();
    /// ```
    pub fn remove(&mut self, key: String) -> Result<()> {
        if !self.index.contains_key(&key) {
            Err(Error::KeyNotFound)
        } else {
            // Write log to file, and store key/command position pairs in index
            let command = Command::Remove { key };
            serde_json::to_writer(&mut self.writer, &command)?;
            self.writer.flush()?;

            if let Command::Remove { key } = command {
                // Remove key from index
                self.index.remove(&key).unwrap();
            }

            Ok(())
        }
    }
}
