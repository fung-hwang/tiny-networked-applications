use anyhow::{anyhow, Ok};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::ffi::OsStr;
use std::fs::{self, create_dir_all, File};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::result;

pub type Result<T> = anyhow::Result<T>;

struct BufReaderWithPos<T: Seek + Read> {
    buf_reader: BufReader<T>,
    pos: u64, // TODO: necessary?
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
                // self.pos = n;
                self.buf_reader.seek(pos)?;
                result::Result::Ok(n)
            }
            _ => panic!(), // Todo
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
    // len: u64,
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
    readers: HashMap<u64, BufReaderWithPos<File>>, // Map: file_id -> reader
    writer: BufWriterWithPos<File>,     // Writer of active data file
    active_file_id: u64,                // Active data file
}

impl KvStore {
    /// Open the KvStore at a given path.
    pub fn open(path: impl Into<PathBuf>) -> anyhow::Result<KvStore> {
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

        // Create new log file, which to write
        let active_file_id = (file_list.len() + 1) as u64;
        let writer = new_log_file(&path, active_file_id, &mut readers)?;

        Ok(KvStore {
            index,
            readers,
            writer,
            active_file_id,
        })
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
                    // len: self.writer.pos - old_writer_pos,
                },
            );
        }

        Ok(())
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
        if let Some(CommandPos { file_id, pos }) = self.index.get(&key) {
            let reader = self.readers.get_mut(&file_id).unwrap();
            reader.seek(SeekFrom::Start(*pos))?;
            let mut a = serde_json::Deserializer::from_reader(reader);
            let cmd = Command::deserialize(&mut a)?;
            if let Command::Set { value, .. } = cmd {
                Ok(Some(value))
            } else {
                // TODO: rm
                Ok(None)
            }
        } else {
            Ok(None)
        }
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
        if !self.index.contains_key(&key) {
            Err(anyhow!("key not found"))
        } else {
            // Write log to file, store key->offset in index
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

// Returns sorted generation numbers in the given directory.
fn sorted_file_list(path: &Path) -> Result<Vec<u64>> {
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

fn log_path(path: &Path, file_id: u64) -> PathBuf {
    path.join(format!("{}.log", file_id))
}

/// Create a new log file with given generation number and add the reader to the readers map.
///
/// Returns the writer to the log.
fn new_log_file(
    path: &Path,
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
                index.insert(
                    key,
                    CommandPos {
                        file_id,
                        pos,
                        // len: new_pos - pos,
                    },
                );
            }
            Command::Remove { key } => {
                index.remove(&key);
            }
        }
        pos = new_pos;
    }
    Ok(())
}
