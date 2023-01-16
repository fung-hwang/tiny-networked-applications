use crate::{KvsEngine, Result};
use redb::{Database, Error, ReadableTable, TableDefinition};

const TABLE: TableDefinition<&str, &str> = TableDefinition::new("table_1");

pub struct Redb {
    db: Database,
}

impl KvsEngine for Redb {
    // The final component of path is redb file(not dir), which is different from KvStore(KvStore is a dir)
    fn open(path: impl AsRef<std::path::Path>) -> Result<Self> {
        if let Some(parent_of_path) = path.as_ref().parent() {
            std::fs::create_dir_all(parent_of_path)?;
        }

        let db = Database::create(path)?;
        Ok(Redb { db })
    }

    fn set(&mut self, key: String, value: String) -> Result<()> {
        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(TABLE)?;
            table.insert(&key, &value)?;
        }
        write_txn.commit()?;

        Ok(())
    }

    fn get(&mut self, key: String) -> Result<Option<String>> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(TABLE)?;

        // need a local var: rst
        let rst = table
            .get(&key)?
            .map_or_else(|| Ok(None), |value| Ok(Some(value.value().to_string())));
        rst
    }

    fn remove(&mut self, key: String) -> Result<()> {
        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(TABLE)?;
            table.remove(&key)?;
        }
        write_txn.commit()?;

        Ok(())
    }
}

// TODO: unit test -> doc test
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn it_works() {
        let mut redb = Redb::open("tmp").unwrap();
        redb.set("key1".to_owned(), "value1".to_owned()).unwrap();
        println!("{:?}", redb.get("key1".to_owned()).unwrap());
        redb.remove("key1".to_owned()).unwrap();
        assert_eq!(2 + 2, 4);
    }
}
