use std::collections::HashMap;

pub struct KvStore {
    map: HashMap<String, String>,
}

impl KvStore {
    pub fn new() -> KvStore {
        KvStore{
            map: HashMap::new(),
        }
    }

    pub fn set(&mut self, key: String, value: String){
        panic!();
    }

    pub fn get(&self, key: String) -> Option<String>{
        panic!();
    }

    pub fn remove(&mut self, key: String){
        panic!();
    }
}