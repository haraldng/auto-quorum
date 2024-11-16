use metronome::common::kv::KVCommand;
use std::collections::HashMap;

pub struct Database {
    db: HashMap<usize, usize>,
}

impl Database {
    pub fn new() -> Self {
        Self {
            db: HashMap::with_capacity(1_000_000),
        }
    }

    pub fn handle_command(&mut self, command: KVCommand) -> Option<Option<usize>> {
        match command {
            KVCommand::Put(key, value) => {
                self.db.insert(key, value);
                None
            }
            KVCommand::Delete(key) => {
                self.db.remove(&key);
                None
            }
            KVCommand::Get(key) => Some(self.db.get(&key).map(|v| *v)),
        }
    }
}
