use std::collections::{HashMap, HashSet};
use std::time::Instant;

pub enum DatabaseValue {
    Null,
    Boolean(bool),
    Integer(i64),
    Double(f64),
    String(String),
    Array(Vec<DatabaseValue>),
    Error(String),
    Set(HashSet<DatabaseValue>),
    Map(HashMap<DatabaseValue, DatabaseValue>),
}

pub enum DatabaseSlot {
    Simple(DatabaseValue),
    Timed {
        expires: Instant,
        value: DatabaseValue,
    },
}

pub struct Database {
    values: HashMap<String, DatabaseSlot>,
}
