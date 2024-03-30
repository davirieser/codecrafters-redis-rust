use std::collections::{HashMap, HashSet};

use crate::RespDataType;

#[derive(Debug)]
pub enum RespValue {
    Null,
    Boolean(bool),
    Integer(i64),
    Double(f64),
    BigNumber(i128),
    SimpleString(String),
    BulkString(String),
    VerbatimString(([u8; 3], String)),
    SimpleError(String),
    BulkError(String),
    Array(Vec<RespValue>),
    Map(HashMap<RespValue, RespValue>),
    Set(HashSet<RespValue>),
    Push(Vec<RespValue>),
}

impl From<&RespValue> for RespDataType {
    fn from(v: &RespValue) -> Self {
        match v {
            RespValue::Null => RespDataType::Null,
            RespValue::Boolean(_) => RespDataType::Boolean,
            RespValue::Integer(_) => RespDataType::Integer,
            RespValue::Double(_) => RespDataType::Double,
            RespValue::BigNumber(_) => RespDataType::BigNumber,
            RespValue::SimpleString(_) => RespDataType::SimpleString,
            RespValue::SimpleError(_) => RespDataType::SimpleError,
            RespValue::BulkString(_) => RespDataType::BulkString,
            RespValue::BulkError(_) => RespDataType::BulkError,
            RespValue::VerbatimString(_) => RespDataType::VerbatimString,
            RespValue::Array(_) => RespDataType::Array,
            RespValue::Set(_) => RespDataType::Set,
            RespValue::Map(_) => RespDataType::Map,
            RespValue::Push(_) => RespDataType::Push,
        }
    }
}

impl PartialEq for RespValue {
    fn eq(&self, other: &RespValue) -> bool {
        match (self, other) {
            (RespValue::Null, RespValue::Null) => true,
            (RespValue::Boolean(b1), RespValue::Boolean(b2)) => b1 == b2,
            (RespValue::Integer(i1), RespValue::Integer(i2)) => i1 == i2,
            (RespValue::Double(d1), RespValue::Double(d2)) => d1 == d2,
            (RespValue::BigNumber(n1), RespValue::BigNumber(n2)) => n1 == n2,
            // TODO: Should different strings also be comparable?
            (RespValue::SimpleString(s1), RespValue::SimpleString(s2)) => s1 == s2,
            (RespValue::BulkString(s1), RespValue::BulkString(s2)) => s1 == s2,
            (RespValue::VerbatimString((e1, s1)), RespValue::VerbatimString((e2, s2))) => {
                e1 == e2 && s1 == s2
            }
            (RespValue::SimpleError(e1), RespValue::SimpleError(e2)) => e1 == e2,
            (RespValue::BulkError(e1), RespValue::BulkError(e2)) => e1 == e2,
            (RespValue::Array(arr1), RespValue::Array(arr2)) => {
                (arr1.len() == arr2.len()) && arr1.iter().zip(arr2.iter()).all(|(e1, e2)| e1 == e2)
            }
            // TODO: Implement Set and Map Equals
            _ => false,
        }
    }
}

impl std::fmt::Display for RespValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let first_byte = char::from(RespDataType::from(self));
        match self {
            RespValue::Null => write!(f, "{first_byte}\r\n"),
            RespValue::Boolean(b) => {
                let v = if *b { 't' } else { 'f' };
                write!(f, "{first_byte}{v}\r\n")
            }
            RespValue::Integer(i) => write!(f, "{first_byte}{i}\r\n"),
            RespValue::Double(d) => write!(f, "{first_byte}{d:?}\r\n"),
            RespValue::BigNumber(i) => write!(f, "{first_byte}{i}\r\n"),
            RespValue::SimpleString(s) | RespValue::SimpleError(s) => write!(f, "{first_byte}{s}\r\n"),
            RespValue::BulkString(s) | RespValue::BulkError(s) => {
                write!(f, "{first_byte}{}\r\n{s}\r\n", s.len())
            }
            // TODO: Will Encoding be rendered correctly here?
            RespValue::VerbatimString((enc, s)) => {
                write!(f, "{first_byte}{}\r\n{}:{s}\r\n", 3 + 1 + s.len(), unsafe {
                    std::str::from_utf8_unchecked(enc)
                })
            }
            RespValue::Array(arr) | RespValue::Push(arr) => {
                write!(f, "{first_byte}{}\r\n", arr.len())?;
                for e in arr {
                    write!(f, "{}", e)?;
                }
                Ok(())
            }
            RespValue::Set(set) => {
                write!(f, "{first_byte}{}\r\n", set.len())?;
                for e in set {
                    write!(f, "{}", e)?;
                }
                Ok(())
            }
            RespValue::Map(map) => {
                write!(f, "{first_byte}{}\r\n", map.len())?;
                for (k, v) in map {
                    write!(f, "{}{}", k, v)?;
                }
                Ok(())
            }
        }
    }
}
