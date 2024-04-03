use std::borrow::Cow;
use std::collections::{HashMap, HashSet};

use crate::RespDataType;

#[derive(Debug)]
pub enum RespValue<'a> {
    Null,
    Boolean(bool),
    Integer(i64),
    Double(f64),
    BigNumber(Cow<'a, str>),
    SimpleString(Cow<'a, str>),
    BulkString(Cow<'a, str>),
    VerbatimString((Cow<'a, str>, Cow<'a, str>)),
    SimpleError(Cow<'a, str>),
    BulkError(Cow<'a, str>),
    Array(Vec<RespValue<'a>>),
    Map(HashMap<RespValue<'a>, RespValue<'a>>),
    Set(HashSet<RespValue<'a>>),
    Push(Vec<RespValue<'a>>),
}

impl<'a> From<&RespValue<'a>> for RespDataType {
    fn from(v: &RespValue<'a>) -> Self {
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

impl<'a> Eq for RespValue<'a> {}

impl<'a> PartialEq for RespValue<'a> {
    fn eq(&self, other: &RespValue<'a>) -> bool {
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

impl<'a> std::hash::Hash for RespValue<'a> {
    fn hash<H>(&self, state: &mut H)
    where
        H: std::hash::Hasher,
    {
        match self {
            RespValue::Boolean(b) => b.hash(state),
            RespValue::Integer(i) => i.hash(state),
            RespValue::Double(d) => d.to_bits().hash(state),
            RespValue::BigNumber(n) => n.hash(state),
            RespValue::SimpleString(s) => s.hash(state),
            RespValue::BulkString(s) => s.hash(state),
            RespValue::VerbatimString(s) => s.hash(state),
            RespValue::SimpleError(e) => e.hash(state),
            RespValue::BulkError(e) => e.hash(state),
            RespValue::Array(vec) => Self::hash_slice(vec, state),
            // TODO: Implement Set and Map Equals
            _ => {}
        }
    }
}

impl<'a> std::fmt::Display for RespValue<'a> {
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
            RespValue::SimpleString(s) | RespValue::SimpleError(s) => {
                write!(f, "{first_byte}{s}\r\n")
            }
            RespValue::BulkString(s) | RespValue::BulkError(s) => {
                write!(f, "{first_byte}{}\r\n{s}\r\n", s.len())
            }
            RespValue::VerbatimString((enc, s)) => {
                write!(f, "{first_byte}{}\r\n{}:{s}\r\n", 3 + 1 + s.len(), enc)
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
