#![allow(unused)]

use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fmt::Display;
use std::future::Future;
use std::marker::{Send, Unpin};
use std::pin::Pin;
use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use bytes::{Buf, BytesMut};

use anyhow::anyhow;

use thiserror::Error;

struct Config {}

#[derive(PartialEq, Eq, Debug, Clone, Copy, Hash)]
enum DataType {
    SimpleString,
    SimpleError,
    Integer,
    BulkString,
    Array,
    Null,
    Boolean,
    Double,
    BigNumber,
    BulkError,
    VerbatimString,
    Map,
    Set,
    Push,
}

#[derive(Debug)]
enum Value {
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
    Array(Vec<Value>),
    Map(HashMap<Value, Value>),
    Set(HashSet<Value>),
    Push(Vec<Value>),
}

impl PartialEq for Value {
    fn eq(&self, other: &Value) -> bool {
        match (self, other) {
            (Value::Null, Value::Null) => true,
            (Value::Boolean(b1), Value::Boolean(b2)) => b1 == b2,
            (Value::Integer(i1), Value::Integer(i2)) => i1 == i2,
            (Value::Double(d1), Value::Double(d2)) => d1 == d2,
            (Value::BigNumber(n1), Value::BigNumber(n2)) => n1 == n2,
            (Value::SimpleString(s1), Value::SimpleString(s2)) => s1 == s2,
            (Value::SimpleError(e1), Value::SimpleError(e2)) => e1 == e2,
            (Value::BulkString(s1), Value::BulkString(s2)) => s1 == s2,
            _ => false,
        }
    }
}

impl Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Null => write!(f, "_\r\n"),
            Value::Boolean(b) if *b => write!(f, "#t\r\n"),
            Value::Boolean(b) if !*b => write!(f, "#f\r\n"),
            Value::Integer(i) => write!(f, ":{i}\r\n"),
            Value::Double(d) => write!(f, ",{d:?}\r\n"),
            Value::BigNumber(i) => write!(f, "({i}\r\n"),
            Value::SimpleString(s) => write!(f, "+{s}\r\n"),
            Value::SimpleError(e) => write!(f, "-{e}\r\n"),
            Value::BulkString(s) => write!(f, "${}\r\n{s}\r\n", s.len()),
            Value::BulkError(s) => write!(f, "!{}\r\n{s}\r\n", s.len()),
            // TODO: Will Encoding be rendered correctly here?
            Value::VerbatimString((enc, s)) => {
                write!(f, "={}\r\n{}:{s}\r\n", 3 + 1 + s.len(), unsafe {
                    std::str::from_utf8_unchecked(enc)
                })
            }
            Value::Array(arr) => {
                write!(f, "*{}\r\n", arr.len())?;
                for e in arr {
                    write!(f, "{}", e)?;
                }
                Ok(())
            }
            Value::Set(set) => {
                write!(f, "~{}\r\n", set.len())?;
                for e in set {
                    write!(f, "{}", e)?;
                }
                Ok(())
            }
            Value::Map(map) => {
                write!(f, "%{}\r\n", map.len())?;
                for (k, v) in map {
                    write!(f, "{}{}", k, v)?;
                }
                Ok(())
            }
            Value::Push(arr) => {
                write!(f, ">{}\r\n", arr.len())?;
                for e in arr {
                    write!(f, "{}", e)?;
                }
                Ok(())
            }
            _ => write!(f, "-ERR unimplemented\r\n"),
        }
    }
}

impl TryFrom<u8> for DataType {
    type Error = ();

    fn try_from(b: u8) -> Result<Self, Self::Error> {
        DataType::try_from(char::from(b))
    }
}

impl TryFrom<char> for DataType {
    type Error = ();

    fn try_from(c: char) -> Result<Self, Self::Error> {
        match c {
            '+' => Ok(DataType::SimpleString),
            '-' => Ok(DataType::SimpleError),
            ':' => Ok(DataType::Integer),
            '$' => Ok(DataType::BulkString),
            '*' => Ok(DataType::Array),
            '_' => Ok(DataType::Null),
            '#' => Ok(DataType::Boolean),
            ',' => Ok(DataType::Double),
            '(' => Ok(DataType::BigNumber),
            '!' => Ok(DataType::BulkError),
            '=' => Ok(DataType::VerbatimString),
            '%' => Ok(DataType::Map),
            '~' => Ok(DataType::Set),
            '>' => Ok(DataType::Push),
            _ => Err(()),
        }
    }
}

#[derive(Error, Debug, PartialEq)]
enum RespReaderError {
    #[error("unimplemented")]
    Unimplemented,
    #[error("buffer finished")]
    BufferFinished,
    #[error("missing newline")]
    MissingNewline,
    #[error("non utf8 string")]
    NonUtf8String,
    #[error("unknown data type {0}")]
    UnknownDataType(char),
    #[error("aggregate Errors")]
    Aggregate { errors: Vec<RespReaderError> },
}

struct AsyncReader<T>
where
    T: AsyncReadExt + Unpin + Send,
{
    stream: T,
    buffer: BytesMut,
}

impl<T> AsyncReader<T>
where
    T: AsyncReadExt + Unpin + Send,
{
    fn new(stream: T, buffer: BytesMut) -> Self {
        Self { stream, buffer }
    }
    async fn fill_buf(&mut self) -> bool {
        // TODO: Check if this writes into the existing memory or allocates more.
        // TODO: This allocates more => Replace Buffer Type
        match self.stream.read_buf(&mut self.buffer).await {
            Ok(0) => false,
            Ok(_) => true,
            _ => false,
        }
    }
    async fn next_line(&mut self) -> Option<Vec<u8>> {
        let mut bytes = Vec::new();

        loop {
            match self.next().await {
                Some(b'\r') => break,
                Some(b) => bytes.push(b),
                None => return None,
            }
        }
        if Some(b'\n') == self.next().await {
            Some(bytes)
        } else {
            None
        }
    }
    async fn assert_newline(&mut self) -> bool {
        Some(b'\r') == self.next().await && Some(b'\n') == self.next().await
    }
    async fn next(&mut self) -> Option<u8> {
        if self.buffer.has_remaining() || self.fill_buf().await {
            Some(self.buffer.get_u8())
        } else {
            None
        }
    }
    async fn take(&mut self, n: usize) -> Option<Vec<u8>> {
        // NOTE: If the length of a Vec is 0 then '&mut vec[...]' will return a slice with length 0.
        //       Because of that the length of the Vector has to be set manually here.
        //       This way avoids having to fill the vector using e.g. 'vec![0; n]'.
        // SAFETY: The Values stored in the Vec will not be read before they are filled by
        //         'self.buffer.copy_to_slice(...)'. If the value is dropped because not
        //         enough bytes could be filled then the length value is set correctly before
        //         dropping.
        #[allow(clippy::uninit_vec)]
        let mut bytes = unsafe {
            let mut bytes = Vec::with_capacity(n);
            bytes.set_len(n);
            bytes
        };
        let mut copied_bytes = 0;

        loop {
            let available = self.buffer.remaining();
            let to_copy = std::cmp::min(available, n - copied_bytes);

            let slice = &mut bytes[copied_bytes..(copied_bytes + to_copy)];
            self.buffer.copy_to_slice(slice);
            copied_bytes += to_copy;

            if copied_bytes == n {
                // NOTE: Here 'bytes' will have every byte initialised.
                return Some(bytes);
            }

            if !self.fill_buf().await {
                // NOTE: Make sure that when 'bytes' is dropped here, that the length is correct.
                unsafe {
                    bytes.set_len(copied_bytes);
                }
                return None;
            }
        }
    }
}

struct RespReader<T>
where
    T: AsyncReadExt + std::marker::Unpin + Send,
{
    buffer: AsyncReader<T>,
}

impl<T> RespReader<T>
where
    T: AsyncReadExt + std::marker::Unpin + Send,
{
    fn new(buffer: AsyncReader<T>) -> Self {
        Self { buffer }
    }
    /// Parses an unsigned, base-10 length value that has to end with a CRLF.
    ///
    /// See [`Redis Bulk String`] as an Example where length is used.
    ///
    /// [`Redis Bulk String`]: https://redis.io/docs/reference/protocol-spec/#bulk-strings
    ///
    /// # Errors
    ///
    /// Will return [`Err`] if length value overflows, a non-digit character is encountered,
    /// the end of the stream is reached before "\r\n" or the '\n' is missing after '\r'.
    ///
    /// [`Err`]: anyhow::Result::Err
    ///
    /// # Examples
    ///
    /// ```
    /// let reader = new RespReader("123\r\n");
    /// assert_eq!(reader.parse_length(), Ok(123));
    /// ```
    ///
    /// ```
    /// let reader = new RespReader("123\r");
    /// assert!(reader.parse_length().is_err());
    /// ```
    ///
    /// ```
    /// let reader = new RespReader("123");
    /// assert!(reader.parse_length().is_err());
    /// ```
    async fn parse_length(&mut self) -> anyhow::Result<usize> {
        // NOTE: https://redis.io/docs/reference/protocol-spec/#high-performance-parser-for-the-redis-protocol

        let mut len = 0;
        let length_overflow_error = || anyhow!("Length Overflowed!");

        loop {
            match self.buffer.next().await {
                Some(b'\r') => break,
                Some(b @ b'0'..=b'9') => {
                    let shifted = usize::checked_mul(len, 10).ok_or_else(length_overflow_error)?;
                    let digit = usize::from(b - b'0');
                    len = usize::checked_add(shifted, digit).ok_or_else(length_overflow_error)?
                }
                Some(b) => return Err(anyhow!("Invalid character in length: {}", char::from(b))),
                _ => return Err(anyhow!("Stream ended while reading length!")),
            }
        }

        if Some(b'\n') == self.buffer.next().await {
            Ok(len)
        } else {
            Err(anyhow!("Missing newline after length!"))
        }
    }
    fn next_boxed(&mut self) -> Pin<Box<dyn Future<Output = anyhow::Result<Value>> + Send + '_>> {
        Box::pin(async move { self.next().await })
    }
    async fn next(&mut self) -> anyhow::Result<Value> {
        let first_byte = self
            .buffer
            .next()
            .await
            .ok_or(RespReaderError::BufferFinished)?;

        match DataType::try_from(first_byte) {
            Ok(DataType::Null) => {
                if !self.buffer.assert_newline().await {
                    Err(RespReaderError::MissingNewline)?
                } else {
                    Ok(Value::Null)
                }
            }
            Ok(DataType::SimpleString) => match self.buffer.next_line().await {
                Some(s) => match String::from_utf8(s) {
                    Ok(string) => Ok(Value::SimpleString(string)),
                    Err(_) => Err(RespReaderError::NonUtf8String)?,
                },
                None => Err(RespReaderError::MissingNewline)?,
            },
            Ok(DataType::BulkString) => {
                let num_elements = self.parse_length().await?;
                match self.buffer.take(num_elements).await {
                    Some(bytes) => {
                        if !self.buffer.assert_newline().await {
                            Err(RespReaderError::MissingNewline)?
                        } else {
                            Ok(Value::BulkString(String::from_utf8(bytes)?))
                        }
                    }
                    None => Err(RespReaderError::BufferFinished)?,
                }
            }
            Ok(DataType::Array) => {
                let num_elements = self.parse_length().await?;
                let mut values = Vec::with_capacity(num_elements);

                for _ in (0..num_elements) {
                    values.push(self.next_boxed().await?);
                }

                Ok(Value::Array(values))
            }
            Ok(_) => {
                let _ = self.buffer.next_line().await;
                Err(RespReaderError::Unimplemented)?
            }
            _ => {
                let _ = self.buffer.next_line().await;
                Err(RespReaderError::UnknownDataType(char::from(first_byte)))?
            }
        }
    }
}

/*
struct Handler<T> {}

struct CommandGroup {
    name: String,
}

struct Command<T>
where
    T:,
{
    name: String,
}
*/

async fn handle_connection(mut stream: TcpStream, config: Arc<Config>) -> anyhow::Result<()> {
    const BUFFER_SIZE: usize = 8 * 1024;

    // NOTE: Wait for the Stream to be readable and writable
    let (readable, writable) = tokio::join!(stream.readable(), stream.writable());
    if readable.is_err() || writable.is_err() {
        return Err(anyhow!("ERROR: Stream could not be opened!"));
    }

    let (mut read_half, mut write_half) = stream.split();
    // TODO: Use different Buffer Type, this one is extended when read.
    let mut buffer = BytesMut::with_capacity(BUFFER_SIZE);
    let mut reader = AsyncReader::new(read_half, buffer);
    let mut resp_reader = RespReader::new(reader);

    let mut values: Vec<Value> = vec![];

    loop {
        let value = resp_reader.next().await;
        println!("Got Value: {value:?}");
        match value {
            Ok(Value::Array(arr)) => {
                if arr[0] == Value::BulkString("COMMAND".to_string()) {
                    let msg = format!("{}", Value::Array(vec![]));
                    print!("{:?}", msg);
                    let _ = write_half.write(msg.as_bytes()).await;
                } else if arr[0] == Value::BulkString("PING".to_string()) {
                    let msg = format!("{}", Value::BulkString("PONG".to_string()));
                    print!("{:?}", msg);
                    let _ = write_half.write(msg.as_bytes()).await;
                } else {
                    let msg = format!("{}", Value::SimpleError("unknown command".to_string()));
                    print!("{:?}", msg);
                    let _ = write_half.write(msg.as_bytes()).await;
                }
            }
            Ok(v) => {
                let msg = format!(
                    "{}",
                    Value::SimpleError("command has to be Array".to_string())
                );
                print!("{}", msg);
                let _ = write_half.write(msg.as_bytes()).await;
            }
            Err(e)
                if e.downcast_ref::<RespReaderError>()
                    == Some(&RespReaderError::BufferFinished) =>
            {
                println!("Connection closed");
                break;
            }
            Err(e) => {
                let msg = format!("{}", Value::SimpleError(format!("unknown error: {e}")));
                print!("{}", msg);
                let _ = write_half.write(msg.as_bytes()).await;
                break;
            }
        }
    }

    Ok(())
}

async fn handle_ping(stream: &mut TcpStream) -> anyhow::Result<usize> {
    let response = Value::SimpleString("PONG".to_string());
    // TODO: If Argument is provided send back Bulk String with Argument.
    Ok(stream.write(format!("{}", response).as_bytes()).await?)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let config = Arc::new(Config {});
    let listener = TcpListener::bind("127.0.0.1:6379").await?;

    loop {
        let (stream, addr) = listener.accept().await?;

        let config_ref = config.clone();
        tokio::spawn(async move {
            match handle_connection(stream, config_ref).await {
                Ok(()) => {}
                Err(e) => eprintln!("{:?}", e),
            }
        });
    }
}
