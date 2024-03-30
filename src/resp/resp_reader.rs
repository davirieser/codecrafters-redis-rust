use std::boxed::Box;
use std::marker::{Send, Unpin};
use std::pin::Pin;
use std::future::Future;

use tokio::io::AsyncReadExt;

use anyhow::anyhow;

use thiserror::Error;

use crate::{AsyncReader, RespValue, RespDataType};

#[derive(Error, Debug, PartialEq)]
pub enum RespReaderError {
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

pub struct RespReader<T>
where
    T: AsyncReadExt + Unpin + Send,
{
    buffer: AsyncReader<T>,
}

impl<T> RespReader<T>
where
    T: AsyncReadExt + Unpin + Send,
{
    pub fn new(buffer: AsyncReader<T>) -> Self {
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
    fn next_boxed(&mut self) -> Pin<Box<dyn Future<Output = anyhow::Result<RespValue>> + Send + '_>> {
        Box::pin(async move { self.next().await })
    }
    pub async fn next(&mut self) -> anyhow::Result<RespValue> {
        let first_byte = self
            .buffer
            .next()
            .await
            .ok_or(RespReaderError::BufferFinished)?;

        match RespDataType::try_from(first_byte) {
            Ok(RespDataType::Null) => {
                if !self.buffer.assert_newline().await {
                    Err(RespReaderError::MissingNewline)?
                } else {
                    Ok(RespValue::Null)
                }
            }
            Ok(RespDataType::SimpleString) => match self.buffer.next_line().await {
                Some(s) => match String::from_utf8(s) {
                    Ok(string) => Ok(RespValue::SimpleString(string)),
                    Err(_) => Err(RespReaderError::NonUtf8String)?,
                },
                None => Err(RespReaderError::MissingNewline)?,
            },
            Ok(RespDataType::BulkString) => {
                let num_elements = self.parse_length().await?;
                match self.buffer.take(num_elements).await {
                    Some(bytes) => {
                        if !self.buffer.assert_newline().await {
                            Err(RespReaderError::MissingNewline)?
                        } else {
                            Ok(RespValue::BulkString(String::from_utf8(bytes)?))
                        }
                    }
                    None => Err(RespReaderError::BufferFinished)?,
                }
            }
            Ok(RespDataType::Array) => {
                let num_elements = self.parse_length().await?;
                let mut values = Vec::with_capacity(num_elements);

                for _ in (0..num_elements) {
                    values.push(self.next_boxed().await?);
                }

                Ok(RespValue::Array(values))
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

