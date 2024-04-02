use std::boxed::Box;
use std::future::Future;
use std::marker::{Send, Unpin};
use std::pin::Pin;

use tokio::io::AsyncReadExt;

use thiserror::Error;

use crate::{AsyncReader, RespDataType, RespValue};

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
    #[error("unknown data type: {0}")]
    UnknownDataType(char),
    #[error("length overflowed")]
    LengthOverflowed,
    #[error("invalid char in length: {0}")]
    InvalidCharInLength(char),
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
    async fn parse_length(&mut self) -> Result<usize, RespReaderError> {
        // NOTE: https://redis.io/docs/reference/protocol-spec/#high-performance-parser-for-the-redis-protocol
        let mut len = 0;

        loop {
            match self.buffer.next().await {
                Some(b'\r') => break,
                Some(b @ b'0'..=b'9') => {
                    let shifted =
                        usize::checked_mul(len, 10).ok_or(RespReaderError::LengthOverflowed)?;
                    let digit = usize::from(b - b'0');
                    len = usize::checked_add(shifted, digit)
                        .ok_or(RespReaderError::LengthOverflowed)?
                }
                Some(b) => return Err(RespReaderError::InvalidCharInLength(char::from(b))),
                _ => return Err(RespReaderError::BufferFinished),
            }
        }

        if Some(b'\n') == self.buffer.next().await {
            Ok(len)
        } else {
            Err(RespReaderError::MissingNewline)
        }
    }
    fn next_boxed(
        &mut self,
    ) -> Pin<Box<dyn Future<Output = Result<RespValue, RespReaderError>> + Send + '_>> {
        Box::pin(async move { self.next().await })
    }
    pub async fn next(&mut self) -> Result<RespValue, RespReaderError> {
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
                    Ok(string) => Ok(RespValue::SimpleString(string.into())),
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
                            let string = String::from_utf8(bytes)
                                .map_err(|_| RespReaderError::NonUtf8String)?;
                            Ok(RespValue::BulkString(string.into()))
                        }
                    }
                    None => Err(RespReaderError::BufferFinished)?,
                }
            }
            Ok(RespDataType::Array) => {
                let num_elements = self.parse_length().await?;
                //let mut values = Vec::with_capacity(num_elements);

                /*
                println!("Parsing Array: {}", num_elements);

                for _ in 0..num_elements {
                    values.push(self.next_boxed().await?);
                }

                Ok(RespValue::Array(values))
                */
                Ok(RespValue::Array(vec![]))
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
