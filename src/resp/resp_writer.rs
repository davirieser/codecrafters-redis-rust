use std::marker::Unpin;

use tokio::io::AsyncWriteExt;

use crate::{RespValue};

// TODO: Use BytesMut as underlying Buffer, eliminating the allocation on each write?
//       What would happen if multiple 'write's are interleaved by different tasks.
pub struct RespWriter<T> 
where
    T: AsyncWriteExt + Unpin
{
    writer: T,
}

impl<T> RespWriter<T>
where
    T: AsyncWriteExt + Unpin
{
    pub fn new(writer: T) -> Self {
        Self { writer }
    }
    pub async fn write(&mut self, value: RespValue) -> anyhow::Result<()> {
        let msg = format!("{}", value);
        self.writer.write_all(msg.as_bytes()).await?;
        Ok(())
    }
}

