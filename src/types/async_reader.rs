use std::marker::{Send, Unpin};
use std::ops::Drop;

use bytes::{Buf, BytesMut};

use tokio::io::AsyncReadExt;

pub struct AsyncReader<T>
where
    T: AsyncReadExt,
{
    stream: T,
    buffer: BytesMut,
}

pub struct Checkpoint<'a, T>
where
    T: AsyncReadExt,
{
    initial: Option<BytesMut>,
    reader: &'a mut AsyncReader<T>,
}

impl<'a, T> Checkpoint<'a, T>
where
    T: AsyncReadExt + Unpin,
{
    pub fn new(reader: &'a mut AsyncReader<T>) -> Self {
        let initial = Some(reader.buffer.clone());
        Self {
            reader,
            initial,
        }
    }
    pub async fn next(&mut self) -> Option<u8> {
        self.reader.next().await
    }
    pub async fn assert_newline(&mut self) -> bool {
        self.reader.assert_newline().await
    }
    pub async fn next_line(&mut self) -> Option<Vec<u8>> {
        self.reader.next_line().await
    }
    pub async fn take(&mut self, n: usize) -> Option<Vec<u8>> {
        self.reader.take(n).await
    }
}

impl<'a, T> Drop for Checkpoint<'a, T>
where
    T: AsyncReadExt,
{
    fn drop(&mut self) {
        match self.initial.take() {
            Some(initial) => self.reader.buffer = initial,
            None => {}
        }
    }
}

impl<T> AsyncReader<T>
where
    T: AsyncReadExt + Unpin,
{
    pub fn new(stream: T) -> Self {
        Self {
            stream,
            buffer: BytesMut::new(),
        }
    }
    pub async fn checkpoint<'a>(&'a mut self) -> Checkpoint<'a, T> {
        Checkpoint::new(self)
    }
    async fn fill_buf(&mut self) -> bool {
        if !self.buffer.has_remaining() {
            self.buffer.clear();
        }
        match self.stream.read_buf(&mut self.buffer).await {
            Ok(0) => false,
            Ok(_) => true,
            _ => false,
        }
    }
    pub async fn next(&mut self) -> Option<u8> {
        if self.buffer.has_remaining() || self.fill_buf().await {
            Some(self.buffer.get_u8())
        } else {
            None
        }
    }
    pub async fn assert_newline(&mut self) -> bool {
        Some(b'\r') == self.next().await && Some(b'\n') == self.next().await
    }
    pub async fn next_line(&mut self) -> Option<Vec<u8>> {
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
    pub async fn take(&mut self, n: usize) -> Option<Vec<u8>> {
        let mut copied_bytes = 0;
        let mut bytes = Vec::with_capacity(n);

        loop {
            let available = self.buffer.remaining();
            let to_copy = std::cmp::min(available, n - copied_bytes);

            let slice = &self.buffer[0..to_copy];
            bytes.copy_from_slice(slice);
            copied_bytes += to_copy;

            if copied_bytes == n {
                return Some(bytes);
            }
            if !self.fill_buf().await {
                return None;
            }
        }
    }
}

