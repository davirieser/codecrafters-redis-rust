#![allow(unused)]

use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fmt::Display;
use std::future::Future;
use std::marker::{Send, Unpin};
use std::ops::Deref;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use bytes::{Buf, BytesMut};

use anyhow::anyhow;

mod config;
use config::Config;

mod types;
use types::AsyncReader;

mod resp;
use resp::{RespDataType, RespReader, RespValue};

pub enum CommandType {
    Command,
    CommandGroup,
    Alias,
}

enum CommandArgType {
    Block,
    Multiple,
    MultipleToken,
    Required,
    Optional,
}

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
    let mut reader = AsyncReader::new(read_half);
    let mut resp_reader = RespReader::new(reader);

    /*
    loop {
        let value = resp_reader.next().await;
        println!("Got Value: {value:?}");
        match value {
            Ok(Value::Array(arr)) => {
                let arg_types = &arr[1..]
                    .iter()
                    .map(DataType::from)
                    .collect::<Vec<DataType>>();
                println!("Arg Types: {:?}", arg_types);
                // TODO: Call command handlers
            }
            Ok(v) => {
                let msg = format!(
                    "{}",
                    Value::SimpleError("ERR command has to be Array".to_string())
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
            // TODO: Differentiate fatal and non-fatal errors
            Err(e) => {
                let msg = format!("{}", Value::SimpleError(format!("ERR unknown error: {e}")));
                print!("{}", msg);
                let _ = write_half.write(msg.as_bytes()).await;
                break;
            }
        }
    }
    */

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let config = Arc::new(Config {});
    let listener = TcpListener::bind("127.0.0.1:6379").await?;

    let mut jhs = vec![];
    loop {
        let (stream, addr) = listener.accept().await?;

        let config_ref = config.clone();
        jhs.push(tokio::spawn(async move {
            match handle_connection(stream, config_ref).await {
                Ok(()) => {}
                Err(e) => eprintln!("{:?}", e),
            }
        }));
    }

    for jh in jhs {
        jh.await?;
    }

    Ok(())
}
