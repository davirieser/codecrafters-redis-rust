#![allow(unused)]
#![warn(unused_must_use)]

use std::collections::{HashMap, HashSet};
use std::pin::Pin;
use std::sync::Arc;

use anyhow::anyhow;

use thiserror::Error;

use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};

use nom::{bytes::streaming::*, IResult};

mod config;
use config::Config;

mod types;
use types::AsyncReader;

mod resp;
use resp::{RespDataType, RespReader, RespReaderError, RespValue, RespWriter};

mod db;
use db::Database;

pub enum CommandArgument {
    String(String),
    Integer(i64),
    Double(f64),
    List(Vec<CommandArgument>),
    Set(HashSet<CommandArgument>),
    Map(HashMap<String, CommandArgument>),
}

pub enum CommandArgumentType {
    Required(CommandArgument),
    Optional(CommandArgument),
    Multiple(String, Vec<CommandArgument>),
}

pub enum Command {
    Command,
    Echo(String),
    Ping(Option<String>),
}

#[derive(Error, Debug)]
pub enum CommandParseError {
    #[error("empty command name")]
    EmptyCommandName,
    #[error("invalid arguments")]
    InvalidArguments,
    #[error("wrong argument type")]
    WrongArgType,
    #[error("command does not exist")]
    CommandDoesNotExist,
    #[error("too many arguments")]
    TooManyArguments,
}

impl TryFrom<Vec<RespValue<'_>>> for Command {
    type Error = CommandParseError;

    fn try_from(values: Vec<RespValue>) -> Result<Self, Self::Error> {
        let num_args = values.len();
        if num_args < 1 {
            return Err(CommandParseError::EmptyCommandName);
        }
        match &values[0] {
            RespValue::BulkString(cmd) if cmd.eq_ignore_ascii_case("PING") => {
                if values.len() > 2 {
                    return Err(CommandParseError::TooManyArguments);
                }
                match values.get(1) {
                    None => Ok(Command::Ping(None)),
                    Some(RespValue::BulkString(string)) => {
                        Ok(Command::Ping(Some(string.to_string())))
                    }
                    Some(_) => Err(CommandParseError::WrongArgType),
                }
            }
            _ => todo!(),
        }
    }
}

async fn handle_connection(
    mut stream: TcpStream,
    config: Arc<Config>,
    commands: Vec<Command>,
) -> anyhow::Result<()> {
    // NOTE: Wait for the Stream to be readable and writable
    let (readable, writable) = tokio::join!(stream.readable(), stream.writable());
    if readable.is_err() || writable.is_err() {
        return Err(anyhow!("ERROR: Stream could not be opened!"));
    }

    let (read_half, write_half) = stream.split();
    let mut reader = AsyncReader::new(read_half);
    let mut resp_reader = RespReader::new(reader);
    let mut resp_writer = RespWriter::new(write_half);

    loop {
        let value = resp_reader.next().await;
        println!("Got value: {value:?}");
        match value {
            Ok(RespValue::Array(arr)) => {
                let arg_types = &arr[1..]
                    .iter()
                    .map(RespDataType::from)
                    .collect::<Vec<RespDataType>>();
            }
            Ok(value) => {
                let error = RespValue::SimpleError("ERR command has to be Array".into());
                let _ = resp_writer.write(error).await;
                break;
            }
            Err(RespReaderError::BufferFinished) => {
                println!("Connection closed");
                break;
            }
            Err(e) => {
                let error = RespValue::SimpleError(e.to_string().into());
                let _ = resp_writer.write(error).await;
                break;
            }
        }
    }

    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = Arc::new(Config {});
    let listener = TcpListener::bind("127.0.0.1:6379").await?;

    let mut jhs = vec![];
    loop {
        // TODO: Add Graceful shutdown

        let (stream, addr) = listener.accept().await?;

        println!("New Connection from {}", addr);

        let config_ref = config.clone();
        jhs.push(tokio::spawn(async move {
            match handle_connection(stream, config_ref, vec![]).await {
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
