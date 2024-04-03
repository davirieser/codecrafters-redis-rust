#![allow(unused)]
#![warn(unused_must_use)]

use std::collections::{HashMap, HashSet};
use std::pin::Pin;
use std::sync::Arc;

use bytes::BytesMut;

use anyhow::anyhow;

use thiserror::Error;

use tokio::io::{AsyncWriteExt, AsyncReadExt};
use tokio::net::{TcpListener, TcpStream};

use nom::{bytes::streaming::*, IResult};

mod config;
use config::Config;

mod types;
use types::AsyncReader;

mod resp;
use resp::{RespDataType, RespReader, RespReaderError, RespValue, RespWriter};
use crate::resp::{parse_resp_value, ParseError};

mod db;
use db::Database;

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

    let (mut read_half, mut write_half) = stream.split();
    let mut buffer = BytesMut::new();

    loop {
        match read_half.read_buf(&mut buffer).await {
            Ok(_) => {}
            _ => break,
        }
        let mut input = buffer.as_ref();
        loop {
            if input.len() == 0 { break; }
            let value;
            (input, value) = match parse_resp_value(input) {
                Ok(x) => x,
                Err(nom::Err::Error(ParseError::Nom(nom::Err::Incomplete(_)))) => break,
                Err(nom::Err::Failure(ParseError::Nom(nom::Err::Incomplete(_)))) => break,
                Err(e) => return Err(anyhow!("{}", e)),
            };
            println!("Got value: {value:?}");

            let response = RespValue::Array(vec![]);
            let msg = format!("{}", response);
            let _ = write_half.write(msg.as_bytes()).await;

            /*
            match value {
                RespValue::Array(arr) => {
                    // TODO
                }
                value => {
                    let error = RespValue::SimpleError("ERR command has to be Array".into());
                    // let _ = resp_writer.write(error).await;
                    break;
                }
                _ => {
                    println!("Connection closed");
                    break;
                }
                Err(e) => {
                    let error = RespValue::SimpleError(e.to_string().into());
                    // let _ = resp_writer.write(error).await;
                    break;
                }
            }
            */
        }
        buffer = BytesMut::from(input);
    }

    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = Arc::new(Config {});
    let listener = TcpListener::bind("127.0.0.1:6379").await?;

    loop {
        // TODO: Add Graceful shutdown

        let (stream, addr) = listener.accept().await?;

        println!("New Connection from {}", addr);

        let config_ref = config.clone();
        match handle_connection(stream, config_ref, vec![]).await {
            Ok(()) => {}
            Err(e) => eprintln!("Shutdown with Error: {:?}", e),
        }
    }

    Ok(())
}
