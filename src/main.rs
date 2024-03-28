use std::sync::Arc;
use std::error::Error;
use std::collections::{HashMap, HashSet};

use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use bytes::BytesMut;

use anyhow::anyhow;

struct Config {

}

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

enum Value<'a> {
    SimpleString(&'a str),
    SimpleError(&'a str),
    Integer(i64),
    BulkString(&'a str),
    Array(Vec<Value<'a>>),
    Null,
    Boolean(bool),
    Double(f64),
    BigNumber(i128),
    BulkError(&'a str),
    VerbatimString((&'a str, &'a str)),
    Map(HashMap<Value<'a>, Value<'a>>),
    Set(HashSet<Value<'a>>),
    Push(Vec<Value<'a>>),
}

impl TryFrom<u8> for DataType {
    type Error = ();

    fn try_from(b: u8) -> Result<Self, Self::Error> {
        let c = b;
        DataType::try_from(c)
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

async fn handle_connection(mut stream: TcpStream, config: Arc<Config>) -> anyhow::Result<()> {
    const BUFFER_SIZE : usize = 8 * 1024;

    // NOTE: Wait for the Stream to be readable and writable
    let (readable, writable) = tokio::join!(
        stream.readable(),
        stream.writable(),
    );
    if readable.is_err() || writable.is_err() {
        return Err(anyhow!("ERROR: Stream could not be opened!"));
    }

    let mut buffer = BytesMut::with_capacity(BUFFER_SIZE);

    stream.read_buf(&mut buffer).await?;
    
    let mut idx : usize = 0;
    let mut num_bytes = buffer.len();

    let mut values : Vec<Value<'_>> = vec![];

    while num_bytes > idx {
        match DataType::try_from(buffer[idx]) {
            Ok(dataType) => {

            }
            Err(_) => {
                let message = format!("-ERR unknown first byte: {}\r\n", buffer[idx]);
                
                stream.write(message.as_bytes()).await?;

                return Err(anyhow!(message));
            }
        }
    }

    stream.read_buf(&mut buffer).await?;

    Ok(())
}

async fn handle_ping(stream: &mut TcpStream) -> anyhow::Result<usize> {
    Ok(stream.write(b"PONG\r\n").await?)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let config = Arc::new(Config {});
    let listener = TcpListener::bind("127.0.0.1:6379").await?;

    loop {
        let (stream, addr) = listener.accept().await?;

        println!("New Connection from {}", addr);

        let config_ref = config.clone();
        tokio::spawn(async move { handle_connection(stream, config_ref).await });
    }
}

