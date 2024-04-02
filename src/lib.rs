#![allow(unused)]
#![warn(unused_must_use)]

mod config;
use config::Config;

mod types;
use types::AsyncReader;

mod resp;
use resp::{parse_resp_value, RespDataType, RespReader, RespReaderError, RespValue, RespWriter};

mod db;
use db::Database;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_parse_resp_simple_string1() {
        let input = b"+Test\r\n";

        assert_eq!(
            (&b""[..], RespValue::SimpleString("Test".into())),
            parse_resp_value(input).unwrap()
        );
    }
    #[test]
    fn test_invalid_parse_resp_simple_string() {
        let inputs: Vec<&[u8]> = vec![b"+Test", b"+Test\r", b"+\r", b"+\r", b"Test\r\n"];

        for input in inputs {
            assert!(parse_resp_value(input).is_err(), "Failed on {:?}", input);
        }
    }
}
