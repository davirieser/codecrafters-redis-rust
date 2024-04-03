use crate::RespValue;

use std::borrow::Cow;
use std::collections::{HashMap, HashSet};

use nom::{
    branch::alt,
    bytes::streaming::{is_not, tag, take},
    character::streaming::{char, crlf, digit1, one_of},
    combinator::{map, map_res, opt, recognize, rest},
    multi::length_value,
    sequence::{pair, preceded, terminated, tuple},
    IResult, Parser,
};

// https://edgarluque.com/blog/bencode-parser-with-nom/
#[derive(Debug, PartialEq, thiserror::Error)]
pub enum ParseError<I> {
    // When there is an error parsing a utf-8 string.
    #[error("parse utf-8 error: {0:?}")]
    ParseUtf8(#[from] std::str::Utf8Error),
    // When there is an error parsing the ascii integer to a i64.
    #[error("parse int error: {0:?}")]
    ParseInt(#[from] std::num::ParseIntError),
    // When there is an error parsing the ascii float to a f64.
    #[error("parse int error: {0:?}")]
    ParseFloat(#[from] std::num::ParseFloatError),
    // Errors from the combinators itself.
    #[error("nom parsing error: {0:?}")]
    Nom(#[from] nom::Err<nom::error::Error<I>>),
}

impl<I> ParseError<I> {
    pub fn incomplete(&self) -> bool {
        match self {
            ParseError::Nom(e) => e.is_incomplete(),
            _ => false,
        }
    }
}

impl<I> From<ParseError<I>> for nom::Err<ParseError<I>> {
    fn from(e: ParseError<I>) -> Self {
        nom::Err::Error(e)
    }
}

impl<I> From<nom::Err<ParseError<I>>> for ParseError<I> {
    fn from(e: nom::Err<ParseError<I>>) -> Self {
        e.into()
    }
}

impl<I> nom::error::ParseError<I> for ParseError<I> {
    fn from_error_kind(input: I, kind: nom::error::ErrorKind) -> Self {
        Self::Nom(nom::Err::Error(nom::error::Error { input, code: kind }))
    }
    fn append(_: I, _: nom::error::ErrorKind, other: Self) -> Self {
        other
    }
}

type ParseResult<I, O> = IResult<I, O, ParseError<I>>;

fn line(input: &[u8]) -> ParseResult<&[u8], &[u8]> {
    terminated(is_not("\r\n"), crlf)(input)
}
fn length_bytes(input: &[u8]) -> ParseResult<&[u8], &[u8]> {
    terminated(length_value(parse_usize, rest), crlf)(input)
}

fn map_str<'a, F>(mut parser: F) -> impl FnMut(&'a [u8]) -> ParseResult<&'a [u8], &str>
where
    F: Parser<&'a [u8], &'a [u8], ParseError<&'a [u8]>>,
{
    move |input| {
        let (input, bytes) = parser.parse(input)?;
        let string = std::str::from_utf8(bytes).map_err(ParseError::from)?;

        Ok((input, string))
    }
}

fn map_cow<'a, F>(mut parser: F) -> impl FnMut(&'a [u8]) -> ParseResult<&'a [u8], Cow<'a, str>>
where
    F: Parser<&'a [u8], &'a [u8], ParseError<&'a [u8]>>,
{
    move |input| {
        let (input, bytes) = parser.parse(input)?;
        let string = std::str::from_utf8(bytes).map_err(ParseError::from)?;

        Ok((input, string.into()))
    }
}

fn parse_null(input: &[u8]) -> ParseResult<&[u8], RespValue> {
    let (input, _) = crlf(input)?;

    Ok((input, RespValue::Null))
}
fn parse_boolean(input: &[u8]) -> ParseResult<&[u8], RespValue> {
    let (input, b) = terminated(one_of("tf"), crlf)(input)?;

    Ok((input, RespValue::Boolean(b == 't')))
}

fn parse_simple_string(input: &[u8]) -> ParseResult<&[u8], RespValue> {
    map(map_cow(line), RespValue::SimpleString)(input)
}
fn parse_simple_error(input: &[u8]) -> ParseResult<&[u8], RespValue> {
    map(map_cow(line), RespValue::SimpleError)(input)
}
fn parse_bulk_string(input: &[u8]) -> ParseResult<&[u8], RespValue> {
    map(map_cow(length_bytes), RespValue::BulkString)(input)
}
fn parse_bulk_error(input: &[u8]) -> ParseResult<&[u8], RespValue> {
    map(map_cow(length_bytes), RespValue::BulkError)(input)
}
fn parse_verbatim_string(input: &[u8]) -> ParseResult<&[u8], RespValue> {
    let (input, bytes) = length_bytes(input)?;

    let (_, (bytes_enc, _, bytes_string)) = tuple((take(3u8), char(':'), rest))(bytes)?;

    let enc = std::str::from_utf8(bytes_enc).map_err(ParseError::from)?;
    let string = std::str::from_utf8(bytes_string).map_err(ParseError::from)?;

    Ok((
        input,
        RespValue::VerbatimString((enc.into(), string.into())),
    ))
}

fn parse_integer(input: &[u8]) -> ParseResult<&[u8], RespValue> {
    map(parse_i64, RespValue::Integer)(input)
}

fn parse_usize(input: &[u8]) -> ParseResult<&[u8], usize> {
    let (input, int_bytes) = digit1(input)?;
    let (input, _) = crlf(input)?;

    // SAFETY: 'digit1' always returns ASCII numbers, which are always valid UTF-8.
    let int_str = unsafe { std::str::from_utf8_unchecked(int_bytes) };
    let int = int_str.parse().map_err(ParseError::from)?;

    Ok((input, int))
}

fn parse_u64(input: &[u8]) -> ParseResult<&[u8], u64> {
    let (input, int_bytes) = digit1(input)?;
    let (input, _) = crlf(input)?;

    // SAFETY: 'digit1' always returns ASCII numbers, which are always valid UTF-8.
    let int_str = unsafe { std::str::from_utf8_unchecked(int_bytes) };
    let int = int_str.parse().map_err(ParseError::from)?;

    Ok((input, int))
}

fn parse_i64(input: &[u8]) -> ParseResult<&[u8], i64> {
    let (input, int_bytes) = alt((
        digit1,
        recognize(pair(char('-'), digit1)),
        recognize(pair(char('+'), digit1)),
    ))(input)?;
    let (input, _) = crlf(input)?;

    // SAFETY: 'digit1' always returns ASCII numbers, which are always valid UTF-8.
    let int_str = unsafe { std::str::from_utf8_unchecked(int_bytes) };
    let int = int_str.parse().map_err(ParseError::from)?;

    Ok((input, int))
}

fn parse_big_number(input: &[u8]) -> ParseResult<&[u8], RespValue> {
    let (input, big_number_bytes) = recognize(pair(opt(one_of("+-")), digit1))(input)?;
    let (input, _) = crlf(input)?;

    // SAFETY: 'digit1' always returns ASCII numbers, which are always valid UTF-8.
    let big_number = unsafe { std::str::from_utf8_unchecked(big_number_bytes) };

    Ok((input, RespValue::BigNumber(big_number.into())))
}

fn parse_double(input: &[u8]) -> ParseResult<&[u8], RespValue> {
    let (input, double_bytes) = recognize(tuple((
        opt(one_of("+-")),
        digit1,
        opt(preceded(tag("."), digit1)),
        opt(preceded(
            one_of("eE"),
            recognize(pair(opt(one_of("+-")), digit1)),
        )),
    )))(input)?;
    let (input, _) = crlf(input)?;

    // SAFETY: 'digit1' always returns ASCII numbers, which are always valid UTF-8.
    let double_string = unsafe { std::str::from_utf8_unchecked(double_bytes) };
    let double: f64 = double_string.parse().map_err(ParseError::from)?;

    Ok((input, RespValue::Double(double)))
}

fn parse_array_internal(input: &[u8]) -> ParseResult<&[u8], Vec<RespValue>> {
    let (mut input, len) = parse_usize(input)?;

    let mut vec = Vec::with_capacity(len);
    for _ in 0..len {
        let value;
        (input, value) = parse_resp_value(input)?;
        vec.push(value);
    }

    Ok((input, vec))
}

fn parse_array(input: &[u8]) -> ParseResult<&[u8], RespValue> {
    let (input, vec) = parse_array_internal(input)?;
    Ok((input, RespValue::Array(vec)))
}

fn parse_push(input: &[u8]) -> ParseResult<&[u8], RespValue> {
    let (input, vec) = parse_array_internal(input)?;
    Ok((input, RespValue::Push(vec)))
}

fn parse_set(input: &[u8]) -> ParseResult<&[u8], RespValue> {
    let (mut input, len) = parse_usize(input)?;

    let mut set = HashSet::with_capacity(len);
    for _ in 0..len {
        let value;
        (input, value) = parse_resp_value(input)?;
        set.insert(value);
    }

    Ok((input, RespValue::Set(set)))
}

fn parse_map(input: &[u8]) -> ParseResult<&[u8], RespValue> {
    let (mut input, len) = parse_usize(input)?;

    let mut map = HashMap::with_capacity(len);
    for _ in 0..len {
        let (key, value);
        (input, key) = parse_resp_value(input)?;
        (input, value) = parse_resp_value(input)?;
        map.insert(key, value);
    }

    Ok((input, RespValue::Map(map)))
}

pub fn parse_resp_value<'b, 'a: 'b>(input: &'a [u8]) -> ParseResult<&'b [u8], RespValue<'a>> {
    let (input, first_byte) = one_of("+-:$*_#,(!=%~>")(input)?;
    match first_byte {
        '_' => parse_null(input),
        '#' => parse_boolean(input),
        ':' => parse_integer(input),
        ',' => parse_double(input),
        '(' => parse_big_number(input),
        '+' => parse_simple_string(input),
        '$' => parse_bulk_string(input),
        '=' => parse_verbatim_string(input),
        '-' => parse_simple_error(input),
        '!' => parse_bulk_error(input),
        '*' => parse_array(input),
        '>' => parse_push(input),
        '~' => parse_set(input),
        '%' => parse_map(input),
        _ => unreachable!(),
    }
}

