#[derive(PartialEq, Eq, Debug, Clone, Copy, Hash)]
pub enum RespDataType {
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

impl From<RespDataType> for char {
    fn from(dt: RespDataType) -> char {
        match dt {
            RespDataType::SimpleString => '+',
            RespDataType::SimpleError => '-',
            RespDataType::Integer => ':',
            RespDataType::BulkString => '$',
            RespDataType::Array => '*',
            RespDataType::Null => '_',
            RespDataType::Boolean => '#',
            RespDataType::Double => ',',
            RespDataType::BigNumber => '(',
            RespDataType::BulkError => '!',
            RespDataType::VerbatimString => '=',
            RespDataType::Map => '%',
            RespDataType::Set => '~',
            RespDataType::Push => '>',
        }
    }
}

impl TryFrom<u8> for RespDataType {
    type Error = ();

    fn try_from(b: u8) -> Result<Self, Self::Error> {
        RespDataType::try_from(char::from(b))
    }
}

impl TryFrom<char> for RespDataType {
    type Error = ();

    fn try_from(c: char) -> Result<Self, Self::Error> {
        match c {
            '+' => Ok(RespDataType::SimpleString),
            '-' => Ok(RespDataType::SimpleError),
            ':' => Ok(RespDataType::Integer),
            '$' => Ok(RespDataType::BulkString),
            '*' => Ok(RespDataType::Array),
            '_' => Ok(RespDataType::Null),
            '#' => Ok(RespDataType::Boolean),
            ',' => Ok(RespDataType::Double),
            '(' => Ok(RespDataType::BigNumber),
            '!' => Ok(RespDataType::BulkError),
            '=' => Ok(RespDataType::VerbatimString),
            '%' => Ok(RespDataType::Map),
            '~' => Ok(RespDataType::Set),
            '>' => Ok(RespDataType::Push),
            _ => Err(()),
        }
    }
}
