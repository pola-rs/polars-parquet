use std::error::Error;
use std::fmt::{Display, Formatter};

#[derive(Debug)]
pub enum ParquetError {
    IO(String),
    InvalidFormat(String),
    EOF,
}
pub type ParquetResult<T> = Result<T, ParquetError>;

impl Error for ParquetError {}

impl Display for ParquetError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl From<std::io::Error> for ParquetError {
    fn from(value: std::io::Error) -> Self {
        ParquetError::IO(format!("{value}"))
    }
}
