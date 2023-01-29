use crate::errors::{ParquetError, ParquetResult};
use std::fs::File;
use std::io::Read;

pub trait ParquetReader: Send + Sync{
    type Reader: Read;

    fn get_reader(&self, start: usize, length: usize) -> ParquetResult<Self::Reader>;

    /// Get the length in bytes from the source
    fn len(&self) -> usize;

    /// Get a range as bytes
    /// This should fail if the exact number of bytes cannot be read
    fn get_bytes(&self, start: usize, length: usize) -> ParquetResult<&[u8]>;
}

impl<'a> ParquetReader for &'a [u8] {
    type Reader = &'a [u8];

    fn get_reader(&self, start: usize, length: usize) -> ParquetResult<Self::Reader> {
        if start + length > self.len() {
            Err(ParquetError::EOF)
        } else {
            Ok(&self[start..start + length])
        }
    }

    fn len(&self) -> usize {
        <[u8]>::len(self)
    }
    fn get_bytes(&self, start: usize, length: usize) -> ParquetResult<&[u8]> {
        let end = start + length;
        if end > self.len() {
            Err(ParquetError::EOF)
        } else {
            Ok(&self[start..end])
        }
    }
}
