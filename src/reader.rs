use crate::errors::{ParquetError, ParquetResult};
use std::fs::File;
use std::io::Read;

pub trait ParquetReader: Send + Sync {
    /// Get the length in bytes from the source
    fn len(&self) -> usize;

    /// Get a range as bytes
    /// This should fail if the exact number of bytes cannot be read
    fn get_bytes(&self, start: usize, length: usize) -> ParquetResult<&[u8]>;
}

impl ParquetReader for &[u8] {
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

impl ParquetReader for Vec<u8> {
    fn len(&self) -> usize {
        self.as_slice().len()
    }

    fn get_bytes(&self, start: usize, length: usize) -> ParquetResult<&[u8]> {
        self.get_bytes(start, length)
    }
}
