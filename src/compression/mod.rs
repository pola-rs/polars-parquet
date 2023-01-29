#[cfg(feature = "zstd")]
mod codec_zstd;

use crate::errors::ParquetResult;
use crate::metadata::Compression;

pub(crate) trait Decompressor: Send {
    /// Decompresses data stored in slice `input` and appends output to `output`.
    ///
    /// If the uncompress_size is provided it will allocate the exact amount of memory.
    /// Otherwise, it will estimate the uncompressed size, allocating an amount of memory
    /// greater or equal to the real uncompress_size.
    ///
    /// Returns the total number of bytes written.
    fn decompress(
        &mut self,
        input: &[u8],
        output: &mut Vec<u8>,
        uncompress_size: Option<usize>,
    ) -> ParquetResult<usize>;
}

pub(crate) fn create_decompressor(compression: Compression) -> Option<Box<dyn Decompressor>> {
    use Compression::*;
    match compression {
        #[cfg(feature = "zstd")]
        Zstd => Some(Box::new(codec_zstd::ZstdDecompressor {})),
        Uncompressed => None,
        _ => todo!(),
    }
}
