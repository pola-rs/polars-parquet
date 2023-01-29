use super::*;
use crate::compression::Decompressor;

pub(crate) struct ZstdDecompressor {}

impl Decompressor for ZstdDecompressor {
    fn decompress(
        &mut self,
        input: &[u8],
        output: &mut Vec<u8>,
        uncompress_size: Option<usize>,
    ) -> ParquetResult<usize> {
        if let Some(size) = uncompress_size {
            output.reserve(size as usize)
        }
        let mut decoder = zstd::Decoder::new(input)?;
        let out = std::io::copy(&mut decoder, output)? as usize;
        Ok(out)
    }
}
