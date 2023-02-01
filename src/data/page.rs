use crate::compression::Decompressor;
use crate::errors::{ParquetError, ParquetResult};
use crate::metadata::{Encoding, PageType, PhysicalType, Statistics, TPageHeader};
use std::io::Read;
use std::os::linux::raw::stat;
use std::sync::Arc;
use thrift::protocol::{TCompactInputProtocol, TSerializable};

pub enum Page {
    Data {
        // TODO: maybe use Bytes crate
        buffer: Vec<u8>,
        num_values: u32,
        encoding: Encoding,
        def_level_encoding: Encoding,
        rep_level_encoding: Encoding,
        statistics: Option<Statistics>,
    },
    DataV2 {
        buffer: Vec<u8>,
        num_values: u32,
        encoding: Encoding,
        num_nulls: u32,
        num_rows: u32,
        def_levels_byte_len: u32,
        rep_levels_byte_len: u32,
        is_compressed: bool,
        statstics: Option<Statistics>,
    },
    Dictionary {
        buffer: Vec<u8>,
        num_values: u32,
        encoding: Encoding,
        is_sorted: bool,
    },
}

impl Page {
    pub fn buffer(&self) -> &[u8] {
        match self {
            Page::Data { buffer, .. } => buffer,
            Page::DataV2 { buffer, .. } => buffer,
            Page::Dictionary { buffer, .. } => buffer,
        }
    }
}

pub trait PageReader: Iterator<Item = ParquetResult<Page>> + Send {
    /// Gets the next page in the column chunk associated with this reader.
    /// Returns `None` if there are no pages left.
    fn get_next_page(&mut self) -> ParquetResult<Option<Page>>;

    /// Gets metadata about the next page, returns an error if no
    /// column index information
    fn peek_next_page(&mut self) -> ParquetResult<Option<PageMetadata>>;

    /// Skips reading the next page, returns an error if no
    /// column index information
    fn skip_next_page(&mut self) -> ParquetResult<()>;
}

#[derive(Clone)]
/// Metadata for a page
pub struct PageMetadata {
    /// Number of rows in this page
    pub num_rows: u64,
    /// True if page is a dictionary page
    pub is_dict: bool,
}

pub fn read_page_header<R: Read>(input: R) -> ParquetResult<(usize, TPageHeader)> {
    /// A wrapper around a [`std::io::Read`] that keeps track of the bytes read
    struct TrackedRead<R: Read> {
        inner: R,
        bytes_read: usize,
    }

    impl<R: Read> Read for TrackedRead<R> {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            let v = self.inner.read(buf)?;
            self.bytes_read += v;
            Ok(v)
        }
    }

    let mut tracked = TrackedRead {
        inner: input,
        bytes_read: 0,
    };

    let mut prot = TCompactInputProtocol::new(&mut tracked);
    let header = TPageHeader::read_from_in_protocol(&mut prot).map_err(|e| {
        ParquetError::InvalidFormat(format!("Could not parse page metadata: {}", e))
    })?;
    Ok((tracked.bytes_read, header))
}

pub(crate) fn decode_page(
    header: TPageHeader,
    input: Vec<u8>,
    physical_type: PhysicalType,
    decompressor: Option<&mut Box<dyn Decompressor>>,
) -> ParquetResult<Page> {
    // When processing data page v2, depending on enabled compression for the
    // page, we should account for uncompressed data ('offset') of
    // repetition and definition levels.
    //
    // We always use 0 offset for other pages other than v2, `true` flag means
    // that compression will be applied if decompressor is defined

    let (offset, can_decompress) = if let Some(header_v2) = &header.data_page_header_v2 {
        let offset = (header_v2.definition_levels_byte_length
            + header_v2.repetition_levels_byte_length) as usize;
        let can_decompress = header_v2.is_compressed.unwrap_or(true);
        (offset, can_decompress)
    } else {
        (0, true)
    };

    let buffer = match (decompressor, can_decompress) {
        (Some(decompressor), true) => {
            let uncompressed_size = header.uncompressed_page_size as usize;
            let mut out = Vec::with_capacity(uncompressed_size);

            let compressed_bytes = &input[offset..];

            // the rep/def levels are written in first in case of v2 page
            out.extend_from_slice(&input[..offset]);
            decompressor.decompress(
                compressed_bytes,
                &mut out,
                Some(uncompressed_size - offset),
            )?;

            if out.len() != uncompressed_size {
                return Err(ParquetError::InvalidFormat(format!(
                    "Actual decompressed size: {uncompressed_size} doesn't match the expected: {}",
                    out.len()
                )));
            }
            out
        }
        _ => input,
    };

    let page = match header.type_.into() {
        PageType::DictionaryPage => {
            let dict_header = header.dictionary_page_header.unwrap();
            let is_sorted = dict_header.is_sorted.unwrap_or(false);
            Page::Dictionary {
                buffer,
                num_values: dict_header.num_values as u32,
                encoding: dict_header.encoding.try_into().unwrap(),
                is_sorted,
            }
        }
        PageType::DataPageV1 => {
            let data_header = header.data_page_header.unwrap();
            Page::Data {
                buffer,
                num_values: data_header.num_values as u32,
                encoding: data_header.encoding.try_into().unwrap(),
                def_level_encoding: data_header.definition_level_encoding.try_into().unwrap(),
                rep_level_encoding: data_header.repetition_level_encoding.try_into().unwrap(),
                statistics: data_header
                    .statistics
                    .map(|stats| Statistics::from_thrift(physical_type, stats))
                    .transpose()?
                    .flatten(),
            }
        }
        _ => todo!(),
    };

    todo!()
}
