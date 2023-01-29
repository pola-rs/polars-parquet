use crate::compression::Decompressor;
use crate::errors::{ParquetError, ParquetResult};
use crate::metadata::statistics::Statistics;
use crate::metadata::{Encoding, PhysicalType, TPageHeader};
use std::io::Read;
use std::sync::Arc;
use thrift::protocol::{TCompactInputProtocol, TSerializable};

pub enum Page {
    Data {
        // TODO: maybe use Bytes crate
        buf: Arc<Vec<u8>>,
        num_values: u32,
        encoding: Encoding,
        def_level_encoding: Encoding,
        rep_level_encoding: Encoding,
        statistics: Option<Statistics>,
    },
    DataV2 {
        buf: Arc<Vec<u8>>,
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
        buf: Arc<Vec<u8>>,
        num_values: u32,
        encoding: Encoding,
        is_sorted: bool,
    },
}

impl Page {
    pub fn buffer(&self) -> &Arc<Vec<u8>> {
        match self {
            Page::Data { buf, .. } => buf,
            Page::DataV2 { buf, .. } => buf,
            Page::Dictionary { buf, .. } => buf,
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
    todo!()
}
