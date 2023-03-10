mod parse;
mod statistics;
mod thrift_defined;
pub mod types;

use crate::errors::{ParquetError, ParquetResult};
pub(crate) use crate::metadata::thrift_defined::*;
use crate::reader::ParquetReader;
use crate::{FOOTER_SIZE, MAGIC_NUMBER};

pub use statistics::*;
use thrift::protocol::{TCompactInputProtocol, TSerializable};
pub use thrift_defined::rosetta::*;

pub use parse::get_metadata;

/// Global Parquet metadata.
#[derive(Debug, Clone)]
pub struct ParquetMetaData {
    pub file_metadata: FileMetaData,
    pub row_groups: Vec<RowGroupMetaData>,
    // Page index for all pages in each column chunk
    // page_indexes: Option<ParquetColumnIndex>,
    // Offset index for all pages in each column chunk
    // offset_indexes: Option<ParquetOffsetIndex>,
}
