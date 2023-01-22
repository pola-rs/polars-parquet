mod parse;

use crate::errors::{ParquetError, ParquetResult};
use crate::reader::ParquetReader;
use crate::thrift_defined::*;
use crate::{FOOTER_SIZE, MAGIC_NUMBER};

use crate::types::{from_thrift, ColumnPath, ParquetType, SchemaDescriptor};
pub use parse::get_metadata;
use thrift::protocol::{TCompactInputProtocol, TSerializable};

/// Global Parquet metadata.
#[derive(Debug, Clone)]
pub struct ParquetMetaData {
    file_metadata: FileMetaData,
    row_groups: Vec<RowGroupMetaData>,
    // Page index for all pages in each column chunk
    // page_indexes: Option<ParquetColumnIndex>,
    // Offset index for all pages in each column chunk
    // offset_indexes: Option<ParquetOffsetIndex>,
}
