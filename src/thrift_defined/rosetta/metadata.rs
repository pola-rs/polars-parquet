use super::*;
use crate::errors::{ParquetError, ParquetResult};
use crate::statistics::Statistics;
use crate::thrift_defined::parquet_format::{ColumnChunk, RowGroup};
use crate::thrift_defined::KeyValue;
use crate::types::{ColumnDescriptorPtr, ColumnPath, SchemaDescriptor, SchemaDescriptorPtr};

#[derive(Debug, Clone, PartialEq)]
pub struct RowGroupMetaData {
    /// Metadata for each column chunk in this row group.
    /// This list must have the same order as the SchemaElement list in FileMetaData.
    pub columns: Vec<ColumnChunkMetaData>,
    /// Total byte size of all the uncompressed column data in this row group *
    pub total_byte_size: u64,
    /// Number of rows in this row group *
    pub num_rows: u64,
    /// If set, specifies a sort ordering of the rows in this RowGroup.
    /// The sorting columns can be a subset of all the columns.
    pub sorting_columns: Option<Vec<SortingColumn>>,
    /// Byte offset from beginning of file to first page (data or dictionary)
    /// in this row group *
    pub file_offset: Option<u64>,
    /// Total byte size of all compressed (and potentially encrypted) column data
    /// in this row group *
    pub total_compressed_size: Option<u64>,
}

impl RowGroupMetaData {
    pub(crate) fn from_thrift(schema_desc: &SchemaDescriptor, rg: RowGroup) -> ParquetResult<Self> {
        let columns = rg
            .columns
            .into_iter()
            .zip(&schema_desc.leaves)
            .map(|(cc, column_desc)| ColumnChunkMetaData::from_thrift(column_desc.clone(), cc))
            .collect::<ParquetResult<_>>()?;

        Ok(RowGroupMetaData {
            columns,
            total_byte_size: rg.total_byte_size as _,
            num_rows: rg.num_rows as _,
            sorting_columns: rg.sorting_columns,
            file_offset: rg.file_offset.map(|v| v as _),
            total_compressed_size: rg.total_compressed_size.map(|v| v as _),
        })
    }
}

/// Metadata for a column chunk.
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnChunkMetaData {
    column_type: PhysicalType,
    column_path: ColumnPath,
    column_descr: ColumnDescriptorPtr,
    encodings: Vec<Encoding>,
    /// File where column data is stored.  If not set, assumed to be same file as
    /// metadata.  This path is relative to the current file.
    file_path: Option<String>,
    file_offset: u64,
    num_values: u64,
    compression: Compression,
    total_compressed_size: u64,
    total_uncompressed_size: u64,
    data_page_offset: u64,
    index_page_offset: Option<u64>,
    dictionary_page_offset: Option<u64>,
    statistics: Option<Statistics>,
    // Maybe add these later?
    // encoding_stats: Option<Vec<PageEncodingStats>>,
    // bloom_filter_offset: Option<i64>,
    offset_index_offset: Option<u64>,
    offset_index_length: Option<u64>,
    column_index_offset: Option<u64>,
    column_index_length: Option<u64>,
}

impl ColumnChunkMetaData {
    pub(crate) fn from_thrift(
        column_descr: ColumnDescriptorPtr,
        cc: ColumnChunk,
    ) -> ParquetResult<Self> {
        if let Some(metatada) = cc.meta_data {
            Ok(ColumnChunkMetaData {
                column_type: metatada.type_.try_into()?,
                column_path: ColumnPath {
                    parts: metatada.path_in_schema,
                },
                column_descr,
                encodings: metatada
                    .encodings
                    .into_iter()
                    .map(|en| en.try_into())
                    .collect::<ParquetResult<Vec<_>>>()?,
                file_path: cc.file_path,
                file_offset: cc.file_offset as _,
                num_values: metatada.num_values as _,
                compression: metatada.codec.try_into()?,
                total_compressed_size: metatada.total_uncompressed_size as _,
                total_uncompressed_size: metatada.total_uncompressed_size as _,
                data_page_offset: metatada.data_page_offset as _,
                index_page_offset: metatada.index_page_offset.map(|v| v as _),
                dictionary_page_offset: metatada.dictionary_page_offset.map(|v| v as _),
                // TODO!
                statistics: None,
                offset_index_offset: cc.offset_index_offset.map(|v| v as _),
                offset_index_length: cc.offset_index_length.map(|v| v as _),
                column_index_offset: cc.column_index_offset.map(|v| v as _),
                column_index_length: cc.column_index_length.map(|v| v as _),
            })
        } else {
            Err(ParquetError::InvalidFormat(
                "Expected column metadata.".into(),
            ))
        }
    }
}
/// Metadata for a Parquet file.
#[derive(Debug, Clone)]
pub struct FileMetaData {
    pub version: i32,
    pub num_rows: u64,
    pub created_by: Option<String>,
    pub key_value_metadata: Option<Vec<KeyValue>>,
    pub schema_descr: SchemaDescriptorPtr,
    pub column_orders: Option<Vec<ColumnOrder>>,
}