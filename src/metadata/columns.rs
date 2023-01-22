use crate::statistics::Statistics;
use crate::types::ColumnDescriptorPtr;
use super::*;

/// Metadata for a column chunk.
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnChunkMetaData {
    column_type: ParquetType,
    column_path: ColumnPath,
    column_descr: ColumnDescriptorPtr,
    encodings: Vec<Encoding>,
    file_path: Option<String>,
    file_offset: i64,
    num_values: i64,
    compression: Compression,
    total_compressed_size: i64,
    total_uncompressed_size: i64,
    data_page_offset: i64,
    index_page_offset: Option<i64>,
    dictionary_page_offset: Option<i64>,
    statistics: Option<Statistics>,
    encoding_stats: Option<Vec<PageEncodingStats>>,
    bloom_filter_offset: Option<i64>,
    offset_index_offset: Option<i64>,
    offset_index_length: Option<i32>,
    column_index_offset: Option<i64>,
    column_index_length: Option<i32>,
}
