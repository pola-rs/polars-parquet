use crate::compression::{create_decompressor, Decompressor};
use crate::metadata::{
    ColumnChunkMetaData, PhysicalType, RowGroupMetaData, TPageHeader, TPageLocation,
};
use crate::reader::ParquetReader;
use std::collections::VecDeque;

struct PageReader<'a> {
    reader: &'a dyn ParquetReader,
    decompressor: Option<Box<dyn Decompressor>>,
    physical_type: PhysicalType,
    state: PageReaderState,
}

enum PageReaderState {
    Values {
        // current offset in the bytes reader
        offset: u64,
        // length of the chunk in bytes
        remaining_bytes: u64,
        // if next page header has been 'peeked' it is cached her
        next_page_header: Option<TPageHeader>,
    },
    Pages {
        /// remaining page locations
        page_locations: VecDeque<TPageLocation>,
        /// reaming dictionary locations if any
        dictionary_page: Option<TPageLocation>,
        total_rows: u64,
    },
}

impl<'a> PageReader<'a> {
    fn new(reader: &'a dyn ParquetReader, metadata: &ColumnChunkMetaData, total_rows: u64) -> Self {
        let (offset, remaining_bytes) = metadata.byte_range();
        let state = PageReaderState::Values {
            offset,
            remaining_bytes,
            next_page_header: None,
        };

        let decompressor = create_decompressor(metadata.compression);
        PageReader {
            reader,
            physical_type: metadata.column_type,
            decompressor,
            state,
        }
    }
}

pub fn read_row_group(reader: &dyn ParquetReader, metadata: &RowGroupMetaData, column_i: usize) {
    let column_md = &metadata.columns[column_i];

    let page_reader = PageReader::new(reader, column_md, 10);
}
