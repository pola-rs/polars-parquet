use crate::compression::{create_decompressor, Decompressor};
use crate::data::page::{read_page_header, Page, PageMetadata, PageReader};
use crate::errors::{ParquetError, ParquetResult};
use crate::metadata::{
    ColumnChunkMetaData, PageType, PhysicalType, RowGroupMetaData, TPageHeader, TPageLocation,
};
use crate::reader::ParquetReader;
use std::collections::VecDeque;

struct SerPageReader<R: ParquetReader> {
    reader: R,
    decompressor: Option<Box<dyn Decompressor>>,
    physical_type: PhysicalType,
    state: PageReaderState,
}

enum PageReaderState {
    Values {
        // current offset in the bytes reader
        offset: usize,
        // length of the chunk in bytes
        remaining_bytes: usize,
        // if next page header has been 'peeked' it is cached her
        next_page_header: Option<TPageHeader>,
    },
    Pages {
        /// remaining page locations
        page_locations: VecDeque<TPageLocation>,
        /// reaming dictionary locations if any
        dictionary_page: Option<TPageLocation>,
        total_rows: usize,
    },
}

impl<R: ParquetReader> SerPageReader<R> {
    fn new(
        reader: R,
        metadata: &ColumnChunkMetaData,
        total_rows: usize,
    ) -> Self {
        let (offset, remaining_bytes) = metadata.byte_range();
        let state = PageReaderState::Values {
            offset,
            remaining_bytes,
            next_page_header: None,
        };

        let decompressor = create_decompressor(metadata.compression);
        SerPageReader {
            reader,
            physical_type: metadata.column_type,
            decompressor,
            state,
        }
    }
}

impl<R: ParquetReader> Iterator for SerPageReader<R> {
    type Item = ParquetResult<Page>;

    fn next(&mut self) -> Option<Self::Item> {
        self.get_next_page().transpose()
    }
}

impl<R: ParquetReader> PageReader for SerPageReader<R> {
    fn get_next_page(&mut self) -> ParquetResult<Option<Page>> {
        use std::io::Read;
        loop {
            match &mut self.state {
                PageReaderState::Values {
                    offset,
                    remaining_bytes,
                    next_page_header,
                } => {
                    if *remaining_bytes == 0 {
                        return Ok(None);
                    }
                    dbg!(*offset, *remaining_bytes);

                    let mut reader = self.reader.get_reader(*offset as usize, *remaining_bytes as usize)?;

                    let header = next_page_header.take().map(Ok).unwrap_or_else(|| {
                        let (read, header) = read_page_header(&mut reader)?;
                        *offset += read;
                        *remaining_bytes -= read;
                        ParquetResult::Ok(header)
                    })?;

                    let data_len = header.compressed_page_size as usize;
                    *offset += data_len;
                    *remaining_bytes -= data_len;

                    if header.type_.0 == PageType::IndexPage as i32 {
                        continue;
                    }

                    let mut buffer = Vec::with_capacity(data_len);
                    let read = (&mut reader).take(data_len as u64).read_to_end(&mut buffer)?;

                    if data_len != read {
                        return Err(ParquetError::EOF);
                    }


                    dbg!(header);

                    todo!()
                }
                PageReaderState::Pages {
                    page_locations,
                    dictionary_page,
                    ..
                } => {
                    todo!()
                }
            }
        }

        todo!()
    }

    fn peek_next_page(&mut self) -> ParquetResult<Option<PageMetadata>> {
        todo!()
    }

    fn skip_next_page(&mut self) -> ParquetResult<()> {
        todo!()
    }
}

pub fn read_row_group<R: ParquetReader>(reader: R, metadata: &RowGroupMetaData, column_i: usize) {
    let column_md = &metadata.columns[column_i];

    let mut page_reader = SerPageReader::new(reader, column_md, 10);
    page_reader.get_next_page();
}
