mod row_groups;
mod columns;

use crate::errors::{ParquetError, ParquetResult};
use crate::{FOOTER_SIZE, MAGIC_NUMBER};
use crate::reader::ParquetReader;
use crate::thrift_utils::rosetta::*;

use thrift::protocol::{
    TCompactInputProtocol, TSerializable
};
use crate::thrift_utils::parquet_format::{
    FileMetaData as ThriftFileMetaData,
};
use crate::thrift_utils::rosetta::Encoding;
use crate::types::{ColumnPath, from_thrift, ParquetType, SchemaDescriptor};

fn decode_footer(footer: &[u8]) -> ParquetResult<usize> {
    let magic_number_offset = FOOTER_SIZE - MAGIC_NUMBER.len();
    if &footer[magic_number_offset..] != MAGIC_NUMBER {
        return Err(ParquetError::InvalidFormat("Invalid parquet file. Corrupt footer".into()))
    }
    let metadata_len = i32::from_le_bytes(footer[..magic_number_offset].try_into().unwrap());

    metadata_len.try_into().map_err(|_| {
        ParquetError::InvalidFormat("Invalid parquet file. Metdata length should be positive".into())
    })

}

/// Layout of Parquet file
/// +---------------------------+-----+---+
/// |      Rest of file         |  B  | A |
/// +---------------------------+-----+---+
/// where A: parquet footer, B: parquet metadata.
pub fn get_metadata<R: ParquetReader>(
    reader: R
) -> ParquetResult<()>{
    let file_size = reader.len();
    if file_size < FOOTER_SIZE {
        return Err(ParquetError::InvalidFormat("Invalid parquet file. Size is smaller than footer".into()))
    }

    let footer = reader.get_bytes(file_size - FOOTER_SIZE, FOOTER_SIZE)?;
    let footer_metadata_len = decode_footer(footer)? + FOOTER_SIZE;

    if footer_metadata_len > file_size {
        return Err(ParquetError::InvalidFormat("Invalid parquet file. Reported metdata length is larger than the file".into()))
    }

    let metadata = reader.get_bytes(file_size - footer_metadata_len, footer_metadata_len)?;
    decode_metadata(metadata)?;

    Ok(())
}

fn decode_metadata(metadata: &[u8]) -> ParquetResult<()> {

    // Use thrift to decode the metadata. They are encoded in thrift
    // compact input messages
    let mut protocol = TCompactInputProtocol::new(metadata);
    let t_file_metadata = ThriftFileMetaData::read_from_in_protocol(&mut protocol)
        .map_err(|e| ParquetError::InvalidFormat(format!("Could not parse metadata: {}", e)))?;

    let schema_tree = from_thrift(&t_file_metadata.schema)?;
    let schema = SchemaDescriptor::new(schema_tree);
    dbg!(schema.leaves);

    todo!()

}

