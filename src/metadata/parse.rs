use super::*;
use crate::types::ColumnDescriptor;
use std::rc::Rc;

fn decode_footer(footer: &[u8]) -> ParquetResult<usize> {
    let magic_number_offset = FOOTER_SIZE - MAGIC_NUMBER.len();
    if &footer[magic_number_offset..] != MAGIC_NUMBER {
        return Err(ParquetError::InvalidFormat(
            "Invalid parquet file. Corrupt footer".into(),
        ));
    }
    let metadata_len = i32::from_le_bytes(footer[..magic_number_offset].try_into().unwrap());

    metadata_len.try_into().map_err(|_| {
        ParquetError::InvalidFormat(
            "Invalid parquet file. Metdata length should be positive".into(),
        )
    })
}

/// Layout of Parquet file
/// +---------------------------+-----+---+
/// |      Rest of file         |  B  | A |
/// +---------------------------+-----+---+
/// where A: parquet footer, B: parquet metadata.
pub fn get_metadata<R: ParquetReader>(reader: R) -> ParquetResult<ParquetMetaData> {
    let file_size = reader.len();
    if file_size < FOOTER_SIZE {
        return Err(ParquetError::InvalidFormat(
            "Invalid parquet file. Size is smaller than footer".into(),
        ));
    }

    let footer = reader.get_bytes(file_size - FOOTER_SIZE, FOOTER_SIZE)?;
    let footer_metadata_len = decode_footer(footer)? + FOOTER_SIZE;

    if footer_metadata_len > file_size {
        return Err(ParquetError::InvalidFormat(
            "Invalid parquet file. Reported metdata length is larger than the file".into(),
        ));
    }

    let metadata = reader.get_bytes(file_size - footer_metadata_len, footer_metadata_len)?;
    decode_metadata(metadata)
}

fn decode_metadata(metadata: &[u8]) -> ParquetResult<ParquetMetaData> {
    // Use thrift to decode the metadata. They are encoded in thrift
    // compact input messages
    let mut protocol = TCompactInputProtocol::new(metadata);
    let t_file_metadata = ThriftFileMetaData::read_from_in_protocol(&mut protocol)
        .map_err(|e| ParquetError::InvalidFormat(format!("Could not parse metadata: {}", e)))?;

    let schema_tree = from_thrift(&t_file_metadata.schema)?;
    let schema_descr = SchemaDescriptor::new(schema_tree);

    let row_groups = t_file_metadata
        .row_groups
        .into_iter()
        .map(|rg| RowGroupMetaData::from_thrift(&schema_descr, rg))
        .collect::<ParquetResult<Vec<_>>>()?;

    let column_orders = t_file_metadata
        .column_orders
        .as_ref()
        .map(|column_orders| parse_column_orders(column_orders, &schema_descr));
    let file_metadata = FileMetaData {
        version: t_file_metadata.version,
        num_rows: t_file_metadata.num_rows as _,
        created_by: t_file_metadata.created_by,
        key_value_metadata: t_file_metadata.key_value_metadata,
        schema_descr: Rc::new(schema_descr),
        column_orders,
    };

    Ok(ParquetMetaData {
        file_metadata,
        row_groups,
    })
}

/// Parses column orders from Thrift definition.
fn parse_column_orders(
    t_columns_orders: &[TColumnOrder],
    schema_desc: &SchemaDescriptor,
) -> Vec<ColumnOrder> {
    debug_assert_eq!(schema_desc.leaves.len(), t_columns_orders.len());

    schema_desc
        .leaves
        .iter()
        .map(|column| {
            let sort_order = ColumnOrder::get_sort_order(
                column.logical_type(),
                column.converted_type(),
                column.physical_type(),
            );
            ColumnOrder::TypeDefinedOrder(sort_order)
        })
        .collect()
}
