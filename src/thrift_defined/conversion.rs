use super::*;
use crate::errors::ParquetError;
use crate::thrift_defined::parquet_format::{ColumnChunk, RowGroup};

impl From<parquet_format::ConvertedType> for rosetta::ConvertedType {
    fn from(tp: parquet_format::ConvertedType) -> Self {
        use rosetta::ConvertedType::*;

        match tp {
            parquet_format::ConvertedType::UTF8 => Utf8,
            parquet_format::ConvertedType::MAP => Map,
            parquet_format::ConvertedType::MAP_KEY_VALUE => MapKeyValue,
            parquet_format::ConvertedType::LIST => List,
            parquet_format::ConvertedType::ENUM => Enum,
            parquet_format::ConvertedType::DECIMAL => Decimal,
            parquet_format::ConvertedType::DATE => Date,
            parquet_format::ConvertedType::TIME_MILLIS => TimeMillis,
            parquet_format::ConvertedType::TIME_MICROS => TimeMicros,
            parquet_format::ConvertedType::TIMESTAMP_MILLIS => TimeStampMillis,
            parquet_format::ConvertedType::TIMESTAMP_MICROS => TimeStampMicros,
            parquet_format::ConvertedType::UINT_8 => UInt8,
            parquet_format::ConvertedType::UINT_16 => UInt16,
            parquet_format::ConvertedType::UINT_32 => UInt32,
            parquet_format::ConvertedType::UINT_64 => UInt64,
            parquet_format::ConvertedType::INT_8 => Int8,
            parquet_format::ConvertedType::INT_16 => Int16,
            parquet_format::ConvertedType::INT_32 => Int32,
            parquet_format::ConvertedType::INT_64 => Int64,
            parquet_format::ConvertedType::JSON => Json,
            parquet_format::ConvertedType::BSON => Bson,
            parquet_format::ConvertedType::INTERVAL => Interval,
            _ => todo!(),
        }
    }
}

impl From<parquet_format::LogicalType> for rosetta::LogicalType {
    fn from(value: parquet_format::LogicalType) -> Self {
        use rosetta::LogicalType::*;
        match value {
            parquet_format::LogicalType::STRING(_) => String,
            parquet_format::LogicalType::MAP(_) => Map,
            parquet_format::LogicalType::LIST(_) => List,
            parquet_format::LogicalType::ENUM(_) => Enum,
            parquet_format::LogicalType::DECIMAL(t) => Decimal {
                scale: t.scale,
                precision: t.precision,
            },
            parquet_format::LogicalType::DATE(_) => Date,
            parquet_format::LogicalType::TIME(t) => Time {
                is_adjusted_to_utc: t.is_adjusted_to_u_t_c,
                unit: t.unit,
            },
            parquet_format::LogicalType::TIMESTAMP(t) => Timestamp {
                is_adjusted_to_utc: t.is_adjusted_to_u_t_c,
                unit: t.unit,
            },
            parquet_format::LogicalType::INTEGER(t) => Integer {
                bit_width: t.bit_width,
                is_signed: t.is_signed,
            },
            parquet_format::LogicalType::UNKNOWN(_) => Unknown,
            parquet_format::LogicalType::JSON(_) => Json,
            parquet_format::LogicalType::BSON(_) => Bson,
            parquet_format::LogicalType::UUID(_) => Uuid,
        }
    }
}

impl TryFrom<parquet_format::FieldRepetitionType> for rosetta::Repetition {
    type Error = ParquetError;

    fn try_from(value: parquet_format::FieldRepetitionType) -> Result<Self, Self::Error> {
        use rosetta::Repetition::*;
        let out = match value.0 {
            0 => Required,
            1 => Optional,
            2 => Repeated,
            _ => {
                return Err(ParquetError::InvalidFormat(
                    "Repetition value should be between 0-3.".into(),
                ))
            }
        };
        Ok(out)
    }
}

impl TryFrom<parquet_format::Type> for rosetta::PhysicalType {
    type Error = ParquetError;

    fn try_from(value: parquet_format::Type) -> Result<Self, Self::Error> {
        use rosetta::PhysicalType::*;
        let out = match value.0 {
            0 => Boolean,
            1 => Int32,
            2 => Int64,
            3 => Int96,
            4 => Float,
            5 => Double,
            6 => ByteArray,
            8 => FixedLenByteArray,
            _ => {
                return Err(ParquetError::InvalidFormat(
                    "Type value should be between 0-8.".into(),
                ))
            }
        };

        Ok(out)
    }
}
impl TryFrom<parquet_format::PageType> for rosetta::PageType {
    type Error = ParquetError;

    fn try_from(value: parquet_format::PageType) -> Result<Self, Self::Error> {
        use rosetta::PageType::*;
        let out = match value.0 {
            0 => DataPageV1,
            1 => IndexPage,
            2 => DictionaryPage,
            3 => DataPageV2,
            _ => {
                return Err(ParquetError::InvalidFormat(
                    "PageType value should be between 0-3.".into(),
                ))
            }
        };

        Ok(out)
    }
}

impl TryFrom<parquet_format::CompressionCodec> for rosetta::Compression {
    type Error = ParquetError;

    fn try_from(value: parquet_format::CompressionCodec) -> Result<Self, Self::Error> {
        use rosetta::Compression::*;
        let out = match value.0 {
            0 => Uncompressed,
            1 => Snappy,
            2 => Gzip,
            3 => Lzo,
            4 => Brotli,
            5 => Lz4,
            6 => Zstd,
            7 => Lz4Raw,
            _ => {
                return Err(ParquetError::InvalidFormat(
                    "PageType value should be between 0-3.".into(),
                ))
            }
        };

        Ok(out)
    }
}

impl TryFrom<parquet_format::Encoding> for rosetta::Encoding {
    type Error = ParquetError;

    fn try_from(value: parquet_format::Encoding) -> Result<Self, Self::Error> {
        use rosetta::Encoding::*;
        let out = match value.0 {
            0 => Plain,
            2 => PlainDictionary,
            3 => RLE,
            4 => BitPacked,
            5 => DeltaBinaryPacked,
            6 => DeltaLengthByteArray,
            7 => DeltaByteArray,
            8 => RLE_Dictionary,
            9 => ByteStreamSplit,
            _ => return Err(ParquetError::InvalidFormat("Invalid encoding.".into())),
        };

        Ok(out)
    }
}

// impl From<parquet_format::RowGroup> for rosetta::RowGroupMetaData {
//     fn from(value: parquet_format::RowGroup) -> Self {
//         RowGroupMetaData {
//             columns: value.columns.into_iter().map(|c| c.into()).collect(),
//             total_byte_size: value.total_byte_size as _,
//             num_rows: value.num_rows as _,
//             sorting_columns: value.sorting_columns,
//             file_offset: value.file_offset.map(|v| v as _),
//             total_compressed_size: value.total_compressed_size.map(|v| v as _)
//         }
//     }
// }
//
// impl From<ColumnChunk> for ColumnChunkMetaData {
//     fn from(value: ColumnChunk) -> Self {
//         todo!()
//     }
// }
