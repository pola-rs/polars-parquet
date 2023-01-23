//! Contains Rust mappings for Thrift definition.
//! Refer to `parquet_format.rs` file to see raw auto-gen definitions.

mod metadata;

use super::parquet_format::TimeUnit;
use super::SortingColumn;
use crate::metadata::types::ParquetType;
pub use metadata::*;

/// Types supported by Parquet.  These types are intended to be used in combination
/// with the encodings to control the on disk storage format.
/// For example Int16 is not included as a type since a good encoding of Int32
/// would handle this.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum PhysicalType {
    Boolean,
    Int32,
    Int64,
    Int96,
    Float,
    Double,
    ByteArray,
    FixedLenByteArray,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConvertedType {
    /// A BYTE_ARRAY actually contains UTF8 encoded chars.
    Utf8,
    /// A map is converted as an optional field containing a repeated key/value pair.
    Map,
    /// A key/value pair is converted into a group of two fields.
    MapKeyValue,
    /// A list is converted into an optional field containing a repeated field for its
    /// values.
    List,
    /// An enum is converted into a binary field
    Enum,
    /// A decimal value.
    /// This may be used to annotate binary or fixed primitive types. The
    /// underlying byte array stores the unscaled value encoded as two's
    /// complement using big-endian byte order (the most significant byte is the
    /// zeroth element).
    ///
    /// This must be accompanied by a (maximum) precision and a scale in the
    /// SchemaElement. The precision specifies the number of digits in the decimal
    /// and the scale stores the location of the decimal point. For example 1.23
    /// would have precision 3 (3 total digits) and scale 2 (the decimal point is
    /// 2 digits over).
    Decimal,
    /// A date stored as days since Unix epoch, encoded as the INT32 physical type.
    Date,
    /// The total number of milliseconds since midnight. The value is stored as an INT32
    /// physical type.
    TimeMillis,
    /// The total number of microseconds since midnight. The value is stored as an INT64
    /// physical type.
    TimeMicros,
    /// Date and time recorded as milliseconds since the Unix epoch.
    /// The value is stored as an `Int64` physical type.
    TimeStampMillis,
    /// Date and time recorded as microseconds since the Unix epoch.
    /// The value is stored as an `Int64` physical type.
    TimeStampMicros,
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    /// A JSON document embedded within a single UTF8 column.
    Json,
    /// A BSON document embedded within a single BINARY column.
    Bson,
    /// An interval of time.
    ///
    /// This type annotates data stored as a FIXED_LEN_BYTE_ARRAY of length 12.
    /// This data is composed of three separate little endian unsigned integers.
    /// Each stores a component of a duration of time. The first integer identifies
    /// the number of months associated with the duration, the second identifies
    /// the number of days associated with the duration and the third identifies
    /// the number of milliseconds associated with the provided duration.
    /// This duration of time is independent of any particular timezone or date.
    Interval,
}
/// Logical types used by version 2.4.0+ of the Parquet format.
///
/// This is an *entirely new* struct as of version
/// 4.0.0. The struct previously named `LogicalType` was renamed to
/// [`ConvertedType`]. Please see the README.md for more details.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogicalType {
    String,
    Map,
    List,
    Enum,
    Decimal {
        scale: i32,
        precision: i32,
    },
    Date,
    Time {
        is_adjusted_to_utc: bool,
        unit: TimeUnit,
    },
    Timestamp {
        is_adjusted_to_utc: bool,
        unit: TimeUnit,
    },
    Integer {
        bit_width: i8,
        is_signed: bool,
    },
    Unknown,
    Json,
    Bson,
    Uuid,
}

/// Representation of field types in schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Repetition {
    /// Field is required (can not be null) and each record has exactly 1 value
    Required,
    /// Field is optional (can be null) and each record has 0 or 1 values
    Optional,
    /// Field is repeated and can contain 0 or more values
    Repeated,
}

/// Encodings supported by Parquet.
/// Not all encodings are valid for all types. These enums are also used to specify the
/// encoding of definition and repetition levels.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(non_camel_case_types)]
pub enum Encoding {
    /// Default byte encoding.
    /// - BOOLEAN - 1 bit per value, 0 is false; 1 is true.
    /// - INT32 - 4 bytes per value, stored as little-endian.
    /// - INT64 - 8 bytes per value, stored as little-endian.
    /// - FLOAT - 4 bytes per value, stored as little-endian.
    /// - DOUBLE - 8 bytes per value, stored as little-endian.
    /// - BYTE_ARRAY - 4 byte length stored as little endian, followed by bytes.
    /// - FIXED_LEN_BYTE_ARRAY - just the bytes are stored.
    Plain,
    /// **Deprecated** dictionary encoding.
    ///
    /// The values in the dictionary are encoded using PLAIN encoding.
    /// Since it is deprecated, RLE_DICTIONARY encoding is used for a data page, and
    /// PLAIN encoding is used for dictionary page.
    PlainDictionary,
    /// Group packed run length encoding.
    ///
    /// Usable for definition/repetition levels encoding and boolean values.
    RLE,
    /// Bit packed encoding.
    ///
    /// This can only be used if the data has a known max width.
    /// Usable for definition/repetition levels encoding.
    BitPacked,
    /// Delta encoding for integers, either INT32 or INT64.
    ///
    /// Works best on sorted data.
    DeltaBinaryPacked,
    /// Encoding for byte arrays to separate the length values and the data.
    ///
    /// The lengths are encoded using DELTA_BINARY_PACKED encoding.
    DeltaLengthByteArray,
    /// Incremental encoding for byte arrays.
    ///
    /// Prefix lengths are encoded using DELTA_BINARY_PACKED encoding.
    /// Suffixes are stored using DELTA_LENGTH_BYTE_ARRAY encoding.
    DeltaByteArray,
    /// Dictionary encoding.
    ///
    /// The ids are encoded using the RLE encoding.
    RLE_Dictionary,
    /// Encoding for floating-point data.
    ///
    /// K byte-streams are created where K is the size in bytes of the data type.
    /// The individual bytes of an FP value are scattered to the corresponding stream and
    /// the streams are concatenated.
    /// This itself does not reduce the size of the data but can lead to better compression
    /// afterwards.
    ByteStreamSplit,
}

pub enum PageType {
    DataPageV1,
    IndexPage,
    DictionaryPage,
    DataPageV2,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Compression {
    Uncompressed,
    Snappy,
    Gzip,
    Lzo,
    Brotli,
    Lz4,
    Zstd,
    Lz4Raw,
}

/// Sort order for page and column statistics.
///
/// Types are associated with sort orders and column stats are aggregated using a sort
/// order, and a sort order should be considered when comparing values with statistics
/// min/max.
///
/// See reference in
/// <https://github.com/apache/parquet-cpp/blob/master/src/parquet/types.h>
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortOrder {
    /// Signed (either value or legacy byte-wise) comparison.
    Signed,
    /// Unsigned (depending on physical type either value or byte-wise) comparison.
    Unsigned,
    /// Comparison is undefined.
    Undefined,
}

impl SortOrder {
    pub fn is_signed(&self) -> bool {
        matches!(self, Self::Signed)
    }
}

/// Column order that specifies what method was used to aggregate min/max values for
/// statistics.
///
/// If column order is undefined, then it is the legacy behaviour and all values should
/// be compared as signed values/bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnOrder {
    /// Column uses the order defined by its logical or physical type
    /// (if there is no logical type), parquet-format 2.4.0+.
    TypeDefinedOrder(SortOrder),
    /// Undefined column order, means legacy behaviour before parquet-format 2.4.0.
    /// Sort order is always SIGNED.
    Undefined,
}

impl ColumnOrder {
    /// Returns sort order for a physical/logical type.
    pub fn get_sort_order(
        logical_type: Option<&LogicalType>,
        converted_type: Option<&ConvertedType>,
        physical_type: PhysicalType,
    ) -> SortOrder {
        // TODO: Should this take converted and logical type, for compatibility?
        match logical_type {
            Some(logical) => match logical {
                LogicalType::String | LogicalType::Enum | LogicalType::Json | LogicalType::Bson => {
                    SortOrder::Unsigned
                }
                LogicalType::Integer { is_signed, .. } => match is_signed {
                    true => SortOrder::Signed,
                    false => SortOrder::Unsigned,
                },
                LogicalType::Map | LogicalType::List => SortOrder::Undefined,
                LogicalType::Decimal { .. } => SortOrder::Signed,
                LogicalType::Date => SortOrder::Signed,
                LogicalType::Time { .. } => SortOrder::Signed,
                LogicalType::Timestamp { .. } => SortOrder::Signed,
                LogicalType::Unknown => SortOrder::Undefined,
                LogicalType::Uuid => SortOrder::Unsigned,
            },
            // Fall back to converted type
            None => Self::get_converted_sort_order(converted_type, physical_type),
        }
    }

    fn get_converted_sort_order(
        converted_type: Option<&ConvertedType>,
        physical_type: PhysicalType,
    ) -> SortOrder {
        match converted_type {
            None => Self::get_default_sort_order(physical_type),
            Some(converted_type) => {
                match converted_type {
                    // Unsigned byte-wise comparison.
                    ConvertedType::Utf8
                    | ConvertedType::Json
                    | ConvertedType::Bson
                    | ConvertedType::Enum => SortOrder::Unsigned,

                    ConvertedType::Int8
                    | ConvertedType::Int16
                    | ConvertedType::Int32
                    | ConvertedType::Int64 => SortOrder::Signed,

                    ConvertedType::UInt8
                    | ConvertedType::UInt16
                    | ConvertedType::UInt32
                    | ConvertedType::UInt64 => SortOrder::Unsigned,

                    // Signed comparison of the represented value.
                    ConvertedType::Decimal => SortOrder::Signed,

                    ConvertedType::Date => SortOrder::Signed,

                    ConvertedType::TimeMillis
                    | ConvertedType::TimeMicros
                    | ConvertedType::TimeStampMillis
                    | ConvertedType::TimeStampMicros => SortOrder::Signed,

                    ConvertedType::Interval => SortOrder::Undefined,

                    ConvertedType::List | ConvertedType::Map | ConvertedType::MapKeyValue => {
                        SortOrder::Undefined
                    }
                }
            }
        }
    }

    fn get_default_sort_order(physical_type: PhysicalType) -> SortOrder {
        use PhysicalType::*;
        match physical_type {
            // Order: false, true
            Boolean => SortOrder::Unsigned,
            Int32 | Int64 => SortOrder::Signed,
            Int96 => SortOrder::Undefined,
            // Notes to remember when comparing float/double values:
            // If the min is a NaN, it should be ignored.
            // If the max is a NaN, it should be ignored.
            // If the min is +0, the row group may contain -0 values as well.
            // If the max is -0, the row group may contain +0 values as well.
            // When looking for NaN values, min and max should be ignored.
            Float | Double => SortOrder::Signed,
            // Unsigned byte-wise comparison
            ByteArray | FixedLenByteArray => SortOrder::Unsigned,
        }
    }
}
