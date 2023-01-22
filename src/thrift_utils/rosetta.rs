//! Contains Rust mappings for Thrift definition.
//! Refer to `parquet_format.rs` file to see raw auto-gen definitions.

use crate::thrift_utils::parquet_format::TimeUnit;

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
    Interval
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
    ByteStreamSplit
}

pub enum PageType {
    DataPageV1,
    IndexPage,
    DictionaryPage,
    DataPageV2,
}