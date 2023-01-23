#[derive(Debug, Clone, PartialEq)]
pub enum Statistics {
    Boolean(ValueStatistics<bool>),
    Int32(ValueStatistics<i32>),
    Int64(ValueStatistics<i64>),
    Int96(ValueStatistics<i128>),
    Float(ValueStatistics<f32>),
    Double(ValueStatistics<f64>),
    // Maybe support these later
    // ByteArray(ValueStatistics<ByteArray>),
    // FixedLenByteArray(ValueStatistics<FixedLenByteArray>),
}

/// Statistics for a particular `ParquetValueType`
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ValueStatistics<T> {
    min: Option<T>,
    max: Option<T>,
    // Distinct count could be omitted in some cases
    distinct_count: Option<u64>,
    null_count: u64,

    /// If `true` populate the deprecated `min` and `max` fields instead of
    /// `min_value` and `max_value`
    is_min_max_deprecated: bool,

    /// If `true` the statistics are compatible with the deprecated `min` and
    /// `max` fields. See [`ValueStatistics::is_min_max_backwards_compatible`]
    is_min_max_backwards_compatible: bool,
}
