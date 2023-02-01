use crate::errors::{ParquetError, ParquetResult};
use crate::metadata::{PhysicalType, TStatistic};
use crate::physical::NativeType;

#[derive(Debug, Clone, PartialEq)]
pub enum Statistics {
    Boolean(ValueStatistics<bool>),
    Int32(ValueStatistics<i32>),
    Int64(ValueStatistics<i64>),
    Float(ValueStatistics<f32>),
    Double(ValueStatistics<f64>),
    // Maybe support these later
    // Int96(ValueStatistics<i128>),
    // ByteArray(ValueStatistics<ByteArray>),
    // FixedLenByteArray(ValueStatistics<FixedLenByteArray>),
}

/// Statistics for a particular `ParquetValueType`
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ValueStatistics<T> {
    min: Option<T>,
    max: Option<T>,
    // Distinct count could be omitted in some cases
    distinct_count: Option<usize>,
    null_count: usize,

    /// If `true` populate the deprecated `min` and `max` fields instead of
    /// `min_value` and `max_value`
    is_min_max_deprecated: bool,
}

fn decode_primitive<T: NativeType>(data: &[u8]) -> T {
    T::from_le_bytes(data[..std::mem::size_of::<T>()].try_into().unwrap())
}

impl Statistics {
    pub fn from_thrift(
        physical_type: PhysicalType,
        statistics: TStatistic,
    ) -> ParquetResult<Option<Statistics>> {
        let null_count: usize = statistics.null_count.unwrap_or(0).try_into().map_err(|_| {
            ParquetError::InvalidFormat("Negative 'null_count' found in statistics".into())
        })?;
        let distinct_count: Option<usize> = statistics
            .distinct_count
            .map(|v| v.try_into())
            .transpose()
            .map_err(|_| {
                ParquetError::InvalidFormat("Negative 'distinct_count' found in statistics".into())
            })?;

        // Whether statistics use deprecated min/max fields
        let old_format = statistics.min_value.is_none() && statistics.max_value.is_none();

        let (min_encoded, max_encoded) = if old_format {
            (statistics.min, statistics.max)
        } else {
            (statistics.min_value, statistics.max_value)
        };

        let out = match physical_type {
            PhysicalType::Double => {
                let min = min_encoded.map(|data| decode_primitive::<f64>(&data));
                let max = max_encoded.map(|data| decode_primitive::<f64>(&data));

                Statistics::Double(ValueStatistics {
                    min,
                    max,
                    null_count,
                    distinct_count,
                    is_min_max_deprecated: old_format,
                })
            }
            PhysicalType::Float => {
                let min = min_encoded.map(|data| decode_primitive::<f32>(&data));
                let max = max_encoded.map(|data| decode_primitive::<f32>(&data));

                Statistics::Float(ValueStatistics {
                    min,
                    max,
                    null_count,
                    distinct_count,
                    is_min_max_deprecated: old_format,
                })
            }
            PhysicalType::Int32 => {
                let min = min_encoded.map(|data| decode_primitive::<i32>(&data));
                let max = max_encoded.map(|data| decode_primitive::<i32>(&data));

                Statistics::Int32(ValueStatistics {
                    min,
                    max,
                    null_count,
                    distinct_count,
                    is_min_max_deprecated: old_format,
                })
            }
            PhysicalType::Int64 => {
                let min = min_encoded.map(|data| decode_primitive::<i64>(&data));
                let max = max_encoded.map(|data| decode_primitive::<i64>(&data));

                Statistics::Int64(ValueStatistics {
                    min,
                    max,
                    null_count,
                    distinct_count,
                    is_min_max_deprecated: old_format,
                })
            }
            PhysicalType::Boolean => {
                let min = min_encoded.map(|data| data[0] != 0);
                let max = max_encoded.map(|data| data[0] != 0);

                Statistics::Boolean(ValueStatistics {
                    min,
                    max,
                    null_count,
                    distinct_count,
                    is_min_max_deprecated: old_format,
                })
            }
            _ => return Ok(None),
        };
        Ok(Some(out))
    }
}
