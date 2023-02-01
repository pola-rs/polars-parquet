use crate::metadata::PhysicalType;
/// A physical native representation of a Parquet fixed-sized type.
pub trait NativeType: 'static + Copy + Clone {
    type Bytes: AsRef<[u8]> + for<'a> TryFrom<&'a [u8], Error = std::array::TryFromSliceError>;

    fn from_le_bytes(bytes: Self::Bytes) -> Self;
}

macro_rules! native {
    ($type:ty, $physical_type:expr) => {
        impl NativeType for $type {
            type Bytes = [u8; std::mem::size_of::<Self>()];

            #[inline]
            fn from_le_bytes(bytes: Self::Bytes) -> Self {
                Self::from_le_bytes(bytes)
            }
        }
    };
}

native!(i32, PhysicalType::Int32);
native!(i64, PhysicalType::Int64);
native!(f32, PhysicalType::Float);
native!(f64, PhysicalType::Double);
