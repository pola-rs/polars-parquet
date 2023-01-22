mod metadata;
mod reader;
mod errors;
mod thrift_utils;
mod types;
mod statistics;

pub(crate) static MAGIC_NUMBER: &[u8;4] =  b"PAR1";
pub(crate) const FOOTER_SIZE: usize = 8;



#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::io::Read;
    use crate::metadata::get_metadata;

    #[test]
    fn test_init() {
        let mut file = File::open("files/test1.parquet").unwrap();

        let mut buf = vec![];
        file.read_to_end(&mut buf);

        // dbg!(String::from_utf8_lossy(&buf));
        get_metadata(buf).unwrap();
    }
}
