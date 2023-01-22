mod errors;
mod metadata;
mod reader;
mod statistics;
mod thrift_defined;
mod types;

pub(crate) static MAGIC_NUMBER: &[u8; 4] = b"PAR1";
pub(crate) const FOOTER_SIZE: usize = 8;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::get_metadata;
    use std::fs::File;
    use std::io::Read;

    #[test]
    fn test_init() {
        let mut file = File::open("files/test1.parquet").unwrap();

        let mut buf = vec![];
        file.read_to_end(&mut buf);

        let md = get_metadata(buf).unwrap();
        dbg!(md);
    }
}
