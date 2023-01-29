mod compression;
mod data;
mod errors;
pub(crate) mod metadata;
mod reader;

pub(crate) static MAGIC_NUMBER: &[u8; 4] = b"PAR1";
pub(crate) const FOOTER_SIZE: usize = 8;

pub use data::read_row_group;

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

        let metadata = get_metadata(buf.as_slice()).unwrap();

        read_row_group(&buf.as_slice(), &metadata.row_groups[0], 0);
    }
}
