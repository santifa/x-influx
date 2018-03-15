//! A basic csv import which ca skip
//! front rows and ignore columns.
use super::*;

#[derive(Debug)]
pub struct Csv {
    files: Vec<String>,
    batch: bool,
    delimiter: char,
    first_row: u32,
}

impl Csv {
    pub fn new(files: Vec<String>, batch: bool, del: char, row: u32) -> Csv {
        Csv {
            files: files,
            batch: batch,
            delimiter: del,
            first_row: row,
        }
    }
}

impl Mapper for Csv {
    fn import(&self, layout: &Layout, client: &InfluxClient) -> ConvertResult<()> {
        Ok(())
    }
}
