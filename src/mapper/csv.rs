//! A basic csv import which ca skip
//! front rows and ignore columns.
use super::*;

use std::fs::File;
use std::iter::Iterator;
use std::io::{BufRead, BufReader, Lines, Read};
use client::Message;
use chrono::{TimeZone, Utc};

/// To react to inappropriate csv files
/// we can change the delimiter, skip initial
/// rows and only columns named by the layout
/// are inserted.
///
/// Todo: [ ] Merge columns
///       [ ] Batch mode
#[derive(Debug)]
pub struct Csv {
    files: Vec<String>,
    batch: bool,
    delimiter: char,
    first_row: usize,
}

impl Csv {
    pub fn new(files: Vec<String>, batch: bool, del: char, row: usize) -> Csv {
        Csv {
            files: files,
            batch: batch,
            delimiter: del,
            first_row: row,
        }
    }

    /// Return the position of some field in the header row or not found error.
    fn find_pos(&self, name: &str, data: &[String]) -> ConvertResult<usize> {
        data.iter()
            .position(|&ref e| e == name)
            .ok_or(ConvertError::NotFound(name.into()))
    }

    /// Return an array of column positions corresponding to the names
    /// or an empty list of nothing is found.
    fn find_positions(&self, names: &[String], data: &[String]) -> Vec<usize> {
        names
            .iter()
            .filter_map(|t| self.find_pos(t, data).ok())
            .collect()
    }

    /// Split a line with a given delimiter
    fn split(&self, line: &str) -> Vec<String> {
        line.split(self.delimiter).map(|e| e.into()).collect()
    }

    /// skip initial rows and return header line if found.
    fn skip<R: Read>(&self, lines: Lines<BufReader<R>>) -> ConvertResult<String> {
        match lines.skip(self.first_row).next() {
            None => Err(ConvertError::Import("Header not found".into())),
            Some(l) => l.map_err(|e| ConvertError::Import(format!("{:?}", e))),
        }
    }

    /// Search for the column positions of measure, time and tags columns.
    fn read_header<R: Read>(
        &self,
        layout: &Layout,
        lines: Lines<BufReader<R>>,
    ) -> ConvertResult<(usize, usize, Vec<usize>)> {
        let header = try!(self.skip(lines).and_then(|e| Ok(self.split(&e))));
        let measure = try!(self.find_pos(&layout.measure, header.as_slice()));
        let time = try!(self.find_pos(&layout.time, header.as_slice()));
        let tags = self.find_positions(&layout.tags, header.as_slice());
        Ok((measure, time, tags))
    }

    fn open(&self, f: &str) -> ConvertResult<BufReader<File>> {
        File::open(f)
            .map_err(|e| ConvertError::Import(format!("Failed to import file {} : {}", f, e)))
            .and_then(|e| Ok(BufReader::new(e)))
    }
}

impl Mapper for Csv {
    // too much trys where skipping this row is a better solution
    fn import(&self, layout: &Layout, client: &InfluxClient) -> ConvertResult<()> {
        for file in &self.files {
            debug!(format!("Opening {:?}", file));
            let reader = try!(self.open(file));

            let (measure, time, tags) = match self.read_header(layout, reader.lines()) {
                Err(e) => {
                    error!(format!("Failed to parse header: {}", e));
                    continue;
                }
                Ok((m, t, ta)) => (m, t, ta),
            };

            debug!(format!(
                "Found header: Measure: {}, time: {}, tags: {:?}",
                measure, time, tags
            ));

            let reader = try!(self.open(file));
            let mut lines = reader.lines().skip(self.first_row + 1);
            for line in lines {
                let data = try!(line.and_then(|l| Ok(self.split(&l))));

                let value = (layout.measure.clone(), data[measure].clone());
                let timestamp = match Utc.datetime_from_str(&data[time], &layout.tformat) {
                    Ok(t) => t,
                    Err(e) => {
                        error!(format!("Failed to parse date {}", e));
                        continue;
                    }
                };

                let mut t = vec![];
                for (i, n) in tags.iter().enumerate() {
                    t.push((layout.tags[i].clone(), data[*n].clone()));
                }

                let msg = Message::new(timestamp, value, t);
                debug!(format!("Sending: {:?}", msg));
                if let Err(e) = client.send(msg) {
                    error!(format!("Failed to import file: {}", e));
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use client::test;

    #[test]
    fn test_get_header_positions() {
        let layout = Layout::default();
        let csv = Csv::new(vec![], false, ',', 2);
        let data = BufReader::new("dsf\nnsdfsdf\ntimestamp,data".as_bytes()).lines();

        let header = csv.read_header(&layout, data);
        assert!(header.is_ok());
        assert_eq!(header.unwrap(), (1, 0, vec![]));
    }

    #[test]
    fn test_no_header() {
        let layout = Layout::default();
        let csv = Csv::new(vec![], false, ',', 2);
        let data = BufReader::new("".as_bytes()).lines();

        let header = csv.read_header(&layout, data);
        assert!(header.is_err());
        assert_eq!(format!("{}", header.err().unwrap()), "Header not found.");
    }

    #[test]
    fn test_import() {
        let client = test::start_client();
        let mut layout = Layout::default();
        layout.measure = "Profilwert kWh".into();
        layout.tformat = "%d.%m.%Y %H:%M".into();
        let csv = Csv::new(vec!["assets/test.csv".into()], false, ';', 10);

        let res = csv.import(&layout, &client);
        assert!(res.is_ok());
        layout.tags = vec!["Status".into()];
        let res = csv.import(&layout, &client);
        assert!(res.is_ok());
    }

    #[test]
    fn test_get_single_col() {
        let csv = Csv::new(vec![], false, ',', 2);
        let data = vec![
            "a".into(),
            "b".into(),
            "c".into(),
            "d".into(),
            "".into(),
            "f".into(),
        ];

        let mut layout = "a";
        assert_eq!(csv.find_pos(&layout, &data).unwrap(), 0);
        layout = "f";
        assert_eq!(csv.find_pos(&layout, &data).unwrap(), 5);
        layout = "data";
        assert!(csv.find_pos(&layout, &data).is_err());
    }

    #[test]
    fn test_get_multiple_cols() {
        let csv = Csv::new(vec![], false, ',', 2);
        let data = vec![
            "a".into(),
            "b".into(),
            "c".into(),
            "d".into(),
            "".into(),
            "f".into(),
        ];

        let mut layout = vec!["data".into()];
        assert_eq!(csv.find_positions(&layout, &data), vec![]);
        layout = vec!["f".into()];
        assert_eq!(csv.find_positions(&layout, &data), vec![5]);
        layout = vec!["f".into(), "a".into()];
        assert_eq!(csv.find_positions(&layout, &data), vec![5, 0]);
    }
}
