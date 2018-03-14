/// The basic conversion between some data and influx is a mapping
/// between the value names for a series and a maybe empty list
/// of additional tags for the series.
/// At last a mapping for timestamps is needed, with name and time format.
/// See https://docs.rs/chrono/0.4.0/chrono/format/strftime/index.html
/// for time formating.

use error::ConvertResult;
use client::InfluxClient;

#[derive(Debug, Clone)]
pub struct Layout {
    pub measure: String,
    pub tags: Vec<String>,
    pub time: String,
    pub tformat: String,
}

impl Default for Layout {
    fn default() -> Self {
        Layout {
            measure: String::from("data"),
            tags: [].to_vec(),
            time: String::from("timestamp"),
            tformat: String::from("%F %H:%M:%S"),
        }
    }
}

impl Layout {
    // search for the positions within the header row
    pub fn apply(&self, data: &[&str]) -> ConvertResult<(usize, Vec<usize>, usize)> {
        let val = data.iter()
            .position(|&e| e == self.measure)
            .ok_or(ConvertError::NotFound("Value"))?;

        // filter positions of tags
        let mut tags: Vec<usize> = Vec::new();
        for ref tag in &self.tags {
            if let Some(p) = data.iter().position(|e| e == tag) {
                tags.push(p);
            }
        }

        let time = data.iter()
            .position(|&e| e == self.time)
            .ok_or(ConvertError::NotFound("Timestamp"))?;

        Ok((val, tags, time))
    }
}

/// A mapper applys the given `Layout` to some
/// piece of data.
pub trait Mapper {
    /// Returns error if the mapping or sending process failed.
    fn apply(&self, layout: Layout, client: InfluxClient) -> ConvertResult<()>;
}
