//! This module defines a basic mapping for
//! data from some source to relevant influxdb
//! fields as well as an abstraction for implementing
//! some data mapper.

pub use error::{ConvertError, ConvertResult};
use client::InfluxClient;

pub use self::interactive::Interactive;
pub use self::csv::Csv;

mod interactive;
mod csv;

/// A layout describes the names for the database
/// fields used by influx.
/// See https://docs.rs/chrono/0.4.0/chrono/format/strftime/index.html
/// for time formating.
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

/// A mapper applys the given `Layout` to some
/// piece of data.
pub trait Mapper {
    /// Returns error if the mapping or sending process failed.
    fn import(&self, layout: &Layout, client: &InfluxClient) -> ConvertResult<()>;
}
