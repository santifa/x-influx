//! This module defines a basic mapping for
//! data from some source to relevant influxdb
//! fields as well as an abstraction for implementing
//! some data mapper.

use error::{ConvertError, ConvertResult};
use client::InfluxClient;

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

impl Layout {
    /// Returns the position of the measure column or error if not found.
    fn get_measure_col(&self, data: &[&str]) -> ConvertResult<usize> {
        data.iter()
            .position(|&e| e == self.measure)
            .ok_or(ConvertError::NotFound("Measure"))
    }

    /// Returns the position of the time column or error if not found.
    fn get_time_col(&self, data: &[&str]) -> ConvertResult<usize> {
        data.iter()
            .position(|&e| e == self.time)
            .ok_or(ConvertError::NotFound("Timestamp"))
    }

    /// Return an array of columns corresponding to the tags
    /// or an empty list of nothing is found.
    fn get_tag_cols(&self, data: &[&str]) -> Vec<usize> {
        self.tags
            .clone()
            .into_iter()
            .filter_map(|t| data.iter().position(|e| e == &&t))
            .collect()
    }
}

/// A mapper applys the given `Layout` to some
/// piece of data.
pub trait Mapper {
    /// Returns error if the mapping or sending process failed.
    fn import(&self, layout: &Layout, client: &InfluxClient) -> ConvertResult<()>;
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_get_measure_col() {
        let data = vec!["a", "b", "c", "d", "", "f"];
        let mut layout = Layout::default();

        layout.measure = "a".into();
        assert_eq!(layout.get_measure_col(&data).unwrap(), 0);
        layout.measure = "b".into();
        assert_eq!(layout.get_measure_col(&data).unwrap(), 1);
        layout.measure = "d".into();
        assert_eq!(layout.get_measure_col(&data).unwrap(), 3);
        layout.measure = "f".into();
        assert_eq!(layout.get_measure_col(&data).unwrap(), 5);
        layout.measure = "data".into();
        assert!(layout.get_measure_col(&data).is_err());
    }

    #[test]
    fn test_get_time_col() {
        let data = vec!["a", "b", "c", "d", "", "f"];
        let mut layout = Layout::default();

        layout.time = "a".into();
        assert_eq!(layout.get_time_col(&data).unwrap(), 0);
        layout.time = "f".into();
        assert_eq!(layout.get_time_col(&data).unwrap(), 5);
        layout.time = "data".into();
        assert!(layout.get_time_col(&data).is_err());
    }

    #[test]
    fn test_get_tag_cols() {
        let data = vec!["a", "b", "c", "d", "", "f"];
        let mut layout = Layout::default();

        layout.time = "data".into();
        assert_eq!(layout.get_tag_cols(&data), vec![]);
        layout.tags = vec!["a".into()];
        assert_eq!(layout.get_tag_cols(&data), vec![0]);
        layout.tags = vec!["f".into()];
        assert_eq!(layout.get_tag_cols(&data), vec![5]);
        layout.tags = vec!["f".into(), "a".into()];
        assert_eq!(layout.get_tag_cols(&data), vec![5, 0]);
    }
}
