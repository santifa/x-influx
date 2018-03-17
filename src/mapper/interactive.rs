//! The interactive import mapper lets you
//! define the influx input by hand.

use super::*;

use std::io::{self, Write};
use chrono::{TimeZone, Utc};
use client::Message;

/// Stub struct for satisfying the trait
/// and some handy input functions.
#[derive(Debug)]
pub struct Interactive {}

impl Interactive {
    /// Read some string from stdin and trim.
    fn read_string(&self, msg: &str) -> ConvertResult<String> {
        let mut buffer = String::new();
        print!("{}", msg);
        try!(io::stdout().flush());
        try!(io::stdin().read_line(&mut buffer));
        Ok(buffer.trim().into())
    }

    /// Return user input tuple or error if some bad io happens.
    fn read_input(&self, layout: &Layout) -> ConvertResult<(String, String, Vec<String>)> {
        let measure = try!(self.read_string(&format!("Measurement [{}]: ", layout.measure)));
        let time = try!(self.read_string(&format!("Time [{}][{}]: ", layout.time, layout.tformat)));
        let tags: Vec<String> =
            try!(self.read_string(&format!("Tags [{}]: ", layout.tags.join(","))))
                .split(",")
                .map(|s| s.into())
                .collect();
        Ok((measure, time, tags))
    }
}

/// The interactive mode allows to provide all needed
/// input data by hand.
impl Mapper for Interactive {
    fn import(&self, layout: &Layout, client: &InfluxClient) -> ConvertResult<()> {
        println!("Interactive mode...");
        println!("Insert tags comma separated.\nExit with C-d");

        loop {
            let (measure, time, tags) = match self.read_input(layout) {
                Ok((m, t, ta)) => (m, t, ta),
                Err(e) => {
                    error!(format!("Failure: {}", e));
                    continue;
                }
            };

            let time = match Utc.datetime_from_str(&time, &layout.tformat) {
                Ok(t) => t,
                Err(e) => {
                    error!(format!("Parsing time failed: {}", e));
                    continue;
                }
            };

            let tags = layout
                .tags
                .iter()
                .filter(|e| !e.is_empty())
                .map(|e| e.to_string())
                .zip(tags)
                .collect();

            debug!(format!("{},{},{:?}", measure, time, tags));
            let msg = Message::new(time, (layout.measure.clone(), measure), tags);
            if let Err(e) = client.send(msg) {
                error!(format!("Sending to background client failed: {}", e));
            }
        }
    }
}
