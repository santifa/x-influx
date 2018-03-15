//    x-influx is a simple cli tool to import data into influxdb.
//    Copyright (C) 2018  Henrik Jürges
//
//    This program is free software: you can redistribute it and/or modify
//    it under the terms of the GNU General Public License as published by
//    the Free Software Foundation, either version 3 of the License, or
//    (at your option) any later version.
//
//    This program is distributed in the hope that it will be useful,
//    but WITHOUT ANY WARRANTY; without even the implied warranty of
//    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//    GNU General Public License for more details.
//
//    You should have received a copy of the GNU General Public License
//    along with this program.  If not, see <http://www.gnu.org/licenses/>.
//! x-influx
//!
//! This program feeds influxdb with input data
//! from some source.
//!
//! License is GPL
//! See `USAGE` for arguments.
extern crate chrono;
extern crate docopt;
extern crate influent;
#[macro_use]
extern crate serde_derive;

use std::io::{self, Write};
use std::fs::File;
use std::iter::Iterator;
use std::io::{BufRead, BufReader, Read};

use docopt::Docopt;
use chrono::{TimeZone, Utc};

#[macro_use]
mod error;
mod client;
mod mapping;

use error::{ConvertError, ConvertResult};
use client::{InfluxClient, Message};
use mapping::{Layout, Mapper};

const VERSION: &'static str = "
Version 0.5 of x-influx.
This is a simple cli tool to import data into influxdb.
    
x-influx  Copyright (C) 2018  Henrik Jürges
This program comes with ABSOLUTELY NO WARRANTY;
This is free software, and you are welcome to redistribute it
under certain conditions; see LICENSE file for details.
";

/// Program Flags and Options
const USAGE: &'static str = "
Usage: 
  x-influx i [options]
  x-influx b [options] <file>... 
  x-influx [options] <file>... 
  x-influx (-h | --help) | --version

Options:
  -h, --help           Print this help message.
  -v, --verbose        Enable verbose logging.
  -V, --version        Shows version and license information.
  -u, --user USER      Username for influxdb [default: test].
  -p, --password PASS  Password for influxdb [default: ].
  -d, --database DB    Influx database [default: test].
  -s, --server SRV     The influxdb server for import 
                       [default: http://localhost:8086].
  
  -S, --series VAL     Name of the measuremnt series [default: series]
  -m, --measure VAL    Name of the measurement value [default: data].
  -t, --tags VAL       Comma seperated list of tags associated to a value.
  -T, --time VAL       Name of the timestamp column [default: timestamp].
  -f, --format FMT     The timestamp format [default: %F %H:%M:%S]
                       See https://docs.rs/chrono/0.4.0/chrono/format/strftime/index.html

  -D, --delimiter DEL  Use another csv delimiter [default: ,].
  --skip-rows NUM      Remove first NUM lines from file [default: 0].
";

#[derive(Debug, Deserialize)]
struct Args {
    flag_verbose: bool,
    flag_version: bool,
    cmd_i: bool, // interactive mode
    cmd_b: bool, // batch mode
    flag_user: String,
    flag_password: String,
    flag_database: String,
    flag_server: String,
    flag_series: String,
    flag_measure: String,
    flag_tags: String,
    flag_time: String,
    flag_format: String,
    flag_delimiter: char,
    flag_skip_rows: u32,
    arg_file: Vec<String>,
}

#[derive(Debug)]
struct Interactive {}

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
        Ok(())
    }
}

struct Csv {}

impl Mapper for Csv {
    fn import(&self, layout: &Layout, client: &InfluxClient) -> ConvertResult<()> {
        Ok(())
    }
}

/// A basic CSV file uses the comma for seperation and
/// has no irrelevant rows or columns.
///
/// A CSV file may be inappropriate for instant parsing,
/// so this allows some modification beforehand.
/// One can kill lines in from the start or remove entire columns.
/// Another use case is to combine columns (not done).
#[derive(Debug, Clone)]
struct CsvLayout {
    layout: Layout,
    del: char,
    skip_rows: usize,
}

impl Default for CsvLayout {
    fn default() -> CsvLayout {
        CsvLayout {
            layout: Layout::default(),
            del: ',',
            skip_rows: 0,
        }
    }
}

impl CsvLayout {
    fn new(d: &str, r: &str, l: Layout) -> CsvLayout {
        CsvLayout {
            del: d.chars().next().unwrap_or(','),
            skip_rows: r.parse::<usize>().unwrap_or(0),
            layout: l,
        }
    }

    fn convert<R: Read>(&self, client: &InfluxClient, reader: BufReader<R>) -> ConvertResult<()> {
        let mut csv_start = reader.lines().skip(self.skip_rows);
        let header: String = match csv_start.next() {
            Some(Ok(h)) => h,
            _ => {
                return Err(ConvertError::Import("header"));
            }
        };

        // get columns of interrest
        let h: Vec<&str> = header.split(self.del).collect();
        let (val_col, tags_col, time_col) = (0, vec![0], 0); //self.layout.apply(h.as_slice())?;

        for (i, row) in csv_start.enumerate() {
            if let Ok(r) = row {
                //let mut msg = Message::default();
                let mut value = ("".into(), "".into());
                let mut time = Utc::now();
                let mut tags = vec![];

                for (j, col) in r.split(self.del).enumerate() {
                    if val_col == j {
                        value = (self.layout.measure.clone(), col.to_owned());
                    } else if time_col == j {
                        // either use the current timezone or a give one
                        time = match Utc.datetime_from_str(col, &self.layout.tformat) {
                            Ok(t) => t,
                            Err(e) => {
                                info!(format!("Failed to parse Date at {}, {}; {}", i, j, e));
                                continue;
                            }
                        };
                    } else if let Some(pos) = (&tags_col).into_iter().position(|e| e == &j) {
                        tags.push((self.layout.tags[pos].to_owned(), col.to_owned()));
                        //      msg.tags
                        //          .push((self.layout.tags[pos].to_owned(), col.to_owned()));
                    }
                }
                let msg = Message::new(time, value, tags);
                debug!(format!("msg: {:?}", msg));
                if let Err(e) = client.send(msg) {
                    info!(format!("Error: Failed to send message. {}", e));
                }
            } else {
                info!(format!("Error: Failed to read row {}", i));
            }
        }
        Ok(())
    }
}

fn main() {
    let _args: Args = Docopt::new(USAGE)
        .and_then(|d| d.deserialize())
        .unwrap_or_else(|e| e.exit());

    if _args.flag_version {
        println!("{}", VERSION);
    }

    if _args.flag_verbose {
        set_debug!();
    }

    debug!(format!("{:?}", _args));
    let client = match InfluxClient::new(
        _args.flag_server,
        _args.flag_user,
        _args.flag_password,
        _args.flag_database,
        _args.flag_series,
    ) {
        Ok(c) => {
            info!("Background influx client up and running.");
            c
        }
        Err(e) => {
            println!("Failed to start client. {}", e);
            return;
        }
    };

    let layout = Layout {
        measure: _args.flag_measure,
        tags: _args.flag_tags.split(',').map(|e| e.to_owned()).collect(),
        time: _args.flag_time,
        tformat: _args.flag_format,
    };
    debug!(format!("{:?}", layout));

    let mapper: Box<Mapper> = match _args.cmd_i {
        true => Box::new(Interactive {}),
        false => Box::new(Csv {}),
    };

    if let Err(e) = mapper.import(&layout, &client) {
        error!(format!("Import failed {}", e));
    }
    /*
    let layout = CsvLayout::new(
        _args.get_str("-D"),
        _args.get_str("--skip-rows"),
        Layout::new(
            _args.get_str("-m"),
            _args.get_str("-t"),
            (_args.get_str("-T"), _args.get_str("-f")),
        ),
    );
    debug!(&layout);

    for file in _args.get_vec("<file>") {
        if let Ok(reader) = File::open(file).map(|e| BufReader::new(e)) {
            if let Err(e) = layout.convert(&client, reader) {
                info!(format!(
                    "Error: Failed to import file {}. Reason: {}",
                    file, e
                ));
                // panic if we can not quit the influx client
                //assert!(client.send(None).is_ok());
            }
        } else {
            info!(format!("Error: Failed to open file {}.", file));
        }
    }
*/
    match client.join() {
        Ok(_) => info!("Gracefull shutdown."),
        Err(e) => error!(format!("{}", e)),
    }
}

// Unit Tests
#[cfg(test)]
mod test {
    /*use super::*;
    use std::sync::mpsc::channel;

    fn expected(l: CsvLayout, del: char, sr: usize) {
        assert_eq!(l.del, del);
        assert_eq!(l.skip_rows, sr);
    }
    
    #[test]
    fn test_csv_layout() {
        let layout = CsvLayout::new(";", "-1", Layout::default());
        expected(layout, ';', 0);
        let layout = CsvLayout::new("\t", "3", Layout::default());
        expected(layout, '\t', 3);
    }

    #[test]
    fn test_layout_with_err() {
        let layout = CsvLayout::new("", "blub", Layout::default());
        expected(layout, ',', 0);
    }

    #[test]
    fn test_layout_apply() {
        let mut layout = Layout::default();
        let pos = layout.apply(&["test", "timestamp", "data"]);
        assert_eq!(pos.unwrap(), (2, [].to_vec(), 1));

        layout.value = String::from("stand in kw/h");
        layout.tags = vec![
            String::from("bereich"),
            String::from("plz"),
            String::from("halle"),
        ];
        layout.time = String::from("datum");

        let pos = layout.apply(&[
            "datum",
            "",
            "stand in kw/h",
            "test",
            "bereich",
            "halle",
            "plz",
        ]);
        assert_eq!(pos.unwrap(), (2, [4, 6, 5].to_vec(), 0));
    }

    #[test]
    fn test_layout_apply_wrong() {
        let mut layout = Layout::default();
        layout.value = String::from("val");
        let pos = layout.apply(&["datum", "", ""]);
        assert_eq!(pos.err().unwrap(), ConvertError::NotFound("Value"));

        layout.value = String::from("datum");
        layout.time = String::from("val");
        let pos = layout.apply(&["datum", "", ""]);
        assert_eq!(pos.err().unwrap(), ConvertError::NotFound("Timestamp"));
    }

    #[test]
    fn test_influx_client() {
        let host = vec!["http://localhost:8086"];
        let (handler, tx) = start_influxdb_client(
            host[0].to_owned(),
            "testuser".to_owned(),
            "testpass".to_owned(),
            "test".to_owned(),
            "try".into(),
            &Logger(false),
        );
        let time = Utc::now();

        let msg = Message {
            time: time.timestamp(),
            value: (String::from("power"), String::from("1")),
            tags: vec![],
        };
        assert!(tx.send(Some(msg)).is_ok());
        assert!(tx.send(None).is_ok());
        assert!(handler.unwrap().join().is_ok());

        //TODO verify result
/*        let cred = Credentials {
            username: "",
            password: "",
            database: "test",
        };
        let client = create_client(cred, host);*/    }

    fn expected_msg(m: Message, v: (&str, &str), t: Vec<(String, String)>, time: i64) {
        assert_eq!(&m.value.0, v.0);
        assert_eq!(&m.value.1, v.1);
        assert_eq!(m.tags, t);
        assert_eq!(m.time, time);
    }

    //    #[test]
    fn test_simple_import() {
        use std::io::Cursor;

        let csv = "timestamp,data\n2017-10-10 00:00:00,0\n2017-10-10 00:01:00,1";
        let csv_layout = CsvLayout::default();
        let reader = BufReader::new(Cursor::new(csv));
        let (tx, rx) = channel();

        let res = csv_layout.convert(&tx, reader, &Logger(false));
        assert_eq!(res, Ok(()));

        let msg = rx.recv().unwrap().unwrap();
        let time = Utc.datetime_from_str("2017-10-10 00:00:00", "%F %H:%M:%S")
            .unwrap();
        expected_msg(msg, ("data", "0"), vec![], time.timestamp());

        let msg = rx.recv().unwrap().unwrap();
        let time = Utc.datetime_from_str("2017-10-10 00:01:00", "%F %H:%M:%S")
            .unwrap();
        expected_msg(msg, ("data", "1"), vec![], time.timestamp());

        let msg = rx.recv().unwrap();
        assert!(msg.is_none());
    }

    #[test]
    fn test_complex_import() {
        use std::io::Cursor;

        let csv = "timestamp;Profilwert kWh;P in kW;Status
01.01.2016 00:15;108;432;220
01.01.2016 00:30;103.5;414;220
01.01.2016 00:45;103.5;414;220
01.01.2016 01:00;103.5;414;220
";
        let reader = BufReader::new(Cursor::new(csv));
        let mut csv_layout = CsvLayout::default();
        csv_layout.del = ';';
        csv_layout.layout.value = "Profilwert kWh".to_owned();
        csv_layout.layout.tformat = "%d.%m.%Y %R".to_owned();
        let (tx, rx) = channel();

        let res = csv_layout.convert(&tx, reader, &Logger(false));
        assert_eq!(res, Ok(()));

        let msg = rx.recv().unwrap().unwrap();
        let time = Utc.datetime_from_str("01.01.2016 00:15", "%d.%m.%Y %R")
            .unwrap();
        expected_msg(msg, ("Profilwert kWh", "108"), vec![], time.timestamp());

        let msg = rx.recv().unwrap().unwrap();
        let time = Utc.datetime_from_str("01.01.2016 00:30", "%d.%m.%Y %R")
            .unwrap();
        expected_msg(msg, ("Profilwert kWh", "103.5"), vec![], time.timestamp());
    }

    //#[test]
    fn test_import() {
        use std::io::Cursor;

        let csv = "timestamp;Profilwert kWh;P in kW;Status
01.01.2016 00:15;108;432;220
01.01.2016 00:30;103.5;414;220
01.01.2016 00:45;103.5;414;220
01.01.2016 01:00;104.5;414;220
";
        let reader = BufReader::new(Cursor::new(csv));
        let mut csv_layout = CsvLayout::default();
        csv_layout.del = ';';
        csv_layout.layout.value = "Profilwert kWh".to_owned();
        csv_layout.layout.tformat = "%d.%m.%Y %R".to_owned();

        let (handle, tx) = start_influxdb_client(
            "http://localhost:8086".to_owned(),
            "testuser".to_owned(),
            "testpass".to_owned(),
            "test".to_owned(),
            "try".into(),
            &Logger(false),
        );
        let res = csv_layout.convert(&tx, reader, &Logger(false));
        assert!(res.is_ok());
        assert!(handle.unwrap().join().is_ok());
    }*/
}
