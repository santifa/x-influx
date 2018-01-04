//! x-influx
//!
//! This program feeds influxdb with input data
//! from some source.
//!
//! At the moment only CSV is supported.
//! See `USAGE` for arguments.
extern crate docopt;
extern crate chrono;
extern crate influent;

use std::env::args;
use std::fmt::Debug;
use std::fmt;
use std::io;
use std::thread;
use std::sync::mpsc::{channel, Sender};
use std::error::Error;
use std::fs::File;
use std::iter::Iterator;
use std::io::{BufReader, BufRead, Read};

use docopt::Docopt;
use chrono::{Local, Utc, TimeZone};
use influent::create_client;
use influent::client::Client;
use influent::client::Credentials;
use influent::measurement::{Measurement, Value};

/// A simple logger which only
/// distinguishes between info and debug
#[derive(Debug, Clone)]
struct Logger(bool);

impl Logger {
    fn info<T: Debug>(&self, s: T) {
        println!("{} Info {:?}", Local::now().to_rfc3339(), s);
    }

    fn debug<T: Debug>(&self, s: T) {
        if self.0 {
            println!("{} Debug {:?}", Local::now().to_rfc3339(), s);
        }
    }
}

// do some error handling
/// Internal result which throws an internal error if
/// something bad happens.
type ConvertResult<T> = Result<T, ConvertError>;

#[derive(Debug, PartialEq)]
pub enum ConvertError {
    /// If some element is not found but required.
    NotFound(&'static str),
    /// If the row conversion failed.
    ImportErr(&'static str, i32),
}

impl fmt::Display for ConvertError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            ConvertError::NotFound(ref s) => write!(f, "{} not found in header.", s),
            ConvertError::ImportErr(ref s, ref i) => {
                write!(f, "Failed to convert {}, row {}.", s, i)
            }
        }
    }
}

impl Error for ConvertError {
    fn description(&self) -> &str {
        match *self {
            ConvertError::NotFound(ref s) => s,
            ConvertError::ImportErr(ref s, _) => s,
        }
    }
}

/// Program Flags and Options
const USAGE: &'static str = "
Usage: rinflux [options] <file>...
       rinflux --help

Options:
  -h, --help           Print this help message.
  -v, --verbose        Enable verbose logging.
  -u, --user USER      Username for influxdb [default: test].
  -p, --password PASS  Password for influxdb [default: ].
  -d, --database DB    Influx database [default: test].
  -s, --server SRV     The influxdb server for import 
                       [default: http://localhost:8086].
  
  -m, --measure VAL    Name of the measurement value [default: data].
  -t, --tags VAL       Comma seperated list of tags associated to a value.
  -T, --time VAL       Name of the timestamp column [default: timestamp].
  -f, --format FMT     The timestamp format [default: %F %H:%M:%S]
                       See https://docs.rs/chrono/0.4.0/chrono/format/strftime/index.html

  -D, --delimiter DEL  Use another csv delimiter [default: ,].
  --skip-rows NUM      Remove first NUM lines from file [default: 0].
";


/// Basic format for passing messages to
/// the influxdb client.
/// The value has as first field the name and then the value.
/// Tags follow the same combination.
#[derive(Debug, Clone)]
struct Message {
    time: i64,
    value: (String, String),
    tags: Vec<(String, String)>,
}

impl Default for Message {
    fn default() -> Self {
        Message {
            time: Utc::now().timestamp(),
            value: ("".to_owned(), "".to_owned()),
            tags: vec![],
        }
    }
}

/// Create an influxdb client as background
/// process. the client runs endless until
/// a None Message is received.
///
///'''
/// let (handle, tx) = start_influx_client(...)
/// tx.send(msg).unwrap();
/// tx.send(None).unwrap();
/// if let Ok(h) = handle {
///   handle.join();
/// }
///'''
fn start_influxdb_client(
    hosts: String,
    user: String,
    pass: String,
    db: String,
    l: &Logger,
) -> (io::Result<thread::JoinHandle<()>>, Sender<Option<Message>>) {
    let (tx, rx) = channel();
    let thread = thread::Builder::new();
    let logger = l.clone();

    // create a client
    let handle = thread.spawn(move || {
        let client = create_client(
            Credentials {
                username: &user,
                password: &pass,
                database: &db,
            },
            vec![&hosts],
        );

        logger.info("Influx client up and running.");
        loop {
            let msg: Option<Message> = match rx.recv() {
                Ok(m) => m,
                Err(e) => {
                    logger.info(format!("Error: Can not recieve message. {}", e));
                    continue;
                }
            };

            // exit if we're done
            let m = match msg {
                Some(m) => m,
                None => break,
            };

            let mut measure = Measurement::new(&db);
            measure.add_field(&m.value.0, Value::String(&m.value.1));
            measure.set_timestamp(m.time);
            for ref tag in &m.tags {
                measure.add_tag(&tag.0, &tag.1);
            }

            if let Err(e) = client.write_one(measure, None) {
                logger.info(format!("Error: Failed to write to influxdb. {:?}", e));
            }
        }
    });

    (handle, tx)
}

/// The basic conversion between some data and influx is a mapping
/// between the value names for a series and a maybe empty list
/// of additional tags for the series.
/// At last a mapping for timestamps is needed, with name and
/// format.
/// See https://docs.rs/chrono/0.4.0/chrono/format/strftime/index.html
/// for time formating.
#[derive(Debug, Clone)]
struct Layout {
    value: String,
    tags: Vec<String>,
    time: String,
    tformat: String,
}

impl Default for Layout {
    fn default() -> Self {
        Layout {
            value: String::from("data"),
            tags: [].to_vec(),
            time: String::from("timestamp"),
            tformat: String::from("%F %H:%M:%S"),
        }
    }
}

impl Layout {
    fn new(value: &str, tags: &str, time: (&str, &str)) -> Self {
        Layout {
            value: value.to_owned(),
            time: time.0.to_owned(),
            tformat: time.1.to_owned(),
            tags: tags.split(',').map(|e| e.to_owned()).collect(),
        }
    }

    // search for the positions within the header row
    fn apply(&self, data: &[&str]) -> ConvertResult<(usize, Vec<usize>, usize)> {
        let val = data.iter().position(|&e| e == self.value).ok_or(
            ConvertError::NotFound("Value"),
        )?;

        // filter positions of tags
        let mut tags: Vec<usize> = Vec::new();
        for ref tag in &self.tags {
            if let Some(p) = data.iter().position(|e| e == tag) {
                tags.push(p);
            }
        }

        let time = data.iter().position(|&e| e == self.time).ok_or(
            ConvertError::NotFound("Timestamp"),
        )?;

        Ok((val, tags, time))
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

    fn convert<R: Read>(
        &self,
        tx: &Sender<Option<Message>>,
        reader: BufReader<R>,
        l: &Logger,
    ) -> ConvertResult<()> {
        let mut csv_start = reader.lines().skip(self.skip_rows);
        let header: String = match csv_start.next() {
            Some(Ok(h)) => h,
            _ => {
                return Err(ConvertError::ImportErr("header", 0));
            }
        };

        // get columns of interrest
        let h: Vec<&str> = header.split(self.del).collect();
        let (val_col, tags_col, time_col) = self.layout.apply(h.as_slice())?;

        for (i, row) in csv_start.enumerate() {
            if let Ok(r) = row {

                let mut msg = Message::default();
                for (j, col) in r.split(self.del).enumerate() {
                    if val_col == j {
                        msg.value = (self.layout.value.clone(), col.to_owned());

                    } else if time_col == j {
                        // either use the current timezone or a give one
                        match Utc.datetime_from_str(col, &self.layout.tformat) {
                            Ok(t) => msg.time = t.timestamp(),
                            Err(e) => {
                                l.info(format!("Failed to parse Date at {}, {}; {}", i, j, e));
                                continue;
                            }
                        }
                    } else if let Some(pos) = (&tags_col).into_iter().position(|e| e == &j) {
                        msg.tags.push(
                            (self.layout.tags[pos].to_owned(), col.to_owned()),
                        );
                    }
                }
                l.debug(format!("msg: {:?}", msg));
                if let Err(e) = tx.send(Some(msg)) {
                    l.info(format!("Error: Failed to send message. {}", e));
                }
            } else {
                l.info(format!("Error: Failed to read row {}", i));
            }
        }

        if let Err(e) = tx.send(None) {
            l.info(format!("Error: Failed to send last message. {}", e));
        }

        Ok(())
    }
}

fn main() {
    let _args = Docopt::new(USAGE)
        .and_then(|d| d.argv(args().into_iter()).parse())
        .unwrap_or_else(|e| e.exit());

    let logger = Logger(_args.get_bool("-v"));
    let (handle, tx) = start_influxdb_client(
        _args.get_str("-s").to_owned(),
        _args.get_str("-u").to_owned(),
        _args.get_str("-p").to_owned(),
        _args.get_str("-d").to_owned(),
        &logger,
    );
    logger.info("Influx client created.");

    let layout = CsvLayout::new(
        _args.get_str("-D"),
        _args.get_str("--skip-rows"),
        Layout::new(_args.get_str("-m"), _args.get_str("-t"), (
            _args.get_str("-T"),
            _args.get_str("-f"),
        )),
    );
    logger.debug(&layout);

    for file in _args.get_vec("<file>") {
        if let Ok(reader) = File::open(file).map(|e| BufReader::new(e)) {
            if let Err(e) = layout.convert(&tx, reader, &logger) {
                logger.info(format!(
                    "Error: Failed to import file {}. Reason: {}",
                    file,
                    e
                ));
                // panic if we can not quit the influx client
                assert!(tx.send(None).is_ok());
            }
        } else {
            logger.info(format!("Error: Failed to open file {}.", file));
        }
    }

    if let Ok(h) = handle {
        assert!(h.join().is_ok());
        logger.info("Successfully imported new data.");
    }
}


// Unit Tests
#[cfg(test)]
mod test {
    use super::*;

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

        let pos = layout.apply(
            &[
                "datum",
                "",
                "stand in kw/h",
                "test",
                "bereich",
                "halle",
                "plz",
            ],
        );
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
        let client = create_client(cred, host);*/
    }


    fn expected_msg(m: Message, v: (&str, &str), t: Vec<(String, String)>, time: i64) {
        assert_eq!(&m.value.0, v.0);
        assert_eq!(&m.value.1, v.1);
        assert_eq!(m.tags, t);
        assert_eq!(m.time, time);
    }

    #[test]
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

    #[test]
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
            &Logger(false),
        );
        let res = csv_layout.convert(&tx, reader, &Logger(false));
        assert!(res.is_ok());
        assert!(handle.unwrap().join().is_ok());
    }
}
