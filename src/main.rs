extern crate csv;
extern crate docopt;
extern crate chrono;
extern crate influent;

use std::env::args;
use std::fmt::Debug;
use std::fmt;
use std::io;
use std::thread;
use std::sync::mpsc::{channel, Sender, Receiver};
use std::error::Error;
use std::fs::File;
use std::iter::Iterator;
use std::io::{BufReader, BufRead, Read};

use chrono::{Local, DateTime, NaiveDateTime, Utc};
use docopt::Docopt;
use influent::create_client;
use influent::client::Client;
use influent::client::http::HttpClient;
use influent::client::Credentials;
use influent::measurement::{Measurement, Value};

// A simple logger which only
// distinguishes between info and debug
#[derive(Debug, Clone)]
struct Logger(bool);

impl Logger {
    fn info<T: Debug>(&self, s: T) {
        println!("{}   {:?}", Local::now().to_rfc3339(), s);
    }

    fn debug<T: Debug>(&self, s: T) {
        if self.0 {
            println!("{}   {:?}", Local::now().to_rfc3339(), s);
        }
    }
}

// do some error handling
type ConvertResult<T> = Result<T, ConvertError>;

#[derive(Debug, PartialEq)]
pub enum ConvertError {
    NotFound(&'static str),
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
  -T, --time VAL       Name of the timestamp column
  -f, --format FMT     The timestamp format [default: %F %H:%M:%S]
                       See https://docs.rs/chrono/0.4.0/chrono/format/strftime/index.html

  -D, --delimiter DEL  Use another csv delimiter [default: ,].
  --skip-rows NUM      Remove first NUM lines from file [default: -1].
";


/// Basic format for passing messages to
/// the influxdb client.
/// The value has as first field the name and then the value.
/// Tags follow the same combination.
#[derive(Debug, Clone)]
struct Message {
    time: NaiveDateTime,
    value: (String, String),
    tags: Vec<(String, String)>,
}

impl Default for Message {
    fn default() -> Message {
        Message {
            time: Utc::now().naive_utc(),
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

            // exit is we're done
            let m = match msg {
                Some(m) => m,
                None => break,
            };

            let mut measure = Measurement::new(&db);
            measure.add_field(&m.value.0, Value::String("msg.value.1"));
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
    time: (String, String),
}

impl Default for Layout {
    fn default() -> Layout {
        Layout {
            value: String::from("data"),
            tags: [].to_vec(),
            time: (String::from("timestamp"), String::from("%F %H:%M:%S")),
        }
    }
}

impl Layout {
    fn new(value: &str, tags: &str, time: (&str, &str)) -> Layout {
        Layout {
            value: value.to_owned(),
            time: (time.0.to_owned(), time.1.to_owned()),
            tags: tags.split(',').map(|e| e.to_owned()).collect(),
        }
    }

    // search for the positions within the header row
    fn apply(&self, data: &[&str]) -> ConvertResult<(i32, Vec<i32>, i32)> {
        let val = data.iter().position(|&e| e == self.value).ok_or(
            ConvertError::NotFound("Value"),
        )?;

        // filter positions of tags
        let mut tags: Vec<i32> = Vec::new();
        for ref tag in &self.tags {
            if let Some(p) = data.iter().position(|e| e == tag) {
                tags.push(p as i32);
            }
        }

        let time = data.iter().position(|&e| e == self.time.0).ok_or(
            ConvertError::NotFound("Timestamp"),
        )?;

        Ok((val as i32, tags, time as i32))
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
    skip_rows: i32,
    skip_cols: Vec<i32>,
}

impl Default for CsvLayout {
    fn default() -> CsvLayout {
        CsvLayout {
            layout: Layout::default(),
            del: ',',
            skip_rows: 0,
            skip_cols: [].to_vec(),
        }
    }
}

impl CsvLayout {
    fn new(d: &str, r: &str, l: Layout) -> CsvLayout {
        CsvLayout {
            del: d.chars().next().unwrap_or(','),
            skip_rows: r.parse::<i32>().unwrap_or(-1),
            skip_cols: vec![],
            layout: l,
        }
    }

    fn convert<R: Read>(
        &self,
        tx: &Sender<Option<Message>>,
        reader: BufReader<R>,
        l: &Logger,
    ) -> ConvertResult<()> {
        let mut csv_start = reader.lines().skip(self.skip_rows as usize);
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

                    if val_col == j as i32 {
                        msg.value = (self.layout.value.clone(), col.to_owned());

                    } else if time_col == j as i32 {
                        match DateTime::parse_from_str(col, &self.layout.time.1) {
                            Ok(t) => msg.time = t.naive_utc(),
                            Err(e) => {
                                l.info(format!("Failed to parse Date at {}, {}; {}", i, i, e));
                                continue;
                            }
                        }
                    } else if let Some(pos) = (&tags_col).into_iter().position(
                        |e| e == &(j as i32),
                    )
                    {
                        //tags_col.contains(&(j as i32)) {
                        //let pos = ;
                        msg.tags.push(
                            (self.layout.tags[pos].to_owned(), col.to_owned()),
                        );
                    }
                }

                if let Err(e) = tx.send(Some(msg)) {
                    l.info(format!("Failed to send message. {}", e));
                }
            } else {
                l.info(format!("Error: Failed to read row {}", i));
            }
        }

        if let Err(e) = tx.send(None) {
            l.info(format!("Failed to send last message. {}", e));
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

    for file in _args.get_vec("") {
        if let Ok(reader) = File::open(file).map(|e| BufReader::new(e)) {
            if let Err(e) = layout.convert(&tx, reader, &logger) {
                logger.info(format!(
                    "Error: Failed to import file {}. Reason: {}",
                    file,
                    e
                ));
            }
        } else {
            logger.info(format!("Error: Failed to open file {}.", file));
        }
    }

    if let Ok(h) = handle {
        assert!(h.join().is_ok());
    }
}

#[cfg(test)]
mod test {
    use super::*;

    fn expected(l: CsvLayout, del: char, sr: i32, sc: Vec<i32>) {
        assert_eq!(l.del, del);
        assert_eq!(l.skip_rows, sr);
        assert_eq!(l.skip_cols, sc);
    }

    #[test]
    fn test_csv_layout() {
        let layout = CsvLayout::new(";", "-1", Layout::default());
        expected(layout, ';', -1, vec![]);
        let layout = CsvLayout::new("\t", "3", Layout::default());
        expected(layout, '\t', 3, vec![]);
    }

    #[test]
    fn test_layout_with_err() {
        let layout = CsvLayout::new("", "blub", Layout::default());
        expected(layout, ',', -1, vec![]);
    }


    #[test]
    fn test_layout() {
        let mut layout = Layout::default();
        let pos = layout.apply(&["test", "timestamp", "data"]);
        assert_eq!(pos.unwrap(), (2, [].to_vec(), 1));

        layout.value = String::from("stand in kw/h");
        layout.tags = vec![
            String::from("bereich"),
            String::from("plz"),
            String::from("halle"),
        ];
        layout.time = (String::from("datum"), String::from(""));

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
    fn test_layout_wrong() {
        let mut layout = Layout::default();
        layout.value = String::from("val");
        let pos = layout.apply(&["datum", "", ""]);
        assert_eq!(pos.err().unwrap(), ConvertError::NotFound("Value"));

        layout.value = String::from("datum");
        layout.time = (String::from("val"), String::from(""));
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
        let time = Utc::now().naive_utc();

        let msg = Message {
            time: time,
            value: (String::from("power"), String::from("1")),
            tags: vec![],
        };
        assert!(tx.send(Some(msg)).is_ok());
        assert!(tx.send(None).is_ok());
        assert!(handler.unwrap().join().is_ok());

        //TODO verify result
        let cred = Credentials {
            username: "",
            password: "",
            database: "test",
        };
        let client = create_client(cred, host);
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
        println!("{:?}", msg);
        assert_eq!(msg.value, ("data".to_owned(), "0".to_owned()));

        let msg = rx.recv().unwrap().unwrap();
        println!("{:?}", msg);
        assert_eq!(msg.value, ("data".to_owned(), "1".to_owned()));

        let msg = rx.recv().unwrap();
        println!("{:?}", msg);
        assert!(msg.is_none());


    }
}
