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

use docopt::Docopt;

#[macro_use]
mod error;
mod client;
mod mapper;

use client::InfluxClient;
use mapper::{Csv, Interactive, Layout, Mapper};

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
    flag_skip_rows: usize,
    arg_file: Vec<String>,
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
        false => Box::new(Csv::new(
            _args.arg_file,
            _args.cmd_b,
            _args.flag_delimiter,
            _args.flag_skip_rows,
        )),
    };

    if let Err(e) = mapper.import(&layout, &client) {
        error!(format!("Import failed {}", e));
    }

    match client.join() {
        Ok(_) => info!("Gracefull shutdown."),
        Err(e) => error!(format!("{}", e)),
    }
}
