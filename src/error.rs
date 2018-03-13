use std::fmt;
use std::fmt::Debug;
use std::error::Error;
use std::io;
use std::mem;
use std::any::Any;
use std::marker::Send;
use std::sync::mpsc;
use chrono::Local;
use client::Message;

/// A simple constant logger which
/// can be used through the whole program
#[derive(Debug)]
pub enum Level {
    Debug,
    Info,
}

#[derive(Debug)]
pub struct Logger(pub Level);
pub static mut LOGGER: &'static Logger = &Logger(Level::Info);

#[macro_export]
macro_rules! info {
    ($e:expr) => {unsafe { LOGGER.info($e)}};
}

#[macro_export]
macro_rules! debug {
    ($e:expr) => {unsafe { LOGGER.debug($e)}};
}

#[macro_export]
macro_rules! error {
    ($e:expr) => {unsafe { LOGGER.error($e)}};
}

#[macro_export]
macro_rules! set_debug {
    () => {unsafe { LOGGER = &Logger(Level::Debug); }}
}

impl Logger {
    pub fn info<T: Debug>(&self, s: T) {
        println!("{} Info: {:?}", Local::now().format("%F %T"), s);
    }

    pub fn error<T: Debug>(&self, s: T) {
        println!("{} Error: {:?}", Local::now().format("%F %T"), s);
    }

    pub fn debug<T: Debug>(&self, s: T) {
        if let Level::Debug = self.0 {
            println!("{} Debug: {:?}", Local::now().format("%F %T"), s);
        }
    }

    /// set debuging to true which is not reversable
    pub fn set_debug(&self) {
        //self.0 = Level::Debug;
        //unsafe {mem::transmute<Level::Info, Level::Debug>(self.0)};
        //mem::replace(&mut self.0, Level::Debug);
        //self.0 = true;
        //self.0 = true;
    }
}

/// Internal result which throws an error if
/// something bad happens.
pub type ConvertResult<T> = Result<T, ConvertError>;

#[derive(Debug)]
pub enum ConvertError {
    /// If some element is not found but required.
    NotFound(&'static str),
    /// If the row conversion failed.
    Import(&'static str, i32),
    /// Failed to start influx client
    Influx(io::Error),
    /// Thread joining failed
    Join(Box<Any + Send>),
    /// Sending to client failed
    Send(mpsc::SendError<Option<Message>>),
}

impl fmt::Display for ConvertError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            ConvertError::NotFound(ref s) => write!(f, "Column {} not found in header line.", s),
            ConvertError::Import(ref s, ref i) => write!(f, "Failed to convert {}, row {}.", s, i),
            ConvertError::Influx(ref err) => fmt::Display::fmt(err, f),
            ConvertError::Join(ref s) => {
                write!(f, "Failed to gracefully shutdown the client: {:?}", s)
            }
            ConvertError::Send(ref err) => fmt::Display::fmt(err, f),
        }
    }
}

impl Error for ConvertError {
    fn description(&self) -> &str {
        match *self {
            ConvertError::NotFound(ref s) => s,
            ConvertError::Import(ref s, _) => s,
            ConvertError::Influx(ref err) => err.description(),
            ConvertError::Join(_) => "Failed to gracefully shutdown the influx client",
            ConvertError::Send(ref err) => err.description(),
        }
    }
}

impl From<io::Error> for ConvertError {
    fn from(err: io::Error) -> ConvertError {
        ConvertError::Influx(err)
    }
}

impl From<Box<Any + Send>> for ConvertError {
    fn from(err: Box<Any + Send>) -> ConvertError {
        ConvertError::Join(err)
    }
}

impl From<mpsc::SendError<Option<Message>>> for ConvertError {
    fn from(err: mpsc::SendError<Option<Message>>) -> ConvertError {
        ConvertError::Send(err)
    }
}
