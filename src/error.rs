use std::fmt;
use std::error::Error;
use std::io;
use std::any::Any;
use std::marker::Send;
use std::sync::mpsc;
use client::Message;

/// A simple constant logger which
/// can be used through the whole program.
/// mut static is required to modify the log level.
#[derive(Debug)]
pub struct Logger(pub Level);
pub static mut LOGGER: &'static Logger = &Logger(Level::Info);

/// Instead of true and false a more verbose declaration of log levels.
#[derive(Debug, PartialEq)]
pub enum Level {
    Debug,
    Info,
}

#[macro_export]
macro_rules! info {
    ($e:expr) => {{
        use chrono::Utc;
        println!("{} Info: {:?}", Utc::now().format("%F %T"), $e);
    }}
}

#[macro_export]
macro_rules! error {
    ($e:expr) => {{
        use chrono::Utc;
        println!("{} Error: {:?}", Utc::now().format("%F %T"), $e);
    }}
}

/// Print debug messages only if the logger struct
/// defines debug as log level.
#[macro_export]
macro_rules! debug {
    ($e:expr) => {unsafe {
        use chrono::Utc;
        use error::{Level, LOGGER};
        if LOGGER.0 == Level::Debug {
            println!("{} Debug: {:?}", Utc::now().format("%F %T"), $e);
        }
    }}
}

/// Change the log level to debug.
#[macro_export]
macro_rules! set_debug {
    () => {unsafe {
        use error::{Level, LOGGER, Logger};
        LOGGER = &Logger(Level::Debug);
    }}
}

/// Internal result which throws an error if
/// something bad happens.
pub type ConvertResult<T> = Result<T, ConvertError>;

#[derive(Debug)]
pub enum ConvertError {
    /// If some element is not found but required.
    NotFound(String),
    /// If the row conversion failed.
    Import(String),
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
            ConvertError::NotFound(ref s) => write!(f, "Header {} not found in top row.", s),
            ConvertError::Import(ref s) => write!(f, "{}.", s),
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
            ConvertError::Import(ref s) => s,
            ConvertError::Join(_) => "Failed to gracefully shutdown the influx client",
            ConvertError::Influx(ref err) => err.description(),
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

#[cfg(test)]
mod test {
    use super::*;

    #[allow(unused_unsafe)]
    #[test]
    fn test_logger() {
        unsafe {
            assert_eq!(LOGGER.0, Level::Info);
            set_debug!();
            assert_eq!(LOGGER.0, Level::Debug);
        }
    }

}
