use influent::create_client;

use std::thread;
use std::sync::mpsc::{channel, Sender};
use influent::client::{Client, Credentials};
use influent::measurement::{Measurement, Value};
use chrono::{DateTime, Utc};
use error::{ConvertError, ConvertResult};

/// Basic format for passing messages to
/// the influxdb client.
/// The value has as first field the name and then the value.
/// Tags follow the same combination.
#[derive(Debug, Clone)]
pub struct Message {
    time: i64, // unix timestamp in seconds
    value: (String, String),
    tags: Vec<(String, String)>,
}

impl Message {
    pub fn new(
        time: DateTime<Utc>,
        value: (String, String),
        tags: Vec<(String, String)>,
    ) -> Message {
        Message {
            time: time.timestamp(),
            value: value,
            tags: tags,
        }
    }
}

#[derive(Debug)]
pub struct InfluxClient {
    tx: Sender<Option<Message>>,
    thread_handle: thread::JoinHandle<()>,
}

impl InfluxClient {
    /// Use this method to shutdown the influx client
    /// instead of simple dropping.
    pub fn join(self) -> ConvertResult<()> {
        self.tx
            .send(None)
            .map_err(ConvertError::Send)
            .and_then(|()| self.thread_handle.join().map_err(ConvertError::Join))
    }

    /// Convient method for sending data to running background influx client.
    pub fn send(&self, msg: Message) -> ConvertResult<()> {
        self.tx.send(Some(msg)).map_err(ConvertError::Send)
    }

    /// Construct a new influx db background client
    /// which accepts messages and stores them.
    /// It stops if it recieves a None message.
    pub fn new(
        hosts: String,
        user: String,
        pass: String,
        db: String,
        series: String,
    ) -> ConvertResult<InfluxClient> {
        let (tx, rx) = channel();
        let thread = thread::Builder::new();

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

            loop {
                let msg: Option<Message> = match rx.recv() {
                    Ok(m) => m,
                    Err(e) => {
                        error!(format!("Can't recieve message. {}", e));
                        continue; // maybe some better error handling
                    }
                };

                // exit if we're done
                let m = match msg {
                    Some(m) => m,
                    None => break,
                };

                debug!(format!("Incoming: {:?}", m));
                let mut measure = Measurement::new(&series);
                measure.add_field(&m.value.0, Value::String(&m.value.1));
                measure.set_timestamp(m.time * 1000000000); // convert to nanoseconds
                for ref tag in &m.tags {
                    measure.add_tag(&tag.0, &tag.1);
                }

                if let Err(e) = client.write_one(measure, None) {
                    error!(format!("Failed to write to influxdb. {:?}", e));
                }
            }
        });

        handle.map_err(ConvertError::Influx).and_then(|handle| {
            Ok(InfluxClient {
                thread_handle: handle,
                tx: tx,
            })
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::time::Duration;
    use chrono::NaiveDateTime;

    // clear and create the test db instance
    fn clean_db() {
        let client = create_client(
            Credentials {
                username: "testuser".into(),
                password: "testpass".into(),
                database: "test".into(),
            },
            vec!["http://localhost:8086"],
        );
        client.query("drop database test".into(), None).unwrap();
        client.query("create database test".into(), None).unwrap();
    }

    // start the test background client
    fn start_client() -> InfluxClient {
        clean_db();
        InfluxClient::new(
            "http://localhost:8086".into(),
            "testuser".into(),
            "testpass".into(),
            "test".into(),
            "try".into(),
        ).unwrap()
    }

    // check if a message is correctly inserted
    fn validate(msg: &Message) {
        thread::sleep(Duration::from_millis(100));
        let client = create_client(
            Credentials {
                username: "testuser".into(),
                password: "testpass".into(),
                database: "test".into(),
            },
            vec!["http://localhost:8086"],
        );
        let res = client
            .query("select last(*) from try".into(), None)
            .unwrap();
        let time = DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(msg.time, 0), Utc);
        let json_msg = format!("{{\"results\":[{{\"statement_id\":0,\"series\":[{{\"name\":\"try\",\"columns\":[\"time\",\"last_{}\"],\"values\":[[\"{:?}\",\"{}\"]]}}]}}]}}\n", msg.value.0, time, msg.value.1);
        assert_eq!(res, json_msg);
    }

    #[test]
    fn test_simple_import() {
        // try insert into test influxdb and query result
        let client = start_client();
        let msg = Message::new(Utc::now(), ("power".into(), "1".into()), vec![]);
        assert!(client.send(msg.clone()).is_ok());
        validate(&msg);

        let msg = Message::new(Utc::now(), ("power".into(), "2".into()), vec![]);
        assert!(client.send(msg.clone()).is_ok());
        validate(&msg);

        assert!(client.join().is_ok());
    }
}
