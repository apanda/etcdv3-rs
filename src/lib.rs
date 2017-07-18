#![deny(warnings)]
extern crate serde;
extern crate serde_json;
extern crate base64;
extern crate futures;
extern crate hyper;
extern crate tokio_core;

#[macro_use]
extern crate serde_derive;
pub mod etcd_proto;
pub mod etcd_actions;

//pub use self::etcd_proto::*;

#[cfg(test)]
mod tests {
    use super::*;
    use super::etcd_proto::*;
    use serde_json;
    use hyper;
    use tokio_core;
    use futures::Future;
    use futures::stream::Stream;
    use std::str;
    use std::io;
    #[test]
    fn basic_test() {
        let req = PutRequest::new("hello", "world 22");
        let json = serde_json::to_string(&req).unwrap();
        println!("{}", json);
        let json = serde_json::to_string(&PutRequest::new_with_previous("hello", "world 23"))
            .unwrap();
        println!("{}", json);
        let resp_json = r#"{"header":{"cluster_id":"14841639068965178418",
                                      "member_id":"10276657743932975437",
                                      "revision":"2",
                                      "raft_term":"4"}}"#;
        let parsed: PutResponse = serde_json::from_str(resp_json).unwrap();
        println!("{}", parsed.header.unwrap().cluster_id.unwrap());
        let resp_json = r#"{"header":{"cluster_id":"14841639068965178418",
                                    "member_id":"10276657743932975437",
                                    "revision":"6",
                                    "raft_term":"4"},
                          "prev_kv":{"key":"aGVsbG8=",
                                     "create_revision":"2",
                                     "mod_revision":"5",
                                     "version":"4",
                                     "value":"d29ybGQgMjI="}}"#;
        let parsed: PutResponse = serde_json::from_str(resp_json).unwrap();
        let prev_kv = parsed.prev_kv.unwrap();
        println!(
            "{} --> {}",
            prev_kv.key().unwrap(),
            prev_kv.value().unwrap()
        );
        let json_range = serde_json::to_string(&RangeRequest::new_with_sort(
            "hello",
            SortOrder::ASCEND,
            SortTarget::VALUE,
        )).unwrap();
        println!("{}", json_range);

        let json_watch = serde_json::to_string(&WatchRequest::new_create_request(
            WatchCreateRequest::new_for_key("hello"),
        )).unwrap();
        println!("{}", json_watch);
    }

    #[test]
    fn connected_test() {
        let put_uri = "http://localhost:2379/v3alpha/kv/put"
            .parse::<hyper::Uri>()
            .unwrap();
        let range_uri = "http://localhost:2379/v3alpha/kv/range"
            .parse::<hyper::Uri>()
            .unwrap();
        let watch_uri = "http://localhost:2379/v3alpha/watch"
            .parse::<hyper::Uri>()
            .unwrap();
        let mut core = tokio_core::reactor::Core::new().unwrap();
        let handle = core.handle();
        let client = hyper::Client::new(&handle);
        let mut put_request = hyper::Request::new(hyper::Method::Post, put_uri);
        put_request.set_body(
            serde_json::to_string(&PutRequest::new("hello", "world 3333")).unwrap(),
        );
        let work = client
            .request(put_request)
            .and_then(|res| {
                println!("Response: {}", res.status());
                res.body().concat2().and_then(|body: hyper::Chunk| {
                    let v: PutResponse = serde_json::from_slice(&body).unwrap();
                    println!(
                        "Cluster ID for put is {}",
                        v.header.unwrap().cluster_id.unwrap()
                    );
                    Ok(())
                })
            })
            .and_then(|_| {
                let mut range_request = hyper::Request::new(hyper::Method::Post, range_uri);
                range_request.set_body(serde_json::to_string(&RangeRequest::new("hello")).unwrap());
                client.request(range_request)
            })
            .and_then(|res| {
                println!("Response: {}", res.status());
                res.body().concat2().and_then(|body: hyper::Chunk| {
                    let v: RangeResponse = serde_json::from_slice(&body).unwrap();
                    println!("Returned {} value(s)", v.count());
                    assert!(v.count() == 0 || v.kvs.as_ref().unwrap().len() == v.count());
                    assert!(v.count() == 1);
                    for kv in v.kvs.as_ref().unwrap() {
                        println!(r#""{}" --> "{}""#, kv.key().unwrap(), kv.value().unwrap());
                    }
                    Ok(v.kvs.unwrap())
                })
            })
            .and_then(|kvs| {
                for kv in &kvs {
                    println!(r#""{}" --> "{}""#, kv.key().unwrap(), kv.value().unwrap());
                }
                Ok(())
            });
        core.run(work).unwrap();

        let mut watch_request = hyper::Request::new(hyper::Method::Post, watch_uri);
        watch_request.set_body(
            serde_json::to_string(&WatchRequest::new_create_request(
                WatchCreateRequest::new_for_key("hello"),
            )).unwrap(),
        );
        let work = client.request(watch_request).and_then(|res| {
            println!("Response: {}", res.status());
            res.body().for_each(|body: hyper::Chunk| {
                let outer: WatchStreamResponse = serde_json::from_slice(&body).unwrap();
                let v = outer.result.as_ref().unwrap();
                if let Some(created) = v.created {
                    println!("Watch created {}", created);
                } else if let Some(ref events) = v.events {
                    for event in events {
                        println!("Body is {}", str::from_utf8(&body).unwrap());
                        println!("Event type {:?}", event.event_type());
                        println!("Key {}", event.kv.as_ref().unwrap().key().unwrap());
                    }
                };
                Err(hyper::Error::Io(
                    io::Error::new(io::ErrorKind::TimedOut, "Done"),
                ))
            })
        });
        match core.run(work) {
            Ok(_) => panic!("Should not return OK"),
            Err(hyper::Error::Io(err)) => {
                assert!(
                    err.kind() == io::ErrorKind::TimedOut,
                    "IO error, but not a timeout"
                )
            }
            _ => panic!("Something went wrong"),
        }
    }

    #[test]
    fn action_test() {
        let mut core = tokio_core::reactor::Core::new().unwrap();
        let session = etcd_actions::EtcdSession::new(&core.handle(), "http://localhost:2379");
        let val = "booooom";
        let work = session.put("action", val);
        core.run(work).unwrap();
        let work = session.get("action");
        let result = core.run(work).unwrap();
        assert_eq!(result, Some(String::from(val)));
        let work = session.watch("pot");
        let stream = core.run(work).unwrap(); // We have now registered a watch?
        let new_put = session.put("pot", "boiled");
        core.run(new_put).unwrap(); // We have now triggered the watch.
        let work = stream.for_each(|inner| {
            if let Some(_) = inner.created {
                println!("Watch created");
                Ok(())
            } else if let Some(ref events) = inner.events {
                println!("Event received");
                assert!(events.len() == 1, "Should not have more than one event");
                let ev = &events[0];
                assert_eq!(ev.kv.as_ref().unwrap().key(), Some(String::from("pot")));
                assert_eq!(
                    ev.kv.as_ref().unwrap().value(),
                    Some(String::from("boiled"))
                );
                // Ugly hack to timeout the watch so we don't wait forever.
                Err(hyper::Error::Io(
                    io::Error::new(io::ErrorKind::TimedOut, "Done"),
                ))
            } else {
                panic!("Unexpected result")
            }
        });
        match core.run(work) {
            Ok(_) => panic!("Should not return OK"),
            Err(hyper::Error::Io(err)) => {
                assert!(
                    err.kind() == io::ErrorKind::TimedOut,
                    "IO error, but not a timeout"
                )
            }
            _ => panic!("Something went wrong"),
        }
    }

    #[test]
    fn watch_range_test() {
        let mut core = tokio_core::reactor::Core::new().unwrap();
        let session = etcd_actions::EtcdSession::new(&core.handle(), "http://localhost:2379");
        let work = session.watch_pfx("kettle");
        let stream = core.run(work).unwrap(); // We have now registered a watch?
        let new_put = session.put("kettle-black", "boiled");
        core.run(new_put).unwrap(); // We have now triggered the watch.
        let work = stream.for_each(|inner| {
            if let Some(_) = inner.created {
                println!("Watch created");
                Ok(())
            } else if let Some(ref events) = inner.events {
                println!("Event received");
                assert!(events.len() == 1, "Should not have more than one event");
                let ev = &events[0];
                assert_eq!(
                    ev.kv.as_ref().unwrap().key(),
                    Some(String::from("kettle-black"))
                );
                assert_eq!(
                    ev.kv.as_ref().unwrap().value(),
                    Some(String::from("boiled"))
                );
                // Ugly hack to timeout the watch so we don't wait forever.
                Err(hyper::Error::Io(
                    io::Error::new(io::ErrorKind::TimedOut, "Done"),
                ))
            } else {
                panic!("Unexpected result")
            }
        });
        match core.run(work) {
            Ok(_) => panic!("Should not return OK"),
            Err(hyper::Error::Io(err)) => {
                assert!(
                    err.kind() == io::ErrorKind::TimedOut,
                    "IO error, but not a timeout"
                )
            }
            _ => panic!("Something went wrong"),
        }
    }
}
