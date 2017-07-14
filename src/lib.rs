//#![deny(warnings)]
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
    }

    #[test]
    fn connected_test() {
        let put_uri = "http://localhost:2379/v3alpha/kv/put"
            .parse::<hyper::Uri>()
            .unwrap();
        let range_uri = "http://localhost:2379/v3alpha/kv/range"
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
    }

    #[test]
    fn action_test() {
        let mut core = tokio_core::reactor::Core::new().unwrap();
        let session = etcd_actions::EtcdSession::new(&core.handle(), "http://localhost:2379");
        let val =  "booooom";
        let work = session.put("action", val);
        core.run(work).unwrap();
        let work = session.get("action");
        let result = core.run(work).unwrap();
        assert_eq!(result, val);
    }
}
