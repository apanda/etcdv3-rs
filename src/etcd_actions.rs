use super::etcd_proto::*;
use serde_json;
use hyper;
use tokio_core;
use futures::Future;
use futures::stream::Stream;

const PUT_ENDPOINT: &str = "/v3alpha/kv/put";
const RANGE_ENDPOINT: &str = "/v3alpha/kv/range";

pub struct EtcdSession {
    client: hyper::Client<hyper::client::HttpConnector>,
    uri: String,
}

impl EtcdSession {
    pub fn new(handle: &tokio_core::reactor::Handle, uri: &str) -> EtcdSession {
        EtcdSession {
            client: hyper::Client::new(handle),
            uri: String::from(uri),
        }
    }

    // FIXME: Consider just using `into` trait by marking this as nightly only.
    pub fn put(&self, key: &str, val: &str) -> Box<Future<Error = hyper::Error, Item = bool>> {
        let uri = format!("{}{}", self.uri, PUT_ENDPOINT)
            .parse::<hyper::Uri>()
            .unwrap();
        let mut put_request = hyper::Request::new(hyper::Method::Post, uri);
        put_request.set_body(serde_json::to_string(&PutRequest::new(key, val)).unwrap());
        Box::new(self.client.request(put_request).and_then(
            |res| if res.status() ==
                hyper::StatusCode::Ok
            {
                Ok(true)
            } else {
                Err(hyper::Error::Status)
            },
        ))
    }

    pub fn get(&self, key: &str) -> Box<Future<Error = hyper::Error, Item = String>> {
        let uri = format!("{}{}", self.uri, RANGE_ENDPOINT)
            .parse::<hyper::Uri>()
            .unwrap();

        let mut range_request = hyper::Request::new(hyper::Method::Post, uri);
        range_request.set_body(serde_json::to_string(&RangeRequest::new(key)).unwrap());
        Box::new(
            self.client
                .request(range_request)
                .and_then(|res| res.body().concat2())
                .and_then(|body| {
                    let v: RangeResponse = serde_json::from_slice(&body).unwrap();
                    if v.count() == 1 {
                        Ok(String::from(v.kvs.as_ref().unwrap()[0].value().unwrap()))
                    } else {
                        Ok(String::from(""))
                    }
                }),
        )
    }
}