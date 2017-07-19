use super::etcd_proto::*;
use serde_json;
use hyper;
use tokio_core;
use futures::Future;
use futures::stream::Stream;

const PUT_ENDPOINT: &str = "/v3alpha/kv/put";
const RANGE_ENDPOINT: &str = "/v3alpha/kv/range";
const WATCH_ENDPOINT: &str = "/v3alpha/watch";

pub struct EtcdSession {
    client: hyper::Client<hyper::client::HttpConnector>,
    _handle: tokio_core::reactor::Handle,
    uri: String,
}

impl EtcdSession {
    pub fn new(handle: &tokio_core::reactor::Handle, uri: &str) -> EtcdSession {
        EtcdSession {
            client: hyper::Client::new(handle),
            _handle: handle.clone(),
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

    pub fn get(&self, key: &str) -> Box<Future<Error = hyper::Error, Item = Option<String>>> {
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
                        Ok(v.kvs.as_ref().unwrap()[0].value().clone())
                    } else {
                        Ok(None)
                    }
                }),
        )
    }

    pub fn get_prefix(&self, key: &str) -> Box<Future<Error = hyper::Error, Item = Vec<(String, String)>>> {
        let uri = format!("{}{}", self.uri, RANGE_ENDPOINT)
            .parse::<hyper::Uri>()
            .unwrap();

        let mut range_request = hyper::Request::new(hyper::Method::Post, uri);
        range_request.set_body(serde_json::to_string(&RangeRequest::new_for_prefix(key)).unwrap());
        Box::new(
            self.client
                .request(range_request)
                .and_then(|res| res.body().concat2())
                .and_then(|body| {
                    let v: RangeResponse = serde_json::from_slice(&body).unwrap();
                    if v.count() > 0 {
                        Ok(v.kvs.as_ref().unwrap().iter().map(|kvs| (kvs.key().unwrap().clone(), kvs.value().unwrap().clone())).collect())
                    } else {
                        Ok(vec![])
                    }
                }),
        )
    }

    /// Create a new stream that reports changes to a key.
    pub fn watch(
        &self,
        key: &str,
    ) -> Box<
        Future<
            Error = hyper::Error,
            Item = Box<Stream<Item = WatchResponse, Error = hyper::Error>>,
        >,
    > {
        let uri = format!("{}{}", self.uri, WATCH_ENDPOINT)
            .parse::<hyper::Uri>()
            .unwrap();
        let mut watch_request = hyper::Request::new(hyper::Method::Post, uri);
        watch_request.set_body(
            serde_json::to_string(&WatchRequest::new_create_request(
                WatchCreateRequest::new_for_key(key),
            )).unwrap(),
        );
        Box::new(self.client.request(watch_request).and_then(|res| {
            Ok(Box::new(res.body().map(|chunk| {
                let outer: WatchStreamResponse = serde_json::from_slice(&chunk).unwrap();
                let inner = outer.result.unwrap();
                inner
            })) as
                Box<Stream<Item = WatchResponse, Error = hyper::Error>>)
        }))
    }

    /// Create a new stream that reports changes to a key.
    pub fn watch_pfx(
        &self,
        key: &str,
    ) -> Box<
        Future<
            Error = hyper::Error,
            Item = Box<Stream<Item = WatchResponse, Error = hyper::Error>>,
        >,
    > {
        let uri = format!("{}{}", self.uri, WATCH_ENDPOINT)
            .parse::<hyper::Uri>()
            .unwrap();
        let mut watch_request = hyper::Request::new(hyper::Method::Post, uri);
        watch_request.set_body(
            serde_json::to_string(&WatchRequest::new_create_request(
                WatchCreateRequest::new_for_prefix(key),
            )).unwrap(),
        );
        Box::new(self.client.request(watch_request).and_then(|res| {
            Ok(Box::new(res.body().map(|chunk| {
                let outer: WatchStreamResponse = serde_json::from_slice(&chunk).unwrap();
                let inner = outer.result.unwrap();
                inner
            })) as
                Box<Stream<Item = WatchResponse, Error = hyper::Error>>)
        }))
    }
}
