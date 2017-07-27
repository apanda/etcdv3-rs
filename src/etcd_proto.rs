use base64;
/// Hand crafted structures from the `etcd` protobuf. This allows us to call into etcd from Rust.
/// Some weird things to note when adding to this file:
///   - All fields in proto3 are optional -- as a result every field needs to be marked as an
///   `Option<T>`. This is necessary to guarantee that changes in the `etcd` implementation
///   do not cause problems.
///   - The `gRPC` bridge converse 64-bit numbers into Strings.
///   - `oneof` needs to be implemented using enums -- `gRPC` bridge barfs if a null element of the
///   other type is included.
///   - Stream responses are encoded in a struct with result as a field.

/// Common response header included in every `etcd` response.
#[derive(Deserialize)]
pub struct ResponseHeader {
    pub cluster_id: Option<String>,
    pub member_id: Option<String>,
    pub revision: Option<String>,
    pub raft_term: Option<String>,
}

/// Mechanism to encode `etcd` key-value responses.
#[derive(Deserialize)]
pub struct KeyValue {
    key: Option<String>,
    pub create_revision: Option<String>,
    pub mod_revision: Option<String>,
    pub version: Option<String>,
    value: Option<String>,
    pub lease: Option<String>,
}

impl KeyValue {
    pub fn key(&self) -> Option<String> {
        match self.key {
            Some(ref k) => {
                base64::decode(&k).ok().map(
                    |v| String::from_utf8(v).unwrap(),
                )
            }
            None => None,
        }
    }

    pub fn value(&self) -> Option<String> {
        match self.value {
            Some(ref v) => {
                base64::decode(&v).ok().map(
                    |v| String::from_utf8(v).unwrap(),
                )
            }
            None => None,
        }
    }
}

/// Response for a `PutRequest`.
#[derive(Deserialize)]
pub struct PutResponse {
    pub header: Option<ResponseHeader>,
    pub prev_kv: Option<KeyValue>, // Optional since is only set when prev_kv is true.
}

/// A `PutRequest` to add or overwrite a key for etcd.
#[derive(Serialize)]
pub struct PutRequest {
    pub key: Option<String>,
    pub value: Option<String>,
    pub lease: Option<String>,
    pub prev_kv: Option<bool>,
    pub ignore_value: Option<bool>,
    pub ignore_lease: Option<bool>,
}

impl PutRequest {
    /// Create a simple `PutRequest`.
    pub fn new(key: &str, val: &str) -> PutRequest {
        PutRequest {
            key: Some(base64::encode(key)),
            value: Some(base64::encode(val)),
            lease: None,
            prev_kv: None,
            ignore_value: None,
            ignore_lease: None,
        }
    }

    pub fn new_with_previous(key: &str, val: &str) -> PutRequest {
        PutRequest {
            prev_kv: Some(true), // Do not get previous by default
            ..PutRequest::new(key, val)
        }
    }
}

// This is by default turned into a string by `serde_json`, hence encoding it correctly.
#[derive(Serialize)]
pub enum SortOrder {
    NONE,
    ASCEND,
    DESCEND,
}

// This is by default turned into a string by `serde_json`, hence encoding it correctly.
#[derive(Serialize)]
pub enum SortTarget {
    KEY,
    CREATE,
    VERSION,
    MOD,
    VALUE,
}

#[derive(Serialize, Default)]
pub struct RangeRequest {
    pub key: Option<String>,
    pub range_end: Option<String>,
    pub limit: Option<String>,
    pub revision: Option<String>,
    pub sort_order: Option<SortOrder>,
    pub sort_target: Option<SortTarget>,
    pub serializable: Option<bool>,
    pub keys_only: Option<bool>,
    pub count_only: Option<bool>,
    pub min_mod_revision: Option<String>,
    pub max_mod_revision: Option<String>,
    pub min_create_revision: Option<String>,
    pub max_create_revision: Option<String>,
}

impl RangeRequest {
    pub fn new(key: &str) -> RangeRequest {
        RangeRequest {
            key: Some(base64::encode(key)),
            ..Default::default()
        }
    }

    pub fn new_with_sort(key: &str, order: SortOrder, target: SortTarget) -> RangeRequest {
        RangeRequest {
            sort_order: Some(order),
            sort_target: Some(target),
            ..RangeRequest::new(key)
        }
    }

    pub fn new_for_prefix(key: &str) -> RangeRequest {
        let mut range_end = String::from(key).into_bytes();
        let end = key.len() - 1;
        range_end[end] += 1;
        RangeRequest {
            key: Some(base64::encode(key)),
            range_end: Some(base64::encode(&range_end[..])),
            ..Default::default()
        }
    }

    pub fn new_for_prefix_with_sort(
        key: &str,
        order: SortOrder,
        target: SortTarget,
    ) -> RangeRequest {
        RangeRequest {
            sort_order: Some(order),
            sort_target: Some(target),
            ..RangeRequest::new_for_prefix(key)
        }
    }
}

#[derive(Deserialize)]
pub struct RangeResponse {
    pub header: Option<ResponseHeader>,
    pub kvs: Option<Vec<KeyValue>>,
    pub more: Option<bool>,
    count: Option<String>,
}

impl RangeResponse {
    pub fn count(&self) -> usize {
        self.count
            .as_ref()
            .map(|v| v.parse::<usize>().unwrap())
            .unwrap_or(0)
    }
}

// This looks different from everything else so we can retain `oneof` semantics.

#[derive(Serialize)]
pub enum WatchRequest {
    #[serde(rename = "create_request")]
    CreateRequest(WatchCreateRequest),
    #[serde(rename = "cancel_request")]
    CancelRequest(WatchCancelRequest),
}

impl WatchRequest {
    pub fn new_create_request(create_request: WatchCreateRequest) -> WatchRequest {
        WatchRequest::CreateRequest(create_request)
    }

    pub fn new_cancel_request(cancel_request: WatchCancelRequest) -> WatchRequest {
        WatchRequest::CancelRequest(cancel_request)
    }
}

#[derive(Serialize, Default)]
pub struct WatchCancelRequest {
    pub watch_id: Option<String>,
}

impl WatchCancelRequest {
    pub fn new(watch_id: i64) -> WatchCancelRequest {
        WatchCancelRequest { watch_id: Some(watch_id.to_string()) }
    }
}

#[derive(Serialize)]
pub enum FilterType {
    NOPUT,
    NODELETE,
}

#[derive(Serialize, Default)]
pub struct WatchCreateRequest {
    pub key: Option<String>,
    pub range_end: Option<String>,
    pub start_revision: Option<String>,
    pub progress_notify: Option<bool>,
    pub filters: Option<Vec<FilterType>>,
    pub prev_kv: Option<bool>,
}

impl WatchCreateRequest {
    pub fn new_for_key(key: &str) -> WatchCreateRequest {
        WatchCreateRequest {
            key: Some(base64::encode(key)),
            ..Default::default()
        }
    }
    pub fn new_for_prefix(key: &str) -> WatchCreateRequest {
        let mut range_end = String::from(key).into_bytes();
        let end = key.len() - 1;
        range_end[end] += 1;
        WatchCreateRequest {
            key: Some(base64::encode(key)),
            range_end: Some(base64::encode(&range_end[..])),
            ..Default::default()
        }
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
pub enum EventType {
    PUT,
    DELETE,
}

/// An event from a set of watched keys.
#[derive(Deserialize)]
pub struct Event {
    #[serde(rename = "type")]
    etype: Option<EventType>,
    pub kv: Option<KeyValue>,
    pub prev_kv: Option<KeyValue>,
}

impl Event {
    /// Type of event. Currently `etcd` seems to only set this when keys are deleted, puts
    /// (both for new keys and exisisting keys) do not show up with anything.
    pub fn event_type(&self) -> &Option<EventType> {
        &self.etype
    }
}

#[derive(Deserialize)]
pub struct WatchResponse {
    pub header: Option<ResponseHeader>,
    pub watch_id: Option<String>,
    pub created: Option<bool>,
    pub canceled: Option<bool>,
    pub compact_revision: Option<String>,
    pub cancel_reason: Option<String>,
    pub events: Option<Vec<Event>>,
}

#[derive(Deserialize)]
pub struct WatchStreamResponse {
    pub result: Option<WatchResponse>,
}
