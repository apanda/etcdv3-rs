use base64;
/// Hand crafted structures from the `etcd` protobuf. This allows us to call into etcd from Rust.
/// Some weird things to note when adding to this file:
///   - All fields in proto3 are optional -- as a result every field needs to be marked as an
///   `Option<T>`. This is necessary to guarantee that changes in the `etcd` implementation
///   do not cause problems.
///   - The `gRPC` bridge converse 64-bit numbers into Strings.

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
}

#[derive(Deserialize)]
pub struct RangeResponse {
    pub header: Option<ResponseHeader>,
    pub kvs: Option<Vec<KeyValue>>, // Optional since is only set when prev_kv is true.
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
