use std::fmt::Display;

use qxsql::{DbValue, ToRecord};
use rpc_type_derive::RecordChange;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, Default, ToRecord, RecordChange)]
#[record_change(derive(Debug, Clone, Default, Serialize, Deserialize))]
pub struct QxChangeRecord {
    pub data_type: String,
    pub foreign_table: Option<String>,
    pub foreign_id: Option<i64>,
    pub data: Option<Data>,
    pub user_id: Option<String>,
    pub status: Status,
    pub status_message: Option<String>,
    pub stage_id: i64,
    #[serde(with = "crate::rpctype::chrono_datetime")]
    #[record_change(with = "crate::rpctype::chrono_datetime_opt")]
    pub created: chrono::DateTime<chrono::FixedOffset>,
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub enum Status {
    Pending,
    Accepted,
    Rejected,
}
impl Default for Status {
    fn default() -> Self {
        Status::Pending
    }
}
impl From<Status> for DbValue {
    fn from(value: Status) -> Self {
        DbValue::String(format!("{}", value))
    }
}
impl Display for Status {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let status = match self {
            Status::Pending => "Pending",
            Status::Accepted => "Accepted",
            Status::Rejected => "Rejected",
        };
        f.write_str(status)
    }
}

impl From<Data> for DbValue {
    fn from(value: Data) -> Self {
        DbValue::String(serde_json::to_string(&value).unwrap_or_default())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LateEntryKind {
    Update {
        #[serde(rename = "runId")]
        run_id: i64,
    },
    NewEntry {
        #[serde(rename = "classId")]
        class_id: i64,
    },
    DeleteEntry {
        #[serde(rename = "runId")]
        run_id: i64,
    },
}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LateEntry {
    pub kind: LateEntryKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub firstname: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lastname: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub registration: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub siid: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub starttimems: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub paid: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Data {
    LateEntry(LateEntry),
}
