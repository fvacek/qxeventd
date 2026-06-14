use std::fmt::Display;

use qxsql::{DbValue, Record, ToRecord};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Default, ToRecord)]
pub struct QxChangeRecord {
    // #[serde(default, skip_serializing_if = "Option::is_none")] pub id: Option<i64>,
    #[to_record(skip_if_none)] pub data_type: Option<String>,
    #[to_record(skip_if_none)] pub foreign_id: Option<i64>,
    #[to_record(skip_if_none)] pub data: Option<Data>,
    #[to_record(skip_if_none)] pub user_id: Option<String>,
    #[to_record(skip_if_none)] pub status: Option<Status>,
    #[to_record(skip_if_none)] pub stage_id: Option<i64>,
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub enum Status {
    Pending,
    Accepted,
    Rejected,
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
pub enum Data {
    LateEntry {
        run_id: Option<i64>,
        record: Record,
        // #[serde(default, skip_serializing_if = "Option::is_none")] comment: Option<String>,
        // #[serde(default, skip_serializing_if = "Option::is_none")] issuer: Option<String>,
    },
}
