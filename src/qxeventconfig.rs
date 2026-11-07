use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::rpctype::RpcTypeStr;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Role {
    Organizer,
    Banned,
}

impl RpcTypeStr for Role {
    const RPC_TYPE: &'static str = "s";
}

#[derive(Debug, Clone, Serialize, Deserialize, RpcTypeStr)]
pub struct QxEventConfig {
    pub members: BTreeMap<String, Role>,
}

/// Config is stored as a JSON string
impl From<QxEventConfig> for qxsql::DbValue {
    fn from(value: QxEventConfig) -> Self {
        serde_json::to_string(&value).expect("config to json should not fail").into()
    }
}

impl Default for QxEventConfig {
    fn default() -> Self {
        Self {
            members: BTreeMap::new(),
        }
    }
}
