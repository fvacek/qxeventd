use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::rpctype::RpcType;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Role {
    Organizer,
    Banned,
}

impl RpcType for Role {
    const RPC_TYPE: &'static str = "s";
}

#[derive(Debug, Clone, Serialize, Deserialize, RpcType)]
pub struct QxEventConfig {
    pub members: BTreeMap<String, Role>,
}

impl Default for QxEventConfig {
    fn default() -> Self {
        Self {
            members: BTreeMap::new(),
        }
    }
}
