use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Role {
    Organizer,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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
