use serde::{Deserialize, Serialize};
use shvrpc::client::ClientConfig;

#[derive(Debug, Serialize, Deserialize)]
pub struct Config {
    pub client: ClientConfig,
    pub data_dir: String,
    pub events_mount_point: String,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            client: ClientConfig::default(),
            data_dir: String::from("/tmp/qxeventd"),
            events_mount_point: String::from("test/qx/event"),
        }
    }
}
