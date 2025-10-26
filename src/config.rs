use serde::{Deserialize, Serialize};
use shvrpc::client::ClientConfig;

#[derive(Debug, Serialize, Deserialize)]
#[derive(Default)]
pub struct Config {
    pub client: ClientConfig,
    pub data_dir: String,
}
