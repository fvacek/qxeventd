use chrono::Duration;
use duration_str::HumanFormat;
use serde::{Deserialize, Serialize};
use shvrpc::client::ClientConfig;

#[derive(Debug, Serialize, Deserialize)]
pub struct Config {
    pub client: ClientConfig,
    pub data_dir: String,
    pub events_mount_point: String,
    #[serde(
        default,
        deserialize_with = "duration_str::deserialize_duration_chrono",
        serialize_with = "serialize_duration_as_string"
    )]
    pub event_expire_duration: chrono::Duration,
}

pub fn serialize_duration_as_string<S>(
    duration: &Duration,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    duration.human_format().serialize(serializer)
}

impl Default for Config {
    fn default() -> Self {
        Self {
            client: ClientConfig::default(),
            data_dir: String::from("/tmp/qxeventd"),
            events_mount_point: String::from("test/qx/event"),
            event_expire_duration: chrono::Duration::days(2),
        }
    }
}
