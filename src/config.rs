use chrono::Duration;
use serde::{Deserialize, Serialize};
use shvrpc::client::ClientConfig;

#[derive(Debug, Serialize, Deserialize)]
pub struct Config {
    pub client: ClientConfig,
    pub data_dir: String,
    pub events_mount_point: String,
    #[serde(with = "chrono_duration_as_string")]
    pub event_expiration: chrono::Duration,
}

pub fn parse_duration(input: &str) -> Result<Duration, String> {
    if input.is_empty() {
        return Err("Duration string cannot be empty".to_string());
    }
    let (num, unit) = input.split_at(input.len() - 1);
    let value: i64 = num.parse()
        .map_err(|_| format!("Invalid number '{}' in duration", num))?;

    match unit {
        "s" => Ok(Duration::seconds(value)),
        "m" => Ok(Duration::minutes(value)),
        "h" => Ok(Duration::hours(value)),
        "d" => Ok(Duration::days(value)),
        _ => Err(format!("Invalid duration unit '{}'. Valid units are: s, m, h, d", unit)),
    }
}

mod chrono_duration_as_string {
    use serde::{Deserialize, Deserializer, Serializer};
    use chrono::Duration;

    pub fn serialize<S>(duration: &Duration, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // Convert duration to our custom format
        let total_seconds = duration.num_seconds();
        let format = if total_seconds % (24 * 3600) == 0 {
            format!("{}d", total_seconds / (24 * 3600))
        } else if total_seconds % 3600 == 0 {
            format!("{}h", total_seconds / 3600)
        } else if total_seconds % 60 == 0 {
            format!("{}m", total_seconds / 60)
        } else {
            format!("{}s", total_seconds)
        };
        serializer.serialize_str(&format)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Duration, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        super::parse_duration(&s).map_err(|e| serde::de::Error::custom(e))
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            client: ClientConfig::default(),
            data_dir: String::from("/tmp/qxeventd"),
            events_mount_point: String::from("test/qx/event"),
            event_expiration: chrono::Duration::days(2),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json;

    #[test]
    fn test_parse_duration_seconds() {
        let duration = parse_duration("30s");
        assert_eq!(duration, Ok(Duration::seconds(30)));
    }

    #[test]
    fn test_parse_duration_minutes() {
        let duration = parse_duration("45m");
        assert_eq!(duration, Ok(Duration::minutes(45)));
    }

    #[test]
    fn test_parse_duration_hours() {
        let duration = parse_duration("12h");
        assert_eq!(duration, Ok(Duration::hours(12)));
    }

    #[test]
    fn test_parse_duration_days() {
        let duration = parse_duration("7d");
        assert_eq!(duration, Ok(Duration::days(7)));
    }

    #[test]
    fn test_parse_duration_zero() {
        let duration = parse_duration("0s");
        assert_eq!(duration, Ok(Duration::seconds(0)));
    }

    #[test]
    fn test_parse_duration_large_values() {
        let duration = parse_duration("999999s");
        assert_eq!(duration, Ok(Duration::seconds(999999)));
    }

    #[test]
    fn test_parse_duration_invalid_unit() {
        let duration = parse_duration("30x");
        assert!(duration.is_err());
        assert!(duration.unwrap_err().contains("Invalid duration unit"));
    }

    #[test]
    fn test_parse_duration_invalid_number() {
        let duration = parse_duration("abc");
        assert!(duration.is_err());
        assert!(duration.unwrap_err().contains("Invalid number"));
    }

    #[test]
    fn test_parse_duration_empty_string() {
        let duration = parse_duration("");
        assert!(duration.is_err());
        assert!(duration.unwrap_err().contains("Duration string cannot be empty"));
    }

    #[test]
    fn test_parse_duration_no_unit() {
        let duration = parse_duration("123");
        assert!(duration.is_err());
        assert!(duration.unwrap_err().contains("Invalid duration unit"));
    }

    #[test]
    fn test_parse_duration_negative_number() {
        let duration = parse_duration("-5s");
        assert_eq!(duration, Ok(Duration::seconds(-5)));
    }

    #[test]
    fn test_chrono_duration_serialize() {
        // Test with a wrapper struct to use the custom serializer
        #[derive(Serialize)]
        struct TestDuration {
            #[serde(with = "chrono_duration_as_string")]
            #[allow(dead_code)]
            duration: Duration,
        }

        let test = TestDuration { duration: Duration::hours(2) };
        let serialized = serde_json::to_string(&test).unwrap();
        assert!(serialized.contains("duration"));
    }

    #[test]
    fn test_chrono_duration_deserialize_seconds() {
        #[derive(Deserialize)]
        struct TestDuration {
            #[serde(with = "chrono_duration_as_string")]
            #[allow(dead_code)]
            duration: Duration,
        }

        let json = r#"{"duration": "30s"}"#;
        let test: TestDuration = serde_json::from_str(json).unwrap();
        assert_eq!(test.duration, Duration::seconds(30));
    }

    #[test]
    fn test_chrono_duration_deserialize_minutes() {
        #[derive(Deserialize)]
        struct TestDuration {
            #[serde(with = "chrono_duration_as_string")]
            #[allow(dead_code)]
            duration: Duration,
        }

        let json = r#"{"duration": "45m"}"#;
        let test: TestDuration = serde_json::from_str(json).unwrap();
        assert_eq!(test.duration, Duration::minutes(45));
    }

    #[test]
    fn test_chrono_duration_deserialize_hours() {
        #[derive(Deserialize)]
        struct TestDuration {
            #[serde(with = "chrono_duration_as_string")]
            #[allow(dead_code)]
            duration: Duration,
        }

        let json = r#"{"duration": "12h"}"#;
        let test: TestDuration = serde_json::from_str(json).unwrap();
        assert_eq!(test.duration, Duration::hours(12));
    }

    #[test]
    fn test_chrono_duration_deserialize_days() {
        #[derive(Deserialize)]
        struct TestDuration {
            #[serde(with = "chrono_duration_as_string")]
            #[allow(dead_code)]
            duration: Duration,
        }

        let json = r#"{"duration": "7d"}"#;
        let test: TestDuration = serde_json::from_str(json).unwrap();
        assert_eq!(test.duration, Duration::days(7));
    }

    #[test]
    fn test_chrono_duration_deserialize_invalid() {
        #[derive(Deserialize)]
        struct TestDuration {
            #[serde(with = "chrono_duration_as_string")]
            #[allow(dead_code)]
            duration: Duration,
        }

        let json = r#"{"duration": "invalid"}"#;
        let result: Result<TestDuration, _> = serde_json::from_str(json);
        assert!(result.is_err());
    }

    #[test]
    fn test_chrono_duration_round_trip() {
        #[derive(Serialize, Deserialize)]
        struct TestDuration {
            #[serde(with = "chrono_duration_as_string")]
            #[allow(dead_code)]
            duration: Duration,
        }

        // Test serialization and deserialization with various durations
        let test_cases = vec![
            Duration::seconds(30),
            Duration::minutes(45),
            Duration::hours(12),
            Duration::days(7),
        ];

        for original_duration in test_cases {
            let test = TestDuration { duration: original_duration };
            let json = serde_json::to_string(&test).unwrap();
            let deserialized: TestDuration = serde_json::from_str(&json).unwrap();
            assert_eq!(deserialized.duration, original_duration);
        }
    }

}
