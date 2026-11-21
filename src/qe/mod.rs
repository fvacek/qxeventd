pub mod eventconfignode;

use std::collections::HashMap;

use chrono::{DateTime, FixedOffset, NaiveDate, NaiveTime};
use qxsql::{QxSqlApi, sql::DbValue};
use rpc_type_derive::RpcTypeStr;
use serde::{Deserialize, Serialize};

const KEY_NAME: &str = "event.name";
const KEY_PLACE: &str = "event.place";
const KEY_COUNTRY: &str = "event.country";
const KEY_STAGE_COUNT: &str = "event.stageCount";
const KEY_CURRENT_STAGE: &str = "event.currentStageId";
const KEY_DATE: &str = "event.date";
const KEY_TIME: &str = "event.time";
const KEY_DIRECTOR: &str = "event.director";
const KEY_MAIN_REFEREE: &str = "event.mainReferee";

fn stage_start_date_time_key(stage: usize) -> String {
    format!("stage.{stage}.startDateTime")
}

/// Event config stored in the event database `config` table.
#[derive(Debug, Clone, Default, Serialize, Deserialize, RpcTypeStr)]
pub(crate) struct EventConfig {
    name: String,
    place: String,
    country: String,
    stage_count: usize,
    current_stage: usize,
    /// Local date `YYYY-MM-DD`, empty if not set.
    date: String,
    /// Local time `HH:MM:SS`, empty if not set.
    time: String,
    director: String,
    main_referee: String,
    /// Stage `n` is `stages[n - 1]`, there are `stage_count` of them.
    stages: Vec<StageConfig>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, RpcTypeStr)]
struct StageConfig {
    #[serde(with = "crate::rpctype::chrono_datetime")]
    start_date_time: DateTime<FixedOffset>,
}

impl EventConfig {
    /// Missing or invalid values are replaced by defaults.
    pub(crate) async fn load(sql_api: &impl QxSqlApi) -> anyhow::Result<Self> {
        let result = sql_api.query("SELECT ckey, CAST(cvalue AS TEXT) AS cvalue FROM config WHERE ckey IS NOT NULL", None).await?;
        let config = result.rows.iter()
            .filter_map(|row| {
                let key = row.first()?.as_str()?;
                let value = row.get(1).and_then(DbValue::as_str).unwrap_or_default();
                Some((key, value.trim()))
            })
            .collect::<HashMap<_, _>>();
        let string = |key: &str| config.get(key).map(|v| v.to_string()).unwrap_or_default();
        let int = |key: &str| config.get(key).and_then(|v| v.parse::<usize>().ok());

        let stage_count = int(KEY_STAGE_COUNT).unwrap_or(1);
        let stages = (1..=stage_count)
            .map(|stage| StageConfig {
                start_date_time: config.get(stage_start_date_time_key(stage).as_str())
                    .and_then(|v| DateTime::parse_from_rfc3339(v).ok())
                    .unwrap_or_default(),
            })
            .collect();
        Ok(Self {
            name: string(KEY_NAME),
            place: string(KEY_PLACE),
            country: string(KEY_COUNTRY),
            stage_count,
            current_stage: int(KEY_CURRENT_STAGE).unwrap_or(1),
            date: config.get(KEY_DATE).and_then(|v| normalize_date(v)).unwrap_or_default(),
            time: config.get(KEY_TIME).and_then(|v| normalize_time(v)).unwrap_or_default(),
            director: string(KEY_DIRECTOR),
            main_referee: string(KEY_MAIN_REFEREE),
            stages,
        })
    }

    fn stage(&self, stage: usize) -> Option<&StageConfig> {
        stage.checked_sub(1).and_then(|ix| self.stages.get(ix))
    }
}

/// `YYYY-MM-DD` date, `None` if invalid.
fn normalize_date(date: &str) -> Option<String> {
    NaiveDate::parse_from_str(date, "%Y-%m-%d").ok().map(|d| d.format("%Y-%m-%d").to_string())
}

/// `HH:MM:SS` time from `HH:MM[:SS]`, `None` if invalid.
fn normalize_time(time: &str) -> Option<String> {
    NaiveTime::parse_from_str(time, "%H:%M:%S")
        .or_else(|_| NaiveTime::parse_from_str(time, "%H:%M"))
        .ok()
        .map(|t| t.format("%H:%M:%S").to_string())
}
