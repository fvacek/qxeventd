
pub mod eventconfignode;

#[derive(Debug, Clone, Default)]
pub(crate) struct EventConfig {
    name: String,
    place: String,
    country: String,
    stage_count: usize,
    current_stage: usize,
    date_time: chrono::DateTime<chrono::FixedOffset>,
    director: String,
    main_referee: String,
    stages: Vec<StageConfig>,
}

#[derive(Debug, Clone, Default)]
struct StageConfig {
    start_date_time: chrono::DateTime<chrono::FixedOffset>,
}
