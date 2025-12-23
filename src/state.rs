pub type EventId = i64;

pub(crate) struct State {
    pub(crate) db_pool: async_sqlite::Pool,
}
// pub type Subscriber = shvclient::client::Subscriber<State>;
// pub type ClientCommandSender = shvclient::ClientCommandSender<State>;
impl State {
    pub fn event_mount_point(&self, event_id: EventId) -> String {
        // format!("qx/event/{}", event_id)
        "test/hsh2025".to_string()
    }
}
