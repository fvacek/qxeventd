use smol::lock::RwLock;

pub(crate) struct QxAppState {
    pub(crate) db_pool: async_sqlite::Pool,
}
pub(crate) type QxLockedAppState = RwLock<QxAppState>;
pub(crate) type QxSharedAppState = shvclient::AppState<QxLockedAppState>;
