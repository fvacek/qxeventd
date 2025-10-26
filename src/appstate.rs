use smol::lock::RwLock;

pub(crate) type QxAppState = u32;
pub(crate) type QxLockedAppState = RwLock<QxAppState>;
pub(crate) type QxSharedAppState = shvclient::AppState<QxLockedAppState>;
