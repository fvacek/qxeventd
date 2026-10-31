use serde::Serialize;
use serde::de::DeserializeOwned;
use shvproto::RpcValue;

/// RpcValue conversions for every type implementing `Serialize` and `DeserializeOwned`
///
/// # Example
///
/// ```ignore
/// #[derive(Serialize, Deserialize)]
/// struct MyParams {
///     event_id: i64,
///     name: String,
/// }
///
/// let params = MyParams::try_from_rpcvalue(rq.param().unwrap_or_default())?;
/// let rpc_value: RpcValue = params.to_rpcvalue();
/// ```
pub trait RpcValueExt: Sized {
    fn try_from_rpcvalue(value: &RpcValue) -> anyhow::Result<Self>;
    fn to_rpcvalue(&self) -> RpcValue;
}

impl<T: Serialize + DeserializeOwned> RpcValueExt for T {
    fn try_from_rpcvalue(value: &RpcValue) -> anyhow::Result<Self> {
        shvproto::from_rpcvalue(value).map_err(|e| anyhow::anyhow!(e))
    }

    fn to_rpcvalue(&self) -> RpcValue {
        shvproto::to_rpcvalue(self)
            .unwrap_or_else(|e| panic!("Failed to convert {} to RpcValue: {e}", std::any::type_name::<T>()))
    }
}
