use serde::Serialize;
use serde::de::DeserializeOwned;
use shvproto::RpcValue;

/// RpcValue conversions for every type implementing `Serialize` and `DeserializeOwned`
///
/// # Example
///
/// ```ignore
/// #[derive(RpcPosParams)]
/// struct MyParams {
///     event_id: i64,
///     name: String,
/// }
///
/// let params = MyParams::from_rpc_param(rq.param())?;
/// let rpc_value: RpcValue = params.to_rpcvalue();
/// ```
pub trait RpcValueExt: Sized {
    fn try_from_rpcvalue(value: &RpcValue) -> anyhow::Result<Self>;
    fn from_rpc_param(param: Option<&RpcValue>) -> anyhow::Result<Self>;
    fn to_rpcvalue(&self) -> RpcValue;
}

impl<T: Serialize + DeserializeOwned> RpcValueExt for T {
    fn try_from_rpcvalue(value: &RpcValue) -> anyhow::Result<Self> {
        shvproto::from_rpcvalue(value).map_err(|e| anyhow::anyhow!(e))
    }

    fn from_rpc_param(param: Option<&RpcValue>) -> anyhow::Result<Self> {
        let value = param.ok_or_else(|| anyhow::anyhow!("RpcValue is None"))?;
        Self::try_from_rpcvalue(value)
    }

    fn to_rpcvalue(&self) -> RpcValue {
        shvproto::to_rpcvalue(self)
            .unwrap_or_else(|e| panic!("Failed to convert {} to RpcValue: {e}", std::any::type_name::<T>()))
    }
}
