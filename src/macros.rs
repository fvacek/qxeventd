/// Macro to generate bidirectional RpcValue conversion implementations for types with Serialize/Deserialize
///
/// This macro generates three `From` trait implementations:
/// - `From<&Type> for RpcValue` - Convert a reference to RpcValue
/// - `From<Type> for RpcValue` - Convert an owned value to RpcValue
/// - `From<&RpcValue> for Type` - Convert RpcValue back to Type
///
/// # Example
///
/// ```
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Debug, Clone, Serialize, Deserialize)]
/// struct MyData {
///     name: String,
///     value: i32,
/// }
///
/// impl_rpcvalue_conversions!(MyData);
///
/// // Now you can use:
/// let data = MyData { name: "test".to_string(), value: 42 };
/// let rpc_value: RpcValue = data.into();
/// let data_back: MyData = (&rpc_value).into();
/// ```
#[macro_export]
macro_rules! impl_rpcvalue_conversions {
    ($type:ty) => {
        impl From<&$type> for shvproto::RpcValue {
            fn from(value: &$type) -> Self {
                shvproto::to_rpcvalue(value).expect(concat!("Failed to convert ", stringify!($type), " to RpcValue"))
            }
        }

        impl From<$type> for shvproto::RpcValue {
            fn from(value: $type) -> Self {
                shvproto::to_rpcvalue(&value).expect(concat!("Failed to convert ", stringify!($type), " to RpcValue"))
            }
        }

        impl TryFrom<&shvproto::RpcValue> for $type {
            type Error = anyhow::Error;

            fn try_from(value: &shvproto::RpcValue) -> Result<Self, Self::Error> {
                shvproto::from_rpcvalue(value).map_err(|e| anyhow::anyhow!(e))
            }
        }

        impl TryFrom<Option<&shvproto::RpcValue>> for $type {
            type Error = anyhow::Error;

            fn try_from(value: Option<&shvproto::RpcValue>) -> Result<Self, Self::Error> {
                value.ok_or_else(|| anyhow::anyhow!("RpcValue is None"))
                    .and_then(|v| shvproto::from_rpcvalue(v).map_err(|e| anyhow::anyhow!(e)))
            }

        }
    };
}
