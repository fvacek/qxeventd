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

        impl From<&shvproto::RpcValue> for $type {
            fn from(value: &shvproto::RpcValue) -> Self {
                shvproto::from_rpcvalue(value).expect(concat!("Failed to convert RpcValue to ", stringify!($type)))
            }
        }
    };
}