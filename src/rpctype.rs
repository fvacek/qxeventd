/// SHV RPC type description, see <https://silicon-heaven.github.io/shv-doc/rpctypes.html>
///
/// Use `#[derive(RpcType)]` to generate it for parameter structs.
pub trait RpcType {
    const RPC_TYPE: &'static str;
}

pub use rpc_type_derive::RpcType;

macro_rules! impl_rpc_type {
    ($rpc_type:literal: $($ty:ty),+) => {
        $(impl RpcType for $ty {
            const RPC_TYPE: &'static str = $rpc_type;
        })+
    };
}

impl_rpc_type!("b": bool);
impl_rpc_type!("i": i8, i16, i32, i64, isize);
impl_rpc_type!("u": u16, u32, u64, usize);
impl_rpc_type!("f": f32, f64);
impl_rpc_type!("s": String, &str);
impl_rpc_type!("x": Vec<u8>);
impl_rpc_type!("t": shvproto::DateTime);
impl_rpc_type!("?": shvproto::RpcValue);

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};
    use shvproto::{DateTime, RpcValue};

    #[derive(Serialize, Deserialize, RpcType)]
    struct AllScalars {
        flag: bool,
        count: i32,
        size: u64,
        ratio: f64,
        name: String,
        data: Vec<u8>,
        created: DateTime,
    }

    #[derive(Serialize, Deserialize, RpcType)]
    #[serde(transparent)]
    struct RunId {
        run_id: i64,
    }

    #[derive(Serialize, Deserialize, RpcType)]
    struct Renamed {
        #[serde(rename = "eventId")]
        event_id: i64,
        #[serde(default)]
        note: String,
    }

    #[derive(Serialize, Deserialize, RpcType)]
    struct Nested {
        run: RunId,
        renamed: Renamed,
    }

    #[derive(Serialize, Deserialize, RpcType)]
    struct Empty {}

    #[derive(Serialize, Deserialize, RpcType)]
    struct OptionalParam {
        run_id: RunId,
        user_id: Option<String>
    }

    #[test]
    fn scalars() {
        assert_eq!(
            AllScalars::RPC_TYPE,
            "{b:flag,i:count,u:size,f:ratio,s:name,x:data,t:created}"
        );
    }

    #[test]
    fn rpc_value_is_any() {
        assert_eq!(RpcValue::RPC_TYPE, "?");
    }

    #[test]
    fn transparent() {
        assert_eq!(RunId::RPC_TYPE, "i");
    }

    #[test]
    fn rename_and_other_serde_attrs() {
        assert_eq!(Renamed::RPC_TYPE, "{i:eventId,s:note}");
    }

    #[test]
    fn nested() {
        assert_eq!(Nested::RPC_TYPE, "{i:run,{i:eventId,s:note}:renamed}");
    }

    #[test]
    fn empty() {
        assert_eq!(Empty::RPC_TYPE, "{}");
    }

    #[test]
    fn usable_in_const_context() {
        const PARAM: &str = Renamed::RPC_TYPE;
        assert_eq!(PARAM, "{i:eventId,s:note}");
    }

    #[test]
    fn optional_param() {
        assert_eq!(OptionalParam::RPC_TYPE, "{i:run_id,s|n:user_id}");
    }

}
