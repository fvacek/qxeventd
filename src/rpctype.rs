/// SHV RPC type description, see <https://silicon-heaven.github.io/shv-doc/rpctypes.html>
///
/// Use `#[derive(RpcTypeStr)]` to generate it for parameter structs.
pub trait RpcTypeStr {
    const RPC_TYPE_STR: &'static str;
}

pub use rpc_type_derive::{RecordChange, RpcTypeStr};

macro_rules! impl_rpc_type {
    ($rpc_type:literal: $($ty:ty),+) => {
        $(impl RpcTypeStr for $ty {
            const RPC_TYPE_STR: &'static str = $rpc_type;
        })+
    };
}

impl_rpc_type!("b": bool);
impl_rpc_type!("i": i8, i16, i32, i64, isize);
impl_rpc_type!("u": u16, u32, u64, usize);
impl_rpc_type!("f": f32, f64);
impl_rpc_type!("s": String, &str);
impl_rpc_type!("x": Vec<u8>);
// chrono serializes itself as an RFC 3339 string, fields sent over RPC have to use
// `#[serde(with = "chrono_datetime")]` or `chrono_datetime_opt` to be SHV DateTime
impl_rpc_type!("t": shvproto::DateTime, chrono::DateTime<chrono::FixedOffset>);
impl_rpc_type!("?": shvproto::RpcValue);

pub(crate) mod chrono_datetime {
    use chrono::{DateTime, FixedOffset};
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub(crate) fn serialize<S: Serializer>(dt: &DateTime<FixedOffset>, serializer: S) -> Result<S::Ok, S::Error> {
        shvproto::DateTime::from_datetime(dt).serialize(serializer)
    }

    pub(crate) fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<DateTime<FixedOffset>, D::Error> {
        Ok(shvproto::DateTime::deserialize(deserializer)?.to_chrono_datetime())
    }
}

pub(crate) mod chrono_datetime_opt {
    use chrono::{DateTime, FixedOffset};
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub(crate) fn serialize<S: Serializer>(dt: &Option<DateTime<FixedOffset>>, serializer: S) -> Result<S::Ok, S::Error> {
        dt.as_ref().map(shvproto::DateTime::from_datetime).serialize(serializer)
    }

    pub(crate) fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Option<DateTime<FixedOffset>>, D::Error> {
        Ok(Option::<shvproto::DateTime>::deserialize(deserializer)?.map(|dt| dt.to_chrono_datetime()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};
    use shvproto::{DateTime, RpcValue};
    use std::collections::{BTreeMap, HashMap};

    #[derive(Serialize, Deserialize, RpcTypeStr)]
    struct AllScalars {
        flag: bool,
        count: i32,
        size: u64,
        ratio: f64,
        name: String,
        data: Vec<u8>,
        created: DateTime,
        modified: chrono::DateTime<chrono::FixedOffset>,
    }

    #[derive(Serialize, Deserialize, RpcTypeStr)]
    #[serde(transparent)]
    struct RunId {
        run_id: i64,
    }

    #[derive(Serialize, Deserialize, RpcTypeStr)]
    struct Renamed {
        #[serde(rename = "eventId")]
        event_id: i64,
        #[serde(default)]
        note: String,
    }

    #[derive(Serialize, Deserialize, RpcTypeStr)]
    struct Nested {
        run: RunId,
        renamed: Renamed,
    }

    #[derive(Serialize, Deserialize, RpcTypeStr)]
    struct Empty {}

    #[derive(Serialize, Deserialize, RpcTypeStr)]
    struct OptionalParam {
        run_id: RunId,
        user_id: Option<String>
    }

    #[derive(Serialize, Deserialize, RpcTypeStr)]
    struct Maps {
        names: BTreeMap<String, String>,
        counts: Option<HashMap<String, i64>>,
        nested: BTreeMap<String, Option<RunId>>,
    }

    #[derive(Serialize, Deserialize, RpcTypeStr)]
    struct Lists {
        ids: Vec<i64>,
        data: Vec<u8>,
        runs: Option<Vec<Option<RunId>>>,
        matrix: Vec<Vec<String>>,
    }

    #[derive(Serialize, Deserialize, RpcTypeStr)]
    #[serde(transparent)]
    struct RenamedList {
        items: Vec<Renamed>,
    }

    #[test]
    fn scalars() {
        assert_eq!(
            AllScalars::RPC_TYPE_STR,
            "{b:flag,i:count,u:size,f:ratio,s:name,x:data,t:created,t:modified}"
        );
    }

    #[test]
    fn rpc_value_is_any() {
        assert_eq!(RpcValue::RPC_TYPE_STR, "?");
    }

    #[test]
    fn transparent() {
        assert_eq!(RunId::RPC_TYPE_STR, "i");
    }

    #[test]
    fn rename_and_other_serde_attrs() {
        assert_eq!(Renamed::RPC_TYPE_STR, "{i:eventId,s:note}");
    }

    #[test]
    fn nested() {
        assert_eq!(Nested::RPC_TYPE_STR, "{i:run,{i:eventId,s:note}:renamed}");
    }

    #[test]
    fn empty() {
        assert_eq!(Empty::RPC_TYPE_STR, "{}");
    }

    #[test]
    fn usable_in_const_context() {
        const PARAM: &str = Renamed::RPC_TYPE_STR;
        assert_eq!(PARAM, "{i:eventId,s:note}");
    }

    #[test]
    fn optional_param() {
        assert_eq!(OptionalParam::RPC_TYPE_STR, "{i:run_id,s|n:user_id}");
    }

    #[test]
    fn lists() {
        assert_eq!(Lists::RPC_TYPE_STR, "{[i]:ids,x:data,[i|n]|n:runs,[[s]]:matrix}");
        assert_eq!(RenamedList::RPC_TYPE_STR, "[{i:eventId,s:note}]");
    }

    #[test]
    fn maps() {
        assert_eq!(Maps::RPC_TYPE_STR, "{{s}:names,{i}|n:counts,{i|n}:nested}");
    }

    #[derive(Debug, PartialEq, Serialize, Deserialize, RpcTypeStr)]
    struct ChronoDates {
        #[serde(with = "chrono_datetime")]
        date: chrono::DateTime<chrono::FixedOffset>,
        #[serde(default, with = "chrono_datetime_opt")]
        opt_date: Option<chrono::DateTime<chrono::FixedOffset>>,
    }

    #[derive(Serialize, Deserialize, RpcTypeStr)]
    enum Mode {
        Archived,
        #[serde(rename = "readOnly")]
        ReadOnly,
    }

    #[derive(Serialize, Deserialize, RpcTypeStr)]
    struct WithEnum {
        mode: Mode,
        opt_mode: Option<Mode>,
    }

    #[test]
    fn unit_enum_is_int_enum() {
        assert_eq!(Mode::RPC_TYPE_STR, "s[Archived,readOnly]");
        assert_eq!(WithEnum::RPC_TYPE_STR, "{s[Archived,readOnly]:mode,s[Archived,readOnly]|n:opt_mode}");
        assert_eq!(shvproto::to_rpcvalue(&Mode::ReadOnly).unwrap().as_str(), "readOnly");
    }

    #[derive(Serialize, Deserialize, RpcTypeStr)]
    #[repr(i32)]
    enum Mixed {
        None,
        Id(i64),
        #[serde(rename = "pos")]
        Position { x: i32, #[serde(rename = "Y")] y: Option<i32> },
        Other = 123,
    }

    #[derive(Serialize, Deserialize, RpcTypeStr)]
    enum Indexed {
        Fail = -1,
        Success,
    }

    #[test]
    fn data_enum_is_externally_tagged() {
        assert_eq!(Indexed::RPC_TYPE_STR, "s[Fail,Success]");
        assert_eq!(Mixed::RPC_TYPE_STR, "s[None,Id:i,pos:{i:x,i|n:Y},Other]");
        assert_eq!(shvproto::to_rpcvalue(&Mixed::None).unwrap().to_cpon(), r#""None""#);
        assert_eq!(shvproto::to_rpcvalue(&Mixed::Id(3)).unwrap().to_cpon(), r#"{"Id":3}"#);
        assert_eq!(
            shvproto::to_rpcvalue(&Mixed::Position { x: 1, y: None }).unwrap().to_cpon(),
            r#"{"pos":{"Y":null,"x":1}}"#
        );
    }

    #[test]
    fn chrono_datetime_is_shv_datetime() {
        assert_eq!(ChronoDates::RPC_TYPE_STR, "{t:date,t|n:opt_date}");

        let date = chrono::DateTime::parse_from_rfc3339("2026-10-31T10:00:00.123+01:00").unwrap();
        let dates = ChronoDates { date, opt_date: Some(date) };
        let value = shvproto::to_rpcvalue(&dates).unwrap();
        assert_eq!(value.get("date").unwrap().type_name(), "DateTime");
        assert_eq!(value.get("opt_date").unwrap().type_name(), "DateTime");
        assert_eq!(shvproto::from_rpcvalue::<ChronoDates>(&value).unwrap(), dates);

        let none = ChronoDates { date, opt_date: None };
        let value = shvproto::to_rpcvalue(&none).unwrap();
        assert_eq!(shvproto::from_rpcvalue::<ChronoDates>(&value).unwrap(), none);
    }

    #[test]
    fn chrono_datetime_accepts_rfc3339_string() {
        let value = RpcValue::from_cpon(r#"{"date":"2026-10-31T10:00:00+01:00","opt_date":"2026-10-31T09:00:00Z"}"#).unwrap();
        let dates = shvproto::from_rpcvalue::<ChronoDates>(&value).unwrap();
        assert_eq!(dates.date, chrono::DateTime::parse_from_rfc3339("2026-10-31T10:00:00+01:00").unwrap());
        assert_eq!(dates.opt_date, Some(chrono::DateTime::parse_from_rfc3339("2026-10-31T09:00:00Z").unwrap()));
    }

    mod record_change {
        use super::super::*;
        use qxsql::{DbValue, Record};

        #[allow(dead_code)]
        #[derive(RecordChange)]
        #[record_change(rpc_type_str)]
        struct Item {
            #[record_change(skip)]
            id: Option<i64>,
            name: String,
            count: i64,
            ratio: f64,
            flag: bool,
            data: Vec<u8>,
            note: Option<String>,
            #[record_change(rename = "item_kind")]
            kind: String,
            #[record_change(with = "crate::rpctype::chrono_datetime_opt")]
            created: chrono::DateTime<chrono::FixedOffset>,
        }

        #[allow(dead_code)]
        #[derive(RecordChange)]
        #[record_change(derive(Default))]
        struct NoSerde {
            name: String,
        }

        fn date() -> chrono::DateTime<chrono::FixedOffset> {
            chrono::DateTime::parse_from_rfc3339("2026-10-31T10:00:00.123+01:00").unwrap()
        }

        fn record(items: &[(&str, DbValue)]) -> Record {
            items.iter().map(|(k, v)| (k.to_string(), v.clone())).collect()
        }

        #[test]
        fn empty_change_is_empty_record() {
            assert_eq!(ItemChange::default().to_record(), Record::new());
        }

        #[test]
        fn all_fields_set() {
            let change = ItemChange {
                name: Some("foo".into()),
                count: Some(42),
                ratio: Some(1.5),
                flag: Some(true),
                data: Some(vec![1, 2, 3]),
                note: Some("bar".into()),
                item_kind: Some("baz".into()),
                created: Some(date()),
            };
            assert_eq!(change.to_record(), record(&[
                ("name", DbValue::String("foo".into())),
                ("count", DbValue::Int(42)),
                ("ratio", DbValue::Double(1.5)),
                ("flag", DbValue::Bool(true)),
                ("data", DbValue::Blob(vec![1, 2, 3])),
                ("note", DbValue::String("bar".into())),
                // renamed field is still keyed by the original field name, the DB column
                ("kind", DbValue::String("baz".into())),
                ("created", DbValue::DateTime(date())),
            ]));
        }

        #[test]
        fn only_set_fields() {
            let change = ItemChange { count: Some(0), item_kind: Some("k".into()), ..Default::default() };
            assert_eq!(change.to_record(), record(&[
                ("count", DbValue::Int(0)),
                ("kind", DbValue::String("k".into())),
            ]));
        }

        #[test]
        fn from_rpc_value() {
            let value = shvproto::RpcValue::from_cpon(
                r#"{"name":"foo","item_kind":"k","created":d"2026-10-31T10:00:00.123+01"}"#
            ).unwrap();
            let change = shvproto::from_rpcvalue::<ItemChange>(&value).unwrap();
            assert_eq!(change.to_record(), record(&[
                ("name", DbValue::String("foo".into())),
                ("kind", DbValue::String("k".into())),
                ("created", DbValue::DateTime(date())),
            ]));
        }

        #[test]
        fn unset_fields_are_not_serialized() {
            let change = ItemChange { name: Some("foo".into()), created: Some(date()), ..Default::default() };
            let value = shvproto::to_rpcvalue(&change).unwrap();
            assert_eq!(value.as_map().keys().collect::<Vec<_>>(), ["created", "name"]);
            assert_eq!(value.get("created").unwrap().type_name(), "DateTime");
        }

        #[test]
        fn rpc_type() {
            assert_eq!(
                ItemChange::RPC_TYPE_STR,
                "{s|n:name,i|n:count,f|n:ratio,b|n:flag,x|n:data,s|n:note,s|n:item_kind,t|n:created}"
            );
        }

        #[test]
        fn custom_derives() {
            let change = NoSerdeChange { name: Some("foo".into()) };
            assert_eq!(change.to_record(), record(&[("name", DbValue::String("foo".into()))]));
            assert_eq!(NoSerdeChange::default().to_record(), Record::new());
        }
    }
}
