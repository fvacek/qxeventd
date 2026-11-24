//! Tests of `#[derive(RecordChange)]`

use rpc_type_derive::RecordChange;
use shvproto::RpcTypeStr;
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
    #[record_change(with = "shvproto::datetime::chrono_datetime_opt")]
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
