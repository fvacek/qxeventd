//! `#[derive(RecordChange)]` generates a `<Name>Change` struct, a partial update of `<Name>`.
//!
//! `to_record()` for `<Name>` itself is not generated, derive `qxsql::ToRecord` for it.
//!
//! Every field of the original struct becomes `Option<T>` in the change struct (fields which
//! are `Option<T>` already are kept as they are), and `to_record()` builds a `qxsql::Record`
//! from the fields which are set, keyed by the original field name, i.e. the DB column.
//! Values are converted with `qxsql::DbValue::from`, so every field type has to implement
//! `From<T> for DbValue`.
//!
//! ```ignore
//! #[derive(RecordChange)]
//! struct EventRecord {
//!     #[record_change(skip)]
//!     id: Option<i64>,
//!     name: String,
//!     #[record_change(with = "crate::rpctype::chrono_datetime_opt")]
//!     date: DateTime<FixedOffset>,
//!     #[record_change(rename = "master_db_mode")]
//!     db_mode: EventDbMode,
//! }
//! // struct EventRecordChange { name: Option<String>, date: Option<DateTime<FixedOffset>>,
//! //                            master_db_mode: Option<EventDbMode> }
//! // EventRecordChange::to_record() -> Record with the set keys of "name", "date", "db_mode"
//! ```
//!
//! # Struct attributes
//! - `#[record_change(derive(...))]` — derives applied to the change struct, replacing the
//!   default `Debug, Clone, Default, serde::Serialize, serde::Deserialize`; `derive()` means
//!   no derives
//! - `#[record_change(rpc_type_str)]` — additionally derive `crate::rpctype::RpcTypeStr` for the
//!   change struct, every field type has to implement `RpcTypeStr` then
//!
//! # Field attributes
//! - `#[record_change(skip)]` — field is not part of the change struct
//! - `#[record_change(rename = "field")]` — name of the change struct field, the record key
//!   is still the original field name
//! - `#[record_change(with = "path")]` — `#[serde(with = "path")]` for the change struct field,
//!   it has to (de)serialize the `Option<T>`
//!
//! Change struct fields are `#[serde(default, skip_serializing_if = "Option::is_none")]`,
//! serde attributes of the original struct are not copied.

use quote::{format_ident, quote};
use syn::{
    Attribute, Data, DeriveInput, Fields, GenericArgument, Ident, LitStr, Path, PathArguments,
    Type, TypePath,
};

#[derive(Default)]
struct FieldAttrs {
    skip: bool,
    rename: Option<Ident>,
    with: Option<LitStr>,
}

fn field_attrs(attrs: &[Attribute]) -> syn::Result<FieldAttrs> {
    let mut res = FieldAttrs::default();
    for attr in attrs.iter().filter(|a| a.path().is_ident("record_change")) {
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("skip") {
                res.skip = true;
            } else if meta.path.is_ident("rename") {
                let name = meta.value()?.parse::<LitStr>()?;
                res.rename = Some(name.parse()?);
            } else if meta.path.is_ident("with") {
                res.with = Some(meta.value()?.parse::<LitStr>()?);
            } else {
                return Err(meta.error("unknown record_change field attribute"));
            }
            Ok(())
        })?;
    }
    Ok(res)
}

#[derive(Default)]
struct StructAttrs {
    /// Derives listed in `#[record_change(derive(...))]`, `None` without the attribute
    derives: Option<Vec<Path>>,
    rpc_type_str: bool,
}

fn struct_attrs(attrs: &[Attribute]) -> syn::Result<StructAttrs> {
    let mut res = StructAttrs::default();
    for attr in attrs.iter().filter(|a| a.path().is_ident("record_change")) {
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("derive") {
                let derives = res.derives.get_or_insert_with(Vec::new);
                meta.parse_nested_meta(|derive| {
                    derives.push(derive.path);
                    Ok(())
                })
            } else if meta.path.is_ident("rpc_type_str") {
                res.rpc_type_str = true;
                Ok(())
            } else {
                Err(meta.error("unknown record_change struct attribute"))
            }
        })?;
    }
    Ok(res)
}

fn is_option(ty: &Type) -> bool {
    matches!(ty, Type::Path(TypePath { qself: None, path })
        if path.segments.last().is_some_and(|seg| seg.ident == "Option"
            && matches!(&seg.arguments, PathArguments::AngleBracketed(args)
                if matches!(args.args.iter().collect::<Vec<_>>()[..], [GenericArgument::Type(_)]))))
}

pub(crate) fn expand(input: &DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    let name = &input.ident;
    let vis = &input.vis;
    let change_name = format_ident!("{name}Change");

    if !input.generics.params.is_empty() {
        return Err(syn::Error::new_spanned(
            &input.generics,
            "RecordChange does not support generic structs",
        ));
    }

    let fields = match &input.data {
        Data::Struct(data) => match &data.fields {
            Fields::Named(fields) => &fields.named,
            _ => {
                return Err(syn::Error::new_spanned(
                    &data.fields,
                    "RecordChange requires a struct with named fields",
                ));
            }
        },
        _ => return Err(syn::Error::new_spanned(input, "RecordChange requires a struct")),
    };

    let struct_attrs = struct_attrs(&input.attrs)?;
    let mut derives = match struct_attrs.derives {
        Some(derives) => derives,
        // full paths, so they work without any imports at the call site
        None => vec![
            syn::parse_quote!(::core::fmt::Debug),
            syn::parse_quote!(::core::clone::Clone),
            syn::parse_quote!(::core::default::Default),
            syn::parse_quote!(::serde::Serialize),
            syn::parse_quote!(::serde::Deserialize),
        ],
    };
    if struct_attrs.rpc_type_str {
        derives.push(syn::parse_quote!(crate::rpctype::RpcTypeStr));
    }
    // `#[serde]` field attributes are only known with a serde derive
    let has_serde = derives.iter().any(|path| {
        path.segments.last().is_some_and(|seg| seg.ident == "Serialize" || seg.ident == "Deserialize")
    });

    let mut change_fields = Vec::new();
    let mut inserts = Vec::new();
    for field in fields {
        let attrs = field_attrs(&field.attrs)?;
        if attrs.skip {
            continue;
        }
        let orig_ident = field.ident.as_ref().unwrap();
        let ident = attrs.rename.as_ref().unwrap_or(orig_ident);
        let field_vis = &field.vis;
        let ty = &field.ty;
        let key = orig_ident.to_string();
        let ty = if is_option(ty) { quote! { #ty } } else { quote! { Option<#ty> } };
        let docs = field.attrs.iter().filter(|a| a.path().is_ident("doc"));
        let serde = if has_serde {
            let with = attrs.with.map(|with| quote! { , with = #with });
            quote! { #[serde(default, skip_serializing_if = "Option::is_none" #with)] }
        } else if let Some(with) = attrs.with {
            return Err(syn::Error::new_spanned(with, "`with` requires a Serialize or Deserialize derive"));
        } else {
            quote! {}
        };
        change_fields.push(quote! {
            #(#docs)*
            #serde
            #field_vis #ident: #ty
        });

        inserts.push(quote! {
            if let Some(v) = &self.#ident {
                record.insert(#key.to_string(), ::qxsql::DbValue::from(v.clone()));
            }
        });
    }

    let doc = format!("Partial update of [`{name}`], generated by `#[derive(RecordChange)]`.");
    Ok(quote! {
        #[doc = #doc]
        #[derive(#(#derives),*)]
        #vis struct #change_name {
            #(#change_fields,)*
        }

        impl #change_name {
            /// Record with the fields which are set, keyed by the column names.
            pub fn to_record(&self) -> ::qxsql::Record {
                let mut record = ::qxsql::Record::new();
                #(#inserts)*
                record
            }
        }
    })
}
