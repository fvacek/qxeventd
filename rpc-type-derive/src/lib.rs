//! `#[derive(RpcTypeStr)]` implements `crate::rpctype::RpcTypeStr` for a struct with named fields
//! or an enum, producing its SHV RPC type description as a compile-time `&'static str`
//! (see <https://silicon-heaven.github.io/shv-doc/rpctypes.html>).
//!
//! ```ignore
//! #[derive(Serialize, Deserialize, RpcTypeStr)]
//! struct PushEventDbParams {
//!     event_id: i64,
//!     compressed_data: Vec<u8>,
//! }
//! // PushEventDbParams::RPC_TYPE_STR == "{i:event_id,x:compressed_data}"
//! ```
//!
//! The description mirrors serde's map serialization (KeyStruct), honouring
//! `#[serde(transparent)]` on the struct and `#[serde(rename = "...")]` on fields.
//! `Option<T>` fields are described as `T|n`, `Vec<T>` as `[T]` (except `Vec<u8>`, which is
//! a Blob `x`), `BTreeMap<K, V>` and `HashMap<K, V>` fields as `{V}`; every other field
//! type has to implement
//! `RpcTypeStr` itself.
//!
//! An enum is described as an SHV Enum `i[KEY,KEY:INDEX,...]` with the variant names as keys.
//! A unit variant with an explicit discriminant `V = N` is `V:N`. As an extension, a data variant
//! has its payload type after the colon: a newtype variant `V(T)` is `V:T` and a struct variant
//! `V { f: T }` is `V:{T:f}`. The keys are the serde variant names, `#[serde(rename = "...")]` is
//! honoured, `tag`, `content` and `untagged` enums are rejected.
//!
//! Note that serde serializes the enum externally tagged, a unit variant as the `"V"` string and
//! a data variant as the `{"V":value}` map, the type string describes the variants only.
//!
//! The generated code refers to `crate::rpctype::RpcTypeStr` and `::const_format`.
//!
//! `#[derive(RecordChange)]` generates a partial update `<Name>Change` struct with a
//! `to_record()` function, see [`macro@RecordChange`].

use proc_macro::TokenStream;
use quote::quote;
use syn::{
    parse_macro_input, Attribute, Data, DeriveInput, Fields, GenericArgument, LitStr,
    PathArguments, Type, TypePath,
};

mod record_change;

#[proc_macro_derive(RpcTypeStr)]
pub fn derive_rpc_type_str(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    expand(&input)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

/// Generates `<Name>Change` with all the fields optional and its `to_record()` function.
///
/// See the `record_change` module docs for the attributes. The generated code refers to `::qxsql`.
#[proc_macro_derive(RecordChange, attributes(record_change))]
pub fn derive_record_change(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    record_change::expand(&input)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

#[derive(Default)]
struct SerdeAttrs {
    transparent: bool,
    rename: Option<String>,
    /// `tag`, `content` or `untagged`, an enum representation other than externally tagged
    non_external_tagging: bool,
}

fn serde_attrs(attrs: &[Attribute]) -> syn::Result<SerdeAttrs> {
    let mut res = SerdeAttrs::default();
    for attr in attrs.iter().filter(|a| a.path().is_ident("serde")) {
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("transparent") {
                res.transparent = true;
            } else if meta.path.is_ident("rename") {
                res.rename = Some(meta.value()?.parse::<LitStr>()?.value());
            } else if meta.path.is_ident("untagged") {
                res.non_external_tagging = true;
            } else if meta.path.is_ident("tag") || meta.path.is_ident("content") {
                res.non_external_tagging = true;
                meta.value()?.parse::<syn::Expr>()?;
            } else if meta.input.peek(syn::Token![=]) {
                meta.value()?.parse::<syn::Expr>()?;
            } else if meta.input.peek(syn::token::Paren) {
                meta.parse_nested_meta(|_| Ok(()))?;
            }
            Ok(())
        })?;
    }
    Ok(res)
}

fn is_u8(ty: &Type) -> bool {
    matches!(ty, Type::Path(TypePath { qself: None, path }) if path.is_ident("u8"))
}

/// Const string pieces describing `ty`.
///
/// `Option<T>` is expanded here as `T|n`, `Vec<T>` as `[T]` and `BTreeMap<K, V>` /
/// `HashMap<K, V>` as `{V}`,
/// because a generic `impl RpcTypeStr` for them cannot concatenate the inner `RPC_TYPE_STR`
/// into a `&'static str` on stable Rust.
fn type_parts(ty: &Type) -> Vec<proc_macro2::TokenStream> {
    if let Type::Path(TypePath { qself: None, path }) = ty
        && let Some(seg) = path.segments.last()
        && let PathArguments::AngleBracketed(args) = &seg.arguments
    {
        let args = args.args.iter().collect::<Vec<_>>();
        match (seg.ident.to_string().as_str(), &args[..]) {
            ("Option", [GenericArgument::Type(inner)]) => {
                let mut parts = type_parts(inner);
                parts.push(quote! { "|n" });
                return parts;
            }
            // Vec<u8> is a Blob, left to its RpcTypeStr impl
            ("Vec", [GenericArgument::Type(item)]) if !is_u8(item) => {
                let mut parts = vec![quote! { "[" }];
                parts.extend(type_parts(item));
                parts.push(quote! { "]" });
                return parts;
            }
            ("BTreeMap" | "HashMap", [GenericArgument::Type(_), GenericArgument::Type(value)]) => {
                let mut parts = vec![quote! { "{" }];
                parts.extend(type_parts(value));
                parts.push(quote! { "}" });
                return parts;
            }
            _ => {}
        }
    }
    vec![quote! { <#ty as crate::rpctype::RpcTypeStr>::RPC_TYPE_STR }]
}

fn expand(input: &DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    let name = &input.ident;

    if !input.generics.params.is_empty() {
        return Err(syn::Error::new_spanned(
            &input.generics,
            "RpcTypeStr does not support generic types",
        ));
    }

    let fields = match &input.data {
        Data::Enum(data) => return expand_enum(input, data),
        Data::Struct(data) => match &data.fields {
            Fields::Named(fields) => &fields.named,
            _ => {
                return Err(syn::Error::new_spanned(
                    &data.fields,
                    "RpcTypeStr requires a struct with named fields",
                ));
            }
        },
        Data::Union(_) => {
            return Err(syn::Error::new_spanned(input, "RpcTypeStr requires a struct or an enum"));
        }
    };

    let rpc_type = if serde_attrs(&input.attrs)?.transparent {
        let [field] = fields.iter().collect::<Vec<_>>()[..] else {
            return Err(syn::Error::new_spanned(
                fields,
                "#[serde(transparent)] requires exactly one field",
            ));
        };
        let parts = type_parts(&field.ty);
        quote! { ::const_format::concatcp!(#(#parts),*) }
    } else {
        let parts = named_fields_parts(fields)?;
        quote! { ::const_format::concatcp!(#(#parts),*) }
    };

    Ok(quote! {
        impl crate::rpctype::RpcTypeStr for #name {
            const RPC_TYPE_STR: &'static str = #rpc_type;
        }
    })
}

/// Const string pieces describing named fields as a map `{T:key,...}`
fn named_fields_parts<'a>(
    fields: impl IntoIterator<Item = &'a syn::Field>,
) -> syn::Result<Vec<proc_macro2::TokenStream>> {
    let mut parts = vec![quote! { "{" }];
    for (i, field) in fields.into_iter().enumerate() {
        if i > 0 {
            parts.push(quote! { "," });
        }
        parts.extend(type_parts(&field.ty));
        let key = match serde_attrs(&field.attrs)?.rename {
            Some(rename) => rename,
            None => field.ident.as_ref().unwrap().to_string(),
        };
        let key = format!(":{key}");
        parts.push(quote! { #key });
    }
    parts.push(quote! { "}" });
    Ok(parts)
}

/// SHV Enum `i[KEY,KEY:INDEX,...]`, extended for data variants with `KEY:TYPE`.
///
/// Every variant is a `KEY` (its serde name). A unit variant with an explicit discriminant
/// `V = N` is `V:N`, a newtype variant `V(T)` is `V:T` and a struct variant `V { f: T }` is
/// `V:{T:f}`. The type is the variant payload as serde serializes it.
fn expand_enum(input: &DeriveInput, data: &syn::DataEnum) -> syn::Result<proc_macro2::TokenStream> {
    let name = &input.ident;
    if serde_attrs(&input.attrs)?.non_external_tagging {
        return Err(syn::Error::new_spanned(
            input,
            "RpcTypeStr supports externally tagged enums only, not `tag`, `content` or `untagged`",
        ));
    }
    if data.variants.is_empty() {
        return Err(syn::Error::new_spanned(input, "RpcTypeStr does not support enums without variants"));
    }

    let mut parts = vec![quote! { "i[" }];
    for (i, variant) in data.variants.iter().enumerate() {
        let mut key = if i > 0 { "," } else { "" }.to_string();
        key.push_str(&match serde_attrs(&variant.attrs)?.rename {
            Some(rename) => rename,
            None => variant.ident.to_string(),
        });
        let type_parts = match &variant.fields {
            Fields::Unit => {
                if let Some((_, expr)) = &variant.discriminant {
                    key.push(':');
                    key.push_str(&discriminant_value(expr)?);
                }
                parts.push(quote! { #key });
                continue;
            }
            _ if variant.discriminant.is_some() => {
                return Err(syn::Error::new_spanned(
                    variant,
                    "RpcTypeStr supports explicit discriminants on unit variants only",
                ));
            }
            Fields::Unnamed(fields) if fields.unnamed.len() == 1 => {
                type_parts(&fields.unnamed[0].ty)
            }
            Fields::Named(fields) => named_fields_parts(&fields.named)?,
            Fields::Unnamed(_) => {
                return Err(syn::Error::new_spanned(
                    variant,
                    "RpcTypeStr does not support tuple variants with more than one field",
                ));
            }
        };
        key.push(':');
        parts.push(quote! { #key });
        parts.extend(type_parts);
    }
    parts.push(quote! { "]" });
    Ok(quote! {
        impl crate::rpctype::RpcTypeStr for #name {
            const RPC_TYPE_STR: &'static str = ::const_format::concatcp!(#(#parts),*);
        }
    })
}

/// Decimal value of an integer literal discriminant, optionally negated
fn discriminant_value(expr: &syn::Expr) -> syn::Result<String> {
    match expr {
        syn::Expr::Lit(syn::ExprLit { lit: syn::Lit::Int(lit), .. }) => {
            Ok(lit.base10_digits().to_string())
        }
        syn::Expr::Unary(syn::ExprUnary { op: syn::UnOp::Neg(_), expr, .. }) => {
            Ok(format!("-{}", discriminant_value(expr)?))
        }
        syn::Expr::Paren(syn::ExprParen { expr, .. }) | syn::Expr::Group(syn::ExprGroup { expr, .. }) => {
            discriminant_value(expr)
        }
        _ => Err(syn::Error::new_spanned(
            expr,
            "RpcTypeStr requires an integer literal discriminant",
        )),
    }
}
