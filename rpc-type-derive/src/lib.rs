//! `#[derive(RpcTypeStr)]` implements `crate::rpctype::RpcTypeStr` for a struct with named fields,
//! producing its SHV RPC type description as a compile-time `&'static str`
//! (see <https://silicon-heaven.github.io/shv-doc/rpctypes.html>).
//!
//! ```ignore
//! #[derive(Serialize, Deserialize, RpcTypeStr)]
//! struct PushEventDbParams {
//!     event_id: i64,
//!     compressed_data: Vec<u8>,
//! }
//! // PushEventDbParams::RPC_TYPE == "{i:event_id,x:compressed_data}"
//! ```
//!
//! The description mirrors serde's map serialization (KeyStruct), honouring
//! `#[serde(transparent)]` on the struct and `#[serde(rename = "...")]` on fields.
//! `Option<T>` fields are described as `T|n`, `Vec<T>` as `[T]` (except `Vec<u8>`, which is
//! a Blob `x`), `BTreeMap<K, V>` and `HashMap<K, V>` fields as `{V}`; every other field
//! type has to implement
//! `RpcTypeStr` itself.
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
}

fn serde_attrs(attrs: &[Attribute]) -> syn::Result<SerdeAttrs> {
    let mut res = SerdeAttrs::default();
    for attr in attrs.iter().filter(|a| a.path().is_ident("serde")) {
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("transparent") {
                res.transparent = true;
            } else if meta.path.is_ident("rename") {
                res.rename = Some(meta.value()?.parse::<LitStr>()?.value());
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
/// because a generic `impl RpcTypeStr` for them cannot concatenate the inner `RPC_TYPE`
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
    vec![quote! { <#ty as crate::rpctype::RpcTypeStr>::RPC_TYPE }]
}

fn expand(input: &DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    let name = &input.ident;

    if !input.generics.params.is_empty() {
        return Err(syn::Error::new_spanned(
            &input.generics,
            "RpcTypeStr does not support generic structs",
        ));
    }

    let fields = match &input.data {
        Data::Struct(data) => match &data.fields {
            Fields::Named(fields) => &fields.named,
            _ => {
                return Err(syn::Error::new_spanned(
                    &data.fields,
                    "RpcTypeStr requires a struct with named fields",
                ));
            }
        },
        _ => return Err(syn::Error::new_spanned(input, "RpcTypeStr requires a struct")),
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
        let mut parts = Vec::new();
        for (i, field) in fields.iter().enumerate() {
            let ty_parts = type_parts(&field.ty);
            let key = match serde_attrs(&field.attrs)?.rename {
                Some(rename) => rename,
                None => field.ident.as_ref().unwrap().to_string(),
            };
            let sep = if i == 0 { "" } else { "," };
            let key = format!(":{key}");
            parts.push(quote! { #sep, #(#ty_parts,)* #key });
        }
        quote! { ::const_format::concatcp!("{", #(#parts,)* "}") }
    };

    Ok(quote! {
        impl crate::rpctype::RpcTypeStr for #name {
            const RPC_TYPE: &'static str = #rpc_type;
        }
    })
}
