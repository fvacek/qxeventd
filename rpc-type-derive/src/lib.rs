//! `#[derive(RpcType)]` implements `crate::rpctype::RpcType` for a struct with named fields,
//! producing its SHV RPC type description as a compile-time `&'static str`
//! (see <https://silicon-heaven.github.io/shv-doc/rpctypes.html>).
//!
//! ```ignore
//! #[derive(Serialize, Deserialize, RpcType)]
//! struct PushEventDbParams {
//!     event_id: i64,
//!     compressed_data: Vec<u8>,
//! }
//! // PushEventDbParams::RPC_TYPE == "{i:event_id,x:compressed_data}"
//! ```
//!
//! The description mirrors serde's map serialization (KeyStruct), honouring
//! `#[serde(transparent)]` on the struct and `#[serde(rename = "...")]` on fields.
//! `Option<T>` fields are described as `T|n`; every other field type has to implement
//! `RpcType` itself.
//!
//! The generated code refers to `crate::rpctype::RpcType` and `::const_format`.

use proc_macro::TokenStream;
use quote::quote;
use syn::{
    parse_macro_input, Attribute, Data, DeriveInput, Fields, GenericArgument, LitStr,
    PathArguments, Type, TypePath,
};

#[proc_macro_derive(RpcType)]
pub fn derive_rpc_type(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    expand(&input)
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

/// Const string pieces describing `ty`.
///
/// `Option<T>` is expanded here as `T|n`, because a generic `impl RpcType for Option<T>`
/// cannot concatenate `T::RPC_TYPE` into a `&'static str` on stable Rust.
fn type_parts(ty: &Type) -> Vec<proc_macro2::TokenStream> {
    if let Type::Path(TypePath { qself: None, path }) = ty
        && let Some(seg) = path.segments.last()
        && seg.ident == "Option"
        && let PathArguments::AngleBracketed(args) = &seg.arguments
        && let [GenericArgument::Type(inner)] = &args.args.iter().collect::<Vec<_>>()[..]
    {
        let mut parts = type_parts(inner);
        parts.push(quote! { "|n" });
        return parts;
    }
    vec![quote! { <#ty as crate::rpctype::RpcType>::RPC_TYPE }]
}

fn expand(input: &DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    let name = &input.ident;

    if !input.generics.params.is_empty() {
        return Err(syn::Error::new_spanned(
            &input.generics,
            "RpcType does not support generic structs",
        ));
    }

    let fields = match &input.data {
        Data::Struct(data) => match &data.fields {
            Fields::Named(fields) => &fields.named,
            _ => {
                return Err(syn::Error::new_spanned(
                    &data.fields,
                    "RpcType requires a struct with named fields",
                ));
            }
        },
        _ => return Err(syn::Error::new_spanned(input, "RpcType requires a struct")),
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
        impl crate::rpctype::RpcType for #name {
            const RPC_TYPE: &'static str = #rpc_type;
        }
    })
}
