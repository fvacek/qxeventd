//! `#[derive(RpcPosParams)]` implements `serde::Serialize` and `serde::Deserialize`
//! for a struct with named fields so that it travels over RPC as a positional list
//! in field declaration order.
//!
//! ```ignore
//! #[derive(RpcPosParams)]
//! struct AddParams {
//!     a: i32,
//!     b: i32,
//! }
//! // AddParams { a: 10, b: 20 } <-> [10, 20]
//! ```
//!
//! A struct with a single field is (de)serialized transparently as that field's value,
//! so `struct P { id: i64 }` travels as `42`, not as `[42]`.
//!
//! Reordering, inserting or removing fields is a wire-protocol breaking change.
//! The generated code refers to `::serde`, so the user crate has to depend on `serde`.

use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{parse_macro_input, Data, DeriveInput, Fields};

#[proc_macro_derive(RpcPosParams)]
pub fn derive_rpc_pos_params(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    expand(&input)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

fn expand(input: &DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    let name = &input.ident;

    if !input.generics.params.is_empty() {
        return Err(syn::Error::new_spanned(
            &input.generics,
            "RpcPosParams does not support generic structs",
        ));
    }

    let fields = match &input.data {
        Data::Struct(data) => match &data.fields {
            Fields::Named(fields) => &fields.named,
            _ => {
                return Err(syn::Error::new_spanned(
                    &data.fields,
                    "RpcPosParams requires a struct with named fields",
                ));
            }
        },
        _ => {
            return Err(syn::Error::new_spanned(
                input,
                "RpcPosParams can only be derived for structs",
            ));
        }
    };

    let field_names: Vec<_> = fields
        .iter()
        .map(|field| field.ident.as_ref().expect("named field"))
        .collect();
    let field_types: Vec<_> = fields.iter().map(|field| &field.ty).collect();
    let len = field_names.len();

    if len == 1 {
        let field = field_names[0];
        let ty = field_types[0];
        return Ok(quote! {
            impl ::serde::Serialize for #name {
                fn serialize<S>(&self, serializer: S) -> ::core::result::Result<S::Ok, S::Error>
                where
                    S: ::serde::Serializer,
                {
                    ::serde::Serialize::serialize(&self.#field, serializer)
                }
            }

            impl<'de> ::serde::Deserialize<'de> for #name {
                fn deserialize<D>(deserializer: D) -> ::core::result::Result<Self, D::Error>
                where
                    D: ::serde::Deserializer<'de>,
                {
                    let #field = <#ty as ::serde::Deserialize>::deserialize(deserializer)?;
                    ::core::result::Result::Ok(#name { #field })
                }
            }
        });
    }

    let locals: Vec<_> = (0..len)
        .map(|i| format_ident!("__rpc_param_{}", i))
        .collect();
    let indexes = 0..len;

    Ok(quote! {
        impl ::serde::Serialize for #name {
            fn serialize<S>(&self, serializer: S) -> ::core::result::Result<S::Ok, S::Error>
            where
                S: ::serde::Serializer,
            {
                use ::serde::ser::SerializeTuple;
                let mut tuple = serializer.serialize_tuple(#len)?;
                #( tuple.serialize_element(&self.#field_names)?; )*
                tuple.end()
            }
        }

        impl<'de> ::serde::Deserialize<'de> for #name {
            fn deserialize<D>(deserializer: D) -> ::core::result::Result<Self, D::Error>
            where
                D: ::serde::Deserializer<'de>,
            {
                struct RpcPosParamsVisitor;

                impl<'de> ::serde::de::Visitor<'de> for RpcPosParamsVisitor {
                    type Value = #name;

                    fn expecting(&self, formatter: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
                        write!(formatter, "a list of {} RPC parameters", #len)
                    }

                    fn visit_seq<A>(self, mut seq: A) -> ::core::result::Result<Self::Value, A::Error>
                    where
                        A: ::serde::de::SeqAccess<'de>,
                    {
                        #(
                            let #locals: #field_types = seq.next_element()?.ok_or_else(|| {
                                ::serde::de::Error::invalid_length(#indexes, &self)
                            })?;
                        )*
                        if seq.next_element::<::serde::de::IgnoredAny>()?.is_some() {
                            return ::core::result::Result::Err(
                                ::serde::de::Error::invalid_length(#len + 1, &self)
                            );
                        }
                        ::core::result::Result::Ok(#name {
                            #( #field_names: #locals, )*
                        })
                    }
                }

                deserializer.deserialize_tuple(#len, RpcPosParamsVisitor)
            }
        }
    })
}
