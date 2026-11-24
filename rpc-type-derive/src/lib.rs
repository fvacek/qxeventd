//! `#[derive(RecordChange)]` generates a partial update `<Name>Change` struct with a
//! `to_record()` function, see [`macro@RecordChange`].
//!
//! `#[derive(RpcTypeStr)]` is provided by `shvproto`.

use proc_macro::TokenStream;
use syn::{parse_macro_input, DeriveInput};

mod record_change;

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
