use qxsql::{QueryAndParams, QxSqlApi, QxSqlApiRecChng, RecDeleteParam, RecInsertParam, RecListParam, RecReadParam, RecUpdateParam, sql::{CREATE_PARAMS, CREATE_RESULT, DELETE_PARAMS, DELETE_RESULT, EXEC_PARAMS, EXEC_RESULT, LIST_PARAMS, LIST_RESULT, QUERY_PARAMS, QUERY_RESULT, READ_PARAMS, READ_RESULT, UPDATE_PARAMS, UPDATE_RESULT}};
use shvproto::to_rpcvalue;
use shvrpc::{metamethod::AccessLevel};

use crate::{anyhow_to_rpc_error, eventsqlapi::EventSqlApi};

struct SqlNode {
    sql_api: EventSqlApi,
}

shvclient::impl_static_node! {
    SqlNode(&self, request, client_cmd_tx) {
        "query" [None, Read, QUERY_PARAMS, QUERY_RESULT] (query: QueryAndParams) => {
            if let Err(auth_error) = check_db_access(&request, AccessLevel::Read, "", AccessOp::Read).await {
                return Some(Err(auth_error));
            }
            Some(self.sql_api.query(query.query(), query.params()).await
                .map(|query_result| to_rpcvalue(&query_result).expect("serde should work"))
                .map_err(anyhow_to_rpc_error))
        }
        "exec" [None, Write, EXEC_PARAMS, EXEC_RESULT] (query: QueryAndParams) => {
            let mut resp = request.prepare_response().unwrap_or_default();
            if let Err(auth_error) = check_db_access(&request, AccessLevel::Write, "", AccessOp::Exec).await {
                return Some(Err(auth_error));
            }
            Some(self.sql_api.exec(query.query(), query.params()).await
                .map(|query_result| to_rpcvalue(&query_result).expect("serde should work"))
                .map_err(anyhow_to_rpc_error))
        }
        // "transaction" [None, Read, TRANSACTION_PARAMS, TRANSACTION_RESULT] (query: QueryAndParamsList) => {
        //     if let Err(auth_error) = check_db_access(&request, AccessLevel::Write, "", AccessOp::Exec).await {
        //         return Some(Err(auth_error));
        //     }
        //     Some(self.sql_api.sql_exec_transaction(db, &query).await
        //         .map(|query_result| to_rpcvalue(&query_result).expect("serde should work"))
        //         .map_err(anyhow_to_rpc_error))
        // }
        "list" [None, Read, LIST_PARAMS, LIST_RESULT] (param: RecListParam) => {
            if let Err(auth_error) = check_db_access(&request, AccessLevel::Read, "", AccessOp::Read).await {
                return Some(Err(auth_error));
            }

            let fields = qxsql::string_list_to_ref_vec(&param.fields);
            Some(self.sql_api.list_records(&param.table, fields, param.ids_above, param.limit).await
                .map(|query_result| to_rpcvalue(&query_result).expect("serde should work"))
                .map_err(anyhow_to_rpc_error))
        }
        "create" [None, Write, CREATE_PARAMS, CREATE_RESULT] (param: RecInsertParam) => {
            if let Err(auth_error) = check_db_access(&request, AccessLevel::Write, &param.table, AccessOp::Create).await {
                return Some(Err(auth_error));
            }
            Some(self.sql_api.create_record_with_recchng(&param.table, &param.record, client_cmd_tx.clone(), param.issuer).await
                .map(|query_result| to_rpcvalue(&query_result).expect("serde should work"))
                .map_err(anyhow_to_rpc_error))
        }
        "read" [None, Read, READ_PARAMS, READ_RESULT] (param: RecReadParam) => {
            if let Err(auth_error) = check_db_access(&request, AccessLevel::Read, &param.table, AccessOp::Read).await {
                return Some(Err(auth_error));
            }
            let fields = qxsql::string_list_to_ref_vec(&param.fields);
            Some(self.sql_api.read_record(&param.table, param.id, fields).await
                .map(|query_result| to_rpcvalue(&query_result).expect("serde should work"))
                .map_err(anyhow_to_rpc_error))
        }
        "update" [None, Write, UPDATE_PARAMS, UPDATE_RESULT] (param: RecUpdateParam) => {
            if let Err(auth_error) = check_db_access(&request, AccessLevel::Write, &param.table, AccessOp::Update).await {
                return Some(Err(auth_error));
            }
            Some(self.sql_api.update_record_with_recchng(&param.table, param.id, &param.record, client_cmd_tx.clone(), param.issuer).await
                .map(|query_result| to_rpcvalue(&query_result).expect("serde should work"))
                .map_err(anyhow_to_rpc_error))
        }
        "delete" [None, Write, DELETE_PARAMS, DELETE_RESULT] (param: RecDeleteParam) => {
            if let Err(auth_error) = check_db_access(&request, AccessLevel::Write, &param.table, AccessOp::Delete).await {
                return Some(Err(auth_error));
            }
            Some(self.sql_api.delete_record_with_recchng(&param.table, param.id, client_cmd_tx.clone(), param.issuer).await
                .map(|query_result| to_rpcvalue(&query_result).expect("serde should work"))
                .map_err(anyhow_to_rpc_error))
        }
    }
}

/// Check write authorization for database operations
async fn check_db_access(
    _request: &shvrpc::RpcMessage,
    _access_level: AccessLevel,
    // app_state: SharedAppState,
    _table: &str,
    _op: AccessOp,
) -> Result<(), shvrpc::rpcmessage::RpcError> {
    // use shvrpc::{rpcmessage::{RpcError, RpcErrorCode}, RpcMessageMetaTags};
    // if request.access_level().unwrap_or(0) >= access_level as i32 {
    //     // access level is set to read to enable user-id driven access control
    //     // skip user-id check, if request access_level is high enough
    //     return Ok(());
    // }
    // let user_id = request.user_id().unwrap_or_default();
    // let access_token = split_first_fragment(user_id, ';').0;
    // let ok = app_state.read().await.db_access.as_ref().map(|db_access| {
    //     db_access.is_authorized(access_token, table, op)
    // }).unwrap_or(false);
    // if !ok {
    //     return Err(RpcError::new(
    //         RpcErrorCode::PermissionDenied,
    //         "Unauthorized",
    //     ));
    // }
    Ok(())
}

fn split_first_fragment(path: &str, sep: char) -> (&str, &str) {
    if let Some(ix) = path.find(sep) {
        let dir = &path[0 .. ix];
        let rest = &path[ix + 1..];
        (dir, rest)
    } else {
        (path, "")
    }
}

enum AccessOp {
    Create,
    Read,
    Update,
    Delete,
    Exec,
}
