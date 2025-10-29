use std::{backtrace::Backtrace, sync::OnceLock};

use clap::{Parser, arg, command};
use log::{error, info};
use shvclient::appnodes::DotAppNode;
use shvrpc::rpcmessage::{RpcError, RpcErrorCode};
use smol::lock::RwLock;
use url::Url;

use crate::{
    appstate::{QxAppState, QxLockedAppState, QxSharedAppState},
    config::Config,
    logger::setup_logger,
    migrate::create_db_connection, sql::QxSql,
};
use shvproto::{to_rpcvalue, RpcValue};
use qxsql::{sql::{RecListParam, SqlProvider}, string_list_to_ref_vec, QueryAndParams, RecChng, RecDeleteParam, RecInsertParam, RecOp, RecReadParam, RecUpdateParam};

mod appstate;
mod config;
mod events;
mod logger;
mod migrate;
mod sql;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Opts {
    /// Path to config file
    #[arg(short, long)]
    config: Option<String>,

    /// SHV broker URL
    #[arg(short = 's', long)]
    url: Option<String>,

    /// Mount point on broker connected to, note that broker might not accept any path.
    #[arg(short, long)]
    mount: Option<String>,

    #[arg(
        short,
        long,
        help = "Data directory, database file will be stored here."
    )]
    data_dir: Option<String>,

    /// Print effective config
    #[arg(long)]
    print_config: bool,

    /// Verbose mode (module, .)
    #[arg(short, long)]
    verbose: Option<String>,
}

static GLOBAL_CONFIG: OnceLock<Config> = OnceLock::new();

fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let cli_opts = Opts::parse();

    setup_logger(&cli_opts)?;

    log::info!("=====================================================");
    log::info!("{} starting", std::module_path!());
    log::info!("=====================================================");
    // log::info!(target: "ahoj", "with target");
    // log::error!("ERROR");
    // log::warn!("WARN");
    // log::info!("INFO");
    // log::debug!("DEBUG");
    // log::trace!("TRACE");

    let mut config = if let Some(config_path) = cli_opts.config {
        info!("Loading config file {config_path}");
        let f = std::fs::File::open(config_path)?;
        serde_yaml::from_reader(f)?
    } else {
        crate::config::Config::default()
    };

    if let Some(url) = cli_opts.url {
        config.client.url = Url::parse(&url)?;
    }
    if let Some(data_dir) = cli_opts.data_dir {
        config.data_dir = data_dir;
    }
    if let Some(mount) = cli_opts.mount {
        config.client.mount = Some(mount);
    }

    if cli_opts.print_config {
        let yaml = serde_yaml::to_string(&config)?;
        println!("{}", yaml);
        return Ok(());
    }

    GLOBAL_CONFIG
        .set(config)
        .expect("Global config should only be set once");

    // Run the async application
    smol::block_on(async_main())
}

async fn async_main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let db_pool = create_db_connection().await?;
    let app_state: QxSharedAppState = shvclient::AppState::new(RwLock::new(QxAppState { db_pool }));

    let dot_app_node = shvclient::fixed_node!(
        handler<QxLockedAppState>(request, _client_cmd_tx, _app_state) {
            "name" [IsGetter, Browse, "n", "s"] => {
                Some(Ok(env!("CARGO_PKG_NAME").into()))
            }
            "version" [IsGetter, Browse, "n", "s"] => {
                Some(Ok(env!("CARGO_PKG_VERSION").into()))
            }
            "ping" [None, Browse, "n", "n"] => {
                Some(Ok(().into()))
            }
        }
    );

    let sql_node = shvclient::fixed_node!(
        handler<QxLockedAppState>(request, client_cmd_tx, app_state) {
            "query" [None, Read, "[s:query,{s|i|b|t|n}:params]", "{{s:name}:fields,[[s|i|b|t|n]]:rows}"] (query: QueryAndParams) => {
                let qxsql = QxSql(app_state);
                let result = qxsql.query(query.query(), query.params()).await;
                Some(res_to_rpcvalue(result))
            }
            "exec" [None, Read, "[s:query,{s|i|b|t|n}:params,s|n:issuer]", "{{s:name}:fields,[[s|i|b|t|n]]:rows}"] (query: QueryAndParams) => {
                let qxsql = QxSql(app_state);
                let result = qxsql.exec(query.query(), query.params()).await;
                Some(res_to_rpcvalue(result))
            }
            "list" [None, Read, "n", "[{n:id,s:name,s:date}]"] (param: RecListParam) => {
                let qxsql = QxSql(app_state);
                let fields = string_list_to_ref_vec(&param.fields);
                let result = qxsql.list_records(&param.table, fields, param.ids_above, param.limit).await;
                Some(res_to_rpcvalue(result))
            }
            "create" [None, Write, "{s:table,{s|i|b|t|n}:record,s:issuer}", "i"] (param: RecInsertParam) => {
                let qxsql = QxSql(app_state);
                let insert_id = qxsql.create_record(&param.table, &param.record).await;
                if let Ok(insert_id) = insert_id {
                    let recchng = RecChng {table:param.table, id:insert_id, record:Some(param.record), op: RecOp::Insert, issuer:param.issuer };
                    let rec = to_rpcvalue(&recchng).expect("serde should work");
                    client_cmd_tx.send_message(shvrpc::RpcMessage::new_signal("sql", "recchng", Some(rec)))
                                    .unwrap_or_else(|err| log::error!("Cannot send signal ({err})"));
                }
                Some(res_to_rpcvalue(insert_id))
            }
            "read" [None, Read, "{s:table,i:id},{s}|n:fields", "{s|i|b|t|n}|n"] (param: RecReadParam) => {
                let qxsql = QxSql(app_state);
                let fields = string_list_to_ref_vec(&param.fields);
                let result = qxsql.read_record(&param.table, param.id, fields).await;
                Some(res_to_rpcvalue(result))
            }
            "update" [None, Write, "{s:table,i:id,{s|i|b|t|n}:record,s:issuer}", "b"] (param: RecUpdateParam) => {
                let qxsql = QxSql(app_state);
                let update_success = qxsql.update_record(&param.table, param.id, &param.record).await;
                if let Ok(update_success) = update_success && update_success {
                    let recchng = RecChng {table:param.table, id:param.id, record:Some(param.record), op: RecOp::Update, issuer:param.issuer };
                    let rec = to_rpcvalue(&recchng).expect("serde should work");
                    client_cmd_tx.send_message(shvrpc::RpcMessage::new_signal("sql", "recchng", Some(rec)))
                                    .unwrap_or_else(|err| log::error!("Cannot send signal ({err})"));
                }
                Some(res_to_rpcvalue(update_success))
            }
            "delete" [None, Write, "{s:table,i:id,s:issuer}", "b"] (param: RecDeleteParam) => {
                let qxsql = QxSql(app_state);
                let was_deleted = qxsql.delete_record(&param.table, param.id).await;
                if let Ok(was_deleted) = was_deleted && was_deleted {
                    let recchng = RecChng {table:param.table, id:param.id, record:None, op: RecOp::Delete, issuer:param.issuer };
                    let rec = to_rpcvalue(&recchng).expect("serde should work");
                    client_cmd_tx.send_message(shvrpc::RpcMessage::new_signal("sql", "recchng", Some(rec)))
                                    .unwrap_or_else(|err| log::error!("Cannot send signal ({err})"));
                }
                Some(res_to_rpcvalue(was_deleted))
            }
        }
    );

    let config = GLOBAL_CONFIG
        .get()
        .expect("Global config should be initialized");

    shvclient::Client::new()
        .app(DotAppNode::new(env!("CARGO_PKG_NAME")))
        .mount(".app", dot_app_node)
        .mount("sql", sql_node)
        .with_app_state(app_state)
        // .run_with_init(&client_config, app_tasks)
        .run(&config.client)
        .await
        .map_err(|err| err.to_string().into())
}

fn anyhow_to_rpc_error(err: anyhow::Error) -> RpcError {
    error!("Error: {err}\nbacktrace: {}", Backtrace::capture());
    RpcError::new(RpcErrorCode::MethodCallException, format!("Error: {err}"))
}

fn res_to_rpcvalue<T: serde::Serialize>(res: anyhow::Result<T>) -> Result<RpcValue, RpcError> {
    res.and_then(|value| {
        to_rpcvalue(&value)
            .map_err(|err| anyhow::anyhow!("Serialization error: {}", err))
    })
    .map_err(anyhow_to_rpc_error)
}
