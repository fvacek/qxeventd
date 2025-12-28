use std::sync::Arc;
use std::{backtrace::Backtrace, sync::OnceLock};
use clap::Parser;
use log::{error, info};
use qxsql::{RecChng, RecDeleteParam, RecInsertParam, RecListParam, RecOp, RecReadParam, RecUpdateParam, string_list_to_ref_vec};
use qxsql::sql::{CREATE_PARAMS, CREATE_RESULT, DELETE_PARAMS, DELETE_RESULT, EXEC_PARAMS, EXEC_RESULT, LIST_PARAMS, LIST_RESULT, READ_PARAMS, READ_RESULT, UPDATE_PARAMS, UPDATE_RESULT};
use shvclient::appnodes::{DotAppNode, DotDeviceNode};
use shvrpc::rpcmessage::{RpcError, RpcErrorCode};
use smol::lock::RwLock;
use url::Url;

use crate::eventnode::request_handler;
use crate::qxappsql::QxAppSql;
use crate::{
    state::{State},
    config::Config,
    logger::setup_logger,
    migrate::create_db_connection,
};
use shvproto::{RpcValue, to_rpcvalue};
use qxsql::{sql::{QxSqlApi, QUERY_PARAMS, QUERY_RESULT, QueryAndParams}};

mod state;
mod config;
mod events;
mod logger;
mod migrate;
mod qxappsql;
mod eventnode;
mod eventrpcproxy;
mod eventdb;

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

type AppState = Arc<RwLock<State>>;

static GLOBAL_CONFIG: OnceLock<Config> = OnceLock::new();

fn global_config() -> &'static Config {
    GLOBAL_CONFIG.get().expect("Global config should be initialized")
}

fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let cli_opts = Opts::parse();

    setup_logger(&cli_opts)?;

    log::info!("=====================================================");
    log::info!("{} starting", env!("CARGO_PKG_NAME"));
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
    const SMOL_THREADS: &str = "SMOL_THREADS";
    if std::env::var(SMOL_THREADS).is_err()
        && let Ok(num_threads) = std::thread::available_parallelism() {
            // set_var called before any other threads and smol runtime
            unsafe { std::env::set_var(SMOL_THREADS, num_threads.to_string()); }
        }
    smol::block_on(async_main())
}

struct SqlNode {
    app_state: AppState,
}

shvclient::impl_static_node! {
    SqlNode(&self, request, client_cmd_tx) {
        "query" [None, Read, QUERY_PARAMS, QUERY_RESULT] (query: QueryAndParams) => {
            let qxsql = QxAppSql(self.app_state.read().await.db_pool.clone());
            let result = qxsql.query(query.query(), query.params()).await;
            Some(res_to_rpcvalue(result))
        }
        "exec" [None, Read, EXEC_PARAMS, EXEC_RESULT] (query: QueryAndParams) => {
            let qxsql = QxAppSql(self.app_state.read().await.db_pool.clone());
            let result = qxsql.exec(query.query(), query.params()).await;
            Some(res_to_rpcvalue(result))
        }
        "list" [None, Read, LIST_PARAMS, LIST_RESULT] (param: RecListParam) => {
            let qxsql = QxAppSql(self.app_state.read().await.db_pool.clone());
            let fields = string_list_to_ref_vec(&param.fields);
            let result = qxsql.list_records(&param.table, fields, param.ids_above, param.limit).await;
            Some(res_to_rpcvalue(result))
        }
        "create" [None, Write, CREATE_PARAMS, CREATE_RESULT] (param: RecInsertParam) => {
            let qxsql = QxAppSql(self.app_state.read().await.db_pool.clone());
            let insert_id = qxsql.create_record(&param.table, &param.record).await;
            if let Ok(insert_id) = insert_id {
                let recchng = RecChng {table:param.table, id:insert_id, record:Some(param.record), op: RecOp::Insert, issuer:param.issuer };
                let rec = to_rpcvalue(&recchng).expect("serde should work");
                client_cmd_tx.send_message(shvrpc::RpcMessage::new_signal("sql", "recchng", Some(rec)))
                                .unwrap_or_else(|err| log::error!("Cannot send signal ({err})"));
            }
            Some(res_to_rpcvalue(insert_id))
        }
        "read" [None, Read, READ_PARAMS, READ_RESULT] (param: RecReadParam) => {
            let qxsql = QxAppSql(self.app_state.read().await.db_pool.clone());
            let fields = string_list_to_ref_vec(&param.fields);
            let result = qxsql.read_record(&param.table, param.id, fields).await;
            Some(res_to_rpcvalue(result))
        }
        "update" [None, Write, UPDATE_PARAMS, UPDATE_RESULT] (param: RecUpdateParam) => {
            let qxsql = QxAppSql(self.app_state.read().await.db_pool.clone());
            let update_success = qxsql.update_record(&param.table, param.id, &param.record).await;
            if let Ok(update_success) = update_success && update_success {
                let recchng = RecChng {table:param.table, id:param.id, record:Some(param.record), op: RecOp::Update, issuer:param.issuer };
                let rec = to_rpcvalue(&recchng).expect("serde should work");
                client_cmd_tx.send_message(shvrpc::RpcMessage::new_signal("sql", "recchng", Some(rec)))
                                .unwrap_or_else(|err| log::error!("Cannot send signal ({err})"));
            }
            Some(res_to_rpcvalue(update_success))
        }
        "delete" [None, Write, DELETE_PARAMS, DELETE_RESULT] (param: RecDeleteParam) => {
            let qxsql = QxAppSql(self.app_state.read().await.db_pool.clone());
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
}

async fn async_main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let db_pool = create_db_connection().await?;
    let app_state = AppState::new(RwLock::new(State { db_pool, open_events: Default::default() }));
    let config = GLOBAL_CONFIG
        .get()
        .expect("Global config should be initialized");

    shvclient::Client::new()
        .app(DotAppNode::new(env!("CARGO_PKG_NAME")))
        .device(DotDeviceNode::new(env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION"), Some("00000".into())))
        .mount_static("sql", SqlNode { app_state: app_state.clone() })
        .mount_dynamic("event", move |rq, client_cmd_tx| {
                        request_handler(rq, client_cmd_tx, app_state.clone())
        })
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

fn generate_api_token() -> String {
    use rand::Rng;
    const LEN: usize = 10;
    const VOWELS: &str = "aeiouy";
    const CONSONANTS: &str = "bcdfghjklmnpqrstvwxz";
    let mut rng = rand::thread_rng();
    (0..LEN)
        .map(|n| {
            let charset = if n % 2 == 0 { CONSONANTS } else { VOWELS };
            let idx = rng.gen_range(0..charset.len());
            charset.chars().nth(idx).unwrap()
        })
        .collect()
}
