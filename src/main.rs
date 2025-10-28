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
    events::{delete_event, read_event},
    logger::setup_logger,
    migrate::create_db_connection, sql::{create_record, list_records, update_record},
};
use shvproto::{rpcvalue, to_rpcvalue, RpcValue, Value};

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

    let events_node = shvclient::fixed_node!(
        handler<QxLockedAppState>(request, _client_cmd_tx, app_state) {
            "list" [None, Read, "n", "[{n:id,s:name,s:date}]"] => {
                Some(vec_to_rpcvalue_into(list_records(app_state, "events", Some(&["id", "name", "date", "owner"])).await))
            }
            "create" [None, Write, "{s|n:name,s|n:date,s:api_token,s:owner}", "i"] (record: rpcvalue::Map) => {
                Some(res_to_rpcvalue(create_record(app_state, "events", record).await))
            }
            "read" [None, Write, "i", "{s|n:name,s|n:date,s:api_token,s:owner}"] (id: i64) => {
                Some(res_to_rpcvalue(read_event(&app_state, id).await))
            }
            "update" [None, Write, "[i:id,{s|n:name,s|n:date,s:api_token,s:owner}:record]", "n"] (param: rpcvalue::List) => {
                if param.len() != 2 {
                    return Some(Err(RpcError::new(RpcErrorCode::MethodCallException, "Parameter must be list of two elements")));
                }
                let mut param = param;
                let id = param.remove(0).as_i64();
                let Value::Map(record) = param.remove(0).value else {
                    return Some(Err(RpcError::new(RpcErrorCode::MethodCallException, "Second parameter must be Map")));
                };
                // let rec2 = *(rec.clone());
                // let res = update_event(&app_state, id, rec2).await;
                // if res.is_ok() {
                //     let mut recchng = rpcvalue::Map::new();
                //     recchng.insert("id".to_string(), id.into());
                //     recchng.insert("record".to_string(), (*rec).into());
                //     // recchng.insert("op".to_string(), "Update".into()));
                //     // recchng.insert("issuer".to_string(), issuer.into());
                //     client_cmd_tx.send_message(shvrpc::RpcMessage::new_signal("sql", "recchng", Some(recchng.into())))
                //                     .unwrap_or_else(|err| log::error!("Cannot send signal ({err})"));
                // }
                Some(res_to_rpcvalue(update_record(app_state, "events", id, *record).await))
            }
            "delete" [None, Write, "i", "n"] (id: i64) => {
                Some(res_to_rpcvalue(delete_event(&app_state, id).await))
            }
        }
    );

    let config = GLOBAL_CONFIG
        .get()
        .expect("Global config should be initialized");

    shvclient::Client::new()
        .app(DotAppNode::new(env!("CARGO_PKG_NAME")))
        .mount(".app", dot_app_node)
        .mount("events", events_node)
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

fn vec_to_rpcvalue_ser<T: serde::Serialize>(res: anyhow::Result<Vec<T>>) -> Result<RpcValue, RpcError> {
    res.and_then(|list| {
        let rpc_values: Result<Vec<RpcValue>, _> = list.into_iter()
            .map(|rec| to_rpcvalue(&rec))
            .collect();
        rpc_values.map(|values| values.into())
            .map_err(|err| anyhow::anyhow!("Serialization error: {}", err))
    })
    .map_err(anyhow_to_rpc_error)
}

fn vec_to_rpcvalue_into<T: Into<RpcValue>>(res: anyhow::Result<Vec<T>>) -> Result<RpcValue, RpcError> {
    res.and_then(|list| {
        let rpc_values: Vec<RpcValue> = list.into_iter()
            .map(|rec| rec.into())
            .collect();
        Ok(rpc_values.into())
    })
    .map_err(anyhow_to_rpc_error)
}
