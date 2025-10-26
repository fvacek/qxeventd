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
    events::{create_event, delete_event, list_events, read_event, update_event, EventRecord},
    logger::setup_logger,
    migrate::migrate_db,
};
use shvproto::{rpcvalue, to_rpcvalue, RpcValue};

mod appstate;
mod config;
mod events;
mod logger;
mod migrate;

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
    let db_pool = migrate_db().await?;
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
                Some(vec_to_rpcvalue(list_events(&app_state).await))
            }
            "create" [None, Write, "{s|n:name,s|n:date,s:api_token,s:owner}", "i"] (rec: EventRecord) => {
                Some(res_to_rpcvalue(create_event(&app_state, rec).await))
            }
            "read" [None, Write, "i", "{s|n:name,s|n:date,s:api_token,s:owner}"] (id: i64) => {
                Some(res_to_rpcvalue(read_event(&app_state, id).await))
            }
            "update" [None, Write, "{s|n:name,s|n:date,s:api_token,s:owner}", "n"] (rec: rpcvalue::Map) => {
                Some(res_to_rpcvalue(update_event(&app_state, rec).await))
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

fn vec_to_rpcvalue<T: serde::Serialize>(res: anyhow::Result<Vec<T>>) -> Result<RpcValue, RpcError> {
    res.and_then(|list| {
        let rpc_values: Result<Vec<RpcValue>, _> = list.into_iter()
            .map(|rec| to_rpcvalue(&rec))
            .collect();
        rpc_values.map(|values| values.into())
            .map_err(|err| anyhow::anyhow!("Serialization error: {}", err))
    })
    .map_err(anyhow_to_rpc_error)
}
