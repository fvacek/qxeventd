
use std::sync::OnceLock;

use clap::{arg, command, Parser};
use log::{info};
use flexi_logger::{Logger};
use shvproto::util::parse_log_verbosity;
use shvclient::appnodes::DotAppNode;
use smol::lock::RwLock;
use url::Url;

use crate::{appstate::{QxLockedAppState, QxSharedAppState}, config::Config};

mod appstate;
mod config;

const DEFAULT_DATA_DIR: &str = "/tmp/qxeventd/data";

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

    /// Database connection string
    #[arg(
        short,
        long,
        help = "Data directory, database file will be stored here."
    )]
    data_dir: Option<String>,

    /// Database connection string
    #[arg(short, long)]
    write_database_token: Option<String>,

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

    // Setup logger
    if let Err(e) = setup_logger(&cli_opts) {
        eprintln!("Failed to initialize logger: {}", e);
        std::process::exit(1);
    }

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
    if config.data_dir.is_empty() {
        config.data_dir = DEFAULT_DATA_DIR.to_string();
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
    smol::block_on(async_main());

    return Ok(());
}

async fn async_main() {
    let dot_app_node = shvclient::fixed_node!(
        sql_handler<QxLockedAppState>(request, _client_cmd_tx, _app_state) {
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

    let app_state: QxSharedAppState = shvclient::AppState::new(RwLock::new(123));
    let config = GLOBAL_CONFIG.get().expect("Global config should be initialized");

    shvclient::Client::new()
        .app(DotAppNode::new(env!("CARGO_PKG_NAME")))
        .mount(".app", dot_app_node)
        // .mount("sql", sql_node)
        .with_app_state(app_state)
        // .run_with_init(&client_config, app_tasks)
        .run(&config.client)
        .await.expect("msg")

}

fn setup_logger(cli_opts: &Opts) -> Result<(), flexi_logger::FlexiLoggerError> {
    // Build the verbosity spec string (e.g. "mycrate=debug,info")
    let verbosity_spec = if let Some(module_names) = &cli_opts.verbose {
        let parsed = parse_log_verbosity(module_names, module_path!());

        // Join module=level pairs into a flexi_logger filter string
        let has_default = parsed.iter().any(|(m, _)| m.is_none());
        let mut specs: Vec<String> = parsed
            .iter()
            .map(|(m, level)| match m {
                Some(name) => format!("{}={}", name, level),
                None => level.to_string(),
            })
            .collect();

        if !has_default {
            specs.push("info".into());
        }
        specs.join(",")
    } else {
        "info".into()
    };
    // println!("verbosity: {}", verbosity_spec);
    Logger::try_with_env_or_str(&verbosity_spec)?
        .format(|w, now, record| {
            let target = if record.target().is_empty() {
                String::new()
            } else {
                format!("({}) ", record.target())
            };

            let level_str = match record.level() {
                log::Level::Error => format!("\x1b[31m{}\x1b[0m", record.level()), // Red
                log::Level::Warn => format!("\x1b[33m{}\x1b[0m", record.level()), // Yellow
                log::Level::Info => format!("\x1b[36m{}\x1b[0m", record.level()), // Cyan
                log::Level::Trace => format!("\x1b[38;5;94m{}\x1b[0m", record.level()), // Orange
                _ => format!("{}", record.level()),
            };

            write!(
                w,
                "{} {} {}[{}:{}] {}",
                now.now().format("%H:%M:%S%.3f"),
                level_str,
                target,
                record.file().unwrap_or(""),
                record.line().unwrap_or(0),
                record.args()
            )
        })
        .start()?;

    Ok(())
}
