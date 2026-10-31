use clap::Parser;
use log::{error, info};
use url::Url;

use qxeventd::config::Config;

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

    /// Mount point base of child qxsqld processes, note that broker might not accept any path.
    #[arg(long)]
    remote_events_mount: Option<String>,

    #[arg(
        short,
        long,
        help = "Data directory, database file will be stored here."
    )]
    data_directory: Option<String>,

    /// Duration of event expiration
    #[arg(long)]
    event_expire_duration: Option<String>,

    /// Print effective config
    #[arg(long)]
    print_config: bool,

    /// Verbose mode (module, .)
    #[arg(short, long)]
    verbose: Option<String>,
}

fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let cli_opts = Opts::parse();

    qxsql::setup_flexi_logger(cli_opts.verbose.as_deref())?;

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
        Config::default()
    };

    if let Some(url) = cli_opts.url {
        config.client.url = Url::parse(&url)?;
    }
    if let Some(data_dir) = cli_opts.data_directory {
        config.data_dir = data_dir;
    }
    if let Some(mount) = cli_opts.mount {
        config.client.mount = Some(mount);
    }
    if config.client.mount.is_none() && config.client.device_id.is_none() {
        config.client.mount = Some("test/qx/qxevent".to_string());
    }
    if let Some(mount) = cli_opts.remote_events_mount {
        config.remote_events_mount_point = mount;
    }
    if let Some(expiration) = cli_opts.event_expire_duration {
        match duration_str::parse_chrono(&expiration) {
            Ok(duration) => config.event_expire_duration = duration,
            Err(err) => error!("Invalid event expiration duration: {}", err),
        }
    }

    info!("local events data directory: {:?}", config.data_dir);
    info!("qxevent mount point: {:?}", config.client.mount);
    info!("qxsql events mount point base: {}", config.remote_events_mount_point);

    if cli_opts.print_config {
        let yaml = serde_yaml::to_string(&config)?;
        println!("{}", yaml);
        return Ok(());
    }

    qxeventd::set_global_config(config);

    // Run the async application
    const SMOL_THREADS: &str = "SMOL_THREADS";
    if std::env::var(SMOL_THREADS).is_err()
        && let Ok(num_threads) = std::thread::available_parallelism() {
            // set_var called before any other threads and smol runtime
            unsafe { std::env::set_var(SMOL_THREADS, num_threads.to_string()); }
        }
    smol::block_on(qxeventd::run())
}
