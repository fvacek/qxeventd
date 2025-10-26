
use clap::{arg, command, Parser};
use log::{info};
use flexi_logger::{Logger};
use shvproto::util::parse_log_verbosity;

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
        help = "Database connection string, examle: postgres://myuser:mypassword@localhost/mydb?options=--search_path%3Dmyschema"
    )]
    database: Option<String>,

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

async fn run_application() {

    log::info!("=====================================================");
    log::info!("{} starting", std::module_path!());
    log::info!("=====================================================");
    log::info!(target: "ahoj", "with target");
    log::error!("ERROR");
    log::warn!("WARN");
    log::info!("INFO");
    log::debug!("DEBUG");
    log::trace!("TRACE");
}

fn setup_logger(cli_opts: &Opts) -> Result<(), flexi_logger::FlexiLoggerError> {
    // Build the verbosity spec string (e.g. "mycrate=debug,net=info")
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

fn main() {
    let cli_opts = Opts::parse();

    // Setup logger
    if let Err(e) = setup_logger(&cli_opts) {
        eprintln!("Failed to initialize logger: {}", e);
        std::process::exit(1);
    }

    info!("Logger initialized successfully");

    // Run the async application
    smol::block_on(run_application());
}
