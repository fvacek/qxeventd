use flexi_logger::Logger;
use shvrpc::util::parse_log_verbosity;

use crate::Opts;

pub(crate) fn setup_logger(cli_opts: &Opts) -> Result<(), flexi_logger::FlexiLoggerError> {
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
