mod app;
mod config;
mod downloader;
mod filter;
mod format;
mod webui;

use std::sync::Arc;

use app::{CONFIG_FILE, Shutdown, run_downloader};
use config::load_config;
use log::{info, warn};
use tokio::runtime::Builder;
use webui::WebState;

fn main() -> anyhow::Result<()> {
    init_logger();
    info!("Telegram Media Downloader (Rust) - starting");

    let cfg = load_config(CONFIG_FILE)?;
    let worker_threads = cfg.max_download_task.max(1);
    Builder::new_multi_thread()
        .worker_threads(worker_threads)
        .enable_all()
        .build()?
        .block_on(run(cfg))
}

async fn run(cfg: config::Config) -> anyhow::Result<()> {
    let web_state = Arc::new(WebState::new());
    tokio::spawn(webui::run(
        web_state.clone(),
        cfg.web_host.clone(),
        cfg.web_port,
    ));

    let shutdown = Shutdown::new();
    install_signal_handler(shutdown.clone());

    let result = run_downloader(cfg, web_state, shutdown.clone()).await;
    shutdown.cancel();
    result
}

fn init_logger() {
    use std::io::Write;

    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .filter_module("grammers_mtsender", log::LevelFilter::Warn)
        .filter_module("grammers_mtproto", log::LevelFilter::Warn)
        .filter_module("grammers_client", log::LevelFilter::Warn)
        .filter_module("grammers_session", log::LevelFilter::Warn)
        .filter_module("turso_core", log::LevelFilter::Warn)
        .filter_module("tracing::span", log::LevelFilter::Warn)
        .filter_module("hyper", log::LevelFilter::Warn)
        .filter_module("axum", log::LevelFilter::Warn)
        .format(|buf, record| {
            let style = buf.default_level_style(record.level());
            writeln!(
                buf,
                "{} {style}{:<5}{style:#} {} | {}",
                buf.timestamp(),
                record.level(),
                record.target(),
                record.args()
            )
        })
        .init();
}

fn install_signal_handler(shutdown: Shutdown) {
    tokio::spawn(async move {
        if !wait_for_interrupt().await {
            return;
        }
        warn!("interrupt received - draining downloads and saving state (Ctrl+C again to force)");
        shutdown.cancel();
        if wait_for_interrupt().await {
            eprintln!("second interrupt - forcing immediate exit");
            std::process::exit(130);
        }
    });
}

#[cfg(unix)]
async fn wait_for_interrupt() -> bool {
    use tokio::signal::unix::{SignalKind, signal};
    match signal(SignalKind::interrupt()) {
        Ok(mut sig) => sig.recv().await.is_some(),
        Err(e) => {
            warn!("cannot install SIGINT handler: {e}; Ctrl+C will not shut down gracefully");
            false
        }
    }
}

#[cfg(not(unix))]
async fn wait_for_interrupt() -> bool {
    tokio::signal::ctrl_c().await.is_ok()
}
