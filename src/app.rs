mod auth;
mod scan;
mod shutdown;
mod state;

use std::sync::Arc;
use std::time::{Duration, Instant};

use grammers_client::Client;
use grammers_mtsender::SenderPool;
use grammers_session::storages::SqliteSession;
use indicatif::MultiProgress;
use log::{debug, info};
use rustc_hash::{FxHashMap as HashMap, FxHashSet as HashSet};
use tokio::sync::{Mutex, Semaphore};

use crate::config::{ChatData, Config, load_app_data};
use crate::webui::WebState;

use auth::authorize_interactive;
use scan::{DownloadRuntime, run_check_cycle};
pub(crate) use shutdown::{Shutdown, flood_wait_secs, sleep_cancellable, wait_paused};
use state::{log_config_summary, log_shutdown_summary, persist_state};

pub(crate) const CONFIG_FILE: &str = "config.yaml";
const DATA_FILE: &str = "data.yaml";
const SESSION_FILE: &str = "sessions/tmd.session";

pub(crate) async fn run_downloader(
    mut cfg: Config,
    web_state: Arc<WebState>,
    shutdown: Shutdown,
) -> anyhow::Result<()> {
    web_state.set_status("running").await;

    let data = load_app_data(DATA_FILE)?;
    std::fs::create_dir_all(&cfg.save_path)?;
    std::fs::create_dir_all("sessions")?;

    let session = Arc::new(SqliteSession::open(SESSION_FILE).await?);
    let SenderPool {
        runner,
        handle: pool_handle,
        ..
    } = SenderPool::new(session.clone(), cfg.api_id);
    let client = Client::new(pool_handle);
    let _runner_handle = tokio::spawn(runner.run());

    if !client.is_authorized().await.unwrap_or(false) {
        authorize_interactive(&client, &cfg.api_hash).await?;
        info!("Session saved to {SESSION_FILE}");
    }
    info!("Authorized - ready");

    let file_ids: Arc<Mutex<HashSet<String>>> =
        Arc::new(Mutex::new(data.downloaded_file_ids.into_iter().collect()));

    let mut data_chats: HashMap<String, ChatData> = data
        .chat
        .into_iter()
        .map(|d| (d.chat_id.clone(), d))
        .collect();

    let concurrency = cfg.max_download_task;
    let dl_semaphore = Arc::new(Semaphore::new(concurrency));
    let mp = Arc::new(MultiProgress::new());
    let runtime = DownloadRuntime {
        file_ids: file_ids.clone(),
        dl_sem: dl_semaphore,
        mp,
        web_state: web_state.clone(),
    };

    log_config_summary(&cfg, &data_chats, concurrency);

    let started = Instant::now();
    let mut cycle_no: u64 = 0;
    loop {
        cycle_no += 1;
        let cycle_started = Instant::now();
        let completed =
            run_check_cycle(&client, &mut cfg, &runtime, &mut data_chats, &shutdown).await?;

        // Persist after every cycle (full or interrupted) so data.yaml tracks
        // the live file-id cache and per-chat retry sets even on shutdown.
        persist_state(&file_ids, &data_chats).await?;
        debug!(
            "cycle {cycle_no}: persisted state to {DATA_FILE} and {CONFIG_FILE} ({} file ids, {} chats pending)",
            file_ids.lock().await.len(),
            data_chats
                .values()
                .map(|c| c.ids_to_retry.len())
                .sum::<usize>()
        );

        if !completed || shutdown.is_cancelled() {
            break;
        }
        info!(
            "cycle {cycle_no} complete in {:.1}s - sleeping {}s (Ctrl+C to shut down)",
            cycle_started.elapsed().as_secs_f64(),
            cfg.check_interval_secs
        );
        web_state
            .set_status(&format!(
                "waiting {}s before next check",
                cfg.check_interval_secs
            ))
            .await;
        tokio::select! {
            _ = shutdown.cancelled() => break,
            _ = tokio::time::sleep(Duration::from_secs(cfg.check_interval_secs)) => {}
        }
        web_state.set_status("running").await;
    }

    web_state.set_status("shutting down").await;
    log_shutdown_summary(&cfg, &data_chats, &file_ids, started).await;
    info!("graceful shutdown complete");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::flood_wait_secs;

    #[test]
    fn flood_wait_parses_grammers_value_form() {
        // The exact format grammers emitted in the wild.
        let err = "request error: rpc error 420: FLOOD_WAIT caused by \
                   auth.sendCode (value: 225)";
        assert_eq!(flood_wait_secs(err), Some(225));
    }

    #[test]
    fn flood_wait_parses_raw_error_type() {
        assert_eq!(flood_wait_secs("rpc error 420: FLOOD_WAIT_90"), Some(90));
    }

    #[test]
    fn flood_wait_non_flood_error_is_none() {
        assert_eq!(
            flood_wait_secs("rpc error 401: AUTH_KEY_UNREGISTERED"),
            None
        );
    }

    #[test]
    fn flood_wait_unreadable_defaults_to_60() {
        assert_eq!(flood_wait_secs("FLOOD_WAIT (no number given)"), Some(60));
    }
}
