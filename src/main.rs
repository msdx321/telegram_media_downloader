mod config;
mod filter;
mod format;
mod webui;

use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, LazyLock};

use std::time::{Duration, Instant};

use anyhow::Context;
use grammers_client::media::Media;
use grammers_client::{Client, SignInError};
use grammers_mtsender::SenderPool;
use grammers_session::storages::SqliteSession;
use log::{debug, error, info, warn};
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use tokio::sync::mpsc;
use tokio::sync::{Mutex, Semaphore};
use tokio_util::sync::CancellationToken;

use crate::config::{
    load_app_data, load_config, save_app_data, save_config, ChatConfig, ChatData, Config,
};
use crate::filter::{Parser, Value, VarLookup};
use crate::format::{format_byte, replace_date_time, truncate_filename, validate_title};
use crate::webui::WebState;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};

const CONFIG_FILE: &str = "config.yaml";
const DATA_FILE: &str = "data.yaml";
const SESSION_FILE: &str = "sessions/tmd.session";
const MAX_FILE_ID_CACHE: usize = 4096;
const RETRY_LIMIT: u32 = 3;
const RETRY_DELAY_SECS: u64 = 5;
const DOWNLOAD_CHUNK_SIZE: u64 = 512 * 1024;
const RESUME_WORKERS: u64 = 4;
const MAX_AUTH_FLOOD_RETRIES: u32 = 3;
const CHUNK_RETRY_LIMIT: u32 = 3;
/// Minimum bytes between `.progress` sidecar flushes, bounding how much
/// progress an interruption can lose.
const PROGRESS_FLUSH_BYTES: u64 = 16 * 1024 * 1024;

static DOWNLOAD_PROGRESS_STYLE: LazyLock<ProgressStyle> = LazyLock::new(|| {
    ProgressStyle::with_template(
        "{msg:>8} {wide_bar:.cyan/blue} {bytes:>8}/{total_bytes:8} {bytes_per_sec:>10} {eta}",
    )
    .unwrap_or_else(|_| ProgressStyle::default_bar())
    .progress_chars("##-")
});

#[cfg(target_os = "linux")]
unsafe extern "C" {
    // Returns 0 on success, errno on failure. Used to preallocate the download
    // file so the NAS doesn't grow it extent-by-extent during chunked writes.
    fn posix_fallocate(fd: i32, offset: i64, len: i64) -> i32;
}

// ── Shutdown coordination ──────────────────────────────────────────────────

/// Process-wide cancellation token, tripped by SIGINT.
///
/// Cloneable and cheap; every downloader coroutine holds a clone so the signal
/// handler can cancel them all at once. `cancelled()` resolves the moment the
/// token is tripped, so futures can race it against network I/O via `select!`
/// for a prompt, cooperative stop.
#[derive(Clone)]
struct Shutdown {
    token: CancellationToken,
}

impl Shutdown {
    fn new() -> Self {
        Self {
            token: CancellationToken::new(),
        }
    }

    /// Trip the token. Idempotent.
    fn cancel(&self) {
        self.token.cancel();
    }

    fn is_cancelled(&self) -> bool {
        self.token.is_cancelled()
    }

    /// Resolves once the token has been tripped.
    async fn cancelled(&self) {
        self.token.cancelled().await;
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_logger();
    info!("Telegram Media Downloader (Rust) — starting");

    let cfg = load_config(CONFIG_FILE)?;
    let web_state = Arc::new(WebState::new());
    tokio::spawn(webui::run(
        web_state.clone(),
        cfg.web_host.clone(),
        cfg.web_port,
    ));

    let shutdown = Shutdown::new();
    install_signal_handler(shutdown.clone());

    let result = run_downloader(cfg, web_state, shutdown.clone()).await;

    // If the downloader returned due to a signal, the token is already
    // cancelled; otherwise cancel it so any straggler tasks stop before exit.
    shutdown.cancel();
    result
}

/// Configure `env_logger` with a verbose single-line format:
/// `YYYY-MM-DDTHH:MM:SS LEVEL  target  message`, level coloured. Verbosity comes
/// from `RUST_LOG` (default `info`); chatty library crates are pinned to `warn`.
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
            // `default_level_style` returns an `anstyle::Style`; rendering it
            // applies the colour, `{style:#}` emits the reset.
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

/// Spawn the SIGINT handler.
///
/// The first Ctrl+C trips the `shutdown` token, which cooperatively cancels
/// every downloader coroutine so their state can be flushed and the process can
/// exit cleanly. A second Ctrl+C bails out immediately with exit code 130.
fn install_signal_handler(shutdown: Shutdown) {
    tokio::spawn(async move {
        if !wait_for_interrupt().await {
            return;
        }
        warn!("interrupt received — draining downloads and saving state (Ctrl+C again to force)");
        shutdown.cancel();
        if wait_for_interrupt().await {
            eprintln!("second interrupt — forcing immediate exit");
            std::process::exit(130);
        }
    });
}

#[cfg(unix)]
async fn wait_for_interrupt() -> bool {
    use tokio::signal::unix::{signal, SignalKind};
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

async fn run_downloader(
    mut cfg: Config,
    web_state: Arc<WebState>,
    shutdown: Shutdown,
) -> anyhow::Result<()> {
    web_state.set_status("running").await;

    let mut data = load_app_data(DATA_FILE)?;
    std::fs::create_dir_all(&cfg.save_path)?;
    std::fs::create_dir_all("sessions")?;

    // ── session + client ────────────────────────────────────────────────
    let session = Arc::new(SqliteSession::open(SESSION_FILE).await?);
    let SenderPool {
        runner,
        handle: pool_handle,
        ..
    } = SenderPool::new(session.clone(), cfg.api_id);
    let client = Client::new(pool_handle);
    let _runner_handle = tokio::spawn(runner.run());

    // ── authorization ────────────────────────────────────────────────────
    if !client.is_authorized().await.unwrap_or(false) {
        authorize_interactive(&client).await?;
        info!("Session saved to {SESSION_FILE}");
    }
    info!("Authorized — ready");

    // ── build per-chat state ──────────────────────────────────────────────
    let file_ids: Arc<Mutex<HashSet<String>>> =
        Arc::new(Mutex::new(data.downloaded_file_ids.drain(..).collect()));

    let mut data_chats: HashMap<String, ChatData> = data
        .chat
        .drain(..)
        .map(|d| (d.chat_id.clone(), d))
        .collect();

    let concurrency = cfg
        .max_concurrent_transmissions
        .unwrap_or(cfg.max_download_task);
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
            data_chats.values().map(|c| c.ids_to_retry.len()).sum::<usize>()
        );

        if !completed || shutdown.is_cancelled() {
            break;
        }
        info!(
            "cycle {cycle_no} complete in {:.1}s — sleeping {}s (Ctrl+C to shut down)",
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

/// Build `data.yaml` from the live file-id cache and per-chat retry sets and
/// write it atomically. `config.yaml` is updated incrementally by
/// `update_chat_state` as each chat finishes, so only `data.yaml` is saved here.
async fn persist_state(
    file_ids: &Arc<Mutex<HashSet<String>>>,
    data_chats: &HashMap<String, ChatData>,
) -> anyhow::Result<()> {
    let mut data = crate::config::AppData::default();
    let mut ids: Vec<String> = file_ids.lock().await.iter().cloned().collect();
    ids.sort();
    data.downloaded_file_ids = ids;
    let mut chats: Vec<ChatData> = data_chats.values().cloned().collect();
    chats.sort_by(|a, b| a.chat_id.cmp(&b.chat_id));
    data.chat = chats;
    save_app_data(DATA_FILE, &data)
}

/// Log a one-time summary of the loaded configuration at startup.
fn log_config_summary(cfg: &Config, data_chats: &HashMap<String, ChatData>, concurrency: usize) {
    let pending: usize = data_chats.values().map(|c| c.ids_to_retry.len()).sum();
    info!(
        "config: {} chat(s), {concurrency} parallel download slot(s), scan every {}s, save_path={}",
        cfg.chat.len(),
        cfg.check_interval_secs,
        cfg.save_path.display()
    );
    info!(
        "config: media_types=[{}] txt_download={} web_ui={}:{}",
        cfg.media_types.join(","),
        cfg.enable_download_txt,
        cfg.web_host,
        cfg.web_port
    );
    for c in &cfg.chat {
        let retry = data_chats
            .get(&c.chat_id)
            .map(|d| d.ids_to_retry.len())
            .unwrap_or(0);
        info!(
            "config: chat '{}' from msg {} ({} id(s) queued for retry){}",
            c.chat_id,
            c.last_read_message_id,
            retry,
            c.download_filter
                .as_deref()
                .map(|f| format!(" filter='{f}'"))
                .unwrap_or_default()
        );
    }
    if pending > 0 {
        info!("config: resuming with {pending} message id(s) pending retry across all chats");
    }
}

/// Log what was preserved across the run so the user knows resume is safe.
async fn log_shutdown_summary(
    cfg: &Config,
    data_chats: &HashMap<String, ChatData>,
    file_ids: &Arc<Mutex<HashSet<String>>>,
    started: Instant,
) {
    let file_id_count = file_ids.lock().await.len();
    let pending: usize = data_chats.values().map(|c| c.ids_to_retry.len()).sum();
    info!(
        "shutdown: ran for {:.1}s; {file_id_count} file id(s) cached, {pending} message id(s) pending retry",
        started.elapsed().as_secs_f64()
    );
    for c in &cfg.chat {
        let retry = data_chats
            .get(&c.chat_id)
            .map(|d| d.ids_to_retry.len())
            .unwrap_or(0);
        info!(
            "shutdown: chat '{}' last_read={} ({} id(s) to retry) — partial downloads kept as .part for resume",
            c.chat_id, c.last_read_message_id, retry
        );
    }
    info!("shutdown: state flushed to {DATA_FILE} and {CONFIG_FILE}");
}

/// Outcome of scanning one chat.
///
/// `completed` is false when the scan was interrupted by shutdown; in that case
/// `last_id` is the chat's existing `last_read_message_id` (unchanged) so the
/// unscanned range is revisited next run, and any in-flight downloads are
/// captured in the live retry set.
struct ChatOutcome {
    completed: bool,
    last_id: i32,
}

/// Run one scan pass over every configured chat.
///
/// Returns `true` if the whole cycle ran to completion, `false` if it was cut
/// short by shutdown. In either case `data_chats` is left holding the live
/// retry sets, which `run_downloader` persists immediately after this returns.
async fn run_check_cycle(
    client: &Client,
    cfg: &mut Config,
    runtime: &DownloadRuntime,
    data_chats: &mut HashMap<String, ChatData>,
    shutdown: &Shutdown,
) -> anyhow::Result<bool> {
    let chats = cfg.chat.clone();
    for chat_cfg in &chats {
        if shutdown.is_cancelled() {
            return Ok(false);
        }
        let chat_id = chat_cfg.chat_id.clone();
        // Retry ids live in data.yaml (data_chats). The scan mutates this set
        // in place as downloads succeed or fail, so an interrupt still leaves a
        // correct resume set behind.
        let old_retry: HashSet<i32> = data_chats
            .get(&chat_id)
            .map(|dc| dc.ids_to_retry.iter().copied().collect())
            .unwrap_or_default();
        let live_retry = Arc::new(Mutex::new(old_retry));

        match process_chat(client, cfg, chat_cfg, live_retry.clone(), runtime, shutdown).await {
            Ok(outcome) => {
                let retry: Vec<i32> = {
                    let mut v: Vec<i32> = live_retry.lock().await.iter().copied().collect();
                    v.sort_unstable();
                    v
                };
                if retry.is_empty() {
                    data_chats.remove(&chat_id);
                } else {
                    data_chats
                        .entry(chat_id.clone())
                        .and_modify(|dc| dc.ids_to_retry = retry.clone())
                        .or_insert_with(|| ChatData {
                            chat_id: chat_id.clone(),
                            ids_to_retry: retry,
                        });
                }

                if outcome.completed {
                    update_chat_state(cfg, chat_cfg, outcome.last_id)?;
                    info!(
                        "chat {}: scan complete — last_read advanced to {}, {} id(s) pending retry",
                        chat_cfg.chat_id,
                        outcome.last_id,
                        data_chats
                            .get(&chat_cfg.chat_id)
                            .map(|d| d.ids_to_retry.len())
                            .unwrap_or(0)
                    );
                } else {
                    info!(
                        "chat {}: scan interrupted at msg {} — state preserved for resume",
                        chat_cfg.chat_id, outcome.last_id
                    );
                    return Ok(false);
                }
            }
            Err(e) => {
                // A real error (peer resolution, etc.). Still flush the live
                // retry set so partial progress survives, then keep going unless
                // we were also asked to shut down.
                let retry: Vec<i32> = {
                    let mut v: Vec<i32> = live_retry.lock().await.iter().copied().collect();
                    v.sort_unstable();
                    v
                };
                if retry.is_empty() {
                    data_chats.remove(&chat_id);
                } else {
                    data_chats
                        .entry(chat_id.clone())
                        .and_modify(|dc| dc.ids_to_retry = retry.clone())
                        .or_insert_with(|| ChatData {
                            chat_id: chat_id.clone(),
                            ids_to_retry: retry,
                        });
                }
                error!("chat {chat_id}: {e:#}");
                if shutdown.is_cancelled() {
                    return Ok(false);
                }
            }
        }
    }
    Ok(true)
}

/// Wait while the web UI has paused downloads, but bail out immediately when
/// `shutdown` is tripped. Returns `false` if shutdown was requested.
async fn wait_paused(web_state: &WebState, shutdown: &Shutdown) -> bool {
    tokio::select! {
        _ = shutdown.cancelled() => false,
        _ = web_state.wait_if_paused() => !shutdown.is_cancelled(),
    }
}

// ── Interactive login ────────────────────────────────────────────────────

/// Seconds to wait for a FLOOD_WAIT error, or `None` if `err` is not one.
///
/// Grammers renders flood waits as `... (value: 225)`; the raw Telegram
/// error type is `FLOOD_WAIT_N`. Defaults to 60 s when the number can't be
/// read, so we never retry too eagerly.
fn flood_wait_secs(err: &str) -> Option<u64> {
    if !err.contains("FLOOD_WAIT") {
        return None;
    }
    for needle in ["value:", "FLOOD_WAIT_"] {
        if let Some((_, rest)) = err.split_once(needle) {
            let digits: String = rest
                .trim_start()
                .chars()
                .take_while(|c| c.is_ascii_digit())
                .collect();
            if let Ok(n) = digits.parse::<u64>() {
                if n > 0 {
                    return Some(n);
                }
            }
        }
    }
    Some(60)
}

async fn authorize_interactive(client: &Client) -> anyhow::Result<()> {
    let cfg = load_config(CONFIG_FILE)?;

    print!("Enter phone number (international format): ");
    io::stdout().flush().ok();
    let mut phone = String::new();
    io::stdin().read_line(&mut phone)?;
    let phone = phone.trim().to_string();

    // request_login_code can hit FLOOD_WAIT if codes have been requested too
    // often; sleep the required time and retry instead of bailing out.
    let token = {
        let mut flood_retries = 0u32;
        loop {
            match client.request_login_code(&phone, &cfg.api_hash).await {
                Ok(token) => break token,
                Err(e) => {
                    let err_str = e.to_string();
                    if let Some(wait_secs) = flood_wait_secs(&err_str) {
                        if flood_retries >= MAX_AUTH_FLOOD_RETRIES {
                            return Err(anyhow::anyhow!(
                                "auth.sendCode still rate-limited after \
                                 {MAX_AUTH_FLOOD_RETRIES} waits: {err_str}"
                            ));
                        }
                        flood_retries += 1;
                        warn!(
                            "auth: FLOOD_WAIT — sleeping {wait_secs}s before retrying \
                             ({flood_retries}/{MAX_AUTH_FLOOD_RETRIES})"
                        );
                        tokio::time::sleep(Duration::from_secs(wait_secs)).await;
                        continue;
                    }
                    return Err(e.into());
                }
            }
        }
    };

    print!("Enter the verification code sent via Telegram: ");
    io::stdout().flush().ok();
    let mut code = String::new();
    io::stdin().read_line(&mut code)?;
    let code = code.trim().to_string();

    match client.sign_in(&token, &code).await {
        Ok(_) => Ok(()),
        Err(SignInError::PasswordRequired(pt)) => {
            let hint = pt.hint().unwrap_or_default();
            print!("2FA password (hint: {hint}): ");
            io::stdout().flush().ok();
            let mut pass = String::new();
            io::stdin().read_line(&mut pass)?;
            client.check_password(pt, pass.trim()).await?;
            Ok(())
        }
        Err(e) => Err(e.into()),
    }
}

// ── Update per-chat config after download ────────────────────────────────

fn update_chat_state(
    cfg: &mut Config,
    chat_cfg: &ChatConfig,
    last_read: i32,
) -> anyhow::Result<()> {
    for c in &mut cfg.chat {
        if c.chat_id == chat_cfg.chat_id {
            c.last_read_message_id = c.last_read_message_id.max(last_read);
            break;
        }
    }
    save_config(CONFIG_FILE, cfg)?;
    Ok(())
}

// ── Chat download orchestrator ───────────────────────────────────────────

struct DownloadRuntime {
    file_ids: Arc<Mutex<HashSet<String>>>,
    dl_sem: Arc<Semaphore>,
    mp: Arc<MultiProgress>,
    web_state: Arc<WebState>,
}

/// Counters for the per-chat scan summary, updated by spawned download tasks.
#[derive(Default)]
struct ChatStats {
    scanned: AtomicU64,
    downloaded: AtomicU64,
    skipped: AtomicU64,
    text_saved: AtomicU64,
    failed: AtomicU64,
}

async fn process_chat(
    client: &Client,
    cfg: &Config,
    chat_cfg: &ChatConfig,
    live_retry: Arc<Mutex<HashSet<i32>>>,
    runtime: &DownloadRuntime,
    shutdown: &Shutdown,
) -> anyhow::Result<ChatOutcome> {
    // `retry_ids` mirrors the live set purely for scan-control: it tracks which
    // retry ids have NOT yet been seen so the scan knows when it can stop. The
    // live set itself is mutated by the download tasks (success removes, failure
    // inserts) and is what gets persisted as `ids_to_retry`.
    let mut retry_ids = live_retry.lock().await.clone();
    let mut last_id = chat_cfg.last_read_message_id;
    let stats = Arc::new(ChatStats::default());

    info!(
        "chat {}: beginning scan (from msg {}, {} id(s) to retry)",
        chat_cfg.chat_id,
        last_id,
        retry_ids.len()
    );

    let peer = client
        .resolve_username(&chat_cfg.chat_id)
        .await?
        .context("cannot resolve peer")?
        .to_ref()
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?
        .context("peer not found")?;

    let mut messages = client.iter_messages(peer);
    let total = messages
        .total()
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))
        .unwrap_or(0);
    info!("chat {}: ~{total} messages available", chat_cfg.chat_id);

    let filter_fn = build_filter_fn(chat_cfg, cfg);
    let mut tasks = Vec::new();

    loop {
        if shutdown.is_cancelled() {
            break;
        }
        if !wait_paused(&runtime.web_state, shutdown).await {
            break;
        }

        let msg = tokio::select! {
            r = messages.next() => match r {
                Ok(Some(m)) => m,
                Ok(None) => break,
                Err(e) => {
                    let err_str = e.to_string();
                    let (secs, label) = match flood_wait_secs(&err_str) {
                        Some(s) => (s, "FLOOD_WAIT"),
                        None => (1, "iterator error"),
                    };
                    warn!("chat {}: {label} — sleeping {secs}s", chat_cfg.chat_id);
                    if sleep_cancellable(shutdown, Duration::from_secs(secs)).await {
                        break;
                    }
                    continue;
                }
            },
            _ = shutdown.cancelled() => break,
        };

        stats.scanned.fetch_add(1, Ordering::Relaxed);
        let msg_id = msg.id();

        if msg_id <= chat_cfg.last_read_message_id && !retry_ids.remove(&msg_id) {
            if retry_ids.is_empty() {
                break;
            }
            continue;
        }

        if msg_id > last_id {
            last_id = msg_id;
        }

        // ── text-only messages ──────────────────────────────────────────
        let has_media = msg.media().is_some();
        let has_text = !msg.text().is_empty();

        if !has_media && cfg.enable_download_txt && has_text {
            let cfg = cfg.clone();
            let msg_clone = msg.clone();
            let live_retry = live_retry.clone();
            let stats = stats.clone();
            let shutdown = shutdown.clone();
            let permit = tokio::select! {
                p = runtime.dl_sem.clone().acquire_owned() => p?,
                _ = shutdown.cancelled() => break,
            };
            tasks.push(tokio::spawn(async move {
                let _permit = permit;
                tokio::select! {
                    _ = shutdown.cancelled() => {
                        debug!("txt msg {}: interrupted", msg_clone.id());
                    }
                    r = save_text_message(&msg_clone, &cfg) => match r {
                        Ok(()) => {
                            stats.text_saved.fetch_add(1, Ordering::Relaxed);
                            live_retry.lock().await.remove(&msg_clone.id());
                        }
                        Err(e) => {
                            warn!("txt msg {}: {e:#}", msg_clone.id());
                            stats.failed.fetch_add(1, Ordering::Relaxed);
                            live_retry.lock().await.insert(msg_clone.id());
                        }
                    }
                }
            }));
            continue;
        }

        if !has_media {
            continue;
        }

        let Some(media) = msg.media() else {
            continue;
        };
        if !media_matches_config(&media, cfg) {
            debug!("msg {msg_id}: media type not in config, skip");
            continue;
        }

        if !filter_fn(&msg) {
            debug!("msg {msg_id}: filtered out");
            continue;
        }

        // Spawn download task
        let client = client.clone();
        let cfg = cfg.clone();
        let file_ids = runtime.file_ids.clone();
        let live_retry = live_retry.clone();
        let stats = stats.clone();
        let shutdown = shutdown.clone();
        let permit = tokio::select! {
            p = runtime.dl_sem.clone().acquire_owned() => p?,
            _ = shutdown.cancelled() => break,
        };

        let mp = runtime.mp.clone();
        let web_state = runtime.web_state.clone();
        tasks.push(tokio::spawn(async move {
            let _permit = permit;
            let _mp = mp;
            // NB: dropped _permit here releases concurrency slot
            match download_media_inner(
                &client,
                &msg,
                &cfg,
                &file_ids,
                _mp.as_ref(),
                &web_state,
                &shutdown,
            )
            .await
            {
                Ok(true) => {
                    stats.downloaded.fetch_add(1, Ordering::Relaxed);
                    live_retry.lock().await.remove(&msg.id());
                }
                Ok(false) => {
                    stats.skipped.fetch_add(1, Ordering::Relaxed);
                    live_retry.lock().await.remove(&msg.id());
                }
                Err(e) => {
                    if shutdown.is_cancelled() {
                        debug!("msg {}: interrupted — kept for resume", msg.id());
                    } else {
                        warn!("msg {}: download failed — {e:#}", msg.id());
                        stats.failed.fetch_add(1, Ordering::Relaxed);
                        live_retry.lock().await.insert(msg.id());
                    }
                }
            }
        }));
    }

    // On shutdown, abort in-flight tasks at once. Their `.part` files hold a
    // contiguous prefix already, so resume picks up cleanly next run.
    if shutdown.is_cancelled() {
        for task in &tasks {
            task.abort();
        }
    }
    for task in tasks {
        match task.await {
            Ok(()) => {}
            Err(e) if e.is_cancelled() => {}
            Err(e) => warn!("download task panicked: {e}"),
        }
    }

    let completed = !shutdown.is_cancelled();
    let last_id = if completed {
        last_id
    } else {
        chat_cfg.last_read_message_id
    };
    info!(
        "chat {}: scanned {} msg(s) — downloaded {}, skipped {}, text {}, failed {}, last_id={}{}",
        chat_cfg.chat_id,
        stats.scanned.load(Ordering::Relaxed),
        stats.downloaded.load(Ordering::Relaxed),
        stats.skipped.load(Ordering::Relaxed),
        stats.text_saved.load(Ordering::Relaxed),
        stats.failed.load(Ordering::Relaxed),
        last_id,
        if completed { "" } else { " [interrupted]" }
    );

    Ok(ChatOutcome { completed, last_id })
}

/// Sleep for `dur`, returning early (`true`) if `shutdown` is tripped.
async fn sleep_cancellable(shutdown: &Shutdown, dur: Duration) -> bool {
    tokio::select! {
        _ = shutdown.cancelled() => true,
        _ = tokio::time::sleep(dur) => false,
    }
}

// ── Text message download ────────────────────────────────────────────────

async fn save_text_message(
    msg: &grammers_client::message::Message,
    cfg: &Config,
) -> anyhow::Result<()> {
    let chat_title = msg
        .peer()
        .and_then(|p| p.name())
        .map(validate_title)
        .unwrap_or_else(|| format!("{:?}", msg.peer_id()));

    let date = msg.date().naive_utc();
    let datetime_str = date.format(&cfg.date_format).to_string();

    // Build path: save_path/chat_title/datetime/
    let mut dir: PathBuf = cfg.save_path.clone();
    for seg in &cfg.file_path_prefix {
        match seg.as_str() {
            "chat_title" => dir.push(&chat_title),
            "media_datetime" => dir.push(&datetime_str),
            _ => {}
        }
    }
    tokio::fs::create_dir_all(&dir).await?;

    let file_path = dir.join(format!("{}.txt", msg.id()));
    if tokio::fs::try_exists(&file_path).await? {
        return Ok(());
    }

    tokio::fs::write(&file_path, msg.text()).await?;
    info!("msg {}: saved text -> {}", msg.id(), file_path.display());
    Ok(())
}

// ── Filter support ───────────────────────────────────────────────────────

fn build_filter_fn(
    chat_cfg: &ChatConfig,
    cfg: &Config,
) -> Box<dyn Fn(&grammers_client::message::Message) -> bool + Send + Sync> {
    let filter_str = match &chat_cfg.download_filter {
        Some(fs) => replace_date_time(fs, &cfg.date_format),
        None => return Box::new(|_| true),
    };
    let parser = Parser::new(&filter_str);

    Box::new(move |msg| {
        let vars = MessageVars(msg);
        match parser.parse(&vars) {
            Ok(Value::Bool(b)) => b,
            Ok(_) => false,
            Err(e) => {
                warn!("filter parse error for msg {}: {e}", msg.id());
                false
            }
        }
    })
}

struct MessageVars<'a>(&'a grammers_client::message::Message);

impl VarLookup for MessageVars<'_> {
    fn get_var(&self, name: &str) -> Option<Value> {
        let msg = self.0;
        Some(match name {
            "message_date" => Value::DateTime(msg.date().naive_utc()),
            "message_id" | "id" => Value::Int(msg.id() as i64),
            "message_caption" | "caption" => Value::Str(msg.text().to_string()),
            "sender_name" => Value::Str(
                msg.peer()
                    .and_then(|p| p.name())
                    .map(str::to_string)
                    .unwrap_or_default(),
            ),
            "sender_id" | "message_thread_id" | "topic_id" => Value::Int(0),
            "reply_to_message_id" => Value::Int(msg.reply_to_message_id().unwrap_or(0) as i64),
            "media_type" => Value::Str(media_type_value(msg)),
            "file_extension" => Value::Str(file_extension_value(msg)),
            "media_file_name" | "file_name" => Value::Str(media_file_name_value(msg)),
            "media_file_size" | "file_size" => Value::Int(media_file_size_value(msg)),
            "media_duration" => Value::Int(media_duration_value(msg)),
            "media_width" => Value::Int(media_resolution_value(msg).0),
            "media_height" => Value::Int(media_resolution_value(msg).1),
            _ => return None,
        })
    }
}

fn media_type_value(msg: &grammers_client::message::Message) -> String {
    match msg.media() {
        Some(Media::Photo(_)) => "photo".into(),
        Some(Media::Document(_)) => "document".into(),
        _ => String::new(),
    }
}

fn file_extension_value(msg: &grammers_client::message::Message) -> String {
    match msg.media() {
        Some(Media::Photo(_)) => "jpg".into(),
        Some(Media::Document(doc)) => mime_to_ext(doc.mime_type().unwrap_or("")).into(),
        _ => String::new(),
    }
}

fn media_file_name_value(msg: &grammers_client::message::Message) -> String {
    match msg.media() {
        Some(Media::Document(doc)) => doc.name().map(str::to_string).unwrap_or_default(),
        _ => String::new(),
    }
}

fn media_file_size_value(msg: &grammers_client::message::Message) -> i64 {
    match msg.media() {
        Some(Media::Photo(photo)) => photo.size().unwrap_or(0) as i64,
        Some(Media::Document(doc)) => doc.size().unwrap_or(0) as i64,
        _ => 0,
    }
}

fn media_duration_value(msg: &grammers_client::message::Message) -> i64 {
    match msg.media() {
        Some(Media::Document(doc)) => doc.duration().unwrap_or(0.0) as i64,
        _ => 0,
    }
}

fn media_resolution_value(msg: &grammers_client::message::Message) -> (i64, i64) {
    match msg.media() {
        Some(Media::Document(doc)) => doc
            .resolution()
            .map(|(width, height)| (width as i64, height as i64))
            .unwrap_or((0, 0)),
        _ => (0, 0),
    }
}

fn mime_to_ext(mime: &str) -> &str {
    match mime {
        "image/jpeg" => "jpg",
        "image/png" => "png",
        "image/gif" => "gif",
        "image/webp" => "webp",
        "audio/mpeg" => "mp3",
        "audio/ogg" => "ogg",
        "audio/mp4" => "m4a",
        "audio/x-wav" | "audio/wav" => "wav",
        "video/mp4" => "mp4",
        "video/webm" => "webm",
        "video/x-matroska" => "mkv",
        "application/pdf" => "pdf",
        "application/zip" => "zip",
        "application/x-rar-compressed" => "rar",
        "application/epub+zip" => "epub",
        _ => "unknown",
    }
}

fn document_kind(mime: &str) -> &str {
    if mime == "image/gif" {
        "animation"
    } else if mime.starts_with("audio/") {
        "audio"
    } else if mime.starts_with("video/") {
        "video"
    } else {
        "document"
    }
}

fn media_kind_and_ext(media: &Media) -> Option<(&str, String)> {
    match media {
        Media::Photo(_) => Some(("photo", "jpg".to_string())),
        Media::Document(doc) => {
            let mime = doc.mime_type().unwrap_or("");
            let ext = doc
                .name()
                .and_then(|name| name.rsplit_once('.').map(|(_, ext)| ext.to_string()))
                .unwrap_or_else(|| mime_to_ext(mime).to_string());
            Some((document_kind(mime), ext))
        }
        _ => None,
    }
}

fn media_matches_config(media: &Media, cfg: &Config) -> bool {
    let Some((kind, ext)) = media_kind_and_ext(media) else {
        return false;
    };
    if !cfg.media_types.iter().any(|item| item == kind) {
        return false;
    }

    let allowed = match kind {
        "audio" => &cfg.file_formats.audio,
        "document" => &cfg.file_formats.document,
        "video" => &cfg.file_formats.video,
        _ => return true,
    };
    allowed.iter().any(|item| item == "all" || item == &ext)
}

// ── Media download ───────────────────────────────────────────────────────

async fn download_media_inner(
    client: &Client,
    msg: &grammers_client::message::Message,
    cfg: &Config,
    file_ids: &Arc<tokio::sync::Mutex<HashSet<String>>>,
    mp: &MultiProgress,
    web_state: &Arc<WebState>,
    shutdown: &Shutdown,
) -> anyhow::Result<bool> {
    let media = match msg.media() {
        Some(m) => m,
        None => return Ok(false),
    };
    let msg_id = msg.id();

    // Check file_unique_id cache
    let fid = match &media {
        Media::Photo(p) => p.id().to_string(),
        Media::Document(d) => d.id().to_string(),
        _ => String::new(),
    };
    if !fid.is_empty() {
        let cache = file_ids.lock().await;
        if cache.contains(&fid) {
            debug!("msg={msg_id}: already downloaded (file_unique_id), skipped");
            return Ok(false);
        }
        drop(cache);
    }

    let (temp_path, final_path) = build_media_paths(msg, &media, cfg)?;

    // Already fully downloaded?
    if tokio::fs::try_exists(&final_path).await? {
        debug!("msg={msg_id}: file already exists, skipped");
        let mut cache = file_ids.lock().await;
        if !fid.is_empty() {
            cache.insert(fid);
        }
        return Ok(false);
    }

    tokio::fs::create_dir_all(temp_path.parent().unwrap_or(Path::new("."))).await?;

    let total = match &media {
        Media::Photo(p) => p.size().unwrap_or(0) as u64,
        Media::Document(d) => d.size().unwrap_or(0) as u64,
        _ => 0,
    };
    let existing = resume_offset(&temp_path, total).await?;
    web_state
        .download_started(msg_id, &final_path, existing, total)
        .await;

    // The sidecar marks a prior run as fully fetched but not yet renamed into
    // place. Promote it directly instead of re-fetching the whole file.
    if total > 0 && existing == total {
        info!(
            "msg={msg_id}: already complete, finalizing {}",
            format_byte(total as f64)
        );
        finalize_download(
            msg_id,
            &fid,
            file_ids,
            &temp_path,
            &final_path,
            total,
            web_state,
        )
        .await?;
        return Ok(true);
    }

    if existing > 0 {
        info!(
            "msg={msg_id}: resuming from {} of {}",
            format_byte(existing as f64),
            format_byte(total as f64)
        );
    } else {
        info!(
            "msg={msg_id}: downloading {} -> {}",
            format_byte(total as f64),
            final_path.display()
        );
    }

    let mut last_err = None;
    for attempt in 0..RETRY_LIMIT {
        if shutdown.is_cancelled() {
            break;
        }
        if attempt > 0 {
            info!("msg={msg_id}: retry {}/{}", attempt + 1, RETRY_LIMIT);
            if sleep_cancellable(shutdown, Duration::from_secs(RETRY_DELAY_SECS)).await {
                break;
            }
        }

        // Re-evaluate the resume offset each attempt: a failed attempt leaves
        // the valid prefix on disk, so we only re-fetch what is missing.
        let downloaded = if attempt == 0 {
            existing
        } else {
            resume_offset(&temp_path, total).await?
        };

        let pb = mp.add(ProgressBar::new(total));
        pb.set_style(DOWNLOAD_PROGRESS_STYLE.clone());
        pb.set_message(format!("{msg_id}"));
        if downloaded > 0 {
            pb.set_position(downloaded);
        }

        if total == 0 {
            if !wait_paused(web_state, shutdown).await {
                pb.finish_and_clear();
                break;
            }
            let outcome = tokio::select! {
                r = client.download_media(&media, &temp_path) => r.map_err(|e| e.into()),
                _ = shutdown.cancelled() => Err(anyhow::anyhow!("interrupted")),
            };
            if let Err(e) = outcome {
                last_err = Some(e);
                pb.finish_and_clear();
                drop(pb);
                if shutdown.is_cancelled() {
                    break;
                }
                discard_partial(&temp_path).await;
                continue;
            }
            pb.set_position(total);
        } else if total > downloaded {
            let progress = DownloadProgress {
                pb: &pb,
                web_state,
                msg_id,
            };
            if let Err(e) = download_concurrent(
                client, &media, &temp_path, downloaded, total, &progress, shutdown,
            )
            .await
            {
                // Keep the partial file so the next attempt resumes; only the
                // unfetched chunks need re-downloading.
                if !shutdown.is_cancelled() {
                    warn!("msg={msg_id}: {e:#}");
                }
                last_err = Some(e);
                pb.finish_and_clear();
                drop(pb);
                continue;
            }
        } else {
            pb.set_position(downloaded);
        }

        pb.finish_and_clear();
        drop(pb);

        let actual = tokio::fs::metadata(&temp_path)
            .await
            .map(|m| m.len())
            .unwrap_or(0);

        if total > 0 && actual != total {
            warn!("msg={msg_id}: size mismatch ({actual} vs {total}) — retrying");
            discard_partial(&temp_path).await;
            last_err = Some(anyhow::anyhow!("size mismatch"));
            continue;
        }

        finalize_download(
            msg_id,
            &fid,
            file_ids,
            &temp_path,
            &final_path,
            actual,
            web_state,
        )
        .await?;
        return Ok(true);
    }

    // On shutdown, keep the `.part` file so the download resumes next run. On a
    // real failure (retries exhausted), delete it so we start fresh.
    if !shutdown.is_cancelled() {
        discard_partial(&temp_path).await;
    }
    web_state.download_finished(msg_id, 0, false).await;
    if shutdown.is_cancelled() {
        return Err(anyhow::anyhow!("download interrupted"));
    }
    Err(last_err.unwrap_or_else(|| anyhow::anyhow!("download failed")))
}

/// Promote a completed `.part` file to its final name, record its file id in
/// the dedup cache, and notify the web UI. The caller has already validated
/// `actual` against the expected size.
async fn finalize_download(
    msg_id: i32,
    fid: &str,
    file_ids: &Arc<tokio::sync::Mutex<HashSet<String>>>,
    temp_path: &Path,
    final_path: &Path,
    actual: u64,
    web_state: &Arc<WebState>,
) -> anyhow::Result<()> {
    tokio::fs::create_dir_all(final_path.parent().unwrap_or(Path::new("."))).await?;
    tokio::fs::rename(temp_path, final_path).await?;
    let _ = tokio::fs::remove_file(progress_path(temp_path)).await;

    if !fid.is_empty() {
        let mut cache = file_ids.lock().await;
        if cache.len() >= MAX_FILE_ID_CACHE {
            // HashSet order is arbitrary, so this evicts a random entry
            // (not truly the oldest). Fine for a dedup cache: the worst
            // case is a one-off re-download of the evicted file.
            if let Some(evicted) = cache.iter().next().cloned() {
                cache.remove(&evicted);
            }
        }
        cache.insert(fid.to_string());
    }

    info!(
        "msg={msg_id}: saved {} -> {}",
        format_byte(actual as f64),
        final_path.display()
    );
    web_state.download_finished(msg_id, actual, true).await;
    Ok(())
}

struct DownloadProgress<'a> {
    pb: &'a ProgressBar,
    web_state: &'a Arc<WebState>,
    msg_id: i32,
}

async fn send_chunk(
    tx: &mpsc::Sender<(u64, Vec<u8>)>,
    chunk: (u64, Vec<u8>),
    shutdown: &Shutdown,
) -> Result<(), mpsc::error::SendError<(u64, Vec<u8>)>> {
    tokio::select! {
        permit = tx.reserve() => match permit {
            Ok(permit) => {
                permit.send(chunk);
                Ok(())
            }
            Err(_) => Err(mpsc::error::SendError(chunk)),
        },
        _ = shutdown.cancelled() => Err(mpsc::error::SendError(chunk)),
    }
}

async fn download_concurrent(
    client: &Client,
    media: &Media,
    path: &Path,
    start: u64,
    total: u64,
    progress: &DownloadProgress<'_>,
    shutdown: &Shutdown,
) -> anyhow::Result<()> {
    let chunk_size = DOWNLOAD_CHUNK_SIZE;
    let start_chunk = start / chunk_size;
    let total_chunks = total.div_ceil(chunk_size);
    let workers = RESUME_WORKERS.min(total_chunks - start_chunk).max(1);

    let (tx, mut rx) = mpsc::channel::<(u64, Vec<u8>)>((workers as usize).max(1));
    // Set by the first worker to fail so its peers stop fetching after their
    // current chunk instead of downloading data that will be discarded.
    let abort = Arc::new(AtomicBool::new(false));
    let mut tasks = Vec::new();

    for worker in 0..workers {
        let client = client.clone();
        let media = media.clone();
        let tx = tx.clone();
        let web_state = progress.web_state.clone();
        let abort = abort.clone();
        let shutdown = shutdown.clone();

        tasks.push(tokio::spawn(async move {
            // Striped assignment: this worker owns chunks worker, worker+n, ...
            // Each chunk is fetched with its own stream so a short read (which
            // ends grammers' stream early) only affects that one piece.
            let mut idx = start_chunk + worker;
            while idx < total_chunks {
                if abort.load(Ordering::Relaxed) || shutdown.is_cancelled() {
                    break;
                }
                if !wait_paused(&web_state, &shutdown).await {
                    break;
                }
                let offset = idx * chunk_size;
                let expected = (total - offset).min(chunk_size);
                match fetch_chunk(&client, &media, idx, expected, &shutdown).await {
                    Ok(chunk) => {
                        if send_chunk(&tx, (offset, chunk), &shutdown).await.is_err() {
                            break; // receiver gone or shutdown requested
                        }
                    }
                    Err(e) => {
                        abort.store(true, Ordering::Relaxed);
                        return Err(e);
                    }
                }
                idx += workers;
            }
            Ok::<(), anyhow::Error>(())
        }));
    }
    drop(tx);

    let mut file = tokio::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(start == 0)
        .open(path)
        .await?;
    // Preallocate the full file up front. On a NAS this avoids growing the
    // file extent-by-extent as chunks land, which fragments the layout and
    // can stall writes. Falls back to a sparse truncate if unsupported.
    preallocate(&file, total).await?;
    file.seek(std::io::SeekFrom::Start(start)).await?;

    let mut next = start;
    let mut fetched = start;
    let mut last_flushed = start;
    let started = Instant::now();
    let mut pending: BTreeMap<u64, Vec<u8>> = BTreeMap::new();
    loop {
        // Receiving the next chunk races against shutdown so a Ctrl+C doesn't
        // block on a stalled worker; anything already in the pipe is still
        // flushed below so the `.part` prefix stays contiguous.
        let (offset, chunk) = tokio::select! {
            m = rx.recv() => match m {
                Some(item) => item,
                None => break,
            },
            _ = shutdown.cancelled() => break,
        };
        fetched = (fetched + chunk.len() as u64).min(total);
        progress.pb.set_position(fetched);
        let elapsed = started.elapsed().as_secs_f64().max(0.001);
        progress
            .web_state
            .download_progress(
                progress.msg_id,
                fetched,
                ((fetched - start) as f64 / elapsed) as u64,
            )
            .await;

        pending.insert(offset, chunk);
        while let Some(chunk) = pending.remove(&next) {
            file.write_all(&chunk).await?;
            next += chunk.len() as u64;
            // Periodically record the contiguous write position so an
            // interruption can resume here. The `.part` file is preallocated to
            // `total`, so its size can't recover this point.
            if next - last_flushed >= PROGRESS_FLUSH_BYTES {
                write_progress(path, next).await;
                last_flushed = next;
            }
        }
    }
    // Final flush: persist the full contiguous position (== `total` on success)
    // so a later run can finalize without re-fetching.
    write_progress(path, next).await;

    // Surface the first worker error (a chunk that exhausted its retries).
    let mut worker_err: Option<anyhow::Error> = None;
    let mut cancelled = shutdown.is_cancelled();
    for task in tasks {
        match task.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) if worker_err.is_none() => worker_err = Some(e),
            Ok(Err(_)) => {}
            Err(e) if e.is_cancelled() => cancelled = true,
            Err(e) if worker_err.is_none() => {
                worker_err = Some(anyhow::anyhow!("download worker join failed: {e}"))
            }
            Err(_) => {}
        }
    }
    if cancelled {
        // Leave the contiguous prefix on disk for resume; report interruption
        // so the caller keeps the `.part` file rather than deleting it.
        return Err(anyhow::anyhow!("download interrupted"));
    }
    if let Some(e) = worker_err {
        return Err(e);
    }
    // No worker errored, so every chunk must have been written contiguously.
    // A gap here would indicate a logic bug rather than a network failure.
    if next != total {
        return Err(anyhow::anyhow!(
            "incomplete download: wrote {next} of {total} bytes"
        ));
    }

    Ok(())
}

/// Fetch a single chunk at index `idx`, retrying transient failures.
///
/// A fresh `iter_download` stream is used per attempt. This matters because
/// grammers marks its stream done as soon as a read returns fewer bytes than
/// requested (see grammers `files.rs`); a transient short read would otherwise
/// end the stream and starve the rest of the fetch. Each chunk is also
/// validated against its expected size so a short piece is never written.
///
/// The fetch and its retry backoff are both raced against `shutdown` so a
/// Ctrl+C cancels the in-flight network read at once instead of waiting for
/// it (or its backoff) to finish.
async fn fetch_chunk(
    client: &Client,
    media: &Media,
    idx: u64,
    expected: u64,
    shutdown: &Shutdown,
) -> anyhow::Result<Vec<u8>> {
    let mut backoff = 0u64;
    let mut last_err: Option<anyhow::Error> = None;
    for attempt in 0..CHUNK_RETRY_LIMIT {
        if shutdown.is_cancelled() {
            break;
        }
        let delay = std::mem::take(&mut backoff);
        if delay > 0 && sleep_cancellable(shutdown, Duration::from_secs(delay)).await {
            break;
        }
        let mut stream = client
            .iter_download(media)
            .chunk_size(DOWNLOAD_CHUNK_SIZE as i32)
            .skip_chunks(i32::try_from(idx)?);
        let result = tokio::select! {
            r = stream.next() => r,
            _ = shutdown.cancelled() => break,
        };
        match result {
            Ok(Some(chunk)) if chunk.len() as u64 == expected => return Ok(chunk),
            Ok(Some(chunk)) => {
                warn!(
                    "chunk {idx}: short read {} B (expected {expected}), retry {}/{}",
                    chunk.len(),
                    attempt + 1,
                    CHUNK_RETRY_LIMIT
                );
                last_err = Some(anyhow::anyhow!(
                    "chunk {idx} short: {} vs {expected} bytes",
                    chunk.len()
                ));
                backoff = RETRY_DELAY_SECS;
            }
            Ok(None) => {
                warn!(
                    "chunk {idx}: stream ended early, retry {}/{}",
                    attempt + 1,
                    CHUNK_RETRY_LIMIT
                );
                last_err = Some(anyhow::anyhow!("chunk {idx} stream ended early"));
                backoff = RETRY_DELAY_SECS;
            }
            Err(e) => {
                backoff = flood_wait_secs(&e.to_string()).unwrap_or(RETRY_DELAY_SECS);
                warn!(
                    "chunk {idx}: fetch error: {e}; retry {}/{} in {backoff}s",
                    attempt + 1,
                    CHUNK_RETRY_LIMIT
                );
                last_err = Some(anyhow::anyhow!("chunk {idx}: {e}"));
            }
        }
    }
    if shutdown.is_cancelled() {
        return Err(anyhow::anyhow!("download interrupted"));
    }
    Err(last_err.unwrap_or_else(|| anyhow::anyhow!("chunk {idx} failed after retries")))
}

/// Resume offset for `temp_path`: the byte position to continue downloading
/// from. The `.part` file is preallocated to `total` up front, so its size does
/// not reflect how much was actually fetched; the `.progress` sidecar (the
/// contiguous byte count the writer flushed) is the source of truth. Returns
/// `total` when the sidecar marks the file complete, the last flushed chunk
/// boundary when partial, or 0 (clearing any stale sidecar) when the temp is
/// missing.
async fn resume_offset(temp_path: &Path, total: u64) -> anyhow::Result<u64> {
    let len = match tokio::fs::metadata(temp_path).await {
        Ok(m) => m.len(),
        Err(_) => {
            let _ = tokio::fs::remove_file(progress_path(temp_path)).await;
            return Ok(0);
        }
    };
    let valid = read_progress(temp_path).await;
    if total > 0 && valid >= total {
        return Ok(total);
    }
    // Partial: resume from the last flushed chunk boundary, clamped to the real
    // file size so a stale sidecar can never make us skip data.
    let aligned = valid - (valid % DOWNLOAD_CHUNK_SIZE);
    Ok(aligned.min(len))
}

/// Path of the `.progress` sidecar recording how many contiguous bytes of
/// `temp_path` have been fetched.
fn progress_path(temp_path: &Path) -> PathBuf {
    let mut p = temp_path.as_os_str().to_owned();
    p.push(".progress");
    PathBuf::from(p)
}

/// Read the recorded contiguous byte count, or 0 when the sidecar is missing or
/// unreadable (treated as a fresh download).
async fn read_progress(temp_path: &Path) -> u64 {
    match tokio::fs::read(progress_path(temp_path)).await {
        Ok(bytes) => String::from_utf8_lossy(&bytes)
            .trim()
            .parse::<u64>()
            .unwrap_or(0),
        Err(_) => 0,
    }
}

/// Best-effort persist of the contiguous byte count. A failed write only means
/// the next resume re-fetches a little more.
async fn write_progress(temp_path: &Path, n: u64) {
    let _ = tokio::fs::write(progress_path(temp_path), n.to_string().into_bytes()).await;
}

/// Drop a partial `.part` download and its sidecar so the next attempt starts
/// fresh.
async fn discard_partial(temp_path: &Path) {
    let _ = tokio::fs::remove_file(temp_path).await;
    let _ = tokio::fs::remove_file(progress_path(temp_path)).await;
}

/// Preallocate `size` bytes for `file`, preferring real block allocation
/// (`posix_fallocate`) so network-attached storage doesn't grow the file
/// incrementally during chunked writes. Falls back to a sparse `set_len` when
/// preallocation is unavailable (or on non-Linux hosts). No-op for `size == 0`.
async fn preallocate(file: &tokio::fs::File, size: u64) -> anyhow::Result<()> {
    if size == 0 {
        return Ok(());
    }
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::io::AsRawFd;
        let fd = file.as_raw_fd();
        let len = i64::try_from(size)?;
        // posix_fallocate is a blocking syscall; run it off the async thread.
        let rc =
            tokio::task::spawn_blocking(move || unsafe { posix_fallocate(fd, 0, len) }).await?;
        if rc != 0 {
            // ENOTSUP (e.g. over NFS) / EDQUOT / etc.: fall back to a sparse truncate.
            file.set_len(size).await?;
        }
        Ok(())
    }
    #[cfg(not(target_os = "linux"))]
    {
        // No portable fallocate here; reserve the size sparsely instead.
        file.set_len(size).await?;
        Ok(())
    }
}

/// Returns (temp_path, final_path) for a media download.
fn build_media_paths(
    msg: &grammers_client::message::Message,
    media: &Media,
    cfg: &Config,
) -> anyhow::Result<(PathBuf, PathBuf)> {
    let chat_title = msg
        .peer()
        .and_then(|p| p.name())
        .map(validate_title)
        .unwrap_or_else(|| format!("{:?}", msg.peer_id()));

    let date = msg.date().naive_utc();
    let datetime_str = date.format(&cfg.date_format).to_string();

    let Some((media_type_str, _)) = media_kind_and_ext(media) else {
        return Err(anyhow::anyhow!("unsupported media"));
    };

    let mut dir: PathBuf = cfg.save_path.clone();
    for seg in &cfg.file_path_prefix {
        match seg.as_str() {
            "chat_title" => dir.push(&chat_title),
            "media_datetime" => dir.push(&datetime_str),
            "media_type" => dir.push(media_type_str),
            _ => {}
        }
    }

    let (stem, ext) = match media {
        Media::Photo(_) => (msg.id().to_string(), "jpg".to_string()),
        Media::Document(doc) => {
            let mut stem = String::new();
            let mut ext = doc
                .mime_type()
                .map(|m| mime_to_ext(m).to_string())
                .unwrap_or_else(|| "unknown".to_string());
            if let Some(ref name) = doc.name() {
                if let Some(dot) = name.rfind('.') {
                    stem = name[..dot].to_string();
                    ext = name[dot + 1..].to_string();
                } else {
                    stem = name.to_string();
                }
            }
            if stem.is_empty() {
                stem = format!("file_{}", doc.id());
            }
            let mut parts: Vec<String> = Vec::new();
            for seg in &cfg.file_name_prefix {
                match seg.as_str() {
                    "message_id" => parts.push(msg.id().to_string()),
                    "file_name" => parts.push(stem.clone()),
                    "caption" => {
                        let txt = msg.text();
                        if !txt.is_empty() {
                            parts.push(validate_title(txt));
                        }
                    }
                    _ => {}
                }
            }
            let sep = &cfg.file_name_prefix_split;
            (
                if parts.is_empty() {
                    msg.id().to_string()
                } else {
                    parts.join(sep)
                },
                ext,
            )
        }
        _ => return Err(anyhow::anyhow!("unsupported media")),
    };

    let fname = format!("{stem}.{ext}");
    let final_path = truncate_filename(&dir.join(&fname), 230);

    let temp_path = final_path.with_extension(format!("{ext}.part"));

    Ok((temp_path, final_path))
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
