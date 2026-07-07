use std::io::{self, Write};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Context;
use grammers_client::{Client, SignInError};
use grammers_mtsender::SenderPool;
use grammers_session::storages::SqliteSession;
use indicatif::MultiProgress;
use log::{debug, error, info, warn};
use rustc_hash::{FxHashMap as HashMap, FxHashSet as HashSet};
use tokio::sync::{Mutex, Semaphore};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::config::{
    load_app_data, load_config, save_app_data, save_config, ChatConfig, ChatData, Config,
};
use crate::downloader::media::download_media_inner;
use crate::downloader::metadata::{
    file_extension_value, media_duration_value, media_file_name_value, media_file_size_value,
    media_matches_config, media_resolution_value, media_type_value,
};
use crate::filter::{Parser, Value, VarLookup};
use crate::format::replace_date_time;
use crate::webui::WebState;

pub(crate) const CONFIG_FILE: &str = "config.yaml";
const DATA_FILE: &str = "data.yaml";
const SESSION_FILE: &str = "sessions/tmd.session";
const MAX_AUTH_FLOOD_RETRIES: u32 = 3;

#[derive(Clone)]
pub(crate) struct Shutdown {
    token: CancellationToken,
}

impl Shutdown {
    pub(crate) fn new() -> Self {
        Self {
            token: CancellationToken::new(),
        }
    }

    pub(crate) fn cancel(&self) {
        self.token.cancel();
    }

    pub(crate) fn is_cancelled(&self) -> bool {
        self.token.is_cancelled()
    }

    pub(crate) async fn cancelled(&self) {
        self.token.cancelled().await;
    }
}

pub(crate) async fn run_downloader(
    mut cfg: Config,
    web_state: Arc<WebState>,
    shutdown: Shutdown,
) -> anyhow::Result<()> {
    web_state.set_status("running").await;

    let mut data = load_app_data(DATA_FILE)?;
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
        authorize_interactive(&client).await?;
        info!("Session saved to {SESSION_FILE}");
    }
    info!("Authorized - ready");

    let file_ids: Arc<Mutex<HashSet<String>>> =
        Arc::new(Mutex::new(data.downloaded_file_ids.drain(..).collect()));

    let mut data_chats: HashMap<String, ChatData> = data
        .chat
        .drain(..)
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
            data_chats.values().map(|c| c.ids_to_retry.len()).sum::<usize>()
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
        "config: media_types=[{}] web_ui={}:{}",
        cfg.media_types.join(","),
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
            "shutdown: chat '{}' last_read={} ({} id(s) to retry) - partial downloads kept as .part for resume",
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
                sync_retry_set(data_chats, &chat_id, &live_retry).await;

                if outcome.completed {
                    update_chat_state(cfg, chat_cfg, outcome.last_id)?;
                    info!(
                        "chat {}: scan complete - last_read advanced to {}, {} id(s) pending retry",
                        chat_cfg.chat_id,
                        outcome.last_id,
                        data_chats
                            .get(&chat_cfg.chat_id)
                            .map(|d| d.ids_to_retry.len())
                            .unwrap_or(0)
                    );
                } else {
                    info!(
                        "chat {}: scan interrupted at msg {} - state preserved for resume",
                        chat_cfg.chat_id, outcome.last_id
                    );
                    return Ok(false);
                }
            }
            Err(e) => {
                // A real error (peer resolution, etc.). Still flush the live
                // retry set so partial progress survives, then keep going unless
                // we were also asked to shut down.
                sync_retry_set(data_chats, &chat_id, &live_retry).await;
                error!("chat {chat_id}: {e:#}");
                if shutdown.is_cancelled() {
                    return Ok(false);
                }
            }
        }
    }
    Ok(true)
}

async fn sync_retry_set(
    data_chats: &mut HashMap<String, ChatData>,
    chat_id: &str,
    live_retry: &Arc<Mutex<HashSet<i32>>>,
) {
    let mut retry: Vec<i32> = live_retry.lock().await.iter().copied().collect();
    retry.sort_unstable();
    if retry.is_empty() {
        data_chats.remove(chat_id);
    } else {
        data_chats.insert(
            chat_id.to_string(),
            ChatData {
                chat_id: chat_id.to_string(),
                ids_to_retry: retry,
            },
        );
    }
}

/// Wait while the web UI has paused downloads, but bail out immediately when
/// `shutdown` is tripped. Returns `false` if shutdown was requested.
pub(crate) async fn wait_paused(web_state: &WebState, shutdown: &Shutdown) -> bool {
    tokio::select! {
        _ = shutdown.cancelled() => false,
        _ = web_state.wait_if_paused() => !shutdown.is_cancelled(),
    }
}

/// Seconds to wait for a FLOOD_WAIT error, or `None` if `err` is not one.
///
/// Grammers renders flood waits as `... (value: 225)`; the raw Telegram
/// error type is `FLOOD_WAIT_N`. Defaults to 60 s when the number can't be
/// read, so we never retry too eagerly.
pub(crate) fn flood_wait_secs(err: &str) -> Option<u64> {
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
    let phone = phone.trim();

    // request_login_code can hit FLOOD_WAIT if codes have been requested too
    // often; sleep the required time and retry instead of bailing out.
    let token = {
        let mut flood_retries = 0u32;
        loop {
            match client.request_login_code(phone, &cfg.api_hash).await {
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
                            "auth: FLOOD_WAIT - sleeping {wait_secs}s before retrying \
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
    let code = code.trim();

    match client.sign_in(&token, code).await {
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
    let task_cfg = Arc::new(cfg.clone());
    let mut tasks = JoinSet::new();

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
                    warn!("chat {}: {label} - sleeping {secs}s", chat_cfg.chat_id);
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

        if msg.media().is_none() {
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

        let client = client.clone();
        let cfg = task_cfg.clone();
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
        tasks.spawn(async move {
            let _permit = permit;
            match download_media_inner(
                &client,
                &msg,
                &cfg,
                &file_ids,
                mp.as_ref(),
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
                        debug!("msg {}: interrupted - kept for resume", msg.id());
                    } else {
                        warn!("msg {}: download failed - {e:#}", msg.id());
                        stats.failed.fetch_add(1, Ordering::Relaxed);
                        live_retry.lock().await.insert(msg.id());
                    }
                }
            }
        });
        drain_finished_tasks(&mut tasks);
    }

    // On shutdown, abort in-flight tasks at once. Their `.part` files hold a
    // contiguous prefix already, so resume picks up cleanly next run.
    if shutdown.is_cancelled() {
        tasks.abort_all();
    }
    while let Some(result) = tasks.join_next().await {
        match result {
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
        "chat {}: scanned {} msg(s) - downloaded {}, skipped {}, failed {}, last_id={}{}",
        chat_cfg.chat_id,
        stats.scanned.load(Ordering::Relaxed),
        stats.downloaded.load(Ordering::Relaxed),
        stats.skipped.load(Ordering::Relaxed),
        stats.failed.load(Ordering::Relaxed),
        last_id,
        if completed { "" } else { " [interrupted]" }
    );

    Ok(ChatOutcome { completed, last_id })
}

fn drain_finished_tasks(tasks: &mut JoinSet<()>) {
    while let Some(result) = tasks.try_join_next() {
        match result {
            Ok(()) => {}
            Err(e) if e.is_cancelled() => {}
            Err(e) => warn!("download task panicked: {e}"),
        }
    }
}

/// Sleep for `dur`, returning early (`true`) if `shutdown` is tripped.
pub(crate) async fn sleep_cancellable(shutdown: &Shutdown, dur: Duration) -> bool {
    tokio::select! {
        _ = shutdown.cancelled() => true,
        _ = tokio::time::sleep(dur) => false,
    }
}

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
