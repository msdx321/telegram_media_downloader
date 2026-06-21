mod config;
mod filter;
mod format;
mod webui;

use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use std::time::{Duration, Instant};

use anyhow::Context;
use grammers_client::media::Media;
use grammers_client::{Client, SignInError};
use grammers_mtsender::SenderPool;
use grammers_session::storages::SqliteSession;
use log::{error, info, warn};
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use tokio::sync::mpsc::unbounded_channel;
use tokio::sync::{Mutex, Semaphore};

use crate::config::{
    load_app_data, load_config, save_app_data, save_config, ChatConfig, ChatData, Config,
};
use crate::filter::{Parser, Value};
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

#[cfg(unix)]
const SIGINT: i32 = 2;

#[cfg(unix)]
const SIG_DFL: usize = 0;

#[cfg(unix)]
unsafe extern "C" {
    fn signal(signum: i32, handler: usize) -> usize;
}

#[cfg(target_os = "linux")]
unsafe extern "C" {
    // Returns 0 on success, errno on failure. Used to preallocate the download
    // file so the NAS doesn't grow it extent-by-extent during chunked writes.
    fn posix_fallocate(fd: i32, offset: i64, len: i64) -> i32;
}

/// Reset SIGINT to its default disposition so Ctrl+C terminates the process
/// immediately instead of going through a graceful-shutdown path.
#[cfg(unix)]
fn reset_sigint_to_default() {
    unsafe {
        signal(SIGINT, SIG_DFL);
    }
}

#[cfg(not(unix))]
fn reset_sigint_to_default() {}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    reset_sigint_to_default();

    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .filter_module("grammers_mtsender", log::LevelFilter::Warn)
        .filter_module("grammers_mtproto", log::LevelFilter::Warn)
        .filter_module("grammers_client", log::LevelFilter::Warn)
        .filter_module("tracing::span", log::LevelFilter::Warn)
        .init();
    info!("Telegram Media Downloader (Rust) — starting");

    let cfg = load_config(CONFIG_FILE)?;
    let web_state = Arc::new(WebState::new());
    tokio::spawn(webui::run(
        web_state.clone(),
        cfg.web_host.clone(),
        cfg.web_port,
    ));

    run_downloader(cfg, web_state).await
}

async fn run_downloader(mut cfg: Config, web_state: Arc<WebState>) -> anyhow::Result<()> {
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
    info!("concurrency: {concurrency} parallel downloads");
    let mp = Arc::new(MultiProgress::new());
    let runtime = DownloadRuntime {
        file_ids,
        dl_sem: dl_semaphore,
        mp,
        web_state: web_state.clone(),
    };
    loop {
        run_check_cycle(&client, &mut cfg, &runtime, &mut data, &mut data_chats).await?;
        web_state
            .set_status(&format!(
                "waiting {}s before next check",
                cfg.check_interval_secs
            ))
            .await;
        info!(
            "cycle complete; sleeping {}s before next check",
            cfg.check_interval_secs
        );
        tokio::time::sleep(Duration::from_secs(cfg.check_interval_secs)).await;
        web_state.set_status("running").await;
    }
}

async fn run_check_cycle(
    client: &Client,
    cfg: &mut Config,
    runtime: &DownloadRuntime,
    data: &mut crate::config::AppData,
    data_chats: &mut HashMap<String, ChatData>,
) -> anyhow::Result<()> {
    let chats = cfg.chat.clone();
    for chat_cfg in &chats {
        let chat_id = chat_cfg.chat_id.clone();
        // Retry ids are persisted in data.yaml (data_chats), not in config.yaml.
        let ids_to_retry: Vec<i32> = data_chats
            .get(&chat_id)
            .map(|dc| dc.ids_to_retry.clone())
            .unwrap_or_default();

        match process_chat(client, cfg, chat_cfg, &ids_to_retry, runtime).await {
            Ok(progress) => {
                update_chat_state(cfg, chat_cfg, progress.last_id)?;
                let failed = progress.failed_ids;
                info!(
                    "chat {}: saved last_read={}, {} failed ids",
                    chat_id,
                    progress.last_id,
                    failed.len()
                );
                // Persist the new retry set back into data.yaml state.
                // Drop the entry when nothing is pending so data.yaml stays clean.
                if failed.is_empty() {
                    data_chats.remove(&chat_id);
                } else {
                    data_chats
                        .entry(chat_id.clone())
                        .and_modify(|dc| dc.ids_to_retry = failed.clone())
                        .or_insert_with(|| ChatData {
                            chat_id,
                            ids_to_retry: failed,
                        });
                }
            }
            Err(e) => {
                error!("chat {chat_id}: {e:#}");
            }
        }
    }

    data.downloaded_file_ids = runtime.file_ids.lock().await.iter().cloned().collect();
    data.chat = data_chats.values().cloned().collect();
    save_app_data(DATA_FILE, data)?;
    Ok(())
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

struct ChatProgress {
    last_id: i32,
    failed_ids: Vec<i32>,
}

struct DownloadRuntime {
    file_ids: Arc<Mutex<HashSet<String>>>,
    dl_sem: Arc<Semaphore>,
    mp: Arc<MultiProgress>,
    web_state: Arc<WebState>,
}

async fn process_chat(
    client: &Client,
    cfg: &Config,
    chat_cfg: &ChatConfig,
    ids_to_retry: &[i32],
    runtime: &DownloadRuntime,
) -> anyhow::Result<ChatProgress> {
    let failed_ids = Arc::new(Mutex::new(HashSet::new()));
    let mut last_id = chat_cfg.last_read_message_id;
    let mut retry_ids: HashSet<i32> = ids_to_retry.iter().copied().collect();

    info!(
        "chat {}: beginning scan (from msg {})",
        chat_cfg.chat_id, last_id
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
    info!("chat {}: ~{total} new messages available", chat_cfg.chat_id);

    // If there are ids_to_retry, also fetch those specific messages first
    if !ids_to_retry.is_empty() {
        info!(
            "chat {}: retrying {} failed messages from previous run",
            chat_cfg.chat_id,
            ids_to_retry.len()
        );
        // grammers doesn't have a convenient get_messages by IDs; we'll catch
        // them during the main scan instead by not filtering them out.
    }

    let filter_fn = build_filter_fn(chat_cfg, cfg);
    let mut msg_count: u64 = 0;
    let mut tasks = Vec::new();

    loop {
        runtime.web_state.wait_if_paused().await;
        let msg = match messages.next().await {
            Ok(Some(msg)) => msg,
            Ok(None) => break,
            Err(e) => {
                let err_str = e.to_string();
                if let Some(wait_secs) = flood_wait_secs(&err_str) {
                    warn!(
                        "chat {}: FLOOD_WAIT — sleeping {wait_secs}s",
                        chat_cfg.chat_id
                    );
                    tokio::time::sleep(Duration::from_secs(wait_secs)).await;
                } else {
                    warn!(
                        "chat {}: message iterator error: {err_str}",
                        chat_cfg.chat_id
                    );
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
                continue;
            }
        };

        msg_count += 1;
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
            // Download as .txt file
            let cfg = cfg.clone();
            let msg_clone = msg.clone();
            let failed_ids = failed_ids.clone();
            let permit = runtime.dl_sem.clone().acquire_owned().await?;
            tasks.push(tokio::spawn(async move {
                let _permit = permit;
                if let Err(e) = save_text_message(&msg_clone, &cfg).await {
                    warn!("txt msg {}: {e:#}", msg_clone.id());
                    failed_ids.lock().await.insert(msg_clone.id());
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
            continue;
        }

        if !filter_fn(&msg) {
            continue;
        }

        // Spawn download task
        let client = client.clone();
        let cfg = cfg.clone();
        let file_ids = runtime.file_ids.clone();
        let failed_ids = failed_ids.clone();
        let permit = runtime.dl_sem.clone().acquire_owned().await?;

        let mp = runtime.mp.clone();
        let web_state = runtime.web_state.clone();
        tasks.push(tokio::spawn(async move {
            let _permit = permit;
            let _mp = mp;
            match download_media_inner(&client, &msg, &cfg, &file_ids, _mp.as_ref(), &web_state)
                .await
            {
                Ok(true) => info!("msg {}: downloaded successfully", msg.id()),
                Ok(false) => {} // skipped
                Err(e) => {
                    warn!("msg {}: download failed — {e:#}", msg.id());
                    failed_ids.lock().await.insert(msg.id());
                }
            }
            // NB: dropped _permit here releases concurrency slot
        }));
    }

    for task in tasks {
        if let Err(e) = task.await {
            warn!("download task join failed: {e}");
        }
    }

    info!(
        "chat {}: scanned {msg_count} messages, last_id={last_id}",
        chat_cfg.chat_id
    );

    let mut failed_ids: Vec<i32> = failed_ids.lock().await.iter().copied().collect();
    failed_ids.sort_unstable();
    Ok(ChatProgress {
        last_id,
        failed_ids,
    })
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
    if file_path.exists() {
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

    Box::new(move |msg| {
        let vars = build_meta_vars(msg);
        let mut parser = Parser::new(&filter_str);
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

fn build_meta_vars(msg: &grammers_client::message::Message) -> HashMap<String, Value> {
    let mut vars = HashMap::new();

    let date = msg.date().naive_utc();
    vars.insert("message_date".into(), Value::DateTime(date));
    vars.insert("message_id".into(), Value::Int(msg.id() as i64));
    vars.insert("id".into(), Value::Int(msg.id() as i64));

    // Seed all filter-relevant fields with defaults so expressions like
    // "media_duration >= 60" work for photos (return false) instead of
    // failing with "undefined variable".
    vars.insert("message_caption".into(), Value::Str(String::new()));
    vars.insert("caption".into(), Value::Str(String::new()));
    vars.insert("sender_name".into(), Value::Str(String::new()));
    vars.insert("sender_id".into(), Value::Int(0));
    vars.insert("reply_to_message_id".into(), Value::Int(0));
    vars.insert("message_thread_id".into(), Value::Int(0));
    vars.insert("topic_id".into(), Value::Int(0));
    vars.insert("media_type".into(), Value::Str(String::new()));
    vars.insert("file_extension".into(), Value::Str(String::new()));
    vars.insert("media_file_name".into(), Value::Str(String::new()));
    vars.insert("file_name".into(), Value::Str(String::new()));
    vars.insert("media_file_size".into(), Value::Int(0));
    vars.insert("file_size".into(), Value::Int(0));
    vars.insert("media_duration".into(), Value::Int(0));
    vars.insert("media_width".into(), Value::Int(0));
    vars.insert("media_height".into(), Value::Int(0));

    let txt = msg.text();
    if !txt.is_empty() {
        vars.insert("message_caption".into(), Value::Str(txt.to_string()));
        vars.insert("caption".into(), Value::Str(txt.to_string()));
    }

    if let Some(p) = msg.peer() {
        if let Some(name) = p.name() {
            vars.insert("sender_name".into(), Value::Str(name.to_string()));
        }
    }

    if let Some(rid) = msg.reply_to_message_id() {
        vars.insert("reply_to_message_id".into(), Value::Int(rid as i64));
    }

    if let Some(ref media) = msg.media() {
        match media {
            Media::Photo(photo) => {
                vars.insert("media_type".into(), Value::Str("photo".into()));
                vars.insert("file_extension".into(), Value::Str("jpg".into()));
                if let Some(s) = photo.size() {
                    vars.insert("media_file_size".into(), Value::Int(s as i64));
                    vars.insert("file_size".into(), Value::Int(s as i64));
                }
            }
            Media::Document(doc) => {
                let mime = doc.mime_type().unwrap_or("");
                let ext = mime_to_ext(mime);
                vars.insert("media_type".into(), Value::Str("document".into()));
                vars.insert("file_extension".into(), Value::Str(ext.to_string()));

                if let Some(name) = doc.name() {
                    vars.insert("media_file_name".into(), Value::Str(name.to_string()));
                    vars.insert("file_name".into(), Value::Str(name.to_string()));
                }
                if let Some(sz) = doc.size() {
                    vars.insert("media_file_size".into(), Value::Int(sz as i64));
                    vars.insert("file_size".into(), Value::Int(sz as i64));
                }
                if let Some(dur) = doc.duration() {
                    vars.insert("media_duration".into(), Value::Int(dur as i64));
                }
                if let Some(res) = doc.resolution() {
                    vars.insert("media_width".into(), Value::Int(res.0 as i64));
                    vars.insert("media_height".into(), Value::Int(res.1 as i64));
                }
            }
            _ => {}
        }
    }

    vars
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
            info!("msg={msg_id}: already downloaded (file_unique_id), skipped");
            return Ok(false);
        }
        drop(cache);
    }

    let (temp_path, final_path) = build_media_paths(msg, &media, cfg)?;

    // Already fully downloaded?
    if final_path.exists() {
        info!("msg={msg_id}: file already exists, skipped");
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

    let mut last_err = None;
    for attempt in 0..RETRY_LIMIT {
        if attempt > 0 {
            tokio::time::sleep(Duration::from_secs(RETRY_DELAY_SECS)).await;
            info!("msg={msg_id}: retry {}/{}", attempt + 1, RETRY_LIMIT);
        }

        // Re-evaluate the resume offset each attempt: a failed attempt leaves
        // the valid prefix on disk, so we only re-fetch what is missing.
        let downloaded = if attempt == 0 {
            existing
        } else {
            resume_offset(&temp_path, total).await?
        };

        let pb = mp.add(ProgressBar::new(total));
        pb.set_style(
            ProgressStyle::with_template(
                "{msg:>8} {wide_bar:.cyan/blue} {bytes:>8}/{total_bytes:8} {bytes_per_sec:>10} {eta}"
            )
            .unwrap_or_else(|_| ProgressStyle::default_bar())
            .progress_chars("##-"),
        );
        pb.set_message(format!("{msg_id}"));
        if downloaded > 0 {
            pb.set_position(downloaded);
            if attempt == 0 {
                info!("msg={msg_id}: resuming from {downloaded} bytes");
            }
        }

        if total == 0 {
            web_state.wait_if_paused().await;
            if let Err(e) = client.download_media(&media, &temp_path).await {
                last_err = Some(e.into());
                pb.finish_and_clear();
                drop(pb);
                tokio::fs::remove_file(&temp_path).await.ok();
                continue;
            }
            pb.set_position(total);
        } else if total > downloaded {
            let progress = DownloadProgress {
                pb: &pb,
                web_state,
                msg_id,
            };
            if let Err(e) =
                download_concurrent(client, &media, &temp_path, downloaded, total, &progress).await
            {
                // Keep the partial file so the next attempt resumes; only the
                // unfetched chunks need re-downloading.
                warn!("msg={msg_id}: {e:#}");
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
            tokio::fs::remove_file(&temp_path).await.ok();
            last_err = Some(anyhow::anyhow!("size mismatch"));
            continue;
        }

        // Move temp -> final
        tokio::fs::create_dir_all(final_path.parent().unwrap_or(Path::new("."))).await?;
        tokio::fs::rename(&temp_path, &final_path).await?;

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
            cache.insert(fid);
        }

        info!(
            "msg={msg_id}: {} -> {}",
            format_byte(actual as f64),
            final_path.display()
        );
        web_state.download_finished(msg_id, actual, true).await;
        return Ok(true);
    }

    tokio::fs::remove_file(&temp_path).await.ok();
    web_state.download_finished(msg_id, 0, false).await;
    Err(last_err.unwrap_or_else(|| anyhow::anyhow!("download failed")))
}

struct DownloadProgress<'a> {
    pb: &'a ProgressBar,
    web_state: &'a Arc<WebState>,
    msg_id: i32,
}

async fn download_concurrent(
    client: &Client,
    media: &Media,
    path: &Path,
    start: u64,
    total: u64,
    progress: &DownloadProgress<'_>,
) -> anyhow::Result<()> {
    let chunk_size = DOWNLOAD_CHUNK_SIZE;
    let start_chunk = start / chunk_size;
    let total_chunks = total.div_ceil(chunk_size);
    let workers = RESUME_WORKERS.min(total_chunks - start_chunk).max(1);

    let (tx, mut rx) = unbounded_channel::<(u64, Vec<u8>)>();
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

        tasks.push(tokio::spawn(async move {
            // Striped assignment: this worker owns chunks worker, worker+n, ...
            // Each chunk is fetched with its own stream so a short read (which
            // ends grammers' stream early) only affects that one piece.
            let mut idx = start_chunk + worker;
            while idx < total_chunks {
                if abort.load(Ordering::Relaxed) {
                    break;
                }
                web_state.wait_if_paused().await;
                let offset = idx * chunk_size;
                let expected = (total - offset).min(chunk_size);
                match fetch_chunk(&client, &media, idx, expected).await {
                    Ok(chunk) => {
                        if tx.send((offset, chunk)).is_err() {
                            break; // receiver gone
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
    let started = Instant::now();
    let mut pending: BTreeMap<u64, Vec<u8>> = BTreeMap::new();
    while let Some((offset, chunk)) = rx.recv().await {
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
        }
    }

    // Surface the first worker error (a chunk that exhausted its retries).
    let mut worker_err: Option<anyhow::Error> = None;
    for task in tasks {
        match task.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) if worker_err.is_none() => worker_err = Some(e),
            Ok(Err(_)) => {}
            Err(e) if worker_err.is_none() => {
                worker_err = Some(anyhow::anyhow!("download worker join failed: {e}"))
            }
            Err(_) => {}
        }
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
async fn fetch_chunk(
    client: &Client,
    media: &Media,
    idx: u64,
    expected: u64,
) -> anyhow::Result<Vec<u8>> {
    let mut backoff = 0u64;
    let mut last_err: Option<anyhow::Error> = None;
    for attempt in 0..CHUNK_RETRY_LIMIT {
        let delay = std::mem::take(&mut backoff);
        if delay > 0 {
            tokio::time::sleep(Duration::from_secs(delay)).await;
        }
        let mut stream = client
            .iter_download(media)
            .chunk_size(DOWNLOAD_CHUNK_SIZE as i32)
            .skip_chunks(i32::try_from(idx)?);
        match stream.next().await {
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
    Err(last_err.unwrap_or_else(|| anyhow::anyhow!("chunk {idx} failed after retries")))
}

/// Resume offset for `temp_path`: the largest chunk-aligned length that does
/// not exceed the current file size (any partial trailing chunk is trimmed).
/// Returns 0 (and clears the file) if the temp is missing or larger than
/// `total`, signalling a fresh download.
async fn resume_offset(temp_path: &Path, total: u64) -> anyhow::Result<u64> {
    let len = match tokio::fs::metadata(temp_path).await {
        Ok(m) => m.len(),
        Err(_) => return Ok(0),
    };
    if total > 0 && len > total {
        tokio::fs::remove_file(temp_path).await.ok();
        return Ok(0);
    }
    let aligned = len - (len % DOWNLOAD_CHUNK_SIZE);
    if aligned != len {
        let file = tokio::fs::OpenOptions::new()
            .write(true)
            .open(temp_path)
            .await?;
        file.set_len(aligned).await?;
    }
    Ok(aligned)
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
