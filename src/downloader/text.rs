use std::path::PathBuf;

use log::info;

use crate::config::Config;
use crate::format::validate_title;

pub(crate) async fn save_text_message(
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
