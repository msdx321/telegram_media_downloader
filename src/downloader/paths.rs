use std::path::PathBuf;

use grammers_client::media::Media;

use crate::config::Config;
use crate::format::{truncate_filename, validate_title};

use super::metadata::media_kind_and_ext;

/// Returns (temp_path, final_path) for a media download.
pub(super) fn build_media_paths(
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

    let Some((media_type_str, ext)) = media_kind_and_ext(media) else {
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

    let stem = match media {
        Media::Photo(_) => msg.id().to_string(),
        Media::Document(doc) => {
            let mut stem = doc
                .name()
                .map(|name| name.rsplit_once('.').map_or(name, |(stem, _)| stem))
                .unwrap_or_default()
                .to_string();
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
            if parts.is_empty() {
                msg.id().to_string()
            } else {
                parts.join(sep)
            }
        }
        _ => return Err(anyhow::anyhow!("unsupported media")),
    };

    let fname = format!("{stem}.{ext}");
    let final_path = truncate_filename(&dir.join(&fname), 230);

    let temp_path = final_path.with_extension(format!("{ext}.part"));

    Ok((temp_path, final_path))
}
