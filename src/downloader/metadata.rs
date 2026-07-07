use grammers_client::media::Media;

use crate::config::Config;

pub(crate) fn media_type_value(msg: &grammers_client::message::Message) -> String {
    match msg.media() {
        Some(Media::Photo(_)) => "photo".into(),
        Some(Media::Document(_)) => "document".into(),
        _ => String::new(),
    }
}

pub(crate) fn file_extension_value(msg: &grammers_client::message::Message) -> String {
    match msg.media() {
        Some(Media::Photo(_)) => "jpg".into(),
        Some(Media::Document(doc)) => mime_to_ext(doc.mime_type().unwrap_or("")).into(),
        _ => String::new(),
    }
}

pub(crate) fn media_file_name_value(msg: &grammers_client::message::Message) -> String {
    match msg.media() {
        Some(Media::Document(doc)) => doc.name().map(str::to_string).unwrap_or_default(),
        _ => String::new(),
    }
}

pub(crate) fn media_file_size_value(msg: &grammers_client::message::Message) -> i64 {
    match msg.media() {
        Some(Media::Photo(photo)) => photo.size().unwrap_or(0) as i64,
        Some(Media::Document(doc)) => doc.size().unwrap_or(0) as i64,
        _ => 0,
    }
}

pub(crate) fn media_duration_value(msg: &grammers_client::message::Message) -> i64 {
    match msg.media() {
        Some(Media::Document(doc)) => doc.duration().unwrap_or(0.0) as i64,
        _ => 0,
    }
}

pub(crate) fn media_resolution_value(msg: &grammers_client::message::Message) -> (i64, i64) {
    match msg.media() {
        Some(Media::Document(doc)) => doc
            .resolution()
            .map(|(width, height)| (width as i64, height as i64))
            .unwrap_or((0, 0)),
        _ => (0, 0),
    }
}

pub(crate) fn mime_to_ext(mime: &str) -> &str {
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

pub(crate) fn media_kind_and_ext(media: &Media) -> Option<(&str, String)> {
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

pub(crate) fn media_matches_config(media: &Media, cfg: &Config) -> bool {
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
