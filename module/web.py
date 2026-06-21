"""web ui for media download"""

import json
import logging
import os

from flask import Flask, render_template, request

import utils
from module.app import Application
from module.download_stat import (
    DownloadState,
    _result_lock,
    get_download_result,
    get_download_state,
    get_total_download_speed,
    set_download_state,
)
from utils.format import format_byte

log = logging.getLogger("werkzeug")
log.setLevel(logging.ERROR)

_flask_app = Flask(__name__)


def get_flask_app() -> Flask:
    """get flask app instance"""
    return _flask_app


def run_web_server(app: Application):
    """Runs a web server using the Flask framework."""
    get_flask_app().run(
        app.web_host, app.web_port, debug=app.debug_web, use_reloader=False, threaded=True
    )


def init_web(app: Application):
    """Start web server in a background thread."""
    import threading

    if app.debug_web:
        threading.Thread(target=run_web_server, args=(app,)).start()
    else:
        threading.Thread(
            target=get_flask_app().run,
            daemon=True,
            args=(app.web_host, app.web_port),
            kwargs={"threaded": True},
        ).start()


@_flask_app.route("/")
def index():
    """Index html"""
    return render_template(
        "index.html",
        download_state=(
            "pause" if get_download_state() is DownloadState.Downloading else "continue"
        ),
    )


@_flask_app.route("/get_download_status")
def get_download_speed():
    """Get download speed"""
    return json.dumps(
        {
            "download_speed": format_byte(get_total_download_speed()) + "/s",
            "upload_speed": "0.00 B/s",
        }
    )


@_flask_app.route("/set_download_state", methods=["POST"])
def web_set_download_state():
    """Set download state"""
    state = request.args.get("state")

    if state == "continue" and get_download_state() is DownloadState.StopDownload:
        set_download_state(DownloadState.Downloading)
        return "pause"

    if state == "pause" and get_download_state() is DownloadState.Downloading:
        set_download_state(DownloadState.StopDownload)
        return "continue"

    return state


@_flask_app.route("/get_app_version")
def get_app_version():
    """Get telegram_media_downloader version"""
    return utils.__version__


@_flask_app.route("/get_download_list")
def get_download_list():
    """get download list"""
    already_down = request.args.get("already_down")
    if already_down is None:
        return "[]"
    already_down = already_down == "true"

    with _result_lock:
        # snapshot under lock: list of dicts avoids iteration-during-mutation crash
        raw = []
        for chat_id, messages in get_download_result().items():
            for idx, value in list(messages.items()):
                total = value["total_size"]
                if total <= 0:
                    continue  # unknown size, skip from both lists
                is_already_down = value["down_byte"] == total
                if already_down != is_already_down:
                    continue
                raw.append(
                    {
                        "chat": str(chat_id),
                        "id": str(idx),
                        "filename": os.path.basename(value["file_name"]),
                        "total_size": format_byte(total),
                        "download_progress": round(value["down_byte"] / total * 100, 1),
                        "download_speed": format_byte(value["download_speed"]) + "/s",
                        "save_path": value["file_name"].replace("\\", "/"),
                    }
                )

    return json.dumps(raw)
