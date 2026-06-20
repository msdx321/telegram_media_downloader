"""Application module"""

import asyncio
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from enum import Enum

from loguru import logger
from ruamel import yaml

from module.filter import Filter
from module.language import Language, set_language
from utils.format import replace_date_time, validate_title
from utils.meta_data import MetaData

_yaml = yaml.YAML()


class DownloadStatus(Enum):
    """Download status"""

    SkipDownload = 1
    SuccessDownload = 2
    FailedDownload = 3
    Downloading = 4


class TaskNode:
    """Task node — tracks download progress for a single chat."""

    def __init__(
        self,
        chat_id: int | str,
        download_filter: str | None = None,
        limit: int = 0,
        start_offset_id: int = 0,
        end_offset_id: int = 0,
        task_id: int = 0,
    ):
        self.chat_id = chat_id
        self.download_filter = download_filter
        self.limit = limit
        self.start_offset_id = start_offset_id
        self.end_offset_id = end_offset_id
        self.task_id = task_id
        self.total_task = 0
        self.total_download_task = 0
        self.failed_download_task = 0
        self.success_download_task = 0
        self.skip_download_task = 0
        self.total_download_byte = 0
        self.is_running: bool = False
        self.client = None
        self.is_stop_transmission = False
        self.download_status: dict = {}

    def skip_msg_id(self, msg_id: int):
        """Skip if message id out of range"""
        if self.start_offset_id and msg_id < self.start_offset_id:
            return True
        if self.end_offset_id and msg_id > self.end_offset_id:
            return True
        return False

    def is_finish(self):
        """If is finish"""
        return self.is_stop_transmission or (
            self.is_running
            and self.total_task == self.total_download_task
        )

    def stop_transmission(self):
        """Stop task"""
        self.is_stop_transmission = True

    def stat(self, status: DownloadStatus):
        """Update download statistics."""
        self.total_download_task += 1
        if status is DownloadStatus.SuccessDownload:
            self.success_download_task += 1
        elif status is DownloadStatus.SkipDownload:
            self.skip_download_task += 1
        else:
            self.failed_download_task += 1


class ChatDownloadConfig:
    """Chat Message Download Status"""

    def __init__(self):
        self.ids_to_retry_dict: dict = {}

        # need storage
        self.download_filter: str | None = None
        self.ids_to_retry: list = []
        self.last_read_message_id = 0
        self.total_task: int = 0
        self.finish_task: int = 0
        self.need_check: bool = False
        self.node: TaskNode = TaskNode(0)


def get_config(config, key, default=None, val_type=str, verbose=True):
    """
    Retrieves a configuration value from the given `config` dictionary
    based on the specified `key`.

    Args:
        config (dict): A dictionary containing the configuration values.
        key (str): The key of the configuration value to retrieve.
        default (Any, optional): The default value to be returned
            if the `key` is not found.
        val_type (type, optional): The data type of the configuration value.
        verbose (bool, optional): A flag indicating whether to print
            a warning message if the `key` is not found.

    Returns:
        The configuration value associated with the specified `key`,
         converted to the specified `type`. If the `key` is not found,
         the `default` value is returned.
    """
    val = config.get(key, default)
    if isinstance(val, val_type):
        return val

    if verbose:
        logger.warning(f"{key} is not {val_type.__name__}")

    return default


class Application:
    """Application load config and update config."""

    def __init__(
        self,
        config_file: str,
        app_data_file: str,
        application_name: str = "UndefineApp",
    ):
        """
        Init and update telegram media downloader config

        Parameters
        ----------
        config_file: str
            Config file name

        app_data_file: str
            App data file

        application_name: str
            Application Name

        """
        self.config_file: str = config_file
        self.app_data_file: str = app_data_file
        self.application_name: str = application_name
        self.download_filter = Filter()
        self.is_running = True

        self.total_download_task = 0

        self.chat_download_config: dict = {}

        self.save_path = os.path.join(os.path.abspath("."), "downloads")
        self.temp_save_path = os.path.join(os.path.abspath("."), "temp")
        self.api_id: str = ""
        self.api_hash: str = ""
        self._chat_id: str = ""
        self.media_types: list[str] = []
        self.file_formats: dict = {}
        self.proxy: dict = {}
        self.restart_program = False
        self.config: dict = {}
        self.app_data: dict = {}
        self.file_path_prefix: list[str] = ["chat_title", "media_datetime"]
        self.file_name_prefix: list[str] = ["message_id", "file_name"]
        self.file_name_prefix_split: str = " - "
        self.log_file_path = os.path.join(os.path.abspath("."), "log")
        self.session_file_path = os.path.join(os.path.abspath("."), "sessions")
        self.hide_file_name = False
        self.caption_name_dict: dict = {}
        self.caption_entities_dict: dict = {}
        self.max_concurrent_transmissions: int = 1
        self.web_host: str = "0.0.0.0"
        self.web_port: int = 5000
        self.max_download_task: int = 5
        self.language = Language.EN
        self.debug_web: bool = False
        self.log_level: str = "INFO"
        self.start_timeout: int = 60
        self.date_format: str = "%Y_%m"
        self.drop_no_audio_video: bool = False
        self.enable_download_txt: bool = False

        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)

        self.executor = ThreadPoolExecutor(
            min(32, (os.cpu_count() or 0) + 4), thread_name_prefix="multi_task"
        )

    def assign_config(self, _config: dict) -> bool:
        """assign config from str.

        Parameters
        ----------
        _config: dict
            application config dict

        Returns
        -------
        bool
        """
        # TODO: judge the storage if enough,and provide more path
        if _config.get("save_path") is not None:
            self.save_path = _config["save_path"]

        self.api_id = _config["api_id"]
        self.api_hash = _config["api_hash"]

        self.media_types = _config["media_types"]
        self.file_formats = _config["file_formats"]

        self.hide_file_name = _config.get("hide_file_name", False)

        # option
        if _config.get("proxy"):
            self.proxy = _config["proxy"]
        if _config.get("restart_program"):
            self.restart_program = _config["restart_program"]
        if _config.get("file_path_prefix"):
            self.file_path_prefix = _config["file_path_prefix"]
        if _config.get("file_name_prefix"):
            self.file_name_prefix = _config["file_name_prefix"]

        self.file_name_prefix_split = _config.get(
            "file_name_prefix_split", self.file_name_prefix_split
        )
        self.web_host = _config.get("web_host", self.web_host)
        self.web_port = _config.get("web_port", self.web_port)

        # TODO: add check if expression exist syntax error

        self.max_download_task = _config.get("max_download_task", self.max_download_task)

        self.max_concurrent_transmissions = self.max_download_task * 5

        self.max_concurrent_transmissions = _config.get(
            "max_concurrent_transmissions", self.max_concurrent_transmissions
        )

        language = _config.get("language", "EN")

        try:
            self.language = Language[language.upper()]
        except KeyError:
            pass

        self.debug_web = _config.get("debug_web", self.debug_web)
        self.log_level = _config.get("log_level", self.log_level)

        self.start_timeout = get_config(_config, "start_timeout", self.start_timeout, int)

        self.date_format = get_config(
            _config,
            "date_format",
            self.date_format,
            str,
        )

        self.drop_no_audio_video = get_config(
            _config, "drop_no_audio_video", self.drop_no_audio_video, bool
        )

        self.enable_download_txt = get_config(
            _config, "enable_download_txt", self.enable_download_txt, bool
        )

        try:
            date = datetime(2023, 10, 31)
            date.strftime(self.date_format)
        except Exception as e:
            logger.warning(f"config date format error: {e}")
            self.date_format = "%Y_%m"

        if _config.get("chat"):
            chat = _config["chat"]
            for item in chat:
                if "chat_id" in item:
                    self.chat_download_config[item["chat_id"]] = ChatDownloadConfig()
                    self.chat_download_config[item["chat_id"]].last_read_message_id = item.get(
                        "last_read_message_id", 0
                    )
                    self.chat_download_config[item["chat_id"]].download_filter = item.get(
                        "download_filter", ""
                    )
        elif _config.get("chat_id"):
            # Compatible with lower versions
            self._chat_id = _config["chat_id"]

            self.chat_download_config[self._chat_id] = ChatDownloadConfig()

            if _config.get("ids_to_retry"):
                self.chat_download_config[self._chat_id].ids_to_retry = _config["ids_to_retry"]
                for it in self.chat_download_config[self._chat_id].ids_to_retry:
                    self.chat_download_config[self._chat_id].ids_to_retry_dict[it] = True

            self.chat_download_config[self._chat_id].last_read_message_id = _config[
                "last_read_message_id"
            ]
            download_filter_dict = _config.get("download_filter")

            self.config["chat"] = [
                {
                    "chat_id": self._chat_id,
                    "last_read_message_id": self.chat_download_config[
                        self._chat_id
                    ].last_read_message_id,
                }
            ]

            if download_filter_dict and self._chat_id in download_filter_dict:
                self.chat_download_config[self._chat_id].download_filter = download_filter_dict[
                    self._chat_id
                ]
                self.config["chat"][0]["download_filter"] = download_filter_dict[self._chat_id]

        for key, value in self.chat_download_config.items():
            self.chat_download_config[key].download_filter = replace_date_time(
                value.download_filter
            )

        return True

    def assign_app_data(self, app_data: dict) -> bool:
        """Assign config from str.

        Parameters
        ----------
        app_data: dict
            application data dict

        Returns
        -------
        bool
        """
        if app_data.get("ids_to_retry"):
            if self._chat_id:
                self.chat_download_config[self._chat_id].ids_to_retry = app_data["ids_to_retry"]
                for it in self.chat_download_config[self._chat_id].ids_to_retry:
                    self.chat_download_config[self._chat_id].ids_to_retry_dict[it] = True
                self.app_data.pop("ids_to_retry")
        else:
            if app_data.get("chat"):
                chats = app_data["chat"]
                for chat in chats:
                    if "chat_id" in chat and chat["chat_id"] in self.chat_download_config:
                        chat_id = chat["chat_id"]
                        self.chat_download_config[chat_id].ids_to_retry = chat.get(
                            "ids_to_retry", []
                        )
                        for it in self.chat_download_config[chat_id].ids_to_retry:
                            self.chat_download_config[chat_id].ids_to_retry_dict[it] = True
        return True

    def get_file_save_path(self, media_type: str, chat_title: str, media_datetime: str) -> str:
        """Get file save path prefix.

        Parameters
        ----------
        media_type: str
            see config.yaml media_types

        chat_title: str
            see channel or group title

        media_datetime: str
            media datetime

        Returns
        -------
        str
            file save path prefix
        """

        res: str = self.save_path
        for prefix in self.file_path_prefix:
            if prefix == "chat_title":
                res = os.path.join(res, chat_title)
            elif prefix == "media_datetime":
                res = os.path.join(res, media_datetime)
            elif prefix == "media_type":
                res = os.path.join(res, media_type)
        return res

    def get_file_name(self, message_id: int, file_name: str | None, caption: str | None) -> str:
        """Get file save path prefix.

        Parameters
        ----------
        message_id: int
            Message id

        file_name: Optional[str]
            File name

        caption: Optional[str]
            Message caption

        Returns
        -------
        str
            File name
        """

        res: str = ""
        for prefix in self.file_name_prefix:
            if prefix == "message_id":
                if res != "":
                    res += self.file_name_prefix_split
                res += f"{message_id}"
            elif prefix == "file_name" and file_name:
                if res != "":
                    res += self.file_name_prefix_split
                res += f"{file_name}"
            elif prefix == "caption" and caption:
                if res != "":
                    res += self.file_name_prefix_split
                res += f"{caption}"
        if res == "":
            res = f"{message_id}"

        return validate_title(res)

    def need_skip_message(self, download_config: ChatDownloadConfig, message_id: int) -> bool:
        """if need skip download message.

        Parameters
        ----------
        chat_id: str
            Config.yaml defined

        message_id: int
            Readily to download message id
        Returns
        -------
        bool
        """
        if message_id in download_config.ids_to_retry_dict:
            return True

        return False

    def exec_filter(self, download_config: ChatDownloadConfig, meta_data: MetaData):
        """
        Executes the filter on the given download configuration.

        Args:
            download_config (ChatDownloadConfig): The download configuration object.
            meta_data (MetaData): The meta data object.

        Returns:
            bool: The result of executing the filter.
        """
        if download_config.download_filter:
            self.download_filter.set_meta_data(meta_data)
            return self.download_filter.exec(download_config.download_filter)

        return True

    def update_config(self, immediate: bool = True):
        """update config

        Parameters
        ----------
        immediate: bool
            If update config immediate,default True
        """
        # TODO: fix this not exist chat
        if not self.app_data.get("chat") and self.config.get("chat"):
            self.app_data["chat"] = [{"chat_id": i} for i in range(len(self.config["chat"]))]
        idx = 0
        for key, value in self.chat_download_config.items():
            unfinished_ids = set(value.ids_to_retry)

            for it in value.ids_to_retry:
                if value.node.download_status.get(it, DownloadStatus.FailedDownload) in [
                    DownloadStatus.SuccessDownload,
                    DownloadStatus.SkipDownload,
                ]:
                    unfinished_ids.remove(it)

            for _idx, _value in value.node.download_status.items():
                if _value not in (
                    DownloadStatus.SuccessDownload,
                    DownloadStatus.SkipDownload,
                ):
                    unfinished_ids.add(_idx)

            self.chat_download_config[key].ids_to_retry = list(unfinished_ids)

            if idx >= len(self.app_data["chat"]):
                self.app_data["chat"].append({})

            if value.finish_task:
                self.config["chat"][idx]["last_read_message_id"] = value.last_read_message_id + 1

            self.app_data["chat"][idx]["chat_id"] = key
            self.app_data["chat"][idx]["ids_to_retry"] = value.ids_to_retry
            idx += 1

        self.config["save_path"] = self.save_path
        self.config["file_path_prefix"] = self.file_path_prefix

        if self.config.get("ids_to_retry"):
            self.config.pop("ids_to_retry")

        if self.config.get("chat_id"):
            self.config.pop("chat_id")

        if self.config.get("download_filter"):
            self.config.pop("download_filter")

        if self.config.get("last_read_message_id"):
            self.config.pop("last_read_message_id")

        self.config["language"] = self.language.name

        if immediate:
            with open(self.config_file, "w", encoding="utf-8") as yaml_file:
                _yaml.dump(self.config, yaml_file)

        if immediate:
            with open(self.app_data_file, "w", encoding="utf-8") as yaml_file:
                _yaml.dump(self.app_data, yaml_file)

    def set_language(self, language: Language):
        """Set Language"""
        self.language = language
        set_language(language)

    def load_config(self):
        """Load user config"""
        with open(os.path.join(os.path.abspath("."), self.config_file), encoding="utf-8") as f:
            config = _yaml.load(f.read())
            if config:
                self.config = config
                self.assign_config(self.config)

        if os.path.exists(os.path.join(os.path.abspath("."), self.app_data_file)):
            with open(
                os.path.join(os.path.abspath("."), self.app_data_file),
                encoding="utf-8",
            ) as f:
                app_data = _yaml.load(f.read())
                if app_data:
                    self.app_data = app_data
                    self.assign_app_data(self.app_data)

    def pre_run(self):
        """before run application do"""
        if not os.path.exists(self.session_file_path):
            os.makedirs(self.session_file_path)
        set_language(self.language)

    def set_caption_name(self, chat_id: int | str, media_group_id: int | str | None, caption: str):
        """set caption name map

        Parameters
        ----------
        chat_id: str
            Unique identifier for this chat.

        media_group_id: Optional[str]
            The unique identifier of a media message group this message belongs to.

        caption: str
            Caption for the audio, document, photo, video or voice, 0-1024 characters.
        """
        if not media_group_id:
            return

        if chat_id in self.caption_name_dict:
            self.caption_name_dict[chat_id][media_group_id] = caption
        else:
            self.caption_name_dict[chat_id] = {media_group_id: caption}

    def get_caption_name(self, chat_id: int | str, media_group_id: int | str | None) -> str | None:
        """set caption name map
                media_group_id: Optional[str]
            The unique identifier of a media message group this message belongs to.

        caption: str
            Caption for the audio, document, photo, video or voice, 0-1024 characters.
        """

        if (
            not media_group_id
            or chat_id not in self.caption_name_dict
            or media_group_id not in self.caption_name_dict[chat_id]
        ):
            return None

        return str(self.caption_name_dict[chat_id][media_group_id])

    def set_caption_entities(
        self, chat_id: int | str, media_group_id: int | str | None, caption_entities
    ):
        """
        set caption entities map
        """
        if not media_group_id:
            return

        if chat_id in self.caption_entities_dict:
            self.caption_entities_dict[chat_id][media_group_id] = caption_entities
        else:
            self.caption_entities_dict[chat_id] = {media_group_id: caption_entities}

    def get_caption_entities(self, chat_id: int | str, media_group_id: int | str | None):
        """
        get caption entities map
        """
        if (
            not media_group_id
            or chat_id not in self.caption_entities_dict
            or media_group_id not in self.caption_entities_dict[chat_id]
        ):
            return None

        return self.caption_entities_dict[chat_id][media_group_id]

    def set_download_id(self, node: TaskNode, message_id: int, download_status: DownloadStatus):
        """Set Download status"""
        if download_status is DownloadStatus.SuccessDownload:
            self.total_download_task += 1

        if node.chat_id not in self.chat_download_config:
            return

        self.chat_download_config[node.chat_id].finish_task += 1

        self.chat_download_config[node.chat_id].last_read_message_id = max(
            self.chat_download_config[node.chat_id].last_read_message_id, message_id
        )
