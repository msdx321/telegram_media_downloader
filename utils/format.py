"""util format"""

import os
import re
import unicodedata
from dataclasses import dataclass
from datetime import datetime


def format_byte(size: float, dot=2):
    """format byte"""

    if 0 <= size < 1:
        human_size = str(round(size / 0.125, dot)) + "b"
    else:
        for unit in ["B", "KB", "MB", "GB", "TB", "PB"]:
            if size < 1024:
                human_size = str(round(size, dot)) + unit
                break
            size /= 1024
        else:
            human_size = str(round(size, dot)) + "PB"
    return human_size


@dataclass
class SearchDateTimeResult:
    """search result for datetime"""
    value: str = ""
    right_str: str = ""
    left_str: str = ""
    match: bool = False


def get_date_time(text: str, fmt: str) -> SearchDateTimeResult:
    """Get first of date time,and split two part

    Parameters
    ----------
    text: str
        ready to search text

    Returns
    -------
    SearchDateTimeResult

    """
    res = SearchDateTimeResult()
    search_text = re.sub(r"\s+", " ", text)
    regex_list = [
        # 2013.8.15 22:46:21
        r"\d{4}[-/\.]{1}\d{1,2}[-/\.]{1}\d{1,2}[ ]{1,}\d{1,2}:\d{1,2}:\d{1,2}",
        # "2013.8.15 22:46"
        r"\d{4}[-/\.]{1}\d{1,2}[-/\.]{1}\d{1,2}[ ]{1,}\d{1,2}:\d{1,2}",
        # "2014.5.11"
        r"\d{4}[-/\.]{1}\d{1,2}[-/\.]{1}\d{1,2}",
        # "2014.5"
        r"\d{4}[-/\.]{1}\d{1,2}",
    ]

    format_list = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d",
        "%Y-%m",
    ]

    for i, value in enumerate(regex_list):
        search_res = re.search(value, search_text)
        if search_res:
            time_str = search_res.group(0)
            try:
                res.value = datetime.strptime(
                    time_str.replace("/", "-").replace(".", "-").strip(), format_list[i]
                ).strftime(fmt)
            except Exception:
                break
            if search_res.start() != 0:
                res.left_str = search_text[0 : search_res.start()]
            if search_res.end() + 1 <= len(search_text):
                res.right_str = search_text[search_res.end() :]
            res.match = True
            return res

    return res


def replace_date_time(text: str, fmt: str = "%Y-%m-%d %H:%M:%S") -> str:
    """Replace text all datetime to the right fmt

    Parameters
    ----------
    text: str
        ready to search text

    fmt: str
        the right datetime format

    Returns
    -------
    str
        The right format datetime str

    """

    if not text:
        return text
    res_str = ""
    res = get_date_time(text, fmt)
    if not res.match:
        return text
    if res.left_str:
        res_str += replace_date_time(res.left_str)
    res_str += res.value
    if res.right_str:
        res_str += replace_date_time(res.right_str)

    return res_str


_BYTE_UNIT = ["B", "KB", "MB", "GB", "TB"]


def get_byte_from_str(byte_str: str) -> int | None:
    """Get byte from str

    Parameters
    ----------
    byte_str: str
        Include byte str

    Returns
    -------
    int
        Byte
    """
    search_res = re.match(r"(\d{1,})(B|KB|MB|GB|TB)", byte_str)
    if search_res:
        unit_str = search_res.group(2)
        unit: int = 1
        for it in _BYTE_UNIT:
            if it == unit_str:
                break
            unit *= 1024

        return int(search_res.group(1)) * unit

    return None


def truncate_filename(path: str, limit: int = 230) -> str:
    """Truncate filename to the max len.

    Parameters
    ----------
    path: str
        File name path

    limit: int
        limit file name len(utf-8 byte)

    Returns
    -------
    str
        if file name len more than limit then return truncate filename or return filename

    """
    p, f = os.path.split(os.path.normpath(path))
    f, e = os.path.splitext(f)
    f_max = limit - len(e.encode("utf-8"))
    f = unicodedata.normalize("NFC", f)
    f_trunc = f.encode()[:f_max].decode("utf-8", errors="ignore")
    return os.path.join(p, f_trunc + e)


def validate_title(title: str) -> str:
    """Fix if title validation fails

    Parameters
    ----------
    title: str
        Chat title

    """

    r_str = r"[/\\:*?\"<>|\n]"  # '/ \ : * ? " < > |'
    new_title = re.sub(r_str, "_", title)
    return new_title
