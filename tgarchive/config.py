""" Defining configuration types and loading configuration from file """
import os
import pathlib
from dataclasses import dataclass
from typing import FrozenSet, Optional

import yaml


@dataclass(slots=True, frozen=True)
class ConfigFileProxyType:
    enable: bool = False
    protocol: Optional[str] = None
    addr: Optional[str] = None
    port: Optional[int] = None


@dataclass(slots=True, frozen=True)
class Config:
    api_id: str = os.getenv("API_ID", "")
    api_hash: str = os.getenv("API_HASH", "")
    group: str = ""
    download_avatars: bool = True
    avatar_size: bool = True
    download_media: bool = True
    media_dir: pathlib.Path = pathlib.Path("media")
    media_tmp_dir: pathlib.Path = pathlib.Path("media") / "tmp"
    media_mime_types: FrozenSet[str] = frozenset()
    proxy: ConfigFileProxyType = ConfigFileProxyType(enable=False)
    use_takeout: bool = False
    fetch_batch_size: int = 100
    fetch_wait: int = 5
    fetch_limit: int = 0
    publish_rss_feed: bool = True
    rss_feed_entries: int = 100
    publish_dir: str = "site"
    site_url: str = "https://localhost"
    static_dir: pathlib.Path = pathlib.Path("static")
    telegram_url: str = "https://t.me/{id}"
    per_page: int = 1000
    show_sender_fullname: bool = False
    timezone: str = ""
    site_name: str = "@{group} (Telegram) archive"
    site_description: str = "Public archive of @{group} Telegram messages."
    meta_description: str = "@{group} {date} Telegram message archive."
    page_title: str = "{date} - @{group} Telegram message archive."


def get_config(path) -> Config:
    with open(path, "r") as f:
        return Config(**yaml.safe_load(f))
