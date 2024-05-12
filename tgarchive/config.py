import os
from typing import TypedDict

import yaml

ConfigFileProxyType = TypedDict(
    'ConfigFileProxyType', {
        'enable': bool,
        'protocol': str,
        'addr': str,
        'port': int,
    },
    total=False)

ConfigFileType = TypedDict(
    'ConfigFileType', {
        'api_id': str,
        'api_hash': str,
        'group': str,
        'download_avatars': bool,
        'avatar_size': list[int],
        'download_media': bool,
        'media_dir': str,
        'media_mime_types': list[str],
        'proxy': dict,
        'fetch_batch_size': int,
        'fetch_wait': int,
        'fetch_limit': int,
        'publish_rss_feed': bool,
        'rss_feed_entries': int,
        'publish_dir': str,
        'site_url': str,
        'static_dir': str,
        'telegram_url': str,
        'per_page': int,
        'show_sender_fullname': bool,
        'timezone': str,
        'site_name': str,
        'site_description': str,
        'meta_description': str,
        'page_title': str,
    })

_CONFIG_DEFAULTS: ConfigFileType = {
    "api_id": os.getenv("API_ID", ""),
    "api_hash": os.getenv("API_HASH", ""),
    "group": "",
    "download_avatars": True,
    "avatar_size": [64, 64],
    "download_media": True,
    "media_dir": "media",
    "media_mime_types": [],
    "proxy": {
        "enable": False,
    },
    "fetch_batch_size": 2000,
    "fetch_wait": 5,
    "fetch_limit": 0,
    "publish_rss_feed": True,
    "rss_feed_entries": 100,
    "publish_dir": "site",
    "site_url": "https://mysite.com",
    "static_dir": "static",
    "telegram_url": "https://t.me/{id}",
    "per_page": 1000,
    "show_sender_fullname": False,
    "timezone": "",
    "site_name": "@{group} (Telegram) archive",
    "site_description": "Public archive of @{group} Telegram messages.",
    "meta_description": "@{group} {date} Telegram message archive.",
    "page_title": "{date} - @{group} Telegram message archive."
}


def get_config(path) -> ConfigFileType:
    with open(path, "r") as f:
        config: ConfigFileType = {
            **_CONFIG_DEFAULTS,
            **yaml.safe_load(f.read())
        }
    return config
