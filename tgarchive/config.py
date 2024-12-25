""" Defining configuration types and loading configuration from file """
from __future__ import annotations

import dataclasses
import os
import pathlib
import sys
from typing import Any, FrozenSet, Optional, Type, Union

import yaml

# Use @dataclasses.dataclass(slots=True) after upgrading to python 3.10
# https://stackoverflow.com/a/69661861/1421036
dataclass_args = {} if sys.version_info < (3, 10) else {'slots': True}


@dataclasses.dataclass(**(dataclass_args | {'frozen': True}))
class ConfigFileProxyType:
    enable: bool = False
    protocol: Optional[str] = None
    addr: Optional[str] = None
    port: Optional[int] = None


@dataclasses.dataclass(**(dataclass_args | {'match_args': False}))
class Config:  # pylint: disable=too-many-instance-attributes
    group: str
    api_id: str = dataclasses.field(
        default_factory=lambda: os.getenv("TELEGRAM_API_ID"))
    api_hash: str = dataclasses.field(
        default_factory=lambda: os.getenv("TELEGRAM_API_HASH"))
    download_avatars: bool = True
    avatar_size: bool = True
    download_media: bool = True
    media_dir: pathlib.Path = pathlib.Path("media")
    media_tmp_dir: pathlib.Path = dataclasses.field(
        default_factory=lambda: pathlib.Path("media") / "tmp")
    media_mime_types: FrozenSet[str] = dataclasses.field(
        default_factory=frozenset,
        metadata={'constructor': frozenset},
    )
    proxy: ConfigFileProxyType = dataclasses.field(
        default_factory=ConfigFileProxyType)
    use_takeout: bool = False
    fetch_batch_size: int = 100
    fetch_wait: int = 5
    fetch_limit: Optional[int] = dataclasses.field(
        default=None,
        metadata={'constructor': int},
    )
    publish_rss_feed: bool = True
    rss_feed_entries: int = 100
    publish_dir: str = "site"
    site_url: str = "https://localhost"
    static_dir: str = "static"
    per_page: int = 1000
    show_day_index: bool = False
    show_sender_fullname: bool = False
    timezone: str = ""
    site_name: str = "@{group} (Telegram) archive"
    site_description: str = "Public archive of @{group} Telegram messages."
    meta_description: str = "@{group} {date} Telegram message archive."
    page_title: str = "{date} - @{group} Telegram message archive."
    html_template: str = "template.html.j2"
    rss_template: str = "rss_item_template.html.j2"
    db_path: str = "data.sqlite"

    def __init__(self, **kwargs) -> None:
        super(Config, self).__init__()
        fields = {k.name: k for k in dataclasses.fields(self)}
        for name in set(fields.keys()) - set(kwargs.keys()):
            field = fields[name]
            if field.default is not dataclasses.MISSING:
                default = field.default
            elif field.default_factory is not dataclasses.MISSING:
                default = field.default_factory()
            else:
                raise ValueError(f"Missing required field {name}")
            setattr(self, name, default)
        for name, raw_value in kwargs.items():
            option = fields[name]
            try:
                vtype = option.metadata['constructor']
            except (AttributeError, KeyError):
                vtype_raw = getattr(option, 'type', None)
                if (vtype_raw is None or
                        str(type(raw_value)) == f"<class '{vtype_raw}'>"):
                    setattr(self, name, raw_value)
                    continue
                vtype = self.constructor_from_name(vtype_raw) if isinstance(
                    vtype_raw, str) else vtype_raw
            setattr(self, name, vtype(raw_value))

    @staticmethod
    def constructor_from_name(type_name: str) -> Type[Any]:
        """ Get the named constructor """
        if '[' in type_name:
            type_name = type_name[:type_name.index('[')]
        if '.' in type_name:
            module, type_name = type_name.rsplit('.', 1)
            return getattr(sys.modules[module], type_name)
        try:
            return (__builtins__[type_name] if isinstance(__builtins__, dict)
                    else getattr(__builtins__, type_name))
        except (KeyError, AttributeError):
            return globals()[type_name]


def get_config(path: Union[pathlib.Path, str]) -> Config:
    with open(path, "r") as f:
        return Config(**yaml.safe_load(f))
