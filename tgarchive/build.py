""" Build the static site from the database """
import logging
import os
import pathlib
import re
import shutil
from collections import deque
from typing import Deque, Iterable, Union

import magic
import pkg_resources
from feedgen.feed import FeedGenerator
from jinja2 import Template

from .config import Config
from .db import DB, Message

_NL2BR = re.compile(r"\n\n+")


class Build:
    config: Config
    html_template: Template
    rss_template: Template
    db: DB
    site_path: pathlib.Path

    def __init__(self, config: Config, db: DB, symlink: bool,
                 site_path: pathlib.Path) -> None:
        self.config = config
        self.db = db
        self.symlink = symlink
        self.site_path = site_path

        self.rss_template: Template = None

        # Map of all message IDs across all months and the slug of the page
        # in which they occur (paginated), used to link replies to their
        # parent messages that may be on arbitrary pages.
        self.page_ids = {}
        self.timeline = dict()

    def build(self) -> None:
        # (Re)create the output directory.
        self._create_publish_dir()

        timeline = list(self.db.get_timeline())
        if not timeline:
            logging.info("no data found to publish site")
            quit()

        for month in timeline:
            if month.date.year not in self.timeline:
                self.timeline[month.date.year] = []
            self.timeline[month.date.year].append(month)

        # Queue to store the latest N items to publish in the RSS feed.
        rss_entries: Deque[Message] = deque([], self.config.rss_feed_entries)
        fname = None
        for month in timeline:
            # Get the days + message counts for the month.
            dayline = dict()
            per_page = self.config.per_page
            for d in self.db.get_dayline(month.date.year, month.date.month,
                                         per_page):
                dayline[d.slug] = d

            # Paginate and fetch messages for the month until the end..
            page = 0
            last_id = 0
            total = self.db.get_message_count(month.date.year, month.date.month)
            total_pages = -(-total // per_page
                           )  # faster math.ceil without import

            while True:
                messages = list(
                    self.db.get_messages(month.date.year, month.date.month,
                                         last_id, per_page))

                if not messages:
                    break

                last_id = messages[-1].id

                page += 1
                fname = self.make_filename(month, page)

                # Collect the message ID -> page name for all messages in the set
                # to link to replies in arbitrary positions across months, paginated pages.
                for m in messages:
                    self.page_ids[m.id] = fname

                if self.config.publish_rss_feed:
                    rss_entries.extend(messages)

                self._render_page(
                    fname, {
                        'messages': messages,
                        'month': month,
                        'dayline': dayline,
                        'pagination': {
                            "current": page,
                            "total": total_pages
                        },
                    })

        # The last page chronologically is the latest page. Make it index.
        publish_dir = self.site_path / self.config.publish_dir
        if fname:
            index_path = pathlib.Path(publish_dir) / "index.html"
            if index_path.exists():
                index_path.unlink()
            if self.symlink:
                index_path.symlink_to(fname)
            else:
                fname_full = pathlib.Path(publish_dir) / fname
                try:
                    fname_full.hardlink_to(index_path)
                except OSError:
                    shutil.copy(fname_full, index_path)

        # Generate RSS feeds.
        if self.config.publish_rss_feed:
            self._build_rss(rss_entries)

    def load_html_template(self, fname: pathlib.Path) -> None:
        with open(fname, "r") as f:
            self.html_template = Template(f.read(), autoescape=True)

    def load_rss_template(self, fname) -> None:
        with open(fname, "r") as f:
            self.rss_template = Template(f.read(), autoescape=True)

    def make_filename(self, month, page) -> str:
        fname = f'{month.slug}{"_" + str(page) if page > 1 else ""}.html'
        return fname

    def _render_page(self, fname: Union[str, pathlib.Path], data: dict) -> None:
        html = self.html_template.render(
            config=self.config,
            timeline=self.timeline,
            page_ids=self.page_ids,
            make_filename=self.make_filename,
            nl2br=self._nl2br,
            **data)

        with (self.site_path / self.config.publish_dir / fname).open(
                "w", encoding="utf8") as f:
            f.write(html)

    def _build_rss(self, messages: Iterable[Message]) -> None:
        f = FeedGenerator()
        f.id(self.config.site_url)
        f.generator("tg-archive " +
                    pkg_resources.get_distribution("tg-archive").version)
        f.link(href=self.config.site_url, rel="alternate")
        f.title(self.config.site_name.format(group=self.config.group))
        f.subtitle(self.config.site_description)

        for m in messages:
            url = f'{self.config.site_url}/{self.page_ids[m.id]}#{m.id}'
            e = f.add_entry()
            e.id(url)
            e.title(f'@{m.user.username} on {m.date} (#{m.id})')
            e.link({"href": url})
            e.published(m.date)

            media_mime = ""
            if m.media and m.media.url:
                media_mime = "application/octet-stream"
                media_size = 0

                if "://" in m.media.url:
                    media_mime = "text/html"
                    murl = m.media.url
                else:
                    media_path = self.config.media_dir / m.media.url
                    murl = f'{self.config.media_dir.name}/{m.media.url}'
                    try:  # pylint: disable=too-many-try-statements
                        media_size = media_path.stat().st_size
                        try:
                            media_mime = magic.from_file(media_path, mime=True)
                        except Exception:  # pylint: disable=broad-exception-caught
                            pass
                    except FileNotFoundError:
                        pass

                e.enclosure(murl, media_size, media_mime)
            e.content(self._make_abstract(m, media_mime), type="html")

        f.rss_file(
            self.site_path / self.config.publish_dir / "index.xml", pretty=True)
        f.atom_file(
            self.site_path / self.config.publish_dir / "index.atom",
            pretty=True)

    def _make_abstract(self, m: Message, media_mime: str) -> str:
        if self.rss_template:
            return self.rss_template.render(
                config=self.config,
                m=m,
                media_mime=media_mime,
                page_ids=self.page_ids,
                nl2br=self._nl2br)
        out = m.content
        if not out and m.media:
            out = m.media.title
        return out if out else ""

    def _nl2br(self, s) -> str:
        """ Replace the newlines in a string with <br> tags.
            There has to be a \n before <br> so as to not break
            Jinja's automatic hyperlinking of URLs.
        """
        return '' if not s else _NL2BR.sub("\n\n", s).replace("\n", "\n<br />")

    def _create_publish_dir(self) -> None:
        publish_dir = self.site_path / self.config.publish_dir

        # Clear the output directory.
        if os.path.exists(publish_dir):
            shutil.rmtree(publish_dir)

        # Re-create the output directory.
        os.mkdir(publish_dir)

        # Copy the static directory into the output directory.
        static_dir_src = self.site_path / self.config.static_dir
        target = publish_dir / static_dir_src.name
        if self.symlink:
            self._relative_symlink(os.path.abspath(static_dir_src), target)
        elif os.path.isfile(static_dir_src):
            shutil.copyfile(static_dir_src, target)
        else:
            shutil.copytree(static_dir_src, target)

        # If media downloading is enabled, copy/symlink the media directory.
        mediadir = self.site_path / self.config.media_dir
        if os.path.exists(mediadir):
            if self.symlink:
                self._relative_symlink(
                    os.path.abspath(mediadir),
                    os.path.join(publish_dir, os.path.basename(mediadir)))
            else:
                shutil.copytree(
                    mediadir,
                    os.path.join(publish_dir, os.path.basename(mediadir)))

    def _relative_symlink(self, src, dst) -> None:
        dir_path = os.path.dirname(dst)
        src = os.path.relpath(src, dir_path)
        dst = os.path.join(dir_path, os.path.basename(src))
        return os.symlink(src, dst)
