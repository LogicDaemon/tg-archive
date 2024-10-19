""" Build the static site from the database """
import logging
import os
import re
import shutil
from collections import OrderedDict, deque
from typing import Deque, Iterable

import magic
import pkg_resources
from config import ConfigFileType
from db import DB, Message
from feedgen.feed import FeedGenerator
from jinja2 import Template

_NL2BR = re.compile(r"\n\n+")


class Build:
    config: ConfigFileType
    template: Template
    db: DB

    def __init__(self, config: ConfigFileType, db: DB, symlink: bool) -> None:
        self.config = config
        self.db = db
        self.symlink = symlink

        self.rss_template: Template = None

        # Map of all message IDs across all months and the slug of the page
        # in which they occur (paginated), used to link replies to their
        # parent messages that may be on arbitrary pages.
        self.page_ids = {}
        self.timeline = OrderedDict()

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
        rss_entries: Deque[Message] = deque([], self.config["rss_feed_entries"])
        fname = None
        for month in timeline:
            # Get the days + message counts for the month.
            dayline = OrderedDict()
            per_page = self.config["per_page"]
            for d in self.db.get_dayline(month.date.year, month.date.month,
                                         per_page):
                dayline[d.slug] = d

            # Paginate and fetch messages for the month until the end..
            page = 0
            last_id = 0
            total = self.db.get_message_count(month.date.year, month.date.month)
            total_pages = -(-total // per_page)  # faster math.ceil without import

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

                if self.config["publish_rss_feed"]:
                    rss_entries.extend(messages)

                self._render_page(messages, month, dayline, fname, page,
                                  total_pages)

        # The last page chronologically is the latest page. Make it index.
        if fname:
            if self.symlink:
                os.symlink(
                    fname, os.path.join(self.config["publish_dir"],
                                        "index.html"))
            else:
                shutil.copy(
                    os.path.join(self.config["publish_dir"], fname),
                    os.path.join(self.config["publish_dir"], "index.html"))

        # Generate RSS feeds.
        if self.config["publish_rss_feed"]:
            self._build_rss(rss_entries, "index.rss", "index.atom")

    def load_template(self, fname) -> None:
        with open(fname, "r") as f:
            self.template = Template(f.read(), autoescape=True)

    def load_rss_template(self, fname) -> None:
        with open(fname, "r") as f:
            self.rss_template = Template(f.read(), autoescape=True)

    def make_filename(self, month, page) -> str:
        fname = f'{month.slug}{"_" + str(page) if page > 1 else ""}.html'
        return fname

    def _render_page(self, messages, month, dayline, fname, page,
                     total_pages) -> None:
        html = self.template.render(
            config=self.config,
            timeline=self.timeline,
            dayline=dayline,
            month=month,
            messages=messages,
            page_ids=self.page_ids,
            pagination={
                "current": page,
                "total": total_pages
            },
            make_filename=self.make_filename,
            nl2br=self._nl2br)

        with open(
                os.path.join(self.config["publish_dir"], fname),
                "w",
                encoding="utf8") as f:
            f.write(html)

    def _build_rss(self, messages: Iterable[Message], rss_file: str,
                   atom_file: str) -> None:
        f = FeedGenerator()
        f.id(self.config["site_url"])
        f.generator("tg-archive " +
                    pkg_resources.get_distribution("tg-archive").version)
        f.link(href=self.config["site_url"], rel="alternate")
        f.title(self.config["site_name"].format(group=self.config["group"]))
        f.subtitle(self.config["site_description"])

        for m in messages:
            url = f'{self.config["site_url"]}/{self.page_ids[m.id]}#{m.id}'
            e = f.add_entry()
            e.id(url)
            e.title(f'@{m.user.username} on {m.date} (#{m.id})')
            e.link({"href": url})
            e.published(m.date)

            media_mime = ""
            if m.media and m.media.url:
                murl = f"{self.config["site_url"]}/{os.path.basename(self.config["media_dir"])}/{m.media.url}"
                media_path = f"{self.config["media_dir"]}/{m.media.url}"
                media_mime = "application/octet-stream"
                media_size = 0

                if "://" in media_path:
                    media_mime = "text/html"
                else:
                    try:
                        media_size = str(os.path.getsize(media_path))
                        try:
                            media_mime = magic.from_file(media_path, mime=True)
                        except Exception:
                            pass
                    except FileNotFoundError:
                        pass

                e.enclosure(murl, media_size, media_mime)
            e.content(self._make_abstract(m, media_mime), type="html")

        f.rss_file(
            os.path.join(self.config["publish_dir"], "index.xml"), pretty=True)
        f.atom_file(
            os.path.join(self.config["publish_dir"], "index.atom"), pretty=True)

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
        return _NL2BR.sub("\n\n", s).replace("\n", "\n<br />")

    def _create_publish_dir(self) -> None:
        pubdir = self.config["publish_dir"]

        # Clear the output directory.
        if os.path.exists(pubdir):
            shutil.rmtree(pubdir)

        # Re-create the output directory.
        os.mkdir(pubdir)

        # Copy the static directory into the output directory.
        for f in [self.config["static_dir"]]:
            target = os.path.join(pubdir, f)
            if self.symlink:
                self._relative_symlink(os.path.abspath(f), target)
            elif os.path.isfile(f):
                shutil.copyfile(f, target)
            else:
                shutil.copytree(f, target)

        # If media downloading is enabled, copy/symlink the media directory.
        mediadir = self.config["media_dir"]
        if os.path.exists(mediadir):
            if self.symlink:
                self._relative_symlink(
                    os.path.abspath(mediadir),
                    os.path.join(pubdir, os.path.basename(mediadir)))
            else:
                shutil.copytree(
                    mediadir, os.path.join(pubdir, os.path.basename(mediadir)))

    def _relative_symlink(self, src, dst) -> None:
        dir_path = os.path.dirname(dst)
        src = os.path.relpath(src, dir_path)
        dst = os.path.join(dir_path, os.path.basename(src))
        return os.symlink(src, dst)
