""" Database module """
import json
import math
import os
import pathlib
import sqlite3
from datetime import datetime
from typing import Generator, Iterator, NamedTuple, Optional, Tuple, Union

import pytz

SCHEMA = ("""
CREATE table messages (
    id INTEGER NOT NULL PRIMARY KEY,
    type TEXT NOT NULL,
    date TIMESTAMP NOT NULL,
    edit_date TIMESTAMP,
    content TEXT,
    reply_to INTEGER,
    user_id INTEGER,
    media_id INTEGER,
    FOREIGN KEY(user_id) REFERENCES users(id),
    FOREIGN KEY(media_id) REFERENCES media(id)
);
""", """
CREATE table users (
    id INTEGER NOT NULL PRIMARY KEY,
    username TEXT,
    first_name TEXT,
    last_name TEXT,
    tags TEXT,
    avatar TEXT
);
""", """
CREATE table media (
    id INTEGER NOT NULL PRIMARY KEY,
    type TEXT,
    url TEXT,
    title TEXT,
    description TEXT,
    thumb TEXT
);
""")


class User(NamedTuple):
    id: int
    username: str
    first_name: str
    last_name: str
    tags: list
    avatar: str


class Media(NamedTuple):
    id: int
    type: str
    url: str
    title: str
    description: str
    thumb: str


class Message(NamedTuple):
    id: int
    type: str
    date: Optional[datetime]
    edit_date: Optional[datetime]
    content: str
    reply_to: int
    user: User
    media: Media


class Month(NamedTuple):
    date: datetime
    slug: str
    label: str
    count: int


class Day(NamedTuple):
    date: datetime
    slug: str
    label: str
    count: int
    page: int


def _page(n, multiple) -> int:
    return math.ceil(n / multiple)


class DB:
    """ SQLite database wrapper """
    conn: sqlite3.Connection
    tz: Optional[pytz.tzinfo.BaseTzInfo]

    def __init__(self,
                 dbfile: Union[pathlib.Path, str],
                 tz: Optional[str] = None) -> None:
        self.tz = pytz.timezone(tz) if tz else None

        # Initialize the SQLite DB. If it's new, create the table schema.
        is_new = not os.path.isfile(dbfile)

        self.conn = sqlite3.Connection(
            dbfile if isinstance(dbfile, str) else str(dbfile),
            detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)

        # Add the custom PAGE() function to get the page number of a row
        # by its row number and a limit multiple.
        self.conn.create_function("PAGE", 2, _page)

        if is_new:
            for s in SCHEMA:
                self.conn.cursor().execute(s)
                self.conn.commit()

    # def _parse_date(self, d) -> str:
    #     return datetime.strptime(d, "%Y-%m-%dT%H:%M:%S%z")

    def get_last_message_id(self) -> Tuple[int, datetime]:
        cur = self.conn.cursor()
        cur.execute("""
            SELECT id, strftime('%Y-%m-%d 00:00:00', date) as "[timestamp]" FROM messages
            ORDER BY id DESC LIMIT 1
        """)
        res = cur.fetchone()
        if not res:
            return 0, None

        # id, date = res
        return res

    def get_timeline(self) -> Iterator[Month]:
        """ Get the list of all unique yyyy-mm month groups and
            the corresponding message counts per period in chronological order.
        """
        cur = self.conn.cursor()
        cur.execute("""
            SELECT strftime('%Y-%m-%d 00:00:00', date) as "[timestamp]",
            COUNT(*) FROM messages AS count
            GROUP BY strftime('%Y-%m', date) ORDER BY date
        """)

        for r in cur.fetchall():
            date = pytz.utc.localize(r[0])
            if self.tz:
                date = date.astimezone(self.tz)

            yield Month(
                date=date,
                slug=date.strftime("%Y-%m"),
                label=date.strftime("%b %Y"),
                count=r[1])

    def get_dayline(self,
                    year: Union[int, str],
                    month: Union[int, str],
                    limit: int = 500) -> Iterator[Day]:
        """ Get the list of all unique yyyy-mm-dd days corresponding
            message counts and the page number of the first occurrence of
            the date in the pool of messages for the whole month.
        """
        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT strftime("%Y-%m-%d 00:00:00", date) AS "[timestamp]",
            COUNT(*), PAGE(rank, ?) FROM (
                SELECT ROW_NUMBER() OVER() as rank, date FROM messages
                WHERE strftime('%Y%m', date) = ? ORDER BY id
            )
            GROUP BY "[timestamp]";
            """, (limit, f'{year}{month:02d}'))

        for r in cur.fetchall():
            date = pytz.utc.localize(r[0])
            if self.tz:
                date = date.astimezone(self.tz)

            yield Day(
                date=date,
                slug=date.strftime("%Y-%m-%d"),
                label=date.strftime("%d %b %Y"),
                count=r[1],
                page=r[2])

    def get_messages(self,
                     year: Union[str, int],
                     month: Union[str, int],
                     last_id: int = 0,
                     limit: int = 500) -> Generator[Message, None, None]:
        date = f'{year}{month:02d}'

        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT messages.id, messages.type, messages.date, messages.edit_date,
            messages.content, messages.reply_to, messages.user_id,
            users.username, users.first_name, users.last_name, users.tags, users.avatar,
            media.id, media.type, media.url, media.title, media.description, media.thumb
            FROM messages
            LEFT JOIN users ON (users.id = messages.user_id)
            LEFT JOIN media ON (media.id = messages.media_id)
            WHERE strftime('%Y%m', date) = ?
            AND messages.id > ? ORDER by messages.id LIMIT ?
            """, (date, last_id, limit))

        for r in cur.fetchall():
            yield self._make_message(r)

    def get_message_count(self, year: Union[str, int],
                          month: Union[str, int]) -> int:
        date = f'{year}{month:02d}'

        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT COUNT(*) FROM messages WHERE strftime('%Y%m', date) = ?
            """, (date,))

        total, = cur.fetchone()
        return total

    def insert_user(self, u: User) -> None:
        """ Insert a user and if they exist, update the fields """
        cur = self.conn.cursor()
        cur.execute(
            """INSERT INTO users (id, username, first_name, last_name, tags, avatar)
            VALUES(?, ?, ?, ?, ?, ?) ON CONFLICT (id)
            DO UPDATE SET username=excluded.username, first_name=excluded.first_name,
                last_name=excluded.last_name, tags=excluded.tags, avatar=excluded.avatar
            """, (u.id, u.username, u.first_name, u.last_name, " ".join(
                u.tags), u.avatar))

    def insert_media(self, m: Media) -> None:
        cur = self.conn.cursor()
        cur.execute(
            """INSERT OR REPLACE INTO media
            (id, type, url, title, description, thumb)
            VALUES(?, ?, ?, ?, ?, ?)""",
            (m.id, m.type, m.url, m.title, m.description, m.thumb))

    def insert_message(self, m: Message) -> None:
        cur = self.conn.cursor()
        cur.execute(
            """INSERT OR REPLACE INTO messages
            (id, type, date, edit_date, content, reply_to, user_id, media_id)
            VALUES(?, ?, ?, ?, ?, ?, ?, ?)""",
            (m.id, m.type, m.date.strftime("%Y-%m-%d %H:%M:%S"),
             m.edit_date.strftime("%Y-%m-%d %H:%M:%S") if m.edit_date else None,
             m.content, m.reply_to, m.user.id, m.media.id if m.media else None))

    def commit(self) -> None:
        """ Commit pending writes to the DB """
        self.conn.commit()

    def _make_message(self, r: tuple) -> Message:
        """ Makes a Message() object from an SQL result tuple """
        m_id, typ, date, edit_date, content, reply_to, \
            user_id, username, first_name, last_name, tags, avatar, \
            media_id, media_type, media_url, media_title, media_description, media_thumb = r

        md = None
        if media_id:
            desc = media_description
            if media_type == "poll":
                desc = json.loads(media_description)

            md = Media(
                id=media_id,
                type=media_type,
                url=media_url,
                title=media_title,
                description=desc,
                thumb=media_thumb)

        date = pytz.utc.localize(date) if date else None
        edit_date = pytz.utc.localize(edit_date) if edit_date else None

        if self.tz:
            date = date.astimezone(self.tz) if date else None
            edit_date = edit_date.astimezone(self.tz) if edit_date else None

        return Message(
            id=m_id,
            type=typ,
            date=date,
            edit_date=edit_date,
            content=content,
            reply_to=reply_to,
            user=User(
                id=user_id,
                username=username,
                first_name=first_name,
                last_name=last_name,
                tags=tags,
                avatar=avatar),
            media=md)
