""" Receive messages from the Telegram group to the local DB """
import atexit
import glob
import itertools
import json
import logging
import os
import pathlib
import shutil
import sys
import threading
import time
from collections import deque
from types import TracebackType
from typing import AsyncGenerator, Deque, NamedTuple, Optional, Type, Union

import telethon.hints
import telethon.tl.types
from telethon import TelegramClient, errors

from .aobject import aobject
from .config import Config
from .db import DB, Media, Message, User

log = logging.getLogger(__name__)

move_event = threading.Event()
move_files: Deque[tuple[str, str]] = deque()
exit_signaled: bool = False


class DownloadMediaReturn(NamedTuple):
    basename: str
    fname: str
    thumb: str


def moving_thread_fn() -> None:
    while True:
        move_event.wait()
        move_event.clear()
        try:
            src, dest = move_files.popleft()
        except IndexError:
            if exit_signaled:
                break
        shutil.move(src, dest)
        log.info('moved "%s" -> "%s"', src, dest)


moving_thread = threading.Thread(target=moving_thread_fn, daemon=True)
moving_thread.start()


def finish_moving_thread() -> None:
    global exit_signaled
    exit_signaled = True
    if moving_thread.is_alive():
        log.info("waiting for moving thread to finish..")
        move_event.set()
        moving_thread.join()


atexit.register(finish_moving_thread)


def fmove(src: Union[str, os.PathLike], dest: Union[str, os.PathLike]) -> None:
    """ Move a file from src to dest """
    move_files.append((src, dest))
    move_event.set()


class TakeoutFailedError(Exception):
    pass


class Sync(aobject):
    """ Sync iterates and receives messages from the Telegram group to the
        local SQLite DB.
    """
    config: Config
    session_file: pathlib.Path
    db: DB
    client: TelegramClient
    media_dir: pathlib.Path
    media_tmp_dir: pathlib.Path
    thumb_dir: pathlib.Path

    def __init__(self, *, config: Config, dl_root: pathlib.Path,
                 session_file: pathlib.Path, db: DB) -> None:
        self.config = config
        self.session_file = session_file
        self.db = db

        self.media_dir = media_dir = dl_root / self.config.media_dir
        media_dir.mkdir(parents=True, exist_ok=True)

        self.media_tmp_dir = media_tmp_dir = (
            dl_root / self.config.media_tmp_dir)
        media_tmp_dir.mkdir(parents=True, exist_ok=True)

        self.thumb_dir = thumb_dir = media_dir / self.config.thumbnails_subdir
        thumb_dir.mkdir(parents=True, exist_ok=True)

    async def sync(self,
                   ids: Optional[list[int]] = None,
                   from_id: Optional[int] = None) -> None:
        """ Sync syncs messages from Telegram from the last synced message
            into the local SQLite DB.
        """

        if ids:
            last_id, last_date = (ids, None)
            log.info('fetching message id=%s', ids)
        elif from_id:
            last_id, last_date = (from_id, None)
            log.info('fetching from last message id=%s', last_id)
        else:
            last_id, last_date = self.db.get_last_message_id()
            log.info('fetching from last message id=%s (%s)', last_id,
                     last_date)

        group_id = await self._get_group_id(self.config.group)

        fetch_limit = self.config.fetch_limit
        wait_s = self.config.fetch_wait
        m_counter = (
            iter(range(1, fetch_limit)) if fetch_limit else itertools.count())
        n = 0
        while True:
            m: Optional[Message] = None
            async for m in self._get_messages(
                    group_id, offset_id=last_id, ids=ids):
                if not m:
                    continue

                # Insert the records into DB.
                self.db.insert_user(m.user)

                if m.media:
                    self.db.insert_media(m.media)

                self.db.insert_message(m)

                last_date = m.date

                try:
                    n = next(m_counter)
                except StopIteration:
                    log.info("reached the fetch limit (%s)", fetch_limit)
                    # If m is not None, the cycle will repear after a sleep.
                    m = None
                    break

                if n % 300 == 0:
                    log.info("fetched %s messages", n)
                    self.db.commit()

            self.db.commit()
            if m is None:
                log.info("fetched %s messages. last message date: %s", n,
                         last_date)
                break
            last_id = m.id
            log.info("fetched %s messages. sleeping for %s seconds", n, wait_s)
            time.sleep(wait_s)

        self.db.commit()
        if self.config.use_takeout:
            await self.finish_takeout()
        log.info("finished. fetched %s messages. last message = %s", n,
                 last_date)

    async def __aenter__(self) -> TelegramClient:
        base_logger = logging.getLogger("telethon")
        base_logger.setLevel(logging.WARNING)
        kwargs = {'base_logger': base_logger}
        proxy = self.config.proxy
        if proxy.enable:
            kwargs["proxy"] = (proxy.protocol, proxy.addr, proxy.port)
        client = TelegramClient(self.session_file, self.config.api_id,
                                self.config.api_hash, **kwargs)

        await client.start()
        self.client = client
        if not self.config.use_takeout:
            return client
        for retry in range(3):
            try:
                takeout_client: TelegramClient = await client.takeout(
                    finalize=True).__aenter__()
            except errors.TakeoutInitDelayError as e:
                log.warning(
                    "please allow the data export request received from Telegram on your device. "
                    "you can also wait for %s seconds.\n"
                    "press Enter key after allowing the data export request to continue..",
                    e.seconds)
                input()
                log.info("trying again.. (%s)", retry + 2)
            try:
                # check if the takeout session gets invalidated
                await takeout_client.get_messages("me")  # default limit=1
            except errors.TakeoutInvalidError:
                log.exception(
                    'takeout invalidated. delete the session file "%s" and try again.',
                    self.session_file)
            return takeout_client
        log.error("could not initiate takeout.")
        raise TakeoutFailedError()

    async def __aexit__(self, exc_type: Optional[Type[BaseException]],
                        exc_value: Optional[BaseException],
                        traceback: Optional[TracebackType]) -> None:
        return await self.client.__aexit__(exc_type, exc_value, traceback)

    async def _get_messages(
            self,
            group: int,
            offset_id: int,
            ids: Optional[int] = None) -> AsyncGenerator[Message, None]:
        messages = await self._fetch_messages(group, offset_id, ids)
        # https://docs.telethon.dev/en/latest/quick-references/objects-reference.html#message
        for m in messages:
            if not m or not m.sender:
                continue

            # Media.
            sticker = None
            med = None
            if m.media:
                # If it's a sticker, get the alt value (unicode emoji).
                if (isinstance(m.media, telethon.tl.types.MessageMediaDocument)
                        and hasattr(m.media, "document") and
                        m.media.document.mime_type
                        == "application/x-tgsticker"):
                    alt = [
                        a.alt
                        for a in m.media.document.attributes
                        if isinstance(
                            a, telethon.tl.types.DocumentAttributeSticker)
                    ]
                    if alt:
                        sticker = alt[0]
                elif isinstance(m.media, telethon.tl.types.MessageMediaPoll):
                    med = self._make_poll(m)
                else:
                    med = await self._get_media(m)

            # Message.
            typ = "message"
            if m.action:
                if isinstance(m.action,
                              telethon.tl.types.MessageActionChatAddUser):
                    typ = "user_joined"
                elif isinstance(m.action,
                                telethon.tl.types.MessageActionChatDeleteUser):
                    typ = "user_left"

            yield Message(
                type=typ,
                id=m.id,
                date=m.date,
                edit_date=m.edit_date,
                content=sticker if sticker else m.raw_text,
                reply_to=m.reply_to_msg_id
                if m.reply_to and m.reply_to.reply_to_msg_id else None,
                user=await self._get_user(m.sender),
                media=med)

    async def _fetch_messages(self,
                              group: int,
                              offset_id: int,
                              ids: Optional[list[int]] = None
                             ) -> telethon.hints.TotalList[Message]:
        """ Fetch messages from the Telegram group """

        try:
            return await self.client.get_messages(
                group,
                offset_id=offset_id,
                limit=self.config.fetch_batch_size,
                wait_time=0 if self.config.use_takeout else None,
                reverse=True,
                **({
                    ids: ids,
                } if ids else {}))
        except errors.FloodWaitError as e:
            log.info("flood waited: have to wait %s seconds", e.seconds)

    async def _get_user(
        self, u: Union[telethon.tl.types.ChannelForbidden,
                       telethon.tl.types.User]
    ) -> User:
        tags = []

        if isinstance(u, telethon.tl.types.ChannelForbidden):
            return User(
                id=u.id,
                username=u.title,
                first_name=None,
                last_name=None,
                tags=tags,
                avatar=None)

        is_normal_user = isinstance(u, telethon.tl.types.User)

        if is_normal_user:
            if u.bot:
                tags.append("bot")

        if u.scam:
            tags.append("scam")

        if u.fake:
            tags.append("fake")

        # Download sender's profile photo if it's not already cached.
        avatar = None
        if self.config.download_avatars:
            try:
                avatar = await self._download_avatar(u)
            except Exception as e:  # pylint: disable=broad-exception-caught
                log.error("Got %s when downloading avatar %s", e, u.id)

        return User(
            id=u.id,
            username=u.username if u.username else str(u.id),
            first_name=u.first_name if is_normal_user else None,
            last_name=u.last_name if is_normal_user else None,
            tags=tags,
            avatar=avatar)

    def _make_poll(self, msg: Message) -> Optional[Media]:
        if not msg.media.results or not msg.media.results.results:
            return None

        options = [{
            "label": a.text,
            "count": 0,
            "correct": False
        } for a in msg.media.poll.answers]

        total = msg.media.results.total_voters
        if msg.media.results.results:
            for i, r in enumerate(msg.media.results.results):
                options[i]["count"] = r.voters
                options[i]["percent"] = r.voters / \
                    total * 100 if total > 0 else 0
                options[i]["correct"] = r.correct

        return Media(
            id=msg.id,
            type="poll",
            url=None,
            title=msg.media.poll.question,
            description=json.dumps(options),
            thumb=None)

    async def _get_media(self, msg: Message) -> Optional[Media]:
        if isinstance(msg.media, telethon.tl.types.MessageMediaWebPage) and \
                not isinstance(msg.media.webpage, telethon.tl.types.WebPageEmpty):
            return Media(
                id=msg.id,
                type="webpage",
                url=msg.media.webpage.url,
                title=msg.media.webpage.title,
                description=msg.media.webpage.description
                if msg.media.webpage.description else None,
                thumb=None)
        elif isinstance(msg.media, telethon.tl.types.MessageMediaPhoto) or \
                isinstance(msg.media, telethon.tl.types.MessageMediaDocument) or \
                isinstance(msg.media, telethon.tl.types.MessageMediaContact):
            if self.config.download_media:
                # Filter by extensions?
                if self.config.media_mime_types:
                    if hasattr(msg, "file") and hasattr(
                            msg.file, "mime_type") and msg.file.mime_type:
                        if msg.file.mime_type not in self.config.media_mime_types:
                            log.info("skipping media #%s / %s", msg.file.name,
                                     msg.file.mime_type)
                            return

                log.info("downloading media #%s", msg.id)
                basename, fname, thumb = await self._download_media(msg)
                return Media(
                    id=msg.id,
                    type="photo",
                    url=fname,
                    title=basename,
                    description=None,
                    thumb=thumb)

    async def _download_media(self, msg: Message) -> DownloadMediaReturn:
        """ Download a media / file attached to a message and return its original
            filename, sanitized name on disk, and the thumbnail (if any).
        """
        # Download the media to the temp dir and copy it back as
        # there does not seem to be a way to get the canonical
        # filename before the download.
        error_sleep_s = 60
        while True:
            try:  # pylint: disable=too-many-try-statements
                fpath = pathlib.Path(await self.client.download_media(
                    msg, file=self.media_tmp_dir))
                break
            except ValueError:
                log.error("error downloading media #%s. Sleeping %ss", msg.id,
                          error_sleep_s)
                time.sleep(error_sleep_s)
                error_sleep_s *= 2

        basename = os.path.basename(fpath)
        stem, ext = os.path.splitext(basename)
        if len(ext) > 6:
            stem = basename
            ext = ''

        newname = f'{msg.id} {stem}'[:250 - len(ext)] + ext
        fmove(fpath, os.path.join(self.media_dir, newname))

        # If it's a photo, download the thumbnail.
        tname = None
        if isinstance(msg.media, telethon.tl.types.MessageMediaPhoto):
            tpath = await self.client.download_media(
                msg, file=self.media_tmp_dir, thumb=1)
            t_stem, t_ext = os.path.splitext(os.path.basename(tpath))
            if len(t_ext) > 6:
                t_stem = os.path.basename(tpath)
                t_ext = ''
            tname = f'thumb_{msg.id} {t_stem}'[:250 - len(t_ext)] + t_ext
            fmove(tpath, self.media_dir / self.config.thumbnails_subdir / tname)

        return DownloadMediaReturn(basename, newname, tname)

    async def _download_avatar(
            self, user: telethon.tl.types.ChannelForbidden) -> Optional[str]:
        fpath_prefix = os.path.join(self.media_dir, f'avatar_{user.id} ')

        for existing in glob.iglob(fpath_prefix + '*'):
            return os.path.basename(existing)

        log.info('downloading avatar #%s', user.id)

        # Download the file into a container, resize it, and then write to disk.
        profile_photo = await self.client.download_profile_photo(
            user, file=self.media_tmp_dir, download_big=self.config.avatar_size)
        if profile_photo is None:
            log.info("user has no avatar #%s", user.id)
            return None
        fpath = fpath_prefix + os.path.basename(profile_photo)
        fmove(profile_photo, fpath)

        return os.path.basename(fpath)

    async def _get_group_id(self, group: Union[str, int]) -> int:
        """ Syncs the Entity cache and returns the Entity ID for the specified group,
            which can be a str/int for group ID, group name, or a group username.

            The authorized user must be a part of the group.
        """
        # Get all dialogs for the authorized user, which also
        # syncs the entity cache to get latest entities
        # ref: https://docs.telethon.dev/en/latest/concepts/entities.html#getting-entities
        await self.client.get_dialogs()

        try:
            # If the passed group is a group ID, extract it.
            group = int(group)
        except ValueError:
            # Not a group ID, we have either a group name or
            # a group username: @group-username
            pass

        try:
            entity = await self.client.get_entity(group)

        except ValueError:
            log.critical(
                "the group: %s does not exist,"
                " or the authorized user is not a participant!", group)
            # This is a critical error, so exit with code: 1
            sys.exit(1)

        return entity.id
