""" Receive messages from the Telegram group to the local DB """
import atexit
import glob
import json
import logging
import os
import pathlib
import shutil
import threading
import time
import typing
from collections import deque
from sys import exit
from typing import AsyncGenerator, Deque, NamedTuple, Optional, Union

import telethon.hints
import telethon.tl.types
from telethon import TelegramClient, errors

from .aobject import aobject
from .config import ConfigFileType
from .db import DB, Media, Message, User

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
            if not exit_signaled:
                continue
        shutil.move(src, dest)
        logging.info('moved "%s" -> "%s"', src, dest)


moving_thread = threading.Thread(target=moving_thread_fn)
moving_thread.start()


def finish_moving_thread() -> None:
    global exit_signaled
    exit_signaled = True
    if moving_thread.is_alive():
        logging.info("waiting for moving thread to finish..")
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
    config: ConfigFileType
    db: DB
    client: TelegramClient
    # root: pathlib.Path
    media_dir: pathlib.Path
    media_tmp_dir: str

    def __init__(self, *, config: ConfigFileType, dl_root: pathlib.Path,
                 session_file: str, db: DB) -> None:
        self.config = config
        # self.root = dl_root
        self.session_file = session_file
        self.db = db

        self.media_dir = media_dir = dl_root / self.config["media_dir"]
        media_dir.mkdir(parents=True, exist_ok=True)
        self.media_tmp_dir = media_tmp_dir = os.path.join(
            dl_root, self.config["media_tmp_dir"])
        os.makedirs(media_tmp_dir, exist_ok=True)

    async def _init(self, *, config: ConfigFileType, dl_root: str,
                    session_file: str, db: DB) -> None:
        self.client = await self.new_client(session_file, config)

    async def sync(self,
                   ids: Optional[list[int]] = None,
                   from_id: Optional[int] = None) -> None:
        """ Sync syncs messages from Telegram from the last synced message
            into the local SQLite DB.
        """

        if ids:
            last_id, last_date = (ids, None)
            logging.info('fetching message id=%s', ids)
        elif from_id:
            last_id, last_date = (from_id, None)
            logging.info('fetching from last message id=%s', last_id)
        else:
            last_id, last_date = self.db.get_last_message_id()
            logging.info('fetching from last message id=%s (%s)', last_id,
                         last_date)

        group_id = await self._get_group_id(self.config["group"])

        n = 0
        while True:
            has = False
            async for m in self._get_messages(
                    group_id, offset_id=last_id, ids=ids):
                if not m:
                    continue

                has = True

                # Insert the records into DB.
                self.db.insert_user(m.user)

                if m.media:
                    self.db.insert_media(m.media)

                self.db.insert_message(m)

                last_date = m.date
                n += 1
                if n % 300 == 0:
                    logging.info("fetched %s messages", n)
                    self.db.commit()

                if 0 < self.config["fetch_limit"] <= n or ids:
                    has = False
                    break

            self.db.commit()
            if has:
                last_id = m.id
                wait_s = self.config["fetch_wait"]
                logging.info("fetched %s messages. sleeping for %s seconds", n,
                             wait_s)
                time.sleep(wait_s)
            else:
                break

        self.db.commit()
        if self.config.get("use_takeout", False):
            await self.finish_takeout()
        logging.info("finished. fetched %s messages. last message = %s", n,
                     last_date)

    async def new_client(self,
                         session: typing.Union[str, pathlib.Path,
                                               'telethon.session.Session'],
                         config: ConfigFileType) -> TelegramClient:
        if "proxy" in config and config["proxy"].get("enable"):
            proxy = config["proxy"]
            client = TelegramClient(
                session,
                config["api_id"],
                config["api_hash"],
                proxy=(proxy["protocol"], proxy["addr"], proxy["port"]))
        else:
            client = TelegramClient(session, config["api_id"],
                                    config["api_hash"])
        # hide log messages
        # upstream issue https://github.com/LonamiWebs/Telethon/issues/3840
        client_logger = client._log["telethon.client.downloads"]
        client_logger._info = client_logger.info

        def patched_info(*args, **kwargs):
            if (args[0] == "File lives in another DC" or args[0] ==
                    "Starting direct file download in chunks of %d at %d, stride %d"
               ):
                return client_logger.debug(*args, **kwargs)
            client_logger._info(*args, **kwargs)

        client_logger.info = patched_info

        await client.start()
        if config.get("use_takeout", False):
            for retry in range(3):
                try:
                    takeout_client = await client.takeout(finalize=True
                                                         ).__aenter__()
                    # check if the takeout session gets invalidated
                    await takeout_client.get_messages("me")
                    return takeout_client
                except errors.TakeoutInitDelayError as e:
                    logging.warning(
                        "please allow the data export request received from Telegram on your device. "
                        "you can also wait for %s seconds.\n"
                        "press Enter key after allowing the data export request to continue..",
                        e.seconds)
                    input()
                    logging.info("trying again.. (%s)", retry + 2)
                except errors.TakeoutInvalidError:
                    logging.exception(
                        "takeout invalidated. delete the session.session file and try again."
                    )
            else:
                logging.warning()("could not initiate takeout.")
                raise TakeoutFailedError()
        else:
            return client

    async def finish_takeout(self) -> None:
        await self.client.__aexit__(None, None, None)

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
                limit=self.config["fetch_batch_size"],
                wait_time=0 if self.config.get("use_takeout", False) else None,
                reverse=True,
                **({
                    ids: ids,
                } if ids else {}))
        except errors.FloodWaitError as e:
            logging.info("flood waited: have to wait %s seconds", e.seconds)

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
        if self.config["download_avatars"]:
            try:
                fname = await self._download_avatar(u)
                avatar = fname
            except Exception as e:
                logging.error("error downloading avatar: #%s: %s", u.id, e)

        return User(
            id=u.id,
            username=u.username if u.username else str(u.id),
            first_name=u.first_name if is_normal_user else None,
            last_name=u.last_name if is_normal_user else None,
            tags=tags,
            avatar=avatar)

    def _make_poll(self, msg: Message) -> None | Media:
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
            if self.config["download_media"]:
                # Filter by extensions?
                if self.config["media_mime_types"]:
                    if hasattr(msg, "file") and hasattr(
                            msg.file, "mime_type") and msg.file.mime_type:
                        if msg.file.mime_type not in self.config[
                                "media_mime_types"]:
                            logging.info("skipping media #%s / %s",
                                         msg.file.name, msg.file.mime_type)
                            return

                logging.info("downloading media #%s", msg.id)
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
            try:
                fpath = await self.client.download_media(
                    msg, file=self.media_tmp_dir)
                break
            except ValueError:
                logging.error("error downloading media #%s. Sleeping %ss",
                              msg.id, error_sleep_s)
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
            tname = os.path.basename(tpath)
            fmove(tpath, os.path.join(self.media_dir, tname))

        return DownloadMediaReturn(basename, newname, tname)

    async def _download_avatar(
            self, user: telethon.tl.types.ChannelForbidden) -> Optional[str]:
        fpath_prefix = os.path.join(self.media_dir, f'avatar_{user.id} ')

        for existing in glob.iglob(fpath_prefix + '*'):
            return os.path.basename(existing)

        logging.info('downloading avatar #%s', user.id)

        # Download the file into a container, resize it, and then write to disk.
        profile_photo = await self.client.download_profile_photo(
            user,
            file=self.media_tmp_dir,
            download_big=self.config["avatar_size"])
        if profile_photo is None:
            logging.info("user has no avatar #%s", user.id)
            return None
        fpath = fpath_prefix + os.path.basename(profile_photo)
        fmove(profile_photo, fpath)

        return os.path.basename(fpath)

    async def _get_group_id(self, group: Union[str, int]) -> int:
        """
        Syncs the Entity cache and returns the Entity ID for the specified group,
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
            logging.critical(
                "the group: %s does not exist,"
                " or the authorized user is not a participant!", group)
            # This is a critical error, so exit with code: 1
            exit(1)

        return entity.id
