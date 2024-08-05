from collections import deque, namedtuple
import json
import logging
import os
import shutil
import threading
import time
from io import BytesIO
from sys import exit
from typing import AsyncGenerator, Deque, Optional, Union

import telethon.hints
import telethon.tl.types
from PIL import Image
from telethon import TelegramClient, errors

from .aobject import aobject
from .config import ConfigFileType
from .db import DB, Media, Message, User

import atexit

moving_thread: Optional[threading.Thread] = None
move_event = threading.Event()
move_files: Deque[tuple[str, str]] = deque()
exit_signaled: bool = False

DownloadMediaReturn = namedtuple("DownloadMediaReturn",
                                 ["basename", "fname", "thumb"])


def moving_thread_fn() -> None:
    while not exit_signaled:
        while move_files:
            src, dest = move_files.popleft()
            shutil.move(src, dest)
        move_event.clear()
        move_event.wait()


def finish_moving_thread() -> None:
    global exit_signaled, moving_thread
    exit_signaled = True
    if moving_thread is not None:
        move_event.set()
        moving_thread.join()
        moving_thread = None


def fmove(src: Union[str, os.PathLike], dest: Union[str, os.PathLike]) -> None:
    """ Move a file from src to dest """
    global moving_thread
    move_files.append((src, dest))
    move_event.set()
    if moving_thread is None:
        moving_thread = threading.Thread(target=moving_thread_fn)
        moving_thread.start()
        atexit.register(finish_moving_thread)


class Sync(aobject):
    """ Sync iterates and receives messages from the Telegram group to the
        local SQLite DB.
    """
    config: ConfigFileType
    db: DB
    client: TelegramClient
    root: str
    media_dir: str
    media_tmp_dir: str

    def __init__(self, *, config: ConfigFileType, dl_root: str,
                 session_file: str, db: DB) -> None:
        self.config = config
        self.root = dl_root
        self.session_file = session_file
        self.db = db

        self.media_dir = media_dir = os.path.join(dl_root,
                                                  self.config["media_dir"])
        os.makedirs(media_dir, exist_ok=True)
        self.media_tmp_dir = media_tmp_dir = os.path.join(
            dl_root, self.config["media_tmp_dir"])
        os.makedirs(media_tmp_dir, exist_ok=True)

    async def _init(self, *, config: ConfigFileType, dl_root: str,
                    session_file: str, db: DB) -> None:
        self.client = await self.new_client(session_file, config)

    async def sync(self, ids=None, from_id=None) -> None:
        """ Sync syncs messages from Telegram from the last synced message
            into the local SQLite DB.
        """

        if ids:
            last_id, last_date = (ids, None)
            logging.info("fetching message id={}".format(ids))
        elif from_id:
            last_id, last_date = (from_id, None)
            logging.info("fetching from last message id={}".format(last_id))
        else:
            last_id, last_date = self.db.get_last_message_id()
            logging.info("fetching from last message id={} ({})".format(
                last_id, last_date))

        group_id = await self._get_group_id(self.config["group"])

        n = 0
        while True:
            has = False
            async for m in self._get_messages(
                    group_id, offset_id=last_id if last_id else 0, ids=ids):
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
                    logging.info("fetched {} messages".format(n))
                    self.db.commit()

                if 0 < self.config["fetch_limit"] <= n or ids:
                    has = False
                    break

            self.db.commit()
            if has:
                last_id = m.id
                logging.info(
                    "fetched {} messages. sleeping for {} seconds".format(
                        n, self.config["fetch_wait"]))
                time.sleep(self.config["fetch_wait"])
            else:
                break

        self.db.commit()
        if self.config.get("use_takeout", False):
            await self.finish_takeout()
        logging.info("finished. fetched {} messages. last message = {}".format(
            n, last_date))

    async def new_client(self, session, config) -> TelegramClient:
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
                    logging.info(
                        "please allow the data export request received from Telegram on your device. "
                        "you can also wait for {} seconds.".format(e.seconds))
                    logging.info(
                        "press Enter key after allowing the data export request to continue.."
                    )
                    input()
                    logging.info("trying again.. ({})".format(retry + 2))
                except errors.TakeoutInvalidError:
                    logging.info(
                        "takeout invalidated. delete the session.session file and try again."
                    )
            else:
                logging.info("could not initiate takeout.")
                raise Exception("could not initiate takeout.")
        else:
            return client

    async def finish_takeout(self) -> None:
        await self.client.__aexit__(None, None, None)

    async def _get_messages(self,
                            group,
                            offset_id,
                            ids=None) -> AsyncGenerator[Message, None]:
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
                    if len(alt) > 0:
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
                              group,
                              offset_id,
                              ids=None) -> telethon.hints.TotalList:
        try:
            if self.config.get("use_takeout", False):
                wait_time = 0
            else:
                wait_time = None
            messages = await self.client.get_messages(
                group,
                offset_id=offset_id,
                limit=self.config["fetch_batch_size"],
                wait_time=wait_time,
                ids=ids,
                reverse=True)
            return messages
        except errors.FloodWaitError as e:
            logging.info("flood waited: have to wait {} seconds".format(
                e.seconds))

    async def _get_user(self, u) -> User:
        tags = []
        is_normal_user = isinstance(u, telethon.tl.types.User)

        if isinstance(u, telethon.tl.types.ChannelForbidden):
            return User(
                id=u.id,
                username=u.title,
                first_name=None,
                last_name=None,
                tags=tags,
                avatar=None)

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
                logging.error("error downloading avatar: #{}: {}".format(
                    u.id, e))

        return User(
            id=u.id,
            username=u.username if u.username else str(u.id),
            first_name=u.first_name if is_normal_user else None,
            last_name=u.last_name if is_normal_user else None,
            tags=tags,
            avatar=avatar)

    def _make_poll(self, msg) -> None | Media:
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

    async def _get_media(self, msg) -> Optional[Media]:
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
                if len(self.config["media_mime_types"]) > 0:
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

    async def _download_media(self, msg) -> DownloadMediaReturn:
        ''' Download a media / file attached to a message and return its original
            filename, sanitized name on disk, and the thumbnail (if any).
        '''
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

        newname = f'{msg.id}.{self._get_file_ext(basename)}'
        fmove(fpath, os.path.join(self.media_dir, newname))

        # If it's a photo, download the thumbnail.
        tname = None
        if isinstance(msg.media, telethon.tl.types.MessageMediaPhoto):
            tpath = await self.client.download_media(
                msg, file=self.media_tmp_dir, thumb=1)
            tname = "thumb_{}.{}".format(
                msg.id, self._get_file_ext(os.path.basename(tpath)))
            fmove(tpath, os.path.join(self.media_dir, tname))

        return basename, newname, tname

    def _get_file_ext(self, f) -> str:
        if "." in f:
            e = f.split(".")[-1]
            if len(e) < 6:
                return e

        return ".file"

    async def _download_avatar(self, user) -> Optional[str]:
        fname = "avatar_{}.jpg".format(user.id)
        fpath = os.path.join(self.media_dir, fname)

        if os.path.exists(fpath):
            return fname

        logging.info("downloading avatar #{}".format(user.id))

        # Download the file into a container, resize it, and then write to disk.
        b = BytesIO()
        profile_photo = await self.client.download_profile_photo(user, file=b)
        if profile_photo is None:
            logging.info("user has no avatar #{}".format(user.id))
            return None

        im = Image.open(b)
        im.thumbnail(self.config["avatar_size"], Image.LANCZOS)
        im.save(fpath, "JPEG")

        return fname

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
