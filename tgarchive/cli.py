""" A tool for exporting and archiving Telegram groups to webpages """

import argparse
import asyncio
import logging
import os
import pathlib
import shutil
import sys

import appdirs

from .db import DB
from .meta import __version__, program_name

log = logging.getLogger(
    os.path.basename(__file__) if __name__ == '__main__' else __name__)


def app_data_dir() -> str:
    d = appdirs.user_data_dir(program_name)
    os.makedirs(d, exist_ok=True)
    return d


def default_session_file() -> str:
    default_filename = f'{program_name}.session'
    secret_data_dir = os.getenv('SecretDataDir') or app_data_dir()

    return os.path.join(secret_data_dir, default_filename)


async def amain() -> None:
    """ Run the CLI """
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    p.add_argument(
        "-c",
        "--config",
        action="store",
        type=pathlib.Path,
        default="config.yaml",
        dest="config",
        help="path to the config file")
    p.add_argument(
        "-d",
        "--data",
        action="store",
        type=pathlib.Path,
        default=os.path.join(app_data_dir(), "data.sqlite"),
        dest="data",
        help='path to the SQLite data file to store messages, default is "{default}"'
    )
    p.add_argument(
        "-se",
        "--session",
        action="store",
        type=pathlib.Path,
        default=default_session_file(),
        dest="session",
        help='path to the session file, default is "{default}"')
    p.add_argument(
        "-V",
        "--version",
        action=argparse.BooleanOptionalAction,
        dest="version",
        help="display version")

    n = p.add_argument_group("new")
    n.add_argument(
        "-n",
        "--new",
        action=argparse.BooleanOptionalAction,
        dest="new",
        help="initialize a new site")
    n.add_argument(
        "-p",
        "--path",
        action="store",
        type=pathlib.Path,
        default="example",
        dest="path",
        help="path to create the site")

    s = p.add_argument_group("sync")
    s.add_argument(
        "-s",
        "--sync",
        action=argparse.BooleanOptionalAction,
        dest="sync",
        help="sync data from telegram group to the local DB")
    s.add_argument(
        "-id",
        "--id",
        action="store",
        type=int,
        nargs="+",
        dest="id",
        help="sync (or update) messages for given ids")
    s.add_argument(
        "-from-id",
        "--from-id",
        action="store",
        type=int,
        dest="from_id",
        help="sync (or update) messages from this id to the latest")

    b = p.add_argument_group("build")
    b.add_argument(
        "-b",
        "--build",
        action=argparse.BooleanOptionalAction,
        dest="build",
        help="build the static site")
    b.add_argument(
        "-t",
        "--template",
        action="store",
        type=pathlib.Path,
        default="template.html",
        dest="template",
        help="path to the template file")
    b.add_argument(
        "--rss-template",
        action="store",
        type=pathlib.Path,
        default=None,
        dest="rss_template",
        help="path to the rss template file")
    b.add_argument(
        "--symlink",
        action=argparse.BooleanOptionalAction,
        dest="symlink",
        help="symlink media and other static files instead of copying")
    p.add_argument(
        "--verbose",
        "-v",
        type=bool,
        action=argparse.BooleanOptionalAction,
        help="logging level",
    )

    args = p.parse_args(args=None if sys.argv[1:] else ['--help'])

    if args.version:
        print(f"v{__version__}")
        return

    logging.basicConfig(
        format="%(asctime)s: %(message)s",
        level=logging.DEBUG
        if args.verbose or os.getenv('DEBUG') else logging.INFO)

    # Setup new site.
    if args.new:
        exdir = os.path.join(os.path.dirname(__file__), "example")
        if not os.path.isdir(exdir):
            logging.error("unable to find bundled example directory")
            sys.exit(1)

        if os.path.exists(os.path.join(args.path, "config.yaml")):
            logging.error("site already exists at '%s'", args.path)
            sys.exit(1)

        logging.info("creating new site at '%s'", args.path)
        shutil.copytree(exdir, args.path, dirs_exist_ok=True)

        logging.info("created directory '%s'", args.path)

        # make sure the directories are writable
        base_mode = os.stat(args.path).st_mode & 0o777
        if base_mode & 0o700 != 0o700:
            os.chmod(args.path, base_mode | 0o700)
            for root, dirnames, filenames in os.walk(args.path):
                for d in dirnames:
                    if os.stat(d).st_mode & 0o700 != 0o700:
                        os.chmod(d, 0o700 | base_mode)
                for f in filenames:
                    if os.stat(f).st_mode & 0o600 != 0o600:
                        os.chmod(f, 0o600 | base_mode)
        log.info("site created, please edit the config file")
        return

    from .config import get_config

    # Sync from Telegram.
    if args.sync:
        # Import because the Telegram client import is quite heavy.
        from .sync import Sync

        if args.id and args.from_id and args.from_id > 0:
            logging.error("pass either --id or --from-id but not both")
            sys.exit(1)

        cfg = get_config(os.path.join(args.path, args.config))
        mode = "takeout" if cfg.get("use_takeout", False) else "standard"

        logging.info(
            "starting Telegram sync (batch_size=%s, limit=%s, wait=%s, mode=%s)",
            cfg["fetch_batch_size"], cfg["fetch_limit"], cfg["fetch_wait"],
            mode)
        s = await Sync(
            config=cfg,
            dl_root=args.path,
            session_file=args.session,
            db=DB(args.data))
        try:
            await s.sync(args.id, args.from_id)
        except KeyboardInterrupt:
            logging.info("sync cancelled manually")
            if cfg.get("use_takeout", False):
                s.finish_takeout()
            sys.exit(1)

    # Build static site.
    if args.build:
        from .build import Build

        logging.info("building site")
        config = get_config(args.config)
        b = Build(config, DB(args.data, config["timezone"]), args.symlink)
        b.load_template(args.template)
        if args.rss_template:
            b.load_rss_template(args.rss_template)
        b.build()

        logging.info('published to directory "%s"', config["publish_dir"])


def main() -> None:
    asyncio.run(amain())
