import argparse
import logging
import os
import shutil
import sys
import asyncio

from .db import DB
from .meta import program_name, __version__

logging.basicConfig(format="%(asctime)s: %(message)s", level=logging.INFO)


def app_data_dir() -> str:
    if sys.platform == "win32":
        d = os.path.join(
            os.getenv('LOCALAPPDATA') or
            os.path.join(os.getenv('USERPROFILE'), 'Application Data'),
            program_name)
    d = os.path.join(os.path.expanduser("~"), '.local', 'share', program_name)
    os.makedirs(d, exist_ok=True)
    return d


def default_session_file() -> str:
    default_filename = 'session.session'
    secret_data_dir = os.getenv('SecretDataDir') or app_data_dir()

    return os.path.join(secret_data_dir, default_filename)


async def amain() -> None:
    """Run the CLI."""
    p = argparse.ArgumentParser(
        description="A tool for exporting and archiving Telegram groups to webpages.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    p.add_argument(
        "-c",
        "--config",
        action="store",
        type=str,
        default="config.yaml",
        dest="config",
        help="path to the config file")
    p.add_argument(
        "-d",
        "--data",
        action="store",
        type=str,
        default=os.path.join(app_data_dir(), "data.sqlite"),
        dest="data",
        help='path to the SQLite data file to store messages, default is "{default}"'
    )
    p.add_argument(
        "-se",
        "--session",
        action="store",
        type=str,
        default=default_session_file(),
        dest="session",
        help='path to the session file, default is "{default}"')
    p.add_argument(
        "-v",
        "--version",
        action="store_true",
        dest="version",
        help="display version")

    n = p.add_argument_group("new")
    n.add_argument(
        "-n",
        "--new",
        action="store_true",
        dest="new",
        help="initialize a new site")
    n.add_argument(
        "-p",
        "--path",
        action="store",
        type=str,
        default="example",
        dest="path",
        help="path to create the site")

    s = p.add_argument_group("sync")
    s.add_argument(
        "-s",
        "--sync",
        action="store_true",
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
        action="store_true",
        dest="build",
        help="build the static site")
    b.add_argument(
        "-t",
        "--template",
        action="store",
        type=str,
        default="template.html",
        dest="template",
        help="path to the template file")
    b.add_argument(
        "--rss-template",
        action="store",
        type=str,
        default=None,
        dest="rss_template",
        help="path to the rss template file")
    b.add_argument(
        "--symlink",
        action="store_true",
        dest="symlink",
        help="symlink media and other static files instead of copying")

    args = p.parse_args(args=None if sys.argv[1:] else ['--help'])

    if args.version:
        print(f"v{__version__}")
        return

    # Setup new site.
    if args.new:
        exdir = os.path.join(os.path.dirname(__file__), "example")
        if not os.path.isdir(exdir):
            logging.error("unable to find bundled example directory")
            sys.exit(1)

        logging.info("creating new site at '%s'", args.path)
        shutil.copytree(exdir, args.path, dirs_exist_ok=True)

        logging.info("created directory '%s'", args.path)

        # make sure the files are writable
        os.chmod(args.path, 0o755)
        for root, dirs, files in os.walk(args.path):
            for d in dirs:
                os.chmod(os.path.join(root, d), 0o755)
            for f in files:
                os.chmod(os.path.join(root, f), 0o644)
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
        s = await Sync(cfg, args.session, DB(args.data))
        try:
            await s.sync(args.id, args.from_id)
        except KeyboardInterrupt:
            logging.info("sync cancelled manually")
            if cfg.get("use_takeout", False):
                s.finish_takeout()
            sys.exit(1)
        return

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

        logging.info("published to directory '{}'".format(
            config["publish_dir"]))


def main() -> None:
    asyncio.run(amain())
