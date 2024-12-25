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


def site_template_dir() -> pathlib.Path:
    site_template = pathlib.Path(__file__).parent / 'new_site_template'
    if not os.path.isdir(site_template):
        logging.error("unable to find bundled new_site_template directory")
        sys.exit(1)
    return site_template


async def amain() -> None:
    """ Run the CLI """
    # pylint: disable=import-outside-toplevel
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    p.add_argument(
        "-c",
        "--config",
        action="store",
        type=pathlib.Path,
        default="config.yaml",
        help="path to the config file")
    p.add_argument(
        "-d",
        "--data",
        action="store",
        type=pathlib.Path,
        default=None,
        help='path to the SQLite data file to store messages, '
        'overrides the db_path value in config.')
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
        help="display version")

    n = p.add_argument_group("new")
    n.add_argument(
        "-n",
        "--new",
        action=argparse.BooleanOptionalAction,
        help="initialize a new site")
    n.add_argument(
        "-p",
        "--path",
        action="store",
        type=pathlib.Path,
        default="example",
        help="path to create the site")

    s = p.add_argument_group("sync")
    s.add_argument(
        "-s",
        "--sync",
        action=argparse.BooleanOptionalAction,
        help="sync data from telegram group to the local DB")
    s.add_argument(
        "-id",
        "--id",
        action="store",
        type=int,
        nargs="+",
        help="sync (or update) messages for given ids")
    s.add_argument(
        "-from-id",
        "--from-id",
        action="store",
        type=int,
        help="sync (or update) messages from this id to the latest")

    build = p.add_argument_group("build")
    build.add_argument(
        "-b",
        "--build",
        action=argparse.BooleanOptionalAction,
        help="build the static site")
    build.add_argument(
        "-t",
        "--html-template",
        action="store",
        type=pathlib.Path,
        default=None,
        help="html template file, overrides the config")
    build.add_argument(
        "--rss-template",
        action="store",
        type=pathlib.Path,
        default=None,
        help="rss template file, overrides the config")
    build.add_argument(
        "--symlink",
        action=argparse.BooleanOptionalAction,
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
        format='%(asctime)s: %(message)s',
        level=logging.DEBUG
        if args.verbose or os.getenv('DEBUG') else logging.INFO)

    # Setup new site.
    if args.new:
        if (args.path / "config.yaml").is_file():
            logging.error('site already exists at "%s"', args.path)
            sys.exit(1)

        logging.info('creating new site at "%s"', args.path)
        shutil.copytree(site_template_dir(), args.path, dirs_exist_ok=True)

        logging.info('created directory "%s"', args.path)

        # make sure the directories are writable
        base_mode = os.stat(args.path).st_mode & 0o777
        if base_mode & 0o700 != 0o700:
            os.chmod(args.path, base_mode | 0o700)
            for root, dirnames, filenames in os.walk(args.path):  # pylint: disable=unused-variable
                for d in dirnames:
                    if os.stat(d).st_mode & 0o700 != 0o700:
                        os.chmod(d, 0o700 | base_mode)
                for f in filenames:
                    if os.stat(f).st_mode & 0o600 != 0o600:
                        os.chmod(f, 0o600 | base_mode)
        log.info('site created, please edit the config file')
        return

    from .config import get_config

    config_path: pathlib.Path = args.config
    if not config_path.is_absolute() and not config_path.is_file():
        config_path = args.path / config_path
    config = get_config(config_path)

    db_path = args.path / (args.data or config.db_path)

    # Sync from Telegram.
    if args.sync:
        # Import because the Telegram client import is quite heavy.
        from .sync import Sync

        if args.id and args.from_id and args.from_id > 0:
            logging.error('pass either --id or --from-id but not both')
            sys.exit(1)

        logging.info(
            'starting Telegram sync (takeout=%s, limit=%s, batch_size=%s, wait=%s)',
            config.use_takeout, config.fetch_batch_size, config.fetch_limit,
            config.fetch_wait)
        s = await Sync(
            config=config,
            dl_root=args.path,
            session_file=args.session,
            db=DB(db_path))
        async with s:
            try:
                await s.sync(args.id, args.from_id)
            except KeyboardInterrupt:
                logging.info("sync cancelled manually")
                sys.exit(1)

    # Build static site.
    if args.build:
        from .build import Build

        logging.info("building site")
        build = Build(config, DB(db_path, config.timezone), args.symlink,
                      args.path)
        if args.html_template is not None:
            config.html_template = args.html_template
        if args.rss_template is not None:
            config.rss_template = args.rss_template

        for tmpl_name, tmpl_func in [
            (config.html_template, build.load_html_template),
            (config.rss_template, build.load_rss_template),
        ]:
            if not tmpl_name:
                continue
            for base_dir_generator in [lambda: args.path, site_template_dir]:
                template: pathlib.Path = base_dir_generator() / tmpl_name
                if template.is_file():
                    tmpl_func(template)
                    break
                logging.warning("No template file at '%s'", template)
            else:
                logging.error('Failed to load %s', tmpl_name)

        build.build()

        logging.info('published to directory "%s"', config.publish_dir)


def main() -> None:
    asyncio.run(amain())
