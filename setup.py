#!/usr/bin/env python
from __future__ import annotations

from setuptools import setup

from tgarchive.meta import __version__

README = open('README.md').read()


def requirements() -> list[str]:
    with open('requirements.txt') as f:
        return f.read().splitlines()


setup(
    name='tg-archive',
    version=__version__,
    description=(
        'is a tool for exporting Telegram group chats into static websites, '
        'preserving the chat history like mailing list archives.'),
    long_description=README,
    long_description_content_type='text/markdown',
    author='Kailash Nadh, LogicDaemon',
    author_email='tg-archive-fork@logicdaemon.ru',
    url='https://github.com/LogicDaemon/tg-archive',
    packages=['tgarchive'],
    install_requires=requirements(),
    include_package_data=True,
    download_url='https://github.com/LogicDaemon/tg-archive',
    license='MIT License',
    entry_points={
        'console_scripts': ['tg-archive = tgarchive.cli:main'],
    },
    python_requires=">=3.14",
    classifiers=[
        'Topic :: Communications :: Chat',
        'Topic :: Internet :: WWW/HTTP :: Site Management',
        'Topic :: Documentation',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.14',
        'Programming Language :: Python :: 3 :: Only',
    ],
)
