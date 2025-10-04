#!/usr/bin/env python3

"""Create duplicate file CSV report via SQLite database with file infos."""

from collections.abc import Iterator
import csv
import datetime
import functools
import hashlib
import os
import pathlib
import sqlite3
import sys
from typing import Any, TypedDict

import sqlalchemy as sa
import sqlalchemy.orm

__title__ = 'duplicates.py'
__author__ = 'Sebastian Bank <sebastian.bank@uni-leipzig.de>'
__license__ = 'MIT, see LICENSE.txt'
__copyright__ = 'Copyright (c) 2014,2017 Sebastian Bank'

START_DIR = pathlib.Path(os.curdir)

DB_PATH = pathlib.Path('duplicates.sqlite3')

OUT_PATH = pathlib.Path('duplicates.csv')

ENGINE = sa.create_engine(f'sqlite:///{DB_PATH}', paramstyle='named', echo=False)

REGISTRY = sa.orm.registry()


class FileInfo(TypedDict):

    location: str
    name: str
    ext: str
    size: int
    mtime: datetime.datetime


def get_file_params(start: os.PathLike[str] | str,
                    dentry: os.PathLike[str] | str, /) -> FileInfo:
    path = pathlib.Path(dentry)
    stat = path.stat()
    return {'location': path.relative_to(start).as_posix(),
            'name': path.name,
            'ext': path.suffix,
            'size': stat.st_size,
            'mtime': make_datetime(stat.st_mtime)}


make_datetime = functools.partial(datetime.datetime.fromtimestamp,
                                  tz=datetime.timezone.utc)


def adapt_datetime(value: datetime.datetime, /) -> str:
    assert value.tzinfo is datetime.timezone.utc
    return value.isoformat(sep=' ')


sqlite3.register_adapter(datetime.datetime, adapt_datetime)


@REGISTRY.mapped
class File:

    __tablename__ = 'file'

    location = sa.Column(sa.Text, sa.CheckConstraint("location != ''"),
                         primary_key=True)

    md5sum = sa.Column(sa.Text, sa.CheckConstraint('length(md5sum) = 32'),
                       index=True)
    size = sa.Column(sa.Integer, sa.CheckConstraint('size >= 0'),
                     nullable=False)
    mtime = sa.Column(sa.DateTime, nullable=False)
    # sqlite3 string funcs cannot right extract
    # denormalize so we can query for name/extension
    name = sa.Column(sa.Text, sa.CheckConstraint("name != ''"), nullable=False)
    ext = sa.Column(sa.Text, nullable=False)

    __table_args__ = (sa.CheckConstraint('substr(location, -length(name))'
                                         ' = name'),
                      sa.CheckConstraint("ext = '' OR"
                                         ' substr(location, -length(ext))'
                                         ' = ext'),)

    def __repr__(self):
        return f'<{self.__class__.__name__} {self.location!r}>'


def iterfilepaths(top: os.PathLike[str] | str, /, *,
                  verbose: bool = False) -> Iterator[pathlib.Path]:
    stack = [pathlib.Path(top)]
    while stack:
        root = stack.pop()
        if verbose:
            print(root)
        try:
            dentries = os.scandir(root)
        except OSError:
            continue
        dirs = []
        for d in dentries:
            if d.is_dir():
                dirs.append(d.path)
            else:
                yield pathlib.Path(d)
        stack.extend(reversed(dirs))


def make_hash(filepath: os.PathLike[str] | str, /, *,
              bufsize: int = 32_768) -> hashlib._hashlib.HASH:
    result = hashlib.md5()
    with open(filepath, mode='rb') as f:
        for data in iter(functools.partial(f.read, bufsize), b''):
            result.update(data)
    return result


def build_db(engine: sa.engine.Engine = ENGINE, /, *,
             start: os.PathLike[str] | str = START_DIR,
             recreate: bool = False,
             verbose: bool = False) -> None:
    db_path = pathlib.Path(engine.url.database)
    if db_path.exists():
        if not recreate:
            return
        db_path.unlink()

    REGISTRY.metadata.create_all(engine)

    with engine.begin() as conn:
        conn.execute(sa.text('PRAGMA synchronous = OFF'))
        conn.execute(sa.text('PRAGMA journal_mode = MEMORY'))
        insert_fileinfos(conn, start, verbose=verbose)

    with engine.begin() as conn:
        add_md5sums(conn, start, verbose=verbose)


def insert_fileinfos(conn: sa.engine.Connection, /, start: os.PathLike[str] | str, *,
                     verbose: bool) -> None:
    cols = [f.name for f in File.__table__.columns if f.name != 'md5sum']
    insert_file = sa.insert(File).compile(bind=conn, column_keys=cols)
    assert not insert_file.positional
    get_params = functools.partial(get_file_params, start)
    iterparams = map(get_params, iterfilepaths(start, verbose=verbose))

    # https://docs.python.org/3/library/sqlite3.html#sqlite3.Cursor.executemany
    conn.connection.executemany(insert_file.string, iterparams)


def add_md5sums(conn: sa.engine.Connection, /, start: os.PathLike[str] | str, *,
                verbose: bool) -> None:
    start = pathlib.Path(start)

    select_duped_sizes = (sa.select(File.size)
                          .group_by(File.size)
                          .having(sa.func.count() > 1))

    query = (sa.select(File.location)
             .where(File.size.in_(select_duped_sizes))
             .order_by(File.location))

    update_file = (sa.update(File)
                   .where(File.location == sa.bindparam('loc'))
                   .values(md5sum=sa.bindparam('md5sum')))

    for location, in conn.execute(query):
        if verbose:
            print(location)
        md5 = make_hash(start / location)
        params = {'loc': location, 'md5sum': md5.hexdigest()}
        conn.execute(update_file, params)


def get_duplicates_query(by_location: bool = False, /) -> sa.sql.Select:
    select_duped_md5sums = (sa.select(File.md5sum)
                            .group_by(File.md5sum)
                            .having(sa.func.count() > 1))

    query = sa.select(File).where(File.md5sum.in_(select_duped_md5sums))
    order_by = [File.location] if by_location else [File.md5sum, File.location]
    return query.order_by(*order_by)


def to_csv(result: sa.engine.Result, /, filepath: os.PathLike[str] | str = OUT_PATH, *,
           dialect: csv.Dialect | type[csv.Dialect] | str = csv.excel,
           encoding: str = 'utf-8') -> None:
    with open(filepath, mode='w', encoding=encoding, newline='') as f:
        writer = csv.writer(f, dialect=dialect)
        writer.writerow(result.keys())
        writer.writerows(result)


def main() -> str | None:
    build_db(recreate=False)
    query = get_duplicates_query()
    with ENGINE.connect() as conn:
        result = conn.execute(query)
        to_csv(result)
    return None


if __name__ == '__main__':
    sys.exit(main())
