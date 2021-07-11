#!/usr/bin/env python3
# duplicates.py - duplicate file report as csv

"""Duplicate file CSV report via SQLite database with file infos."""

import csv
import datetime
import functools
import hashlib
import os
import pathlib

import sqlalchemy as sa
import sqlalchemy.ext.declarative

__title__ = 'duplicates.py'
__author__ = 'Sebastian Bank <sebastian.bank@uni-leipzig.de>'
__license__ = 'MIT, see LICENSE.txt'
__copyright__ = 'Copyright (c) 2014,2017 Sebastian Bank'

START_DIR = pathlib.Path('.')

DB_PATH = pathlib.Path('duplicates.sqlite3')

OUT_PATH = pathlib.Path('duplicates.csv')

ENGINE = sa.create_engine(f'sqlite:///{DB_PATH}',
                          paramstyle='named', echo=False)


def get_file_params(start, dentry):
    path = pathlib.Path(dentry)
    return {'location': path.relative_to(start).as_posix(),
            'name': path.name,
            'ext': path.suffix,
            'size': dentry.stat().st_size,
            'mtime': datetime.datetime.fromtimestamp(dentry.stat().st_mtime)}


class File(sa.ext.declarative.declarative_base()):

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


def iterfilepaths(top, *, verbose=False):
    stack = [top]
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
        stack.extend(dirs[::-1])


def make_hash(filepath, *, hash_func=hashlib.md5, bufsize=32_768):
    result = hash_func()
    with filepath.open('rb') as f:
        for data in iter(functools.partial(f.read, bufsize), b''):
            result.update(data)
    return result


def build_db(engine=ENGINE, *, start=START_DIR, recreate=False, verbose=False):
    db_path = pathlib.Path(engine.url.database)
    if db_path.exists():
        if not recreate:
            return
        db_path.unlink()

    File.metadata.create_all(engine)

    with engine.begin() as conn:
        conn.execute('PRAGMA synchronous = OFF')
        conn.execute('PRAGMA journal_mode = MEMORY')
        insert_fileinfos(conn, start, verbose=verbose)

    with engine.begin() as conn:
        conn = conn.execution_options(compiled_cache={})
        add_md5sums(conn, start, verbose=verbose)


def insert_fileinfos(conn, start, *, verbose):
    cols = [f.name for f in File.__table__.columns if f.name != 'md5sum']
    insert_file = sa.insert(File, bind=conn).compile(column_keys=cols)
    assert not insert_file.positional
    get_params = functools.partial(get_file_params, start)
    iterparams = map(get_params, iterfilepaths(start, verbose=verbose))

    conn.connection.executemany(insert_file.string, iterparams)


def add_md5sums(conn, start, *, verbose):
    select_duped_sizes = sa.select([File.size])\
                         .group_by(File.size)\
                         .having(sa.func.count() > 1)

    query = sa.select([File.location])\
            .where(File.size.in_(select_duped_sizes))\
            .order_by(File.location)

    update_file = sa.update(File, bind=conn)\
                  .where(File.location == sa.bindparam('loc'))\
                  .values(md5sum=sa.bindparam('md5sum'))

    for location, in conn.execute(query):
        if verbose:
            print(location)
        md5 = make_hash(start / location)
        params = {'loc': location, 'md5sum': md5.hexdigest()}
        conn.execute(update_file, params)


def duplicates_query(by_location=False):
    select_duped_md5sums = sa.select([File.md5sum])\
                           .group_by(File.md5sum)\
                           .having(sa.func.count() > 1)

    query = sa.select([File]).where(File.md5sum.in_(select_duped_md5sums))
    order_by = [File.location] if by_location else [File.md5sum, File.location]
    return query.order_by(*order_by)


def to_csv(result, filepath=OUT_PATH, *, encoding='utf-8', dialect=csv.excel):
    with filepath.open('w', encoding=encoding, newline='') as f:
        writer = csv.writer(f, dialect=dialect)
        writer.writerow(result.keys())
        writer.writerows(result)


if __name__ == '__main__':
    build_db(recreate=False)
    query = duplicates_query()
    to_csv(ENGINE.execute(query))
