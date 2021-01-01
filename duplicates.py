#!/usr/bin/env python
# duplicates.py - duplicate file report as csv

"""Duplicate file CSV report via SQLite database with file infos."""

import csv
import datetime
import functools
import hashlib
import os

import sqlalchemy as sa
import sqlalchemy.ext.declarative

__title__ = 'duplicates.py'
__author__ = 'Sebastian Bank <sebastian.bank@uni-leipzig.de>'
__license__ = 'MIT, see LICENSE.txt'
__copyright__ = 'Copyright (c) 2014,2017 Sebastian Bank'

STARTDIR = '.'

DBFILE = 'duplicates.sqlite3'

OUTFILE = 'duplicates.csv'

ENGINE = sa.create_engine('sqlite:///%s' % DBFILE,
                          paramstyle='named', echo=False)


class File(sa.ext.declarative.declarative_base()):

    __tablename__ = 'file'

    location = sa.Column(sa.Text, sa.CheckConstraint("location != ''"), primary_key=True)

    md5sum = sa.Column(sa.Text, sa.CheckConstraint('length(md5sum) = 32'), index=True)
    size = sa.Column(sa.Integer, sa.CheckConstraint('size >= 0'), nullable=False)
    mtime = sa.Column(sa.DateTime, nullable=False)
    # sqlite3 string funcs cannot right extract
    # denormalize so we can query for name/extension
    name = sa.Column(sa.Text, sa.CheckConstraint("name != ''"), nullable=False)
    ext = sa.Column(sa.Text, nullable=False)

    __table_args__ = (
        sa.CheckConstraint('substr(location, -length(name)) = name'),
        sa.CheckConstraint("ext = '' OR substr(location, -length(ext)) = ext"),
    )

    @staticmethod
    def get_infos(start, dentry):
        return {
            'location': os.path.relpath(dentry.path, start).replace('\\', '/'),
            'name': dentry.name,
            'ext': os.path.splitext(dentry.name)[1].lstrip('.'),
            'size': dentry.stat().st_size,
            'mtime': datetime.datetime.fromtimestamp(dentry.stat().st_mtime),
        }

    def __repr__(self):
        return '<%s %r>' % (self.__class__.__name__, self.location)


def iterfiles(top, verbose=False):
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
                yield d
        stack.extend(dirs[::-1])


def md5sum(filename, bufsize=32768):
    m = hashlib.md5()
    with open(filename, 'rb') as f:
        for data in iter(functools.partial(f.read, bufsize), b''):
            m.update(data)
    return m.hexdigest()


def build_db(engine=ENGINE, start=STARTDIR, recreate=False, verbose=False):
    dbfile = engine.url.database
    if os.path.exists(dbfile):
        if not recreate:
            return
        os.remove(dbfile)

    File.metadata.create_all(engine)

    with engine.begin() as conn:
        conn.execute('PRAGMA synchronous = OFF')
        conn.execute('PRAGMA journal_mode = MEMORY')
        insert_fileinfos(conn, start, verbose=verbose)

    with engine.begin() as conn:
        conn = conn.execution_options(compiled_cache={})
        add_md5sums(conn, start, verbose=verbose)


def insert_fileinfos(conn, start, verbose):
    cols = [f.name for f in File.__table__.columns if f.name != 'md5sum']
    insert_file = sa.insert(File, bind=conn).compile(column_keys=cols)
    assert not insert_file.positional
    get_params = functools.partial(File.get_infos, start)
    iterparams = map(get_params, iterfiles(start, verbose=verbose))

    conn.connection.executemany(insert_file.string, iterparams)


def add_md5sums(conn, start, verbose):
    select_duped_sizes = sa.select([File.size])\
                         .group_by(File.size)\
                         .having(sa.func.count() > 1)

    query = sa.select([File.location])\
            .where(File.size.in_(select_duped_sizes))\
            .order_by(File.location)

    update_file = sa.update(File, bind=conn)\
                  .where(File.location == sa.bindparam('loc'))\
                  .values(md5sum=sa.bindparam('md5sum')).execute

    for location, in conn.execute(query):
        if verbose:
            print(location)
        digest = md5sum(os.path.join(start, location))
        update_file(loc=location, md5sum=digest)


def duplicates_query(by_location=False):
    select_duped_md5sums = sa.select([File.md5sum])\
                           .group_by(File.md5sum)\
                           .having(sa.func.count() > 1)

    query = sa.select([File]).where(File.md5sum.in_(select_duped_md5sums))
    if by_location:
        query.append_order_by(File.location)
    else:
        query.append_order_by(File.md5sum, File.location)
    return query


def to_csv(result, filename=OUTFILE, encoding='utf-8', dialect=csv.excel):
    with open(filename, 'w', encoding=encoding, newline='') as f:
        writer = csv.writer(f, dialect=dialect)
        writer.writerow(result.keys())
        writer.writerows(result)


if __name__ == '__main__':
    build_db(recreate=False)
    query = duplicates_query()
    to_csv(ENGINE.execute(query))
