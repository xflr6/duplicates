# duplicates.py - duplicate file report as csv

"""Duplicate file CSV report via SQLite database with file infos."""

from __future__ import unicode_literals, print_function

import io
import os
import csv
import sys
import hashlib
import datetime
import functools

try:
    from itertools import imap as map
except ImportError:
    map = map

try:
    from os import scandir
except ImportError:
    from scandir import scandir

import sqlalchemy as sa
import sqlalchemy.ext.declarative

__author__ = 'Sebastian Bank <sebastian.bank@uni-leipzig.de>'
__license__ = 'MIT, see LICENSE'
__copyright__ = 'Copyright (c) 2014,2017 Sebastian Bank'

STARTDIR = os.curdir

DBFILE = 'duplicates.sqlite3'
OUTFILE = 'duplicates.csv'

PY2 = sys.version_info < (3,)

engine = sa.create_engine('sqlite:///%s' % DBFILE, paramstyle='named')


class File(sa.ext.declarative.declarative_base()):

    __tablename__ = 'file'

    location = sa.Column(sa.Text, primary_key=True)
    md5sum = sa.Column(sa.Text, index=True)
    size = sa.Column(sa.Integer, nullable=False)
    mtime = sa.Column(sa.DateTime, nullable=False)
    # sqlite3 string funcs cannot right extract
    # denormalize so we can query for name/extension
    name = sa.Column(sa.Text, nullable=False)
    ext = sa.Column(sa.Text, nullable=False)

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
            dentries = scandir(root)
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
    with io.open(filename, 'rb') as fd:
        while True:
            data = fd.read(bufsize)
            if not data:
                break
            m.update(data)
    return m.hexdigest()


def build_db(engine=engine, start=STARTDIR, recreate=False, verbose=False):
    dbfile = engine.url.database
    if os.path.exists(dbfile):
        if not recreate:
            return
        os.remove(dbfile)

    File.metadata.create_all(engine)

    with engine.begin() as conn:
        insert_fileinfos(conn, start, verbose)

    with engine.begin() as conn:
        add_md5sums(conn, start, verbose)


def insert_fileinfos(conn, start, verbose):
    conn.execute('PRAGMA synchronous = OFF')
    conn.execute('PRAGMA journal_mode = MEMORY')
    conn = conn.execution_options(compiled_cache={})

    assert conn.engine.dialect.paramstyle == 'named'
    cols = [f.name for f in File.__table__.columns if f.name != 'md5sum']
    insert_file = sa.insert(File, bind=conn).compile(column_keys=cols).string
    get_params = functools.partial(File.get_infos, start)
    iterparams = map(get_params, iterfiles(start))

    conn.connection.executemany(insert_file, iterparams)


def add_md5sums(conn, start, verbose):
    conn = conn.execution_options(compiled_cache={})
    query = sa.select([File.location])\
        .where(File.size.in_(sa.select([File.size])
            .group_by(File.size).having(sa.func.count() > 1)))\
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
    query = sa.select([File])\
        .where(File.md5sum.in_(sa.select([File.md5sum])
            .group_by(File.md5sum).having(sa.func.count() > 1)))
    if by_location:
        query = query.order_by(File.location)
    else:
        query = query.order_by(File.md5sum, File.location)
    return query


def to_csv(results, filename=OUTFILE, encoding='utf-8', dialect='excel'):
    if PY2:
        open_kwargs = {'mode': 'wb'}

        def writerow(writer, row, encoding):
            writer.writerow([('%s' % r).encode(encoding) for r in row])
    else:
        open_kwargs = {'mode': 'w', 'encoding': encoding, 'newline': ''}

        def writerow(writer, row, encoding):
            writer.writerow(row)

    with io.open(filename, **open_kwargs) as fd:
        csvwriter = csv.writer(fd, dialect=dialect)
        writerow(csvwriter, results.keys(), encoding)
        for row in results:
            writerow(csvwriter, row, encoding)


if __name__ == '__main__':
    build_db(recreate=False)
    query = duplicates_query()
    to_csv(engine.execute(query))
