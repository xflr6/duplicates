# duplicates.py - duplicate file report as csv

"""Duplicate file CSV report via SQLite database with file infos."""

from __future__ import unicode_literals, print_function

import io
import os
import csv
import sys
import hashlib
import datetime

import sqlalchemy as sa
import sqlalchemy.ext.declarative

__author__ = 'Sebastian Bank <sebastian.bank@uni-leipzig.de>'
__license__ = 'MIT, see LICENSE'
__copyright__ = 'Copyright (c) 2014,2017 Sebastian Bank'

STARTDIR = os.curdir
FSENCODING = 'latin-1'
DBFILE = 'duplicates.sqlite3'
OUTFILE = 'duplicates.csv'

PY2 = sys.version_info < (3,)

engine = sa.create_engine('sqlite:///%s' % DBFILE)


class File(sa.ext.declarative.declarative_base()):

    __tablename__ = 'file'

    location = sa.Column(sa.Text, primary_key=True)
    md5sum = sa.Column(sa.Text, index=True)
    size = sa.Column(sa.Integer, nullable=False)
    mtime = sa.Column(sa.DateTime, nullable=False)
    # sqlite3 string funcs cannot right extract, denormalize
    name = sa.Column(sa.Text, nullable=False)
    ext = sa.Column(sa.Text, nullable=False)

    @staticmethod
    def getinfo(start, path, encoding=FSENCODING):
        if PY2:
            path = path.decode(encoding)
        location = os.path.relpath(path, start).replace('\\', '/')
        name = os.path.basename(path)
        ext = os.path.splitext(name)[1].lstrip('.')
        statinfo = os.stat(path)
        size = statinfo.st_size
        mtime = datetime.datetime.fromtimestamp(statinfo.st_mtime)
        return {'location': location, 'name': name, 'ext': ext, 'size': size, 'mtime': mtime}

    @classmethod
    def from_path(cls, start, path, encoding=FSENCODING):
        kwargs = cls.getinfo(start, path, encoding=encoding)
        return cls(**kwargs)

    def __repr__(self):
        return '<%s %r>' % (self.__class__.__name__, self.location)


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
    conn = conn.execution_options(compiled_cache={})
    insert_file = sa.insert(File, bind=conn).execute
    for root, dirs, files in os.walk(start):
        if verbose:
            print(root)
        params = [File.getinfo(start, os.path.join(root, f)) for f in files]
        if params:
            insert_file(params)


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
    build_db()
    query = duplicates_query()
    to_csv(engine.execute(query))
