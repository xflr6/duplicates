Duplicates
==========

This Python 2/3 script walks the current directory creating an SQLite_ database
with the path, size, and mtime for each file. After computing and adding the
md5 checksum for all duplicate candidates (files with the same size), it
generates a CSV_ giving the following infos for each file that has the same
content (md5sum_) as another file:

- ``location`` (path relative to the initial directory)
- ``md5sum``, ``size``, and ``mtime``
- ``name`` (basename) and ``ext`` (file extension)


Dependencies
------------

- Python_ 2.7 or 3.4+
- SQLAlchemy_


.. _SQLite: https://www.sqlite.org
.. _CSV: https://en.wikipedia.org/wiki/Comma-separated_values
.. _md5sum: https://en.wikipedia.org/wiki/Md5sum
.. _Python: https://www.python.org
.. _SQLAlchemy: https://www.sqlalchemy.org/
