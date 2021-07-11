Duplicates
==========

|Build| |Codecov|

This Python script walks the current directory creating an SQLite_ database
with the path, size, and mtime for each file. After computing and adding the
md5 checksum for all duplicate candidates (files with the same byte size), it
generates a CSV_ giving the following infos for each file that has the same
content (md5sum_) as another file:

- ``location`` (path relative to the initial directory)
- ``md5sum``, ``size``, and ``mtime``
- ``name`` (basename) and ``ext`` (file extension)


Dependencies
------------

- Python_ 3.6+
- SQLAlchemy_


.. _SQLite: https://www.sqlite.org
.. _CSV: https://en.wikipedia.org/wiki/Comma-separated_values
.. _md5sum: https://en.wikipedia.org/wiki/Md5sum
.. _Python: https://www.python.org
.. _SQLAlchemy: https://www.sqlalchemy.org/

.. |Build| image:: https://github.com/xflr6/duplicates/actions/workflows/build.yaml/badge.svg
    :target: https://github.com/xflr6/duplicates/actions/workflows/build.yaml?query=branch%3Amaster
    :alt: Build
.. |Codecov| image:: https://codecov.io/gh/xflr6/duplicates/branch/master/graph/badge.svg
    :target: https://codecov.io/gh/xflr6/duplicates
    :alt: Codecov
