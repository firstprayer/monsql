========================
API reference for MonSQL
========================

.. py:currentmodule:: monsql

There're several basic concepts in MonSQL:

The MonSQL API is organised as follows:

:py:class:`~monsql.db.Database`:
   The :py:class:`~monsql.db.Database` manages operations such as create or drop tables, truncate table, return a table object. It has several subclasses implemented for different databases. A Database object should always be obtained through :py:function:'MonSQL':

   >>> db = MonSQL(host='localhost', port=3306, dbtype=DB_TYPES.MYSQL)

:py:class:`~monsql.table.Table`:
   The :py:class:`monsql.table.Table` class is the main class for interacting with data in
   tables. This class offers methods for data retrieval and data manipulation.
   Instances of this class can be obtained using the
   :py:meth:`Database.get()` method.

:py:class:`~monsql.queryset.QuerySet`:
   The :py:class:`QuerySet` class is a lazy-load object for representing data. A QuerySet object can turn into a new QuerySet object by adding filter, setting limits, etc. Read all rows in a query set is simple, just:

   >>> queryset = table.find(...)
   >>> for row in queryset:
   >>>     # do something

:py:class:`~monsql.queryset.DataRow`:
   A wrapper class for more readable codes. It works like this:

   >>> queryset = table.find(filter={'id': {'$gt': 10}}, fields=['id', 'name'])
   >>> for row in queryset:
   >>>     print row.id, print row.name

Database
========

.. autoclass:: monsql.db.Database


Table
=====

.. autoclass:: monsql.table.Table


QuerySet
========

.. autoclass:: monsql.queryset.QuerySet

DataRow
=======

.. autoclass:: monsql.queryset.DataRow


.. vim: set spell spelllang=en: