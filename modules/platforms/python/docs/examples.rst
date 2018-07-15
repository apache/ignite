=================
Examples of usage
=================

Open connection
---------------

.. literalinclude:: ../examples/get_and_put.py
  :language: python
  :lines: 16-23

Create cache
------------

.. literalinclude:: ../examples/get_and_put.py
  :language: python
  :lines: 25-28

Put value in cache
------------------

.. literalinclude:: ../examples/get_and_put.py
  :language: python
  :lines: 30-31

Get value from cache
--------------------

.. literalinclude:: ../examples/get_and_put.py
  :language: python
  :lines: 33-37

List keys in cache
------------------

.. literalinclude:: ../examples/get_and_put.py
  :language: python
  :lines: 39-40

Type hints usage
----------------

.. literalinclude:: ../examples/type_hints.py
  :language: python
  :lines: 30-49

Scan queries
------------

Scan queries allows you to browse cache contents with pagination.

.. literalinclude:: ../examples/scans.py
  :language: python
  :lines: 30-31, 45-58

Subsequent scans could be made using cursor ID.

.. literalinclude:: ../examples/scans.py
  :language: python
  :lines: 60-73

When cursor have no more data, it automatically destroys.

.. literalinclude:: ../examples/scans.py
  :language: python
  :lines: 75-77

If your cursor still holds some data, but you have no use of it anymore,
you may destroy it manually.

.. literalinclude:: ../examples/scans.py
  :language: python
  :lines: 79

Inspect cache configuration
---------------------------

.. literalinclude:: ../examples/cache_config.py
  :language: python
  :lines: 30-63

Create cache with a certain configuration
-----------------------------------------
You must supply at least cache name.

.. literalinclude:: ../examples/cache_config.py
  :language: python
  :lines: 66-78

Do cleanup
----------

Destroy created cache and close connection.

.. literalinclude:: ../examples/get_and_put.py
  :language: python
  :lines: 42-43

SQL examples
------------

This examples are similar to the ones given in the Apache Ignite SQL
Documentation: `Getting Started`_.

Setup
=====

First let us establish a connection and create a database schema.

.. literalinclude:: ../examples/sql.py
  :language: python
  :lines: 26, 200-206

Then create tables. Begin with `Country` table, than proceed with related
tables `City` and `CountryLanguage`.

.. literalinclude:: ../examples/sql.py
  :language: python
  :lines: 32-49, 58-66, 74-81, 208-214

Create indexes.

.. literalinclude:: ../examples/sql.py
  :language: python
  :lines: 67-69, 82-84, 216-218

Fill tables with data.

.. literalinclude:: ../examples/sql.py
  :language: python
  :lines: 50-57, 70-73, 85-88, 220-246

Data samples is taken from `Ignite GitHub repository`_.

That concludes the preparation of data. Now let us answer some questions.

What are the 10 largest cities in our data sample (population-wise)?
====================================================================

.. literalinclude:: ../examples/sql.py
  :language: python
  :lines: 24, 248-267

We were happy with :py:func:`~pyignite.api.sql.sql_fields` so far. But this
time we configured `PAGE_SIZE` to be 5, but requested 10 rows in the query.
To get the rest of the rows we should use
:py:func:`~pyignite.api.sql.sql_fields_cursor_get_page` repeatedly.

.. literalinclude:: ../examples/sql.py
  :language: python
  :lines: 269-282

What are the 10 most populated cities throughout the 3 chosen countries?
========================================================================

.. literalinclude:: ../examples/sql.py
  :language: python
  :lines: 285-326

Display all the information about a given city
==============================================

.. literalinclude:: ../examples/sql.py
  :language: python
  :lines: 329-351

Finally, delete the tables used in this example with the following queries:

.. literalinclude:: ../examples/sql.py
  :language: python
  :lines: 89-90, 354-369

.. _Getting Started: https://apacheignite-sql.readme.io/docs/getting-started
.. _Ignite GitHub repository: https://github.com/apache/ignite/blob/master/examples/sql/world.sql