=================
Examples of usage
=================

Key-value
---------

Open connection
===============

.. literalinclude:: ../examples/get_and_put.py
  :language: python
  :lines: 16-23

Create cache
============

.. literalinclude:: ../examples/get_and_put.py
  :language: python
  :lines: 25-28

Put value in cache
==================

.. literalinclude:: ../examples/get_and_put.py
  :language: python
  :lines: 30-31

Get value from cache
====================

.. literalinclude:: ../examples/get_and_put.py
  :language: python
  :lines: 33-37

List keys in cache
==================

.. literalinclude:: ../examples/get_and_put.py
  :language: python
  :lines: 39-40

Type hints usage
================

.. literalinclude:: ../examples/type_hints.py
  :language: python
  :lines: 30-49

Scan queries
============

Scan queries allows you to browse cache contents with pagination.

.. literalinclude:: ../examples/scans.py
  :language: python
  :lines: 30-31, 45-58

Subsequent scans could be made using cursor ID.

.. literalinclude:: ../examples/scans.py
  :language: python
  :lines: 60-73

When cursor have no more data, it gets automatically destroyed.

.. literalinclude:: ../examples/scans.py
  :language: python
  :lines: 75-77

If your cursor still holds some data, but you have no use of it anymore,
you may destroy it manually.

.. literalinclude:: ../examples/scans.py
  :language: python
  :lines: 79

Do cleanup
==========

Destroy created cache and close connection.

.. literalinclude:: ../examples/get_and_put.py
  :language: python
  :lines: 42-43

.. _sql_examples:

SQL
---

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

Complex objects
---------------

Reading
=======

`Complex object`_ (that is often called ‘Binary object’) is used to represent
user-defined complex data types. It have the following features:

- have a unique ID,
- have an associated schema, that describes its inner structure (the order
  and types of its fields).

The schemas are stored in Ignite metadata storage. That is why Complex object
must be registered with the Ignite cluster before use.

The most obvious example of Complex object usage is SQL tables.

In the :ref:`previous examples <sql_examples>` we have created some SQL tables.
Let us do it again and examine the Ignite storage afterwards.

.. literalinclude:: ../examples/binary_types.py
  :language: python
  :lines: 251-259

We can see that Ignite created a cache for each of our table. The caches are
conveniently named using ‘`SQL_<schema name>_<table name>`’ pattern.

Now let us examine a configuration of a cache that contains SQL data
using a :py:func:`~pyignite.api.cache_config.cache_get_configuration` function.

.. literalinclude:: ../examples/binary_types.py
  :language: python
  :lines: 261-287

The values of `value_type_name` and `key_type_name` are names of the binary
types, in which the `Cities` table rows' values and keys are stored. Let us
check the types' registration and properties.

.. literalinclude:: ../examples/binary_types.py
  :language: python
  :lines: 289-307

Let us take a closer look to the value type.

.. literalinclude:: ../examples/binary_types.py
  :language: python
  :lines: 309-332

We have 3 fields in the row value: `Name`, `District`, and `Population`.
The complex primary key field, `ID` + `CountryCode`, is in the row key.

To support this theory let us try to read the data without using SQL
functions.

.. literalinclude:: ../examples/binary_types.py
  :language: python
  :lines: 334-339

Not exactly what we expected. That's because the Binary objects are always
come wrapped in a content-agnostic
:class:`~pyignite.datatypes.complex.WrappedDataObject`. We need to take
an additional step to explicitly decode it.

.. literalinclude:: ../examples/binary_types.py
  :language: python
  :lines: 341-362

Creating
========

Now, that we aware of the internal structure of the Ignite SQL storage,
we can create a table and put data in it using only key-value functions.

For example, let us create a table to register High School students:
a rough equivalent of the following SQL DDL statement:

::

    CREATE TABLE Student (
        sid CHAR(9),
        name VARCHAR(20),
        login CHAR(8),
        age INTEGER(11),
        gpa REAL
    )

These are the necessary steps to perform the task.

1. Create scheme cache.

.. literalinclude:: ../examples/create_binary.py
  :language: python
  :lines: 30

2. Create table cache.

.. literalinclude:: ../examples/create_binary.py
  :language: python
  :lines: 32-83

3. Register binary type.

.. literalinclude:: ../examples/create_binary.py
  :language: python
  :lines: 85-97

4. Insert row.

.. literalinclude:: ../examples/create_binary.py
  :language: python
  :lines: 99-116

Now read the row using an SQL function.

.. literalinclude:: ../examples/create_binary.py
  :language: python
  :lines: 118-134

.. _Getting Started: https://apacheignite-sql.readme.io/docs/getting-started
.. _Ignite GitHub repository: https://github.com/apache/ignite/blob/master/examples/sql/world.sql
.. _Complex object: https://apacheignite.readme.io/v2.5/docs/binary-client-protocol-data-format#section-complex-object
