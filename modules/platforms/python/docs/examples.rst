..  Copyright 2019 GridGain Systems, Inc. and Contributors.

..  Licensed under the GridGain Community Edition License (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

..      https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license

..  Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.

.. _examples_of_usage:

=================
Examples of usage
=================
File: `get_and_put.py`_.

Key-value
---------

Open connection
===============

.. literalinclude:: ../examples/get_and_put.py
  :language: python
  :lines: 16-19

.. _create_cache:

Create cache
============

.. literalinclude:: ../examples/get_and_put.py
  :language: python
  :lines: 21

Put value in cache
==================

.. literalinclude:: ../examples/get_and_put.py
  :language: python
  :lines: 23

Get value from cache
====================

.. literalinclude:: ../examples/get_and_put.py
  :language: python
  :lines: 25-29

Get multiple values from cache
==============================

.. literalinclude:: ../examples/get_and_put.py
  :language: python
  :lines: 31-36

Type hints usage
================
File: `type_hints.py`_

.. literalinclude:: ../examples/type_hints.py
  :language: python
  :lines: 24-48

As a rule of thumb:

- when a `pygridgain` method or function deals with a single value or key, it
  has an additional parameter, like `value_hint` or `key_hint`, which accepts
  a parser/constructor class,

- nearly any structure element (inside dict or list) can be replaced with
  a two-tuple of (said element, type hint).

Refer the :ref:`data_types` section for the full list
of parser/constructor classes you can use as type hints.

Scan
====
File: `scans.py`_.

Cache's :py:meth:`~pygridgain.cache.Cache.scan` method queries allows you
to get the whole contents of the cache, element by element.

Let us put some data in cache.

.. literalinclude:: ../examples/scans.py
  :language: python
  :lines: 23-33

:py:meth:`~pygridgain.cache.Cache.scan` returns a generator, that yields
two-tuples of key and value. You can iterate through the generated pairs
in a safe manner:

.. literalinclude:: ../examples/scans.py
  :language: python
  :lines: 34-41

Or, alternatively, you can convert the generator to dictionary in one go:

.. literalinclude:: ../examples/scans.py
  :language: python
  :lines: 44-52

But be cautious: if the cache contains a large set of data, the dictionary
may eat too much memory!

Do cleanup
==========

Destroy created cache and close connection.

.. literalinclude:: ../examples/scans.py
  :language: python
  :lines: 54-55

.. _sql_examples:

SQL
---
File: `sql.py`_.

These examples are similar to the ones given in the GridGain SQL
Documentation: `Getting Started`_.

Setup
=====

First let us establish a connection.

.. literalinclude:: ../examples/sql.py
  :language: python
  :lines: 195-196

Then create tables. Begin with `Country` table, than proceed with related
tables `City` and `CountryLanguage`.

.. literalinclude:: ../examples/sql.py
  :language: python
  :lines: 25-42, 51-59, 67-74, 199-204

Create indexes.

.. literalinclude:: ../examples/sql.py
  :language: python
  :lines: 60-62, 75-77, 207-208

Fill tables with data.

.. literalinclude:: ../examples/sql.py
  :language: python
  :lines: 43-50, 63-66, 78-81, 211-218

Data samples are taken from `GridGain CE GitHub repository`_.

That concludes the preparation of data. Now let us answer some questions.

What are the 10 largest cities in our data sample (population-wise)?
====================================================================

.. literalinclude:: ../examples/sql.py
  :language: python
  :lines: 24, 221-238

The :py:meth:`~pygridgain.client.Client.sql` method returns a generator,
that yields the resulting rows.

What are the 10 most populated cities throughout the 3 chosen countries?
========================================================================

If you set the `include_field_names` argument to `True`, the
:py:meth:`~pygridgain.client.Client.sql` method will generate a list of
column names as a first yield. You can access field names with Python built-in
`next` function.

.. literalinclude:: ../examples/sql.py
  :language: python
  :lines: 241-269

Display all the information about a given city
==============================================

.. literalinclude:: ../examples/sql.py
  :language: python
  :lines: 272-290

Finally, delete the tables used in this example with the following queries:

.. literalinclude:: ../examples/sql.py
  :language: python
  :lines: 82-83, 293-298

.. _complex_object_usage:

Complex objects
---------------
File: `binary_basics.py`_.

`Complex object`_ (that is often called ‘Binary object’) is a GridGain data
type, that is designed to represent a Java class. It have the following
features:

- have a unique ID (type id), which is derives from a class name (type name),
- have one or more associated schemas, that describes its inner structure (the
  order, names and types of its fields). Each schema have its own ID,
- have an optional version number, that is aimed towards the end users
  to help them distinguish between objects of the same type, serialized
  with different schemas.

Unfortunately, these distinctive features of the Complex object have few to no
meaning outside of Java language. Python class can not be defined by its name
(it is not unique), ID (object ID in Python is volatile; in CPython it is just
a pointer in the interpreter's memory heap), or complex of its fields (they
do not have an associated data types, moreover, they can be added or deleted
in run-time). For the `pygridgain` user it means that for all purposes
of storing native Python data it is better to use GridGain
:class:`~pygridgain.datatypes.complex.CollectionObject`
or :class:`~pygridgain.datatypes.complex.MapObject` data types.

However, for interoperability purposes, `pygridgain` has a mechanism of creating
special Python classes to read or write Complex objects. These classes have
an interface, that simulates all the features of the Complex object: type name,
type ID, schema, schema ID, and version number.

Assuming that one concrete class for representing one Complex object can
severely limit the user's data manipulation capabilities, all the
functionality said above is implemented through the metaclass:
:class:`~pygridgain.binary.GenericObjectMeta`. This metaclass is used
automatically when reading Complex objects.

.. literalinclude:: ../examples/binary_basics.py
  :language: python
  :lines: 18-20, 30-34, 39-42, 48-49

Here you can see how :class:`~pygridgain.binary.GenericObjectMeta` uses
`attrs`_ package internally for creating nice `__init__()` and `__repr__()`
methods.

In this case the autogenerated dataclass's name `Person` is exactly matches
the type name of the Complex object it represents (the content of the
:py:attr:`~pygridgain.datatypes.base.GridGainDataTypeProps.type_name`
property). But when Complex object's class name contains characters, that
can not be used in a Python identifier, for example:

- `.`, when fully qualified Java class names are used,
- `$`, a common case for Scala classes,
- `+`, internal class name separator in C#,

then `pygridgain` can not maintain this match. In such cases `pyignite` tries
to sanitize a type name to derive a “good” dataclass name from it.

If your code needs consistent naming between the server and the client, make
sure that your GridGain cluster is configured to use `simple class names`_.

Anyway, you can reuse the autogenerated dataclass for subsequent writes:

.. literalinclude:: ../examples/binary_basics.py
  :language: python
  :lines: 53, 34-37

:class:`~pygridgain.binary.GenericObjectMeta` can also be used directly
for creating custom classes:

.. literalinclude:: ../examples/binary_basics.py
  :language: python
  :lines: 22-27

Note how the `Person` class is defined. `schema` is a
:class:`~pygridgain.binary.GenericObjectMeta` metaclass parameter.
Another important :class:`~pygridgain.binary.GenericObjectMeta` parameter
is a `type_name`, but it is optional and defaults to the class name (‘Person’
in our example).

Note also, that `Person` do not have to define its own attributes, methods and
properties (`pass`), although it is completely possible.

Now, when your custom `Person` class is created, you are ready to send data
to GridGain server using its objects. The client will implicitly register your
class as soon as the first Complex object is sent. If you intend to use your
custom class for reading existing Complex objects' values before all, you must
register said class explicitly with your client:

.. literalinclude:: ../examples/binary_basics.py
  :language: python
  :lines: 51

Now, when we dealt with the basics of `pygridgain` implementation of Complex
Objects, let us move on to more elaborate examples.

.. _sql_cache_read:

Read
====
File: `read_binary.py`_.

GridGain SQL uses Complex objects internally to represent keys and rows
in SQL tables. Normally SQL data is accessed via queries (see `SQL`_),
so we will consider the following example solely for the demonstration
of how Binary objects (not GridGain SQL) work.

In the :ref:`previous examples <sql_examples>` we have created some SQL tables.
Let us do it again and examine the GridGain storage afterwards.

.. literalinclude:: ../examples/read_binary.py
  :language: python
  :lines: 222-229

We can see that GridGain created a cache for each of our tables. The caches are
conveniently named using ‘`SQL_<schema name>_<table name>`’ pattern.

Now let us examine a configuration of a cache that contains SQL data
using a :py:attr:`~pygridgain.cache.Cache.settings` property.

.. literalinclude:: ../examples/read_binary.py
  :language: python
  :lines: 231-251

The values of `value_type_name` and `key_type_name` are names of the binary
types. The `City` table's key fields are stored using `key_type_name` type,
and the other fields − `value_type_name` type.

Now when we have the cache, in which the SQL data resides, and the names
of the key and value data types, we can read the data without using SQL
functions and verify the correctness of the result.

.. literalinclude:: ../examples/read_binary.py
  :language: python
  :lines: 253-267

What we see is a tuple of key and value, extracted from the cache. Both key
and value are represent Complex objects. The dataclass names are the same
as the `value_type_name` and `key_type_name` cache settings. The objects'
fields correspond to the SQL query.

.. _sql_cache_create:

Create
======
File: `create_binary.py`_.

Now, that we aware of the internal structure of the GridGain SQL storage,
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

1. Create table cache.

.. literalinclude:: ../examples/create_binary.py
  :language: python
  :lines: 22-63

2. Define Complex object data class.

.. literalinclude:: ../examples/create_binary.py
  :language: python
  :lines: 66-76

3. Insert row.

.. literalinclude:: ../examples/create_binary.py
  :language: python
  :lines: 79-83

Now let us make sure that our cache really can be used with SQL functions.

.. literalinclude:: ../examples/create_binary.py
  :language: python
  :lines: 85-93

Note, however, that the cache we create can not be dropped with DDL command.

.. literalinclude:: ../examples/create_binary.py
  :language: python
  :lines: 95-100

It should be deleted as any other key-value cache.

.. literalinclude:: ../examples/create_binary.py
  :language: python
  :lines: 102

Migrate
=======
File: `migrate_binary.py`_.

Suppose we have an accounting app that stores its data in key-value format.
Our task would be to introduce the following changes to the original expense
voucher's format and data:

- rename `date` to `expense_date`,
- add `report_date`,
- set `report_date` to the current date if `reported` is True, None if False,
- delete `reported`.

First get the vouchers' cache.

.. literalinclude:: ../examples/migrate_binary.py
  :language: python
  :lines: 108-111

If you do not store the schema of the Complex object in code, you can obtain
it as a dataclass property with
:py:meth:`~pygridgain.client.Client.query_binary_type` method.

.. literalinclude:: ../examples/migrate_binary.py
  :language: python
  :lines: 116-123

Let us modify the schema and create a new Complex object class with an updated
schema.

.. literalinclude:: ../examples/migrate_binary.py
  :language: python
  :lines: 125-138

Now migrate the data from the old schema to the new one.

.. literalinclude:: ../examples/migrate_binary.py
  :language: python
  :lines: 141-190

At this moment all the fields, defined in both of our schemas, can be
available in the resulting binary object, depending on which schema was used
when writing it using :py:meth:`~pygridgain.cache.Cache.put` or similar methods.
GridGain Binary API do not have the method to delete Complex object schema;
all the schemas ever defined will stay in cluster until its shutdown.

This versioning mechanism is quite simple and robust, but it have its
limitations. The main thing is: you can not change the type of the existing
field. If you try, you will be greeted with the following message:

```org.apache.ignite.binary.BinaryObjectException: Wrong value has been set
[typeName=SomeType, fieldName=f1, fieldType=String, assignedValueType=int]```

As an alternative, you can rename the field or create a new Complex object.

Failover
--------
File: `failover.py`_.

When connection to the server is broken or timed out,
:class:`~pygridgain.client.Client` object propagates an original exception
(`OSError` or `SocketError`), but keeps its constructor's parameters intact
and tries to reconnect transparently.

When :class:`~pygridgain.client.Client` detects that all nodes in the list are
failed without the possibility of restoring connection, it raises a special
:class:`~pygridgain.exceptions.ReconnectError` exception.

Gather 3 GridGain nodes on `localhost` into one cluster and run:

.. literalinclude:: ../examples/failover.py
  :language: python
  :lines: 16-51

Then try shutting down and restarting nodes, and see what happens.

.. literalinclude:: ../examples/failover.py
  :language: python
  :lines: 53-65

Client reconnection do not require an explicit user action, like calling
a special method or resetting a parameter.

.. literalinclude:: ../examples/failover.py
  :language: python
  :lines: 48

It means that instead of checking the connection status it is better for
`pygridgain` user to just try the supposed data operations and catch
the resulting exception.

SSL/TLS
-------

There are some special requirements for testing SSL connectivity.

The GridGain server must be configured for securing the binary protocol port.
The server configuration process can be split up into these basic steps:

1. Create a key store and a trust store using `Java keytool`_. When creating
   the trust store, you will probably need a client X.509 certificate. You
   will also need to export the server X.509 certificate to include in the
   client chain of trust.

2. Turn on the `SslContextFactory` for your GridGain cluster according to this
   document: `Securing Connection Between Nodes`_.

3. Tell GridGain to encrypt data on its thin client port, using the settings for
   `ClientConnectorConfiguration`_. If you only want to encrypt connection,
   not to validate client's certificate, set `sslClientAuth` property to
   `false`. You'll still have to set up the trust store on step 1 though.

Client SSL settings is summarized here:
:class:`~pygridgain.client.Client`.

To use the SSL encryption without certificate validation just `use_ssl`.

.. code-block:: python3

    from pygridgain import Client

    client = Client(use_ssl=True)
    client.connect('127.0.0.1', 10800)

To identify the client, create an SSL keypair and a certificate with
`openssl`_ command and use them in this manner:

.. code-block:: python3

    from pygridgain import Client

    client = Client(
        use_ssl=True,
        ssl_keyfile='etc/.ssl/keyfile.key',
        ssl_certfile='etc/.ssl/certfile.crt',
    )
    client.connect('gridgain-example.com', 10800)

To check the authenticity of the server, get the server certificate or
certificate chain and provide its path in the `ssl_ca_certfile` parameter.

.. code-block:: python3

    import ssl

    from pygridgain import Client

    client = Client(
        use_ssl=True,
        ssl_ca_certfile='etc/.ssl/ca_certs',
        ssl_cert_reqs=ssl.CERT_REQUIRED,
    )
    client.connect('gridgain-example.com', 10800)

You can also provide such parameters as the set of ciphers (`ssl_ciphers`) and
the SSL version (`ssl_version`), if the defaults
(:py:obj:`ssl._DEFAULT_CIPHERS` and TLS 1.1) do not suit you.

Password authentication
-----------------------

To authenticate you must set `authenticationEnabled` property to `true` and
enable persistance in GridGain XML configuration file, as described in
`Authentication`_ section of GridGain documentation.

Be advised that sending credentials over the open channel is greatly
discouraged, since they can be easily intercepted. Supplying credentials
automatically turns SSL on from the client side. It is highly recommended
to secure the connection to the GridGain server, as described
in `SSL/TLS`_ example, in order to use password authentication.

Then just supply `username` and `password` parameters to
:class:`~pygridgain.client.Client` constructor.

.. code-block:: python3

    from pygridgain import Client

    client = Client(username='gridgain', password='gridgain')
    client.connect('gridgain-example.com', 10800)

If you still do not wish to secure the connection is spite of the warning,
then disable SSL explicitly on creating the client object:

.. code-block:: python3

    client = Client(username='gridgain', password='gridgain', use_ssl=False)

Note, that it is not possible for GridGain thin client to obtain the cluster's
authentication settings through the binary protocol. Unexpected credentials
are simply ignored by the server. In the opposite case, the user is greeted
with the following message:

.. code-block:: python3

    # pygridgain.exceptions.HandshakeError: Handshake error: Unauthenticated sessions are prohibited.

.. _get_and_put.py: https://github.com/gridgain/gridgain/tree/master/modules/platforms/python/examples/get_and_put.py
.. _type_hints.py: https://github.com/gridgain/gridgain/tree/master/modules/platforms/python/examples/type_hints.py
.. _failover.py: https://github.com/gridgain/gridgain/tree/master/modules/platforms/python/examples/failover.py
.. _scans.py: https://github.com/gridgain/gridgain/tree/master/modules/platforms/python/examples/scans.py
.. _sql.py: https://github.com/gridgain/gridgain/tree/master/modules/platforms/python/examples/sql.py
.. _binary_basics.py: https://github.com/gridgain/gridgain/tree/master/modules/platforms/python/examples/binary_basics.py
.. _read_binary.py: https://github.com/gridgain/gridgain/tree/master/modules/platforms/python/examples/read_binary.py
.. _create_binary.py: https://github.com/gridgain/gridgain/tree/master/modules/platforms/python/examples/create_binary.py
.. _migrate_binary.py: https://github.com/gridgain/gridgain/tree/master/modules/platforms/python/examples/migrate_binary.py
.. _Getting Started: https://apacheignite-sql.readme.io/docs/getting-started
.. _GridGain CE GitHub repository: https://github.com/gridgain/gridgain/blob/master/examples/sql/world.sql
.. _Complex object: https://apacheignite.readme.io/docs/binary-client-protocol-data-format#section-complex-object
.. _Java keytool: https://docs.oracle.com/javase/8/docs/technotes/tools/unix/keytool.html
.. _Securing Connection Between Nodes: https://apacheignite.readme.io/docs/ssltls#section-securing-connection-between-nodes
.. _ClientConnectorConfiguration: https://ignite.apache.org/releases/latest/javadoc/org/apache/ignite/configuration/ClientConnectorConfiguration.html
.. _openssl: https://www.openssl.org/docs/manmaster/man1/openssl.html
.. _Authentication: https://apacheignite.readme.io/docs/advanced-security#section-authentication
.. _attrs: https://pypi.org/project/attrs/
.. _simple class names: https://apacheignite.readme.io/docs/binary-marshaller#binary-name-mapper-and-binary-id-mapper