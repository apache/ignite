..  Licensed to the Apache Software Foundation (ASF) under one or more
    contributor license agreements.  See the NOTICE file distributed with
    this work for additional information regarding copyright ownership.
    The ASF licenses this file to You under the Apache License, Version 2.0
    (the "License"); you may not use this file except in compliance with
    the License.  You may obtain a copy of the License at

..      http://www.apache.org/licenses/LICENSE-2.0

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
  :dedent: 4
  :lines: 20

Put value in cache
==================

.. literalinclude:: ../examples/get_and_put.py
  :language: python
  :dedent: 4
  :lines: 22

Get value from cache
====================

.. literalinclude:: ../examples/get_and_put.py
  :language: python
  :dedent: 4
  :lines: 24-28

Get multiple values from cache
==============================

.. literalinclude:: ../examples/get_and_put.py
  :language: python
  :dedent: 4
  :lines: 30-35

Type hints usage
================
File: `type_hints.py`_

.. literalinclude:: ../examples/type_hints.py
  :language: python
  :dedent: 4
  :lines: 23-47

As a rule of thumb:

- when a `pyignite` method or function deals with a single value or key, it
  has an additional parameter, like `value_hint` or `key_hint`, which accepts
  a parser/constructor class,

- nearly any structure element (inside dict or list) can be replaced with
  a two-tuple of (said element, type hint).

Refer the :ref:`data_types` section for the full list
of parser/constructor classes you can use as type hints.

ExpiryPolicy
============
File: `expiry_policy.py`_.

You can enable expiry policy (TTL) by two approaches.

Firstly, expiry policy can be set for entire cache by setting :py:attr:`~pyignite.datatypes.prop_codes.PROP_EXPIRY_POLICY`
in cache settings dictionary on creation.

.. literalinclude:: ../examples/expiry_policy.py
  :language: python
  :dedent: 12
  :lines: 33-36

.. literalinclude:: ../examples/expiry_policy.py
  :language: python
  :dedent: 12
  :lines: 42-48

Secondly, expiry policy can be set for all cache operations, which are done under decorator. To create it use
:py:meth:`~pyignite.cache.BaseCache.with_expire_policy`

.. literalinclude:: ../examples/expiry_policy.py
  :language: python
  :dedent: 12
  :lines: 55-62

Scan
====
File: `scans.py`_.

Cache's :py:meth:`~pyignite.cache.Cache.scan` method queries allows you
to get the whole contents of the cache, element by element.

Let us put some data in cache.

.. literalinclude:: ../examples/scans.py
  :language: python
  :dedent: 4
  :lines: 22-31

:py:meth:`~pyignite.cache.Cache.scan` returns a cursor, that yields
two-tuples of key and value. You can iterate through the generated pairs
in a safe manner:

.. literalinclude:: ../examples/scans.py
  :language: python
  :dedent: 4
  :lines: 33-41

Or, alternatively, you can convert the cursor to dictionary in one go:

.. literalinclude:: ../examples/scans.py
  :language: python
  :dedent: 4
  :lines: 43-52

But be cautious: if the cache contains a large set of data, the dictionary
may consume too much memory!

.. _sql_examples:

Object collections
------------------

File: `get_and_put_complex.py`_.

Ignite collection types are represented in `pyignite` as two-tuples.
First comes collection type ID or deserialization hint, which is specific for
each of the collection type. Second comes the data value.

.. literalinclude:: ../examples/get_and_put_complex.py
  :language: python
  :lines: 17

Map
===

For Python prior to 3.6, it might be important to distinguish between ordered
(`collections.OrderedDict`) and unordered (`dict`) dictionary types, so you
could use :py:attr:`~pyignite.datatypes.complex.Map.LINKED_HASH_MAP`
for the former and :py:attr:`~pyignite.datatypes.complex.Map.HASH_MAP`
for the latter.

Since CPython 3.6 all dictionaries became de facto ordered. You can always use
`LINKED_HASH_MAP` as a safe default.

.. literalinclude:: ../examples/get_and_put_complex.py
  :language: python
  :dedent: 4
  :lines: 22-36

Collection
==========

See :class:`~pyignite.datatypes.complex.CollectionObject` and Ignite
documentation on `Collection`_ type for the description of various Java
collection types. Note that not all of them have a direct Python
representative. For example, Python do not have ordered sets (it is indeed
recommended to use `OrderedDict`'s keys and disregard its values).

As for the `pyignite`, the rules are simple: pass any iterable as a data,
and you always get `list` back.

.. literalinclude:: ../examples/get_and_put_complex.py
  :language: python
  :dedent: 4
  :lines: 38-52

Object array
============

:class:`~pyignite.datatypes.complex.ObjectArrayObject` has a very limited
functionality in `pyignite`, since no type checks can be enforced on its
contents. But it still can be used for interoperability with Java.

.. literalinclude:: ../examples/get_and_put_complex.py
  :language: python
  :dedent: 4
  :lines: 54-63


Transactions
------------
File: `transactions.py`_.

Client transactions are supported for caches with
:py:attr:`~pyignite.datatypes.cache_config.CacheAtomicityMode.TRANSACTIONAL` mode.

Let's create transactional cache:

.. literalinclude:: ../examples/transactions.py
  :language: python
  :dedent: 8
  :lines: 84-87

Let's start a transaction and commit it:

.. literalinclude:: ../examples/transactions.py
  :language: python
  :dedent: 8
  :lines: 90-96

Let's check that the transaction was committed successfully:

.. literalinclude:: ../examples/transactions.py
  :language: python
  :dedent: 8
  :lines: 98-99

Let's check that raising exception inside `with` block leads to transaction's rollback

.. literalinclude:: ../examples/transactions.py
  :language: python
  :dedent: 8
  :lines: 102-113

Let's check that timed out transaction is successfully rolled back

.. literalinclude:: ../examples/transactions.py
  :language: python
  :dedent: 8
  :lines: 116-126

See more info about transaction's parameters in a documentation of :py:meth:`~pyignite.client.Client.tx_start`

SQL
---
File: `sql.py`_.

These examples are similar to the ones given in the Apache Ignite SQL
Documentation: `Getting Started`_.

Setup
=====

First let us establish a connection.

.. literalinclude:: ../examples/sql.py
  :language: python
  :lines: 20-21

Then create tables. Begin with `Country` table, than proceed with related
tables `City` and `CountryLanguage`.

.. literalinclude:: ../examples/helpers/sql_helper.py
  :language: python
  :dedent: 4
  :lines: 27-43, 53-60, 68-74

.. literalinclude:: ../examples/sql.py
  :language: python
  :dedent: 4
  :lines: 23-28

Create indexes.

.. literalinclude:: ../examples/helpers/sql_helper.py
  :language: python
  :dedent: 4
  :lines: 62, 76

.. literalinclude:: ../examples/sql.py
  :language: python
  :dedent: 4
  :lines: 31-32

Fill tables with data.

.. literalinclude:: ../examples/helpers/sql_helper.py
  :language: python
  :dedent: 4
  :lines: 45-51, 64-66, 78-80

.. literalinclude:: ../examples/sql.py
  :language: python
  :dedent: 4
  :lines: 35-42

Data samples are taken from `PyIgnite GitHub repository`_.

That concludes the preparation of data. Now let us answer some questions.

What are the 10 largest cities in our data sample (population-wise)?
====================================================================

.. literalinclude:: ../examples/sql.py
  :language: python
  :dedent: 4
  :lines: 45-59

The :py:meth:`~pyignite.client.Client.sql` method returns a generator,
that yields the resulting rows.

What are the 10 most populated cities throughout the 3 chosen countries?
========================================================================

If you set the `include_field_names` argument to `True`, the
:py:meth:`~pyignite.client.Client.sql` method will generate a list of
column names as a first yield. You can access field names with Python built-in
`next` function.

.. literalinclude:: ../examples/sql.py
  :language: python
  :dedent: 4
  :lines: 62-88

Display all the information about a given city
==============================================

.. literalinclude:: ../examples/sql.py
  :language: python
  :dedent: 4
  :lines: 92-103

Finally, delete the tables used in this example with the following queries:

.. literalinclude:: ../examples/helpers/sql_helper.py
  :language: python
  :lines: 82

.. literalinclude:: ../examples/sql.py
  :language: python
  :dedent: 4
  :lines: 106-107

.. _complex_object_usage:

Complex objects
---------------
File: `binary_basics.py`_.

`Complex object`_ (that is often called ‘Binary object’) is an Ignite data
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
in run-time). For the `pyignite` user it means that for all purposes
of storing native Python data it is better to use Ignite
:class:`~pyignite.datatypes.complex.CollectionObject`
or :class:`~pyignite.datatypes.complex.MapObject` data types.

However, for interoperability purposes, `pyignite` has a mechanism of creating
special Python classes to read or write Complex objects. These classes have
an interface, that simulates all the features of the Complex object: type name,
type ID, schema, schema ID, and version number.

Assuming that one concrete class for representing one Complex object can
severely limit the user's data manipulation capabilities, all the
functionality said above is implemented through the metaclass:
:class:`~pyignite.binary.GenericObjectMeta`. This metaclass is used
automatically when reading Complex objects.

.. literalinclude:: ../examples/binary_basics.py
  :language: python
  :dedent: 4
  :lines: 36-38, 40-43, 45-46

Here you can see how :class:`~pyignite.binary.GenericObjectMeta` uses
`attrs`_ package internally for creating nice `__init__()` and `__repr__()`
methods.

In this case the autogenerated dataclass's name `Person` is exactly matches
the type name of the Complex object it represents (the content of the
:py:attr:`~pyignite.datatypes.base.IgniteDataTypeProps.type_name` property).
But when Complex object's class name contains characters, that can not be used
in a Python identifier, for example:

- `.`, when fully qualified Java class names are used,
- `$`, a common case for Scala classes,
- `+`, internal class name separator in C#,

then `pyignite` can not maintain this match. In such cases `pyignite` tries
to sanitize a type name to derive a “good” dataclass name from it.

If your code needs consistent naming between the server and the client, make
sure that your Ignite cluster is configured to use `simple class names`_.

Anyway, you can reuse the autogenerated dataclass for subsequent writes:

.. literalinclude:: ../examples/binary_basics.py
  :language: python
  :dedent: 4
  :lines: 50, 32-34

:class:`~pyignite.binary.GenericObjectMeta` can also be used directly
for creating custom classes:

.. literalinclude:: ../examples/binary_basics.py
  :language: python
  :lines: 20-25

Note how the `Person` class is defined. `schema` is a
:class:`~pyignite.binary.GenericObjectMeta` metaclass parameter.
Another important :class:`~pyignite.binary.GenericObjectMeta` parameter
is a `type_name`, but it is optional and defaults to the class name (‘Person’
in our example).

Note also, that `Person` do not have to define its own attributes, methods and
properties (`pass`), although it is completely possible.

Now, when your custom `Person` class is created, you are ready to send data
to Ignite server using its objects. The client will implicitly register your
class as soon as the first Complex object is sent. If you intend to use your
custom class for reading existing Complex objects' values before all, you must
register said class explicitly with your client:

.. literalinclude:: ../examples/binary_basics.py
  :language: python
  :dedent: 4
  :lines: 48

Now, when we dealt with the basics of `pyignite` implementation of Complex
Objects, let us move on to more elaborate examples.

.. _sql_cache_read:

Read
====
File: `read_binary.py`_.

Ignite SQL uses Complex objects internally to represent keys and rows
in SQL tables. Normally SQL data is accessed via queries (see `SQL`_),
so we will consider the following example solely for the demonstration
of how Binary objects (not Ignite SQL) work.

In the :ref:`previous examples <sql_examples>` we have created some SQL tables.
Let us do it again and examine the Ignite storage afterwards.

.. literalinclude:: ../examples/read_binary.py
  :language: python
  :dedent: 4
  :lines: 49-51

We can see that Ignite created a cache for each of our tables. The caches are
conveniently named using ‘`SQL_<schema name>_<table name>`’ pattern.

Now let us examine a configuration of a cache that contains SQL data
using a :py:attr:`~pyignite.cache.Cache.settings` property.

.. literalinclude:: ../examples/read_binary.py
  :language: python
  :dedent: 4
  :lines: 53-103

The values of `value_type_name` and `key_type_name` are names of the binary
types. The `City` table's key fields are stored using `key_type_name` type,
and the other fields − `value_type_name` type.

Now when we have the cache, in which the SQL data resides, and the names
of the key and value data types, we can read the data without using SQL
functions and verify the correctness of the result.

.. literalinclude:: ../examples/read_binary.py
  :language: python
  :dedent: 4
  :lines: 106-115

What we see is a tuple of key and value, extracted from the cache. Both key
and value are represent Complex objects. The dataclass names are the same
as the `value_type_name` and `key_type_name` cache settings. The objects'
fields correspond to the SQL query.

.. _sql_cache_create:

Create
======
File: `create_binary.py`_.

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

1. Create table cache.

.. literalinclude:: ../examples/create_binary.py
  :language: python
  :dedent: 4
  :lines: 31-69

2. Define Complex object data class.

.. literalinclude:: ../examples/create_binary.py
  :language: python
  :lines: 21-26

3. Insert row.

.. literalinclude:: ../examples/create_binary.py
  :language: python
  :dedent: 4
  :lines: 71-75

Now let us make sure that our cache really can be used with SQL functions.

.. literalinclude:: ../examples/create_binary.py
  :language: python
  :dedent: 4
  :lines: 77-82

Note, however, that the cache we create can not be dropped with DDL command.
It should be deleted as any other key-value cache.

.. literalinclude:: ../examples/create_binary.py
  :language: python
  :dedent: 4
  :lines: 84-91

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
  :dedent: 4
  :lines: 109

If you do not store the schema of the Complex object in code, you can obtain
it as a dataclass property with
:py:meth:`~pyignite.client.Client.query_binary_type` method.

.. literalinclude:: ../examples/migrate_binary.py
  :language: python
  :dedent: 4
  :lines: 115-119

Let us modify the schema and create a new Complex object class with an updated
schema.

.. literalinclude:: ../examples/migrate_binary.py
  :language: python
  :lines: 121-137

Now migrate the data from the old schema to the new one.

.. literalinclude:: ../examples/migrate_binary.py
  :language: python
  :lines: 140-190

At this moment all the fields, defined in both of our schemas, can be
available in the resulting binary object, depending on which schema was used
when writing it using :py:meth:`~pyignite.cache.Cache.put` or similar methods.
Ignite Binary API do not have the method to delete Complex object schema;
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
:class:`~pyignite.client.Client` object propagates an original exception
(`OSError` or `SocketError`), but keeps its constructor's parameters intact
and tries to reconnect transparently.

When :class:`~pyignite.client.Client` detects that all nodes in the list are
failed without the possibility of restoring connection, it raises a special
:class:`~pyignite.exceptions.ReconnectError` exception.

Gather 3 Ignite nodes on `localhost` into one cluster and run:

.. literalinclude:: ../examples/failover.py
  :language: python
  :lines: 16-52

Then try shutting down and restarting nodes, and see what happens.

.. literalinclude:: ../examples/failover.py
  :language: python
  :lines: 54-66

Client reconnection do not require an explicit user action, like calling
a special method or resetting a parameter.
It means that instead of checking the connection status it is better for
`pyignite` user to just try the supposed data operations and catch
the resulting exception.

SSL/TLS
-------

There are some special requirements for testing SSL connectivity.

The Ignite server must be configured for securing the binary protocol port.
The server configuration process can be split up into these basic steps:

1. Create a key store and a trust store using `Java keytool`_. When creating
   the trust store, you will probably need a client X.509 certificate. You
   will also need to export the server X.509 certificate to include in the
   client chain of trust.

2. Turn on the `SslContextFactory` for your Ignite cluster according to this
   document: `Securing Connection Between Nodes`_.

3. Tell Ignite to encrypt data on its thin client port, using the settings for
   `ClientConnectorConfiguration`_. If you only want to encrypt connection,
   not to validate client's certificate, set `sslClientAuth` property to
   `false`. You'll still have to set up the trust store on step 1 though.

Client SSL settings is summarized here:
:class:`~pyignite.client.Client`.

To use the SSL encryption without certificate validation just `use_ssl`.

.. code-block:: python3

    from pyignite import Client

    client = Client(use_ssl=True)
    client.connect('127.0.0.1', 10800)

To identify the client, create an SSL keypair and a certificate with
`openssl`_ command and use them in this manner:

.. code-block:: python3

    from pyignite import Client

    client = Client(
        use_ssl=True,
        ssl_keyfile='etc/.ssl/keyfile.key',
        ssl_certfile='etc/.ssl/certfile.crt',
    )
    client.connect('ignite-example.com', 10800)

To check the authenticity of the server, get the server certificate or
certificate chain and provide its path in the `ssl_ca_certfile` parameter.

.. code-block:: python3

    import ssl

    from pyignite import Client

    client = Client(
        use_ssl=True,
        ssl_ca_certfile='etc/.ssl/ca_certs',
        ssl_cert_reqs=ssl.CERT_REQUIRED,
    )
    client.connect('ignite-example.com', 10800)

You can also provide such parameters as the set of ciphers (`ssl_ciphers`) and
the SSL version (`ssl_version`), if the defaults
(:py:obj:`ssl._DEFAULT_CIPHERS` and TLS 1.1) do not suit you.

Password authentication
-----------------------

To authenticate you must set `authenticationEnabled` property to `true` and
enable persistance in Ignite XML configuration file, as described in
`Authentication`_ section of Ignite documentation.

Be advised that sending credentials over the open channel is greatly
discouraged, since they can be easily intercepted. Supplying credentials
automatically turns SSL on from the client side. It is highly recommended
to secure the connection to the Ignite server, as described
in `SSL/TLS`_ example, in order to use password authentication.

Then just supply `username` and `password` parameters to
:class:`~pyignite.client.Client` constructor.

.. code-block:: python3

    from pyignite import Client

    client = Client(username='ignite', password='ignite')
    client.connect('ignite-example.com', 10800)

If you still do not wish to secure the connection is spite of the warning,
then disable SSL explicitly on creating the client object:

.. code-block:: python3

    client = Client(username='ignite', password='ignite', use_ssl=False)

Note, that it is not possible for Ignite thin client to obtain the cluster's
authentication settings through the binary protocol. Unexpected credentials
are simply ignored by the server. In the opposite case, the user is greeted
with the following message:

.. code-block:: python3

    # pyignite.exceptions.HandshakeError: Handshake error: Unauthenticated sessions are prohibited.

.. _get_and_put.py: https://github.com/apache/ignite-python-thin-client/blob/master/examples/get_and_put.py
.. _async_key_value.py: https://github.com/apache/ignite-python-thin-client/blob/master/examples/async_key_value.py
.. _type_hints.py: https://github.com/apache/ignite-python-thin-client/blob/master/examples/type_hints.py
.. _failover.py: https://github.com/apache/ignite-python-thin-client/blob/master/examples/failover.py
.. _scans.py: https://github.com/apache/ignite-python-thin-client/blob/master/examples/scans.py
.. _expiry_policy.py: https://github.com/apache/ignite-python-thin-client/blob/master/examples/expiry_policy.py
.. _sql.py: https://github.com/apache/ignite-python-thin-client/blob/master/examples/sql.py
.. _async_sql.py: https://github.com/apache/ignite-python-thin-client/blob/master/examples/async_sql.py
.. _binary_basics.py: https://github.com/apache/ignite-python-thin-client/blob/master/examples/binary_basics.py
.. _read_binary.py: https://github.com/apache/ignite-python-thin-client/blob/master/examples/read_binary.py
.. _create_binary.py: https://github.com/apache/ignite-python-thin-client/blob/master/examples/create_binary.py
.. _migrate_binary.py: https://github.com/apache/ignite-python-thin-client/blob/master/examples/migrate_binary.py
.. _transactions.py: https://github.com/apache/ignite-python-thin-client/blob/master/examples/transactions.py
.. _Getting Started: https://ignite.apache.org/docs/latest/thin-clients/python-thin-client
.. _PyIgnite GitHub repository: https://github.com/apache/ignite-python-thin-client/blob/master
.. _Complex object: https://ignite.apache.org/docs/latest/binary-client-protocol/data-format#complex-object
.. _Java keytool: https://docs.oracle.com/javase/8/docs/technotes/tools/unix/keytool.html
.. _Securing Connection Between Nodes: https://ignite.apache.org/docs/latest/security/ssl-tls#ssltls-for-nodes
.. _ClientConnectorConfiguration: https://ignite.apache.org/releases/latest/javadoc/org/apache/ignite/configuration/ClientConnectorConfiguration.html
.. _openssl: https://www.openssl.org/docs/manmaster/man1/openssl.html
.. _Authentication: https://ignite.apache.org/docs/latest/security/authentication
.. _attrs: https://pypi.org/project/attrs/
.. _get_and_put_complex.py: https://github.com/apache/ignite-python-thin-client/blob/master/examples/get_and_put.py
.. _Collection: https://ignite.apache.org/docs/latest/binary-client-protocol/data-format#collection
.. _simple class names: https://ignite.apache.org/docs/latest/data-modeling/binary-marshaller#binary-name-mapper-and-binary-id-mapper
