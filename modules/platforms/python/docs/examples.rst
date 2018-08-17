.. _examples_of_usage:

=================
Examples of usage
=================

Key-value
---------

Open connection
===============

.. literalinclude:: ../examples/get_and_put.py
  :language: python
  :lines: 16-19

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

.. literalinclude:: ../examples/type_hints.py
  :language: python
  :lines: 24-48

Refer the :ref:`data_types` section for the full list
of parser/constructor classes you can use as type hints.

Scan
====

Cache's :py:meth:`~pyignite.cache.Cache.scan` method queries allows you
to get the whole contents of the cache, element by element.

Let us put some data in cache.

.. literalinclude:: ../examples/scans.py
  :language: python
  :lines: 23-33

:py:meth:`~pyignite.cache.Cache.scan` returns a generator, that yields
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

These examples are similar to the ones given in the Apache Ignite SQL
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

Data samples are taken from `Ignite GitHub repository`_.

That concludes the preparation of data. Now let us answer some questions.

What are the 10 largest cities in our data sample (population-wise)?
====================================================================

.. literalinclude:: ../examples/sql.py
  :language: python
  :lines: 24, 221-238

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

Complex objects
---------------

Read
====

`Complex object`_ (that is often called ‘Binary object’) is used to represent
user-defined complex data types. It have the following features:

- have a unique ID,
- have an associated schema, that describes its inner structure (the order
  and types of its fields).

The schemas are stored in Ignite metadata storage. That is why Complex object
must be registered with the Ignite cluster before use.

The most obvious example of Complex object usage is SQL tables. Normally SQL
data is accessed via queries (see `SQL`_), so we will consider the following
example solely for the demonstration of how Binary objects (not Ignite SQL)
work.

In the :ref:`previous examples <sql_examples>` we have created some SQL tables.
Let us do it again and examine the Ignite storage afterwards.

.. literalinclude:: ../examples/binary_types.py
  :language: python
  :lines: 222-229

We can see that Ignite created a cache for each of our table. The caches are
conveniently named using ‘`SQL_<schema name>_<table name>`’ pattern.

Now let us examine a configuration of a cache that contains SQL data
using a :py:attr:`~pyignite.cache.Cache.settings` property.

.. literalinclude:: ../examples/binary_types.py
  :language: python
  :lines: 231-260

The values of `value_type_name` and `key_type_name` are names of the binary
types, in which the `Cities` table rows' values and keys are stored. Let us
check the types' registration and properties.

.. literalinclude:: ../examples/binary_types.py
  :language: python
  :lines: 262-276

Binary types are really exists, so we are on the right track. Let us take
a closer look to the value type.

.. literalinclude:: ../examples/binary_types.py
  :language: python
  :lines: 278-300

We have 3 fields in the row value: `Name`, `District`, and `Population`.
The complex primary key field, `ID` + `CountryCode`, is in the row key.

To support this theory let us try to read the data without using SQL
functions.

.. literalinclude:: ../examples/binary_types.py
  :language: python
  :lines: 302-326

Create
======

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
  :lines: 20-74

2. Register binary type.

.. literalinclude:: ../examples/create_binary.py
  :language: python
  :lines: 76-84

3. Insert row.

.. literalinclude:: ../examples/create_binary.py
  :language: python
  :lines: 86-101

Now let us make sure that our cache really can be used with SQL functions.

.. literalinclude:: ../examples/create_binary.py
  :language: python
  :lines: 103-111

Note, however, that the cache we create can not be dropped with DDL command.

.. literalinclude:: ../examples/create_binary.py
  :language: python
  :lines: 113-118

It should be deleted as any other key-value cache.

.. literalinclude:: ../examples/create_binary.py
  :language: python
  :lines: 120

Migrate
=======

Suppose we have an accounting app that stores its data in key-value format.
Our task would be to introduce the following changes to the original expense
voucher's format and data:

- rename `date` to `expense_date`,
- add `report_date`,
- set `report_date` to the current date if `reported` is True, None if False,
- delete `reported`.

First obtain the binary type ID and the initial schema.

.. literalinclude:: ../examples/migrate_binary.py
  :language: python
  :lines: 126-152

The binary type `ExpenseVoucher` has 6 fields and one schema. All the fields
are present in that one schema. Note also, that each field has an ID and
a type ID. Field type ID can be either ordinal value of one of the
:mod:`~pyignite.datatypes.type_codes` or an ID of the registered binary type.

Let us modify the schema dictionary and update the type.

.. literalinclude:: ../examples/migrate_binary.py
  :language: python
  :lines: 154-169

Revisit the `ExpenseVoucher` type.

.. literalinclude:: ../examples/migrate_binary.py
  :language: python
  :lines: 171-206

Now our binary type have two schemes. The old scheme (ID=-231598180) remained
unchanged, while the new scheme (ID=547629991) has only those fields specified
in the most recent :py:meth:`~pyignite.client.Client.put_binary_type`
call. None of the binary fields were actually removed, but two newly described
fields, `expense_date` and `report_date`, were added.

Now migrate the data from the old schema to the new one.

.. literalinclude:: ../examples/migrate_binary.py
  :language: python
  :lines: 209-249

As you can see, old or new fields are available in the resulting binary object,
depending on which schema was used when writing them using
:py:meth:`~pyignite.cache.Cache.put` or similar methods.

This versioning mechanism is quite simple and robust, but it have its
limitations. The main thing is: you can not change the type of the existing
field. If you try, you will be greeted with the following message:

```org.apache.ignite.binary.BinaryObjectException: Wrong value has been set
[typeName=SomeType, fieldName=f1, fieldType=String, assignedValueType=int]```

As an alternative (which feels more like a workaround) you can rename
the field or create a new schema.

Failover
--------

When connection to the server is broken or timed out,
:class:`~pyignite.client.Client` object raises an appropriate
exception, but keeps its constructor's parameters intact, so user can
reconnect, and the connection object remains valid.

The following example features a simple round-robin failover mechanism.
Launch 3 Ignite nodes on `localhost` and run:

.. literalinclude:: ../examples/failover.py
  :language: python
  :lines: 16-40

Then try shutting down and restarting nodes, and see what happens. At least
one node should remain active.

.. code-block:: python3

    # Connected to node 0
    # Error: Socket connection broken.
    # Connected to node 1
    # Error: Socket connection broken.
    # Error: [Errno 111] Client refused
    # Connected to node 0

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

Ignite binary protocol has no support for sending credentials over the open
channel. Supplying credentials automatically turns SSL on from the client side.
So it is necessary to secure the connection to the Ignite server, as described
in `SSL/TLS`_ example, in order to use password authentication.

Plus, you must set `authenticationEnabled` property to `true` and enable
persistance in Ignite XML configuration file, as described in
`Authentication`_ section of Ignite documentation.

Then just supply `username` and `password` parameters to
:class:`~pyignite.client.Client` constructor.

.. code-block:: python3

    from pyignite import Client

    client = Client(username='ignite', password='ignite')
    client.connect('ignite-example.com', 10800)

Note, that it is not possible for Ignite thin client to obtain the cluster's
authentication settings through the binary protocol. Unexpected credentials
are simply ignored by the server. In the opposite case, the user is greeted
with the following message:

.. code-block:: python3

    # pyignite.exceptions.HandshakeError: Handshake error: Unauthenticated sessions are prohibited. Expected protocol version: 0.0.0.

.. _Getting Started: https://apacheignite-sql.readme.io/docs/getting-started
.. _Ignite GitHub repository: https://github.com/apache/ignite/blob/master/examples/sql/world.sql
.. _Complex object: https://apacheignite.readme.io/v2.5/docs/binary-client-protocol-data-format#section-complex-object
.. _Java keytool: https://docs.oracle.com/javase/8/docs/technotes/tools/unix/keytool.html
.. _Securing Connection Between Nodes: https://apacheignite.readme.io/docs/ssltls#section-securing-connection-between-nodes
.. _ClientConnectorConfiguration: https://ignite.apache.org/releases/latest/javadoc/org/apache/ignite/configuration/ClientConnectorConfiguration.html
.. _openssl: https://www.openssl.org/docs/manmaster/man1/openssl.html
.. _Authentication: https://apacheignite.readme.io/docs/advanced-security#section-authentication
