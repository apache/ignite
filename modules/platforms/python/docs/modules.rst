================
Module Structure
================

The client library consists of several modules.

The most important for the end user are :mod:`connection` and :mod:`api`.

:mod:`connection`
-----------------

To connect to Ignite server socket, instantiate a :mod:`connection.Connection`
class with host name and port number.

You can then pass a :mod:`connection.Connection` instance to various API
functions.

:mod:`api`
----------

This is a collection of functions, split into three parts:

- :mod:`api.cache_config` allows you to manipulate caches;

- :mod:`api.key_value` brings a key-value-style data manipulation, similar to
  `memcached` or `Redis` APIs;

- :mod:`api.sql` gives you the ultimate power of SQL queries.

Each function returns operation status and result data (or verbose error
message) in :mod:`api.result.APIResult` object.

All data manipulations are handled with native Python data types, without the
need for the end user to construct complex data objects or parse blobs.