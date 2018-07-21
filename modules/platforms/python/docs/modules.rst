================
Module Structure
================

The client library consists of several modules.

The most important for the end user are `connection`_ and
`api`_.

:mod:`~pyignite.datatypes`
--------------------------

Apache Ignite uses a sophisticated system of serializable data types
to store and retrieve user data, as well as to manage the configuration
of its caches through the Ignite binary protocol.

The complexity of data types varies from simple integer or character types
to arrays, maps, collections and structures.

Each data type is defined by its code. `Type code` is byte-sized. Thus,
every data object can be represented as a payload of fixed or variable size,
logically divided into one or more fields, prepended by the `type_code` field.

Most of Ignite data types can be represented by some of the standard Python
data type or class. Some of them, however, are conceptually alien, overly
complex, or ambiguous to Python dynamic typing system.

The following table summarizes the notion of Apache Ignite data types,
as well as their representation and handling in Python. For the nice
description, as well as gory implementation details, you may follow the link
to the parser/constructor class definition.

*Note:* you are not obliged to actually use those parser/constructor classes.
Pythonic types will suffice to interact with Apache Ignite binary API.
However, in some rare cases of type ambiguity, as well as for the needs
of interoperability, you may have to sneak one or the other class, along
with your data, in to some API function as a *type conversion hint*.

+-----------+--------------------+-------------------------------+---------------------------------------------------------------+
|`type_code`|Apache Ignite       |Python type                    |Parser/constructor                                             |
|           |docs reference      |or class                       |class                                                          |
+===========+====================+===============================+===============================================================+
|*Primitive data types*                                                                                                          |
+-----------+--------------------+-------------------------------+---------------------------------------------------------------+
|0x01       |Byte_               |int                            |:mod:`~pyignite.datatypes.primitive_objects.ByteObject`        |
+-----------+--------------------+-------------------------------+---------------------------------------------------------------+
|0x02       |Short_              |int                            |:mod:`~pyignite.datatypes.primitive_objects.ShortObject`       |
+-----------+--------------------+-------------------------------+---------------------------------------------------------------+
|0x03       |Int_                |int                            |:mod:`~pyignite.datatypes.primitive_objects.IntObject`         |
+-----------+--------------------+-------------------------------+---------------------------------------------------------------+
|0x04       |Long_               |int                            |:mod:`~pyignite.datatypes.primitive_objects.LongObject`        |
+-----------+--------------------+-------------------------------+---------------------------------------------------------------+
|0x05       |Float_              |float                          |:mod:`~pyignite.datatypes.primitive_objects.FloatObject`       |
+-----------+--------------------+-------------------------------+---------------------------------------------------------------+
|0x06       |Double_             |float                          |:mod:`~pyignite.datatypes.primitive_objects.DoubleObject`      |
+-----------+--------------------+-------------------------------+---------------------------------------------------------------+
|0x07       |Char_               |str                            |:mod:`~pyignite.datatypes.primitive_objects.CharObject`        |
+-----------+--------------------+-------------------------------+---------------------------------------------------------------+
|0x08       |Bool_               |bool                           |:mod:`~pyignite.datatypes.primitive_objects.BoolObject`        |
+-----------+--------------------+-------------------------------+---------------------------------------------------------------+
|0x65       |Null_               |NoneType                       |:mod:`~pyignite.datatypes.null_object.Null`                    |
+-----------+--------------------+-------------------------------+---------------------------------------------------------------+
|*Standard objects*                                                                                                              |
+-----------+--------------------+-------------------------------+---------------------------------------------------------------+
|0x09       |String_             |Str                            |:mod:`~pyignite.datatypes.standard.String`                     |
+-----------+--------------------+-------------------------------+---------------------------------------------------------------+
|0x0a       |UUID_               |uuid.UUID                      |:mod:`~pyignite.datatypes.standard.UUIDObject`                 |
+-----------+--------------------+-------------------------------+---------------------------------------------------------------+
|0x21       |Timestamp_          |tuple                          |:mod:`~pyignite.datatypes.standard.TimestampObject`            |
+-----------+--------------------+-------------------------------+---------------------------------------------------------------+
|0x0b       |Date_               |datetime.datetime              |:mod:`~pyignite.datatypes.standard.DateObject`                 |
+-----------+--------------------+-------------------------------+---------------------------------------------------------------+
|0x24       |Time_               |datetime.timedelta             |:mod:`~pyignite.datatypes.standard.TimeObject`                 |
+-----------+--------------------+-------------------------------+---------------------------------------------------------------+
|0x1e       |Decimal_            |decimal.Decimal                |:mod:`~pyignite.datatypes.standard.DecimalObject`              |
+-----------+--------------------+-------------------------------+---------------------------------------------------------------+
|0x1c       |Enum_               |tuple                          |:mod:`~pyignite.datatypes.standard.EnumObject`                 |
+-----------+--------------------+-------------------------------+---------------------------------------------------------------+
|0x67       |`Binary enum`_      |tuple                          |:mod:`~pyignite.datatypes.standard.BinaryEnumObject`           |
+-----------+--------------------+-------------------------------+---------------------------------------------------------------+
|*Arrays of primitives*                                                                                                          |
+-----------+--------------------+-------------------------------+---------------------------------------------------------------+
|0x0c       |`Byte array`_       |iterable/list                  |:mod:`~pyignite.datatypes.primitive_arrays.ByteArrayObject`    |
+-----------+--------------------+-------------------------------+---------------------------------------------------------------+
|0x0d       |`Short array`_      |iterable/list                  |:mod:`~pyignite.datatypes.primitive_arrays.ShortArrayObject` ` |
+-----------+--------------------+-------------------------------+---------------------------------------------------------------+
|0x0e       |`Int array`_        |iterable/list                  |:mod:`~pyignite.datatypes.primitive_arrays.IntArrayObject`     |
+-----------+--------------------+-------------------------------+---------------------------------------------------------------+
|0x0f       |`Long array`_       |iterable/list                  |:mod:`~pyignite.datatypes.primitive_arrays.LongArrayObject`    |
+-----------+--------------------+-------------------------------+---------------------------------------------------------------+
|0x10       |`Float array`_      |iterable/list                  |:mod:`~pyignite.datatypes.primitive_arrays.FloatArrayObject` ` |
+-----------+--------------------+-------------------------------+---------------------------------------------------------------+
|0x11       |`Double array`_     |iterable/list                  |:mod:`~pyignite.datatypes.primitive_arrays.DoubleArrayObject`  |
+-----------+--------------------+-------------------------------+---------------------------------------------------------------+
|0x12       |`Char array`_       |iterable/list                  |:mod:`~pyignite.datatypes.primitive_arrays.CharArrayObject`    |
+-----------+--------------------+-------------------------------+---------------------------------------------------------------+
|0x13       |`Bool array`_       |iterable/list                  |:mod:`~pyignite.datatypes.primitive_arrays.BoolArrayObject`    |
+-----------+--------------------+-------------------------------+---------------------------------------------------------------+
|*Arrays of standard objects*                                                                                                    |
+-----------+--------------------+-------------------------------+---------------------------------------------------------------+
|0x14       |`String array`_     |iterable/list                  |:mod:`~pyignite.datatypes.standard.StringArrayObject`          |
+-----------+--------------------+-------------------------------+---------------------------------------------------------------+
|0x15       |`UUID array`_       |iterable/list                  |:mod:`~pyignite.datatypes.standard.UUIDArrayObject`            |
+-----------+--------------------+-------------------------------+---------------------------------------------------------------+
|0x22       |`Timestamp array`_  |iterable/list                  |:mod:`~pyignite.datatypes.standard.TimestampArrayObject`       |
+-----------+--------------------+-------------------------------+---------------------------------------------------------------+
|0x16       |`Date array`_       |iterable/list                  |:mod:`~pyignite.datatypes.standard.DateArrayObject`            |
+-----------+--------------------+-------------------------------+---------------------------------------------------------------+
|0x23       |`Time array`_       |iterable/list                  |:mod:`~pyignite.datatypes.standard.TimeArrayObject`            |
+-----------+--------------------+-------------------------------+---------------------------------------------------------------+
|0x1f       |`Decimal array`_    |iterable/list                  |:mod:`~pyignite.datatypes.standard.DecimalArrayObject`         |
+-----------+--------------------+-------------------------------+---------------------------------------------------------------+
|*Object collections, special types, and complex object*                                                                         |
+-----------+--------------------+-------------------------------+---------------------------------------------------------------+
|0x17       |`Object array`_     |iterable/list                  |:mod:`~pyignite.datatypes.complex.ObjectArrayObject`           |
+-----------+--------------------+-------------------------------+---------------------------------------------------------------+
|0x18       |`Collection`_       |tuple                          |:mod:`~pyignite.datatypes.complex.CollectionObject`            |
+-----------+--------------------+-------------------------------+---------------------------------------------------------------+
|0x19       |`Map`_              |dict, collections.OrderedDict  |:mod:`~pyignite.datatypes.complex.MapObject`                   |
+-----------+--------------------+-------------------------------+---------------------------------------------------------------+
|0x1d       |`Enum array`_       |iterable/list                  |:mod:`~pyignite.datatypes.standard.EnumArrayObject`            |
+-----------+--------------------+-------------------------------+---------------------------------------------------------------+
|0x67       |`Complex object`_   |dict                           |:mod:`~pyignite.datatypes.complex.BinaryObject`                |
+-----------+--------------------+-------------------------------+---------------------------------------------------------------+
|0x1b       |`Wrapped data`_     |tuple                          |:mod:`~pyignite.datatypes.complex.WrappedDataObject`           |
+-----------+--------------------+-------------------------------+---------------------------------------------------------------+

All type codes are stored in module :mod:`pyignite.datatypes.type_codes`.

On top of all concrete parser/constructor classes, there are classes that
do not have their corresponding Ignite binary types. These classes are used
to simplify the task of encoding and/or decoding data.

:class:`~pyignite.datatypes.internal.AnyDataObject`
===================================================

It is an omnivorous data type that calls other classes' deserializers when
decoding the byte stream. It also does some guesswork when serializing
your Python data.

It is not overly smart or omnipotent though: it can not choose CharObject
for you; it will use String. It will also use LongArrayObject for representing
two-integer tuple, even if you mean Enum or Collection.

This is the summary of its type guessing:

+-----------------------+---------------------------------------------------------------+
|Native                 |Ignite                                                         |
|data types             |data object                                                    |
+=======================+===============================================================+
|None                   |:class:`~pyignite.datatypes.null_object.Null`                  |
+-----------------------+---------------------------------------------------------------+
|int                    |:class:`~pyignite.datatypes.primitive_objects.LongObject`      |
+-----------------------+---------------------------------------------------------------+
|float                  |:class:`~pyignite.datatypes.primitive_objects.DoubleObject`    |
+-----------------------+---------------------------------------------------------------+
|str, bytes             |:class:`~pyignite.datatypes.standard.String`                   |
+-----------------------+---------------------------------------------------------------+
|datetime.datetime      |:class:`~pyignite.datatypes.standard.DateObject`               |
+-----------------------+---------------------------------------------------------------+
|datetime.timedelta     |:class:`~pyignite.datatypes.standard.TimeObject`               |
+-----------------------+---------------------------------------------------------------+
|decimal.Decimal        |:class:`~pyignite.datatypes.standard.DecimalObject`            |
+-----------------------+---------------------------------------------------------------+
|uuid.UUID              |:class:`~pyignite.datatypes.standard.UUIDObject`               |
+-----------------------+---------------------------------------------------------------+
|iterable               |:mod:`~pyignite.datatypes` will inspect its contents to find   |
|                       |the right \*\ArrayObject class                                 |
+-----------------------+---------------------------------------------------------------+

Bottom line: use type hints when you need to pick up a certain data type
for your data, not just store that data.

:class:`~pyignite.datatypes.internal.Struct`
============================================

This class describes a sequence of binary fields with or without `type_id`.
When `type_id` is expected, :class:`~pyignite.datatypes.internal.AnyDataObject`
can be used as a `Struct` member. Otherwise use payload classes like
:class:`~pyignite.datatypes.primitive.Bool` instead of
:class:`~pyignite.datatypes.primitive_objects.BoolObject`.

Note that any standard object can accept
:class:`~pyignite.datatypes.null_object.Null` in its position; you do not have
to explicitly handle standard objects' nullability.

:class:`~pyignite.datatypes.internal.StructArray`
=================================================

An idiomatic construct of uniform `Struct` sequence, prepended by counter
field. Counter is of type :class:`~pyignite.datatypes.primitive.Int`
by default, but its type can be changed by parameterizing the `StructArray`
object. Any integer data type is acceptable.

:class:`~pyignite.datatypes.internal.AnyDataArray`
==================================================

A sequence of :class:`~pyignite.datatypes.internal.AnyDataObject` objects
prepended by :class:`~pyignite.datatypes.primitive.Int` counter. Unlike
:class:`~pyignite.datatypes.complex.MapObject` it do not have common `type_id`
or `type` fields.

:mod:`~pyignite.connection`
---------------------------

To connect to Ignite server socket, instantiate a
:mod:`~pyignite.connection.Connection` class with host name and port number.
Connection will negotiate a handshake with the Ignite server and raise a
:mod:`~pyignite.connection.SocketError` in case of client/server API versions
mismatch or data flow errors.

You can then pass a :mod:`~pyignite.connection.Connection` instance to
various API functions.

:mod:`~pyignite.api`
--------------------

This is a collection of functions, split into four parts:

- :mod:`~pyignite.api.cache_config` allows you to manipulate caches;

- :mod:`~pyignite.api.key_value` brings a key-value-style data manipulation,
  similar to `memcached` or `Redis` APIs;

- :mod:`~pyignite.api.sql` gives you the ultimate power of SQL queries;

- :mod:`~pyignite.api.binary` allows you to query the Ignite registry of
  binary types or register your own binary type.

To construct client queries and process server responses, all API functions
uses :mod:`~pyignite.queries.Query` and :mod:`~pyignite.queries.Response` base
classes respectively under their hoods. These classes are a natural extension
of the data type parsing/constructing module (:mod:`~pyignite.datatypes`) and
uses all the power of the indigenous
:mod:`~pyignite.datatypes.any_object.internal.AnyDataObject`.

Each function returns operation status and result data (or verbose error
message) in :mod:`~pyignite.api.result.APIResult` object.

All data manipulations are handled with native Python data types, without the
need for the end user to construct complex data objects or parse blobs.

.. _Byte: https://apacheignite.readme.io/v2.5/docs/binary-client-protocol-data-format#section-byte
.. _Short: https://apacheignite.readme.io/v2.5/docs/binary-client-protocol-data-format#section-short
.. _Int: https://apacheignite.readme.io/v2.5/docs/binary-client-protocol-data-format#section-int
.. _Long: https://apacheignite.readme.io/v2.5/docs/binary-client-protocol-data-format#section-long
.. _Float: https://apacheignite.readme.io/v2.5/docs/binary-client-protocol-data-format#section-float
.. _Double: https://apacheignite.readme.io/v2.5/docs/binary-client-protocol-data-format#section-double
.. _Char: https://apacheignite.readme.io/v2.5/docs/binary-client-protocol-data-format#section-char
.. _Bool: https://apacheignite.readme.io/v2.5/docs/binary-client-protocol-data-format#section-bool
.. _Null: https://apacheignite.readme.io/v2.5/docs/binary-client-protocol-data-format#section-null
.. _String: https://apacheignite.readme.io/v2.5/docs/binary-client-protocol-data-format#section-string
.. _UUID: https://apacheignite.readme.io/v2.5/docs/binary-client-protocol-data-format#section-uuid-guid-
.. _Timestamp: https://apacheignite.readme.io/v2.5/docs/binary-client-protocol-data-format#section-timestamp
.. _Date: https://apacheignite.readme.io/v2.5/docs/binary-client-protocol-data-format#section-date
.. _Time: https://apacheignite.readme.io/v2.5/docs/binary-client-protocol-data-format#section-time
.. _Decimal: https://apacheignite.readme.io/v2.5/docs/binary-client-protocol-data-format#section-decimal
.. _Enum: https://apacheignite.readme.io/v2.5/docs/binary-client-protocol-data-format#section-enum
.. _Byte array: https://apacheignite.readme.io/v2.5/docs/binary-client-protocol-data-format#section-byte-array
.. _Short array: https://apacheignite.readme.io/v2.5/docs/binary-client-protocol-data-format#section-short-array
.. _Int array: https://apacheignite.readme.io/v2.5/docs/binary-client-protocol-data-format#section-int-array
.. _Long array: https://apacheignite.readme.io/v2.5/docs/binary-client-protocol-data-format#section-long-array
.. _Float array: https://apacheignite.readme.io/v2.5/docs/binary-client-protocol-data-format#section-float-array
.. _Double array: https://apacheignite.readme.io/v2.5/docs/binary-client-protocol-data-format#section-double-array
.. _Char array: https://apacheignite.readme.io/v2.5/docs/binary-client-protocol-data-format#section-char-array
.. _Bool array: https://apacheignite.readme.io/v2.5/docs/binary-client-protocol-data-format#section-bool-array
.. _String array: https://apacheignite.readme.io/v2.5/docs/binary-client-protocol-data-format#section-string-array
.. _UUID array: https://apacheignite.readme.io/v2.5/docs/binary-client-protocol-data-format#section-uuid-guid-array
.. _Timestamp array: https://apacheignite.readme.io/v2.5/docs/binary-client-protocol-data-format#section-timestamp-array
.. _Date array: https://apacheignite.readme.io/v2.5/docs/binary-client-protocol-data-format#section-date-array
.. _Time array: https://apacheignite.readme.io/v2.5/docs/binary-client-protocol-data-format#section-time-array
.. _Decimal array: https://apacheignite.readme.io/v2.5/docs/binary-client-protocol-data-format#section-decimal-array
.. _Object array: https://apacheignite.readme.io/v2.5/docs/binary-client-protocol-data-format#section-object-collections
.. _Collection: https://apacheignite.readme.io/v2.5/docs/binary-client-protocol-data-format#section-collection
.. _Map: https://apacheignite.readme.io/v2.5/docs/binary-client-protocol-data-format#section-map
.. _Enum array: https://apacheignite.readme.io/v2.5/docs/binary-client-protocol-data-format#section-enum-array
.. _Binary enum: https://apacheignite.readme.io/v2.5/docs/binary-client-protocol-data-format#section-binary-enum
.. _Wrapped data: https://apacheignite.readme.io/v2.5/docs/binary-client-protocol-data-format#section-wrapped-data
.. _Complex object: https://apacheignite.readme.io/v2.5/docs/binary-client-protocol-data-format#section-complex-object
