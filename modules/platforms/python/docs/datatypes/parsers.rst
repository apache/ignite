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

.. _data_types:

==========
Data Types
==========

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
complex, or ambiguous to Python dynamic type system.

The following table summarizes the notion of Apache Ignite data types,
as well as their representation and handling in Python. For the nice
description, as well as gory implementation details, you may follow the link
to the parser/constructor class definition. Note that parser/constructor
classes are not instantiatable. The `class` here is used mostly as a sort of
tupperware for organizing methods together.

*Note:* you are not obliged to actually use those parser/constructor classes.
Pythonic types will suffice to interact with Apache Ignite binary API.
However, in some rare cases of type ambiguity, as well as for the needs
of interoperability, you may have to sneak one or the other class, along
with your data, in to some API function as a *type conversion hint*.

+-----------+--------------------+-------------------------------+-----------------------------------------------------------------+
|`type_code`|Apache Ignite       |Python type                    |Parser/constructor                                               |
|           |docs reference      |or class                       |class                                                            |
+===========+====================+===============================+=================================================================+
|*Primitive data types*                                                                                                            |
+-----------+--------------------+-------------------------------+-----------------------------------------------------------------+
|0x01       |Byte_               |int                            |:class:`~pyignite.datatypes.primitive_objects.ByteObject`        |
+-----------+--------------------+-------------------------------+-----------------------------------------------------------------+
|0x02       |Short_              |int                            |:class:`~pyignite.datatypes.primitive_objects.ShortObject`       |
+-----------+--------------------+-------------------------------+-----------------------------------------------------------------+
|0x03       |Int_                |int                            |:class:`~pyignite.datatypes.primitive_objects.IntObject`         |
+-----------+--------------------+-------------------------------+-----------------------------------------------------------------+
|0x04       |Long_               |int                            |:class:`~pyignite.datatypes.primitive_objects.LongObject`        |
+-----------+--------------------+-------------------------------+-----------------------------------------------------------------+
|0x05       |Float_              |float                          |:class:`~pyignite.datatypes.primitive_objects.FloatObject`       |
+-----------+--------------------+-------------------------------+-----------------------------------------------------------------+
|0x06       |Double_             |float                          |:class:`~pyignite.datatypes.primitive_objects.DoubleObject`      |
+-----------+--------------------+-------------------------------+-----------------------------------------------------------------+
|0x07       |Char_               |str                            |:class:`~pyignite.datatypes.primitive_objects.CharObject`        |
+-----------+--------------------+-------------------------------+-----------------------------------------------------------------+
|0x08       |Bool_               |bool                           |:class:`~pyignite.datatypes.primitive_objects.BoolObject`        |
+-----------+--------------------+-------------------------------+-----------------------------------------------------------------+
|0x65       |Null_               |NoneType                       |:class:`~pyignite.datatypes.null_object.Null`                    |
+-----------+--------------------+-------------------------------+-----------------------------------------------------------------+
|*Standard objects*                                                                                                                |
+-----------+--------------------+-------------------------------+-----------------------------------------------------------------+
|0x09       |String_             |Str                            |:class:`~pyignite.datatypes.standard.String`                     |
+-----------+--------------------+-------------------------------+-----------------------------------------------------------------+
|0x0a       |UUID_               |uuid.UUID                      |:class:`~pyignite.datatypes.standard.UUIDObject`                 |
+-----------+--------------------+-------------------------------+-----------------------------------------------------------------+
|0x21       |Timestamp_          |tuple                          |:class:`~pyignite.datatypes.standard.TimestampObject`            |
+-----------+--------------------+-------------------------------+-----------------------------------------------------------------+
|0x0b       |Date_               |datetime.datetime              |:class:`~pyignite.datatypes.standard.DateObject`                 |
+-----------+--------------------+-------------------------------+-----------------------------------------------------------------+
|0x24       |Time_               |datetime.timedelta             |:class:`~pyignite.datatypes.standard.TimeObject`                 |
+-----------+--------------------+-------------------------------+-----------------------------------------------------------------+
|0x1e       |Decimal_            |decimal.Decimal                |:class:`~pyignite.datatypes.standard.DecimalObject`              |
+-----------+--------------------+-------------------------------+-----------------------------------------------------------------+
|0x1c       |Enum_               |tuple                          |:class:`~pyignite.datatypes.standard.EnumObject`                 |
+-----------+--------------------+-------------------------------+-----------------------------------------------------------------+
|0x67       |`Binary enum`_      |tuple                          |:class:`~pyignite.datatypes.standard.BinaryEnumObject`           |
+-----------+--------------------+-------------------------------+-----------------------------------------------------------------+
|*Arrays of primitives*                                                                                                            |
+-----------+--------------------+-------------------------------+-----------------------------------------------------------------+
|0x0c       |`Byte array`_       |iterable/list                  |:class:`~pyignite.datatypes.primitive_arrays.ByteArrayObject`    |
+-----------+--------------------+-------------------------------+-----------------------------------------------------------------+
|0x0d       |`Short array`_      |iterable/list                  |:class:`~pyignite.datatypes.primitive_arrays.ShortArrayObject`   |
+-----------+--------------------+-------------------------------+-----------------------------------------------------------------+
|0x0e       |`Int array`_        |iterable/list                  |:class:`~pyignite.datatypes.primitive_arrays.IntArrayObject`     |
+-----------+--------------------+-------------------------------+-----------------------------------------------------------------+
|0x0f       |`Long array`_       |iterable/list                  |:class:`~pyignite.datatypes.primitive_arrays.LongArrayObject`    |
+-----------+--------------------+-------------------------------+-----------------------------------------------------------------+
|0x10       |`Float array`_      |iterable/list                  |:class:`~pyignite.datatypes.primitive_arrays.FloatArrayObject`   |
+-----------+--------------------+-------------------------------+-----------------------------------------------------------------+
|0x11       |`Double array`_     |iterable/list                  |:class:`~pyignite.datatypes.primitive_arrays.DoubleArrayObject`  |
+-----------+--------------------+-------------------------------+-----------------------------------------------------------------+
|0x12       |`Char array`_       |iterable/list                  |:class:`~pyignite.datatypes.primitive_arrays.CharArrayObject`    |
+-----------+--------------------+-------------------------------+-----------------------------------------------------------------+
|0x13       |`Bool array`_       |iterable/list                  |:class:`~pyignite.datatypes.primitive_arrays.BoolArrayObject`    |
+-----------+--------------------+-------------------------------+-----------------------------------------------------------------+
|*Arrays of standard objects*                                                                                                      |
+-----------+--------------------+-------------------------------+-----------------------------------------------------------------+
|0x14       |`String array`_     |iterable/list                  |:class:`~pyignite.datatypes.standard.StringArrayObject`          |
+-----------+--------------------+-------------------------------+-----------------------------------------------------------------+
|0x15       |`UUID array`_       |iterable/list                  |:class:`~pyignite.datatypes.standard.UUIDArrayObject`            |
+-----------+--------------------+-------------------------------+-----------------------------------------------------------------+
|0x22       |`Timestamp array`_  |iterable/list                  |:class:`~pyignite.datatypes.standard.TimestampArrayObject`       |
+-----------+--------------------+-------------------------------+-----------------------------------------------------------------+
|0x16       |`Date array`_       |iterable/list                  |:class:`~pyignite.datatypes.standard.DateArrayObject`            |
+-----------+--------------------+-------------------------------+-----------------------------------------------------------------+
|0x23       |`Time array`_       |iterable/list                  |:class:`~pyignite.datatypes.standard.TimeArrayObject`            |
+-----------+--------------------+-------------------------------+-----------------------------------------------------------------+
|0x1f       |`Decimal array`_    |iterable/list                  |:class:`~pyignite.datatypes.standard.DecimalArrayObject`         |
+-----------+--------------------+-------------------------------+-----------------------------------------------------------------+
|*Object collections, special types, and complex object*                                                                           |
+-----------+--------------------+-------------------------------+-----------------------------------------------------------------+
|0x17       |`Object array`_     |iterable/list                  |:class:`~pyignite.datatypes.complex.ObjectArrayObject`           |
+-----------+--------------------+-------------------------------+-----------------------------------------------------------------+
|0x18       |`Collection`_       |tuple                          |:class:`~pyignite.datatypes.complex.CollectionObject`            |
+-----------+--------------------+-------------------------------+-----------------------------------------------------------------+
|0x19       |`Map`_              |dict, collections.OrderedDict  |:class:`~pyignite.datatypes.complex.MapObject`                   |
+-----------+--------------------+-------------------------------+-----------------------------------------------------------------+
|0x1d       |`Enum array`_       |iterable/list                  |:class:`~pyignite.datatypes.standard.EnumArrayObject`            |
+-----------+--------------------+-------------------------------+-----------------------------------------------------------------+
|0x67       |`Complex object`_   |object                         |:class:`~pyignite.datatypes.complex.BinaryObject`                |
+-----------+--------------------+-------------------------------+-----------------------------------------------------------------+
|0x1b       |`Wrapped data`_     |tuple                          |:class:`~pyignite.datatypes.complex.WrappedDataObject`           |
+-----------+--------------------+-------------------------------+-----------------------------------------------------------------+

.. _Byte: https://apacheignite.readme.io/docs/binary-client-protocol-data-format#section-byte
.. _Short: https://apacheignite.readme.io/docs/binary-client-protocol-data-format#section-short
.. _Int: https://apacheignite.readme.io/docs/binary-client-protocol-data-format#section-int
.. _Long: https://apacheignite.readme.io/docs/binary-client-protocol-data-format#section-long
.. _Float: https://apacheignite.readme.io/docs/binary-client-protocol-data-format#section-float
.. _Double: https://apacheignite.readme.io/docs/binary-client-protocol-data-format#section-double
.. _Char: https://apacheignite.readme.io/docs/binary-client-protocol-data-format#section-char
.. _Bool: https://apacheignite.readme.io/docs/binary-client-protocol-data-format#section-bool
.. _Null: https://apacheignite.readme.io/docs/binary-client-protocol-data-format#section-null
.. _String: https://apacheignite.readme.io/docs/binary-client-protocol-data-format#section-string
.. _UUID: https://apacheignite.readme.io/docs/binary-client-protocol-data-format#section-uuid-guid-
.. _Timestamp: https://apacheignite.readme.io/docs/binary-client-protocol-data-format#section-timestamp
.. _Date: https://apacheignite.readme.io/docs/binary-client-protocol-data-format#section-date
.. _Time: https://apacheignite.readme.io/docs/binary-client-protocol-data-format#section-time
.. _Decimal: https://apacheignite.readme.io/docs/binary-client-protocol-data-format#section-decimal
.. _Enum: https://apacheignite.readme.io/docs/binary-client-protocol-data-format#section-enum
.. _Byte array: https://apacheignite.readme.io/docs/binary-client-protocol-data-format#section-byte-array
.. _Short array: https://apacheignite.readme.io/docs/binary-client-protocol-data-format#section-short-array
.. _Int array: https://apacheignite.readme.io/docs/binary-client-protocol-data-format#section-int-array
.. _Long array: https://apacheignite.readme.io/docs/binary-client-protocol-data-format#section-long-array
.. _Float array: https://apacheignite.readme.io/docs/binary-client-protocol-data-format#section-float-array
.. _Double array: https://apacheignite.readme.io/docs/binary-client-protocol-data-format#section-double-array
.. _Char array: https://apacheignite.readme.io/docs/binary-client-protocol-data-format#section-char-array
.. _Bool array: https://apacheignite.readme.io/docs/binary-client-protocol-data-format#section-bool-array
.. _String array: https://apacheignite.readme.io/docs/binary-client-protocol-data-format#section-string-array
.. _UUID array: https://apacheignite.readme.io/docs/binary-client-protocol-data-format#section-uuid-guid-array
.. _Timestamp array: https://apacheignite.readme.io/docs/binary-client-protocol-data-format#section-timestamp-array
.. _Date array: https://apacheignite.readme.io/docs/binary-client-protocol-data-format#section-date-array
.. _Time array: https://apacheignite.readme.io/docs/binary-client-protocol-data-format#section-time-array
.. _Decimal array: https://apacheignite.readme.io/docs/binary-client-protocol-data-format#section-decimal-array
.. _Object array: https://apacheignite.readme.io/docs/binary-client-protocol-data-format#section-object-collections
.. _Collection: https://apacheignite.readme.io/docs/binary-client-protocol-data-format#section-collection
.. _Map: https://apacheignite.readme.io/docs/binary-client-protocol-data-format#section-map
.. _Enum array: https://apacheignite.readme.io/docs/binary-client-protocol-data-format#section-enum-array
.. _Binary enum: https://apacheignite.readme.io/docs/binary-client-protocol-data-format#section-binary-enum
.. _Wrapped data: https://apacheignite.readme.io/docs/binary-client-protocol-data-format#section-wrapped-data
.. _Complex object: https://apacheignite.readme.io/docs/binary-client-protocol-data-format#section-complex-object
