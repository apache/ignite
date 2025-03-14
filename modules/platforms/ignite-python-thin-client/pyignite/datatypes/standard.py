# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import ctypes
from datetime import date, datetime, time, timedelta
import decimal
from io import SEEK_CUR
from math import ceil
from typing import Tuple, Union
import uuid

from pyignite.constants import *
from pyignite.utils import datetime_hashcode, decimal_hashcode, hashcode
from .base import IgniteDataType
from .type_codes import *
from .type_ids import *
from .type_names import *
from .null_object import Nullable

__all__ = [
    'String', 'DecimalObject', 'UUIDObject', 'TimestampObject', 'DateObject',
    'TimeObject',

    'StringArray', 'DecimalArray', 'UUIDArray', 'TimestampArray', 'DateArray',
    'TimeArray',

    'StringArrayObject', 'DecimalArrayObject', 'UUIDArrayObject',
    'TimestampArrayObject', 'TimeArrayObject', 'DateArrayObject',

    'EnumObject', 'EnumArray', 'EnumArrayObject', 'BinaryEnumObject',
    'BinaryEnumArrayObject', 'ObjectArray',
]


class StandardObject(Nullable):
    _type_name = None
    _type_id = None
    type_code = None

    @classmethod
    def build_c_type(cls):
        raise NotImplementedError('This object is generic')

    @classmethod
    def parse_not_null(cls, stream):
        data_type = cls.build_c_type()
        stream.seek(ctypes.sizeof(data_type), SEEK_CUR)
        return data_type


class String(Nullable):
    """
    Pascal-style string: `c_int` counter, followed by count*bytes.
    UTF-8-encoded, so that one character may take 1 to 4 bytes.
    """
    _type_name = NAME_STRING
    _type_id = TYPE_STRING
    type_code = TC_STRING
    pythonic = str

    @classmethod
    def hashcode(cls, value: str, **kwargs) -> int:
        return hashcode(value)

    @classmethod
    def build_c_type(cls, length: int):
        return type(
            cls.__name__,
            (ctypes.LittleEndianStructure,),
            {
                '_pack_': 1,
                '_fields_': [
                    ('type_code', ctypes.c_byte),
                    ('length', ctypes.c_int),
                    ('data', ctypes.c_char * length),
                ],
            },
        )

    @classmethod
    def parse_not_null(cls, stream):
        length = int.from_bytes(
            stream.slice(stream.tell() + ctypes.sizeof(ctypes.c_byte), ctypes.sizeof(ctypes.c_int)),
            byteorder=PROTOCOL_BYTE_ORDER
        )

        data_type = cls.build_c_type(length)
        stream.seek(ctypes.sizeof(data_type), SEEK_CUR)
        return data_type

    @classmethod
    def to_python_not_null(cls, ctypes_object, **kwargs):
        if ctypes_object.length > 0:
            return ctypes_object.data.decode(PROTOCOL_STRING_ENCODING)

        return ''

    @classmethod
    def from_python_not_null(cls, stream, value, **kwargs):
        if isinstance(value, str):
            value = value.encode(PROTOCOL_STRING_ENCODING)
        length = len(value)
        data_type = cls.build_c_type(length)
        data_object = data_type()
        data_object.type_code = int.from_bytes(
            cls.type_code,
            byteorder=PROTOCOL_BYTE_ORDER
        )
        data_object.length = length
        data_object.data = value

        stream.write(data_object)


class DecimalObject(Nullable):
    _type_name = NAME_DECIMAL
    _type_id = TYPE_DECIMAL
    type_code = TC_DECIMAL
    pythonic = decimal.Decimal
    default = decimal.Decimal('0.00')

    @classmethod
    def hashcode(cls, value: decimal.Decimal, **kwargs) -> int:
        return decimal_hashcode(value)

    @classmethod
    def build_c_type(cls, length):
        return type(
            cls.__name__,
            (ctypes.LittleEndianStructure,),
            {
                '_pack_': 1,
                '_fields_': [
                    ('type_code', ctypes.c_byte),
                    ('scale', ctypes.c_int),
                    ('length', ctypes.c_int),
                    ('data', ctypes.c_ubyte * length)
                ]
            }
        )

    @classmethod
    def parse_not_null(cls, stream):
        int_sz, b_sz = ctypes.sizeof(ctypes.c_int), ctypes.sizeof(ctypes.c_byte)
        length = int.from_bytes(
            stream.slice(stream.tell() + int_sz + b_sz, int_sz),
            byteorder=PROTOCOL_BYTE_ORDER
        )
        data_type = cls.build_c_type(length)
        stream.seek(ctypes.sizeof(data_type), SEEK_CUR)
        return data_type

    @classmethod
    def to_python_not_null(cls, ctypes_object, **kwargs):
        sign = 1 if ctypes_object.data[0] & 0x80 else 0
        data = ctypes_object.data[1:]
        data.insert(0, ctypes_object.data[0] & 0x7f)
        # decode n-byte integer
        result = sum([
            [x for x in reversed(data)][i] * 0x100 ** i for i in
            range(len(data))
        ])
        # apply scale
        result = result / decimal.Decimal('10') ** decimal.Decimal(ctypes_object.scale)
        if sign:
            # apply sign
            result = -result
        return result

    @classmethod
    def from_python_not_null(cls, stream, value: decimal.Decimal, **kwargs):
        sign, digits, scale = value.normalize().as_tuple()
        integer = int(''.join([str(d) for d in digits]))
        # calculate number of bytes (at least one, and not forget the sign bit)
        length = ceil((integer.bit_length() + 1) / 8)
        # write byte string
        data = []
        for i in range(length):
            digit = integer % 0x100
            integer //= 0x100
            data.insert(0, digit)
        # apply sign
        if sign:
            data[0] |= 0x80
        else:
            data[0] &= 0x7f
        data_class = cls.build_c_type(length)
        data_object = data_class()
        data_object.type_code = int.from_bytes(
            cls.type_code,
            byteorder=PROTOCOL_BYTE_ORDER
        )
        data_object.length = length
        data_object.scale = -scale
        for i in range(length):
            data_object.data[i] = data[i]

        stream.write(data_object)


class UUIDObject(StandardObject):
    """
    Universally unique identifier (UUID), aka Globally unique identifier
    (GUID). Payload takes up 16 bytes.

    Byte order in :py:meth:`~pyignite.datatypes.standard.UUIDObject.to_python`
    and :py:meth:`~pyignite.datatypes.standard.UUIDObject.from_python` methods
    is changed for compatibility with `java.util.UUID`.
    """
    _type_name = NAME_UUID
    _type_id = TYPE_UUID
    _object_c_type = None
    type_code = TC_UUID

    UUID_BYTE_ORDER = (7, 6, 5, 4, 3, 2, 1, 0, 15, 14, 13, 12, 11, 10, 9, 8)

    @classmethod
    def hashcode(cls, value: 'UUID', **kwargs) -> int:
        msb = value.int >> 64
        lsb = value.int & 0xffffffffffffffff
        hilo = msb ^ lsb
        return (hilo >> 32) ^ (hilo & 0xffffffff)

    @classmethod
    def build_c_type(cls):
        if cls._object_c_type is None:
            cls._object_c_type = type(
                cls.__name__,
                (ctypes.LittleEndianStructure,),
                {
                    '_pack_': 1,
                    '_fields_': [
                        ('type_code', ctypes.c_byte),
                        ('value', ctypes.c_byte * 16),
                    ],
                }
            )
        return cls._object_c_type

    @classmethod
    def from_python_not_null(cls, stream, value: uuid.UUID, **kwargs):
        data_type = cls.build_c_type()
        data_object = data_type()
        data_object.type_code = int.from_bytes(
            cls.type_code,
            byteorder=PROTOCOL_BYTE_ORDER
        )
        for i, byte in zip(cls.UUID_BYTE_ORDER, bytearray(value.bytes)):
            data_object.value[i] = byte

        stream.write(data_object)

    @classmethod
    def to_python_not_null(cls, ctypes_object, **kwargs):
        uuid_array = bytearray(ctypes_object.value)
        return uuid.UUID(
            bytes=bytes([uuid_array[i] for i in cls.UUID_BYTE_ORDER])
        )


class TimestampObject(StandardObject):
    """
    A signed integer number of milliseconds past 1 Jan 1970, aka Epoch
    (8 bytes long integer), plus the delta in nanoseconds (4 byte integer,
    only 0..999 range used).

    The accuracy is ridiculous. For instance, common HPETs have
    less than 10ms accuracy. Therefore no ns range calculations is made;
    `epoch` and `fraction` stored separately and represented as
    tuple(datetime.datetime, integer).
    """
    _type_name = NAME_TIMESTAMP
    _type_id = TYPE_TIMESTAMP
    _object_c_type = None
    type_code = TC_TIMESTAMP
    pythonic = tuple
    default = (datetime(1970, 1, 1), 0)

    @classmethod
    def hashcode(cls, value: Tuple[datetime, int], **kwargs) -> int:
        return datetime_hashcode(int(value[0].timestamp() * 1000))

    @classmethod
    def build_c_type(cls):
        if cls._object_c_type is None:
            cls._object_c_type = type(
                cls.__name__,
                (ctypes.LittleEndianStructure,),
                {
                    '_pack_': 1,
                    '_fields_': [
                        ('type_code', ctypes.c_byte),
                        ('epoch', ctypes.c_longlong),
                        ('fraction', ctypes.c_int),
                    ],
                }
            )
        return cls._object_c_type

    @classmethod
    def from_python_not_null(cls, stream, value: tuple, **kwargs):
        data_type = cls.build_c_type()
        data_object = data_type()
        data_object.type_code = int.from_bytes(
            cls.type_code,
            byteorder=PROTOCOL_BYTE_ORDER
        )
        data_object.epoch = int(value[0].timestamp() * 1000)
        data_object.fraction = value[1]

        stream.write(data_object)

    @classmethod
    def to_python_not_null(cls, ctypes_object, **kwargs):
        return (
            datetime.fromtimestamp(ctypes_object.epoch / 1000),
            ctypes_object.fraction
        )


class DateObject(StandardObject):
    """
    A signed integer number of milliseconds past 1 Jan 1970, aka Epoch
    (8 bytes long integer).

    Represented as a naive datetime.datetime in Python.
    """
    _type_name = NAME_DATE
    _type_id = TYPE_DATE
    _object_c_type = None
    type_code = TC_DATE
    pythonic = datetime
    default = datetime(1970, 1, 1)

    @classmethod
    def hashcode(cls, value: datetime, **kwargs) -> int:
        return datetime_hashcode(int(value.timestamp() * 1000))

    @classmethod
    def build_c_type(cls):
        if cls._object_c_type is None:
            cls._object_c_type = type(
                cls.__name__,
                (ctypes.LittleEndianStructure,),
                {
                    '_pack_': 1,
                    '_fields_': [
                        ('type_code', ctypes.c_byte),
                        ('epoch', ctypes.c_longlong),
                    ],
                }
            )
        return cls._object_c_type

    @classmethod
    def from_python_not_null(cls, stream, value: Union[date, datetime], **kwargs):
        if type(value) is date:
            value = datetime.combine(value, time())
        data_type = cls.build_c_type()
        data_object = data_type()
        data_object.type_code = int.from_bytes(
            cls.type_code,
            byteorder=PROTOCOL_BYTE_ORDER
        )
        data_object.epoch = int(value.timestamp() * 1000)

        stream.write(data_object)

    @classmethod
    def to_python_not_null(cls, ctypes_object, **kwargs):
        return datetime.fromtimestamp(ctypes_object.epoch / 1000)


class TimeObject(StandardObject):
    """
    Time of the day as a number of milliseconds since midnight.

    Represented as a datetime.timedelta in Python.
    """
    _type_name = NAME_TIME
    _type_id = TYPE_TIME
    _object_c_type = None
    type_code = TC_TIME
    pythonic = timedelta
    default = timedelta()

    @classmethod
    def hashcode(cls, value: timedelta, **kwargs) -> int:
        return datetime_hashcode(int(value.total_seconds() * 1000))

    @classmethod
    def build_c_type(cls):
        if cls._object_c_type is None:
            cls._object_c_type = type(
                cls.__name__,
                (ctypes.LittleEndianStructure,),
                {
                    '_pack_': 1,
                    '_fields_': [
                        ('type_code', ctypes.c_byte),
                        ('value', ctypes.c_longlong),
                    ],
                }
            )
        return cls._object_c_type

    @classmethod
    def from_python_not_null(cls, stream, value: timedelta, **kwargs):
        data_type = cls.build_c_type()
        data_object = data_type()
        data_object.type_code = int.from_bytes(
            cls.type_code,
            byteorder=PROTOCOL_BYTE_ORDER
        )
        data_object.value = int(value.total_seconds() * 1000)

        stream.write(data_object)

    @classmethod
    def to_python_not_null(cls, ctypes_object, **kwargs):
        return timedelta(milliseconds=ctypes_object.value)


class EnumObject(StandardObject):
    """
    Two integers used as the ID of the enumeration type, and its value.

    This type itself is useless in Python, but can be used for interoperability
    (using language-specific type serialization is a good way to kill the
    interoperability though), so it represented by tuple(int, int) in Python.
    """
    _type_name = 'Enum'
    _type_id = TYPE_ENUM
    _object_c_type = None
    type_code = TC_ENUM

    @classmethod
    def build_c_type(cls):
        if cls._object_c_type is None:
            cls._object_c_type = type(
                cls.__name__,
                (ctypes.LittleEndianStructure,),
                {
                    '_pack_': 1,
                    '_fields_': [
                        ('type_code', ctypes.c_byte),
                        ('type_id', ctypes.c_int),
                        ('ordinal', ctypes.c_int),
                    ],
                }
            )
        return cls._object_c_type

    @classmethod
    def from_python_not_null(cls, stream, value: tuple, **kwargs):
        data_type = cls.build_c_type()
        data_object = data_type()
        data_object.type_code = int.from_bytes(
            cls.type_code,
            byteorder=PROTOCOL_BYTE_ORDER
        )
        data_object.type_id, data_object.ordinal = value

        stream.write(data_object)

    @classmethod
    def to_python_not_null(cls, ctypes_object, **kwargs):
        return ctypes_object.type_id, ctypes_object.ordinal


class BinaryEnumObject(EnumObject):
    """
    Another way of representing the enum type. Same, but different.
    """
    _type_name = 'Enum'
    _type_id = TYPE_BINARY_ENUM
    type_code = TC_BINARY_ENUM


class _StandardArrayBase:
    standard_type = None

    @classmethod
    def _parse_header(cls, stream):
        raise NotImplementedError

    @classmethod
    def _parse(cls, stream):
        fields, length = cls._parse_header(stream)

        for i in range(length):
            c_type = cls.standard_type.parse(stream)
            fields.append((f'element_{i}', c_type))

        return type(
            cls.__name__,
            (ctypes.LittleEndianStructure,),
            {
                '_pack_': 1,
                '_fields_': fields,
            }
        )

    @classmethod
    def _write_header(cls, stream, value, **kwargs):
        raise NotImplementedError

    @classmethod
    def _from_python(cls, stream, value, **kwargs):
        cls._write_header(stream, value, **kwargs)
        for x in value:
            cls.standard_type.from_python(stream, x)

    @classmethod
    def _to_python(cls, ctypes_object, *args, **kwargs):
        length = ctypes_object.length
        return [
            cls.standard_type.to_python(
                getattr(ctypes_object, f'element_{i}'), *args, **kwargs
            ) for i in range(length)
        ]


class StandardArray(IgniteDataType, _StandardArrayBase):
    """
    Base class for array of primitives. Payload-only.
    """
    _type_name = None
    _type_id = None
    type_code = None

    @classmethod
    def _parse_header(cls, stream):
        int_sz = ctypes.sizeof(ctypes.c_int)
        length = int.from_bytes(
            stream.slice(stream.tell(), int_sz),
            byteorder=PROTOCOL_BYTE_ORDER
        )
        stream.seek(int_sz, SEEK_CUR)

        return [('length', ctypes.c_int)], length

    @classmethod
    def parse(cls, stream):
        return cls._parse(stream)

    @classmethod
    def _write_header(cls, stream, value, **kwargs):
        stream.write(
            len(value).to_bytes(
                length=ctypes.sizeof(ctypes.c_int),
                byteorder=PROTOCOL_BYTE_ORDER
            )
        )

    @classmethod
    def from_python(cls, stream, value, **kwargs):
        cls._from_python(stream, value, **kwargs)

    @classmethod
    def to_python(cls, ctypes_object, **kwargs):
        return cls._to_python(ctypes_object, **kwargs)


class StringArray(StandardArray):
    """
    Array of Pascal-like strings. Payload-only, i.e. no `type_code` field
    in binary representation.

    List(str) in Python.
    """
    _type_name = NAME_STRING_ARR
    _type_id = TYPE_STRING_ARR
    standard_type = String


class DecimalArray(StandardArray):
    _type_name = NAME_DECIMAL_ARR
    _type_id = TYPE_DECIMAL_ARR
    standard_type = DecimalObject


class UUIDArray(StandardArray):
    _type_name = NAME_UUID_ARR
    _type_id = TYPE_UUID_ARR
    standard_type = UUIDObject


class TimestampArray(StandardArray):
    _type_name = NAME_TIMESTAMP_ARR
    _type_id = TYPE_TIMESTAMP_ARR
    standard_type = TimestampObject


class DateArray(StandardArray):
    _type_name = NAME_DATE_ARR
    _type_id = TYPE_DATE_ARR
    standard_type = DateObject


class TimeArray(StandardArray):
    _type_name = NAME_TIME_ARR
    _type_id = TYPE_TIME_ARR
    standard_type = TimeObject


class EnumArray(StandardArray):
    _type_name = 'Enum[]'
    _type_id = TYPE_ENUM_ARR
    standard_type = EnumObject


class StandardArrayObject(Nullable, _StandardArrayBase):
    _type_name = None
    _type_id = None
    standard_type = None
    type_code = None
    pythonic = list
    default = []

    @classmethod
    def _parse_header(cls, stream):
        int_sz, b_sz = ctypes.sizeof(ctypes.c_int), ctypes.sizeof(ctypes.c_byte)
        length = int.from_bytes(
            stream.slice(stream.tell() + b_sz, int_sz),
            byteorder=PROTOCOL_BYTE_ORDER
        )
        stream.seek(int_sz + b_sz, SEEK_CUR)

        return [('type_code', ctypes.c_byte), ('length', ctypes.c_int)], length

    @classmethod
    def parse_not_null(cls, stream):
        return cls._parse(stream)

    @classmethod
    def _write_header(cls, stream, value, **kwargs):
        stream.write(cls.type_code)
        stream.write(
            len(value).to_bytes(
                length=ctypes.sizeof(ctypes.c_int),
                byteorder=PROTOCOL_BYTE_ORDER
            )
        )

    @classmethod
    def from_python_not_null(cls, stream, value, **kwargs):
        cls._from_python(stream, value, **kwargs)

    @classmethod
    def to_python_not_null(cls, ctypes_object, **kwargs):
        return cls._to_python(ctypes_object, **kwargs)


class StringArrayObject(StandardArrayObject):
    """ List of strings. """
    _type_name = NAME_STRING_ARR
    _type_id = TYPE_STRING_ARR
    standard_type = String
    type_code = TC_STRING_ARRAY


class DecimalArrayObject(StandardArrayObject):
    """ List of decimal.Decimal objects. """
    _type_name = NAME_DECIMAL_ARR
    _type_id = TYPE_DECIMAL_ARR
    standard_type = DecimalObject
    type_code = TC_DECIMAL_ARRAY


class UUIDArrayObject(StandardArrayObject):
    """ Translated into Python as a list(uuid.UUID). """
    _type_name = NAME_UUID_ARR
    _type_id = TYPE_UUID_ARR
    standard_type = UUIDObject
    type_code = TC_UUID_ARRAY


class TimestampArrayObject(StandardArrayObject):
    """
    Translated into Python as a list of (datetime.datetime, integer) tuples.
    """
    _type_name = NAME_TIMESTAMP_ARR
    _type_id = TYPE_TIMESTAMP_ARR
    standard_type = TimestampObject
    type_code = TC_TIMESTAMP_ARRAY


class DateArrayObject(StandardArrayObject):
    """ List of datetime.datetime type values. """
    _type_name = NAME_DATE_ARR
    _type_id = TYPE_DATE_ARR
    standard_type = DateObject
    type_code = TC_DATE_ARRAY


class TimeArrayObject(StandardArrayObject):
    """ List of datetime.timedelta type values. """
    _type_name = NAME_TIME_ARR
    _type_id = TYPE_TIME_ARR
    standard_type = TimeObject
    type_code = TC_TIME_ARRAY


class EnumArrayObject(StandardArrayObject):
    """
    Array of (int, int) tuples, plus it holds a `type_id` in its header.
    The only `type_id` value of -1 (user type) works from Python perspective.
    """
    _type_name = 'Enum[]'
    _type_id = TYPE_ENUM_ARR
    standard_type = EnumObject
    type_code = TC_ENUM_ARRAY

    OBJECT = -1

    @classmethod
    def _parse_header(cls, stream):
        int_sz, b_sz = ctypes.sizeof(ctypes.c_int), ctypes.sizeof(ctypes.c_byte)
        length = int.from_bytes(
            stream.slice(stream.tell() + b_sz + int_sz, int_sz),
            byteorder=PROTOCOL_BYTE_ORDER
        )
        stream.seek(2 * int_sz + b_sz, SEEK_CUR)
        return [('type_code', ctypes.c_byte), ('type_id', ctypes.c_int), ('length', ctypes.c_int)], length

    @classmethod
    def _write_header(cls, stream, value, type_id=-1):
        stream.write(cls.type_code)
        stream.write(
            type_id.to_bytes(
                length=ctypes.sizeof(ctypes.c_int),
                byteorder=PROTOCOL_BYTE_ORDER,
                signed=True
            )
        )
        stream.write(
            len(value).to_bytes(
                length=ctypes.sizeof(ctypes.c_int),
                byteorder=PROTOCOL_BYTE_ORDER
            )
        )

    @classmethod
    def from_python_not_null(cls, stream, value, **kwargs):
        type_id, value = value
        super().from_python_not_null(stream, value, type_id=type_id)

    @classmethod
    def to_python_not_null(cls, ctypes_object, **kwargs):
        return ctypes_object.type_id, cls._to_python(ctypes_object, **kwargs)


class BinaryEnumArrayObject(EnumArrayObject):
    standard_type = BinaryEnumObject


class ObjectArray(EnumArrayObject):
    standard_type = BinaryEnumObject
