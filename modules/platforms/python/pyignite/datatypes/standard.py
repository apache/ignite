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
from math import ceil
import uuid

from pyignite.constants import *
from .base import IgniteDataType
from .type_codes import *
from .null_object import Null


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


class StandardObject(IgniteDataType):
    type_code = None

    @classmethod
    def build_c_type(cls):
        raise NotImplementedError('This object is generic')

    @classmethod
    def parse(cls, client: 'Client'):
        tc_type = client.recv(ctypes.sizeof(ctypes.c_byte))

        if tc_type == TC_NULL:
            return Null.build_c_type(), tc_type

        c_type = cls.build_c_type()
        buffer = tc_type + client.recv(ctypes.sizeof(c_type) - len(tc_type))
        return c_type, buffer


class String(IgniteDataType):
    """
    Pascal-style string: `c_int` counter, followed by count*bytes.
    UTF-8-encoded, so that one character may take 1 to 4 bytes.
    """
    type_code = TC_STRING
    pythonic = str

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
    def parse(cls, client: 'Client'):
        tc_type = client.recv(ctypes.sizeof(ctypes.c_byte))
        # String or Null
        if tc_type == TC_NULL:
            return Null.build_c_type(), tc_type

        buffer = tc_type + client.recv(ctypes.sizeof(ctypes.c_int))
        length = int.from_bytes(buffer[1:], byteorder=PROTOCOL_BYTE_ORDER)

        data_type = cls.build_c_type(length)
        buffer += client.recv(ctypes.sizeof(data_type) - len(buffer))

        return data_type, buffer

    @staticmethod
    def to_python(ctype_object, *args, **kwargs):
        length = getattr(ctype_object, 'length', None)
        if length is None:
            return None
        elif length > 0:
            return ctype_object.data.decode(PROTOCOL_STRING_ENCODING)
        else:
            return ''

    @classmethod
    def from_python(cls, value):
        if value is None:
            return Null.from_python()

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
        return bytes(data_object)


class DecimalObject(IgniteDataType):
    type_code = TC_DECIMAL
    pythonic = decimal.Decimal
    default = decimal.Decimal('0.00')

    @classmethod
    def build_c_header(cls):
        return type(
            cls.__name__,
            (ctypes.LittleEndianStructure,),
            {
                '_pack_': 1,
                '_fields_': [
                    ('type_code', ctypes.c_byte),
                    ('scale', ctypes.c_int),
                    ('length', ctypes.c_int),
                ],
            }
        )

    @classmethod
    def parse(cls, client: 'Client'):
        tc_type = client.recv(ctypes.sizeof(ctypes.c_byte))
        # Decimal or Null
        if tc_type == TC_NULL:
            return Null.build_c_type(), tc_type

        header_class = cls.build_c_header()
        buffer = tc_type + client.recv(
            ctypes.sizeof(header_class)
            - len(tc_type)
        )
        header = header_class.from_buffer_copy(buffer)
        data_type = type(
            cls.__name__,
            (header_class,),
            {
                '_pack_': 1,
                '_fields_': [
                    ('data', ctypes.c_ubyte * header.length),
                ],
            }
        )
        buffer += client.recv(
            ctypes.sizeof(data_type)
            - ctypes.sizeof(header_class)
        )
        return data_type, buffer

    @classmethod
    def to_python(cls, ctype_object, *args, **kwargs):
        if getattr(ctype_object, 'length', None) is None:
            return None

        sign = 1 if ctype_object.data[0] & 0x80 else 0
        data = ctype_object.data[1:]
        data.insert(0, ctype_object.data[0] & 0x7f)
        # decode n-byte integer
        result = sum([
            [x for x in reversed(data)][i] * 0x100 ** i for i in
            range(len(data))
        ])
        # apply scale
        result = (
            result
            / decimal.Decimal('10')
            ** decimal.Decimal(ctype_object.scale)
        )
        if sign:
            # apply sign
            result = -result
        return result

    @classmethod
    def from_python(cls, value: decimal.Decimal):
        if value is None:
            return Null.from_python()

        sign, digits, scale = value.normalize().as_tuple()
        integer = int(''.join([str(d) for d in digits]))
        # calculate number of bytes (at least one, and not forget the sign bit)
        length = ceil((integer.bit_length() + 1)/8)
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
        header_class = cls.build_c_header()
        data_class = type(
            cls.__name__,
            (header_class,),
            {
                '_pack_': 1,
                '_fields_': [
                    ('data', ctypes.c_ubyte * length),
                ],
            }
        )
        data_object = data_class()
        data_object.type_code = int.from_bytes(
            cls.type_code,
            byteorder=PROTOCOL_BYTE_ORDER
        )
        data_object.length = length
        data_object.scale = -scale
        for i in range(length):
            data_object.data[i] = data[i]
        return bytes(data_object)


class UUIDObject(StandardObject):
    """
    Universally unique identifier (UUID), aka Globally unique identifier
    (GUID). Payload takes up 16 bytes.

    Byte order in :py:meth:`~pyignite.datatypes.standard.UUIDObject.to_python`
    and :py:meth:`~pyignite.datatypes.standard.UUIDObject.from_python` methods
    is changed for compatibility with `java.util.UUID`.
    """
    type_code = TC_UUID
    _object_c_type = None

    UUID_BYTE_ORDER = (7, 6, 5, 4, 3, 2, 1, 0, 15, 14, 13, 12, 11, 10, 9, 8)

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
    def from_python(cls, value: uuid.UUID):
        data_type = cls.build_c_type()
        data_object = data_type()
        data_object.type_code = int.from_bytes(
            cls.type_code,
            byteorder=PROTOCOL_BYTE_ORDER
        )
        for i, byte in zip(cls.UUID_BYTE_ORDER, bytearray(value.bytes)):
            data_object.value[i] = byte
        return bytes(data_object)

    @classmethod
    def to_python(cls, ctypes_object, *args, **kwargs):
        if ctypes_object.type_code == int.from_bytes(
            TC_NULL,
            byteorder=PROTOCOL_BYTE_ORDER
        ):
            return None
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
    type_code = TC_TIMESTAMP
    pythonic = tuple
    default = (datetime(1970, 1, 1), 0)
    _object_c_type = None

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
    def from_python(cls, value: tuple):
        if value is None:
            return Null.from_python()
        data_type = cls.build_c_type()
        data_object = data_type()
        data_object.type_code = int.from_bytes(
            cls.type_code,
            byteorder=PROTOCOL_BYTE_ORDER
        )
        data_object.epoch = int(value[0].timestamp() * 1000)
        data_object.fraction = value[1]
        return bytes(data_object)

    @classmethod
    def to_python(cls, ctypes_object, *args, **kwargs):
        if ctypes_object.type_code == int.from_bytes(
            TC_NULL,
            byteorder=PROTOCOL_BYTE_ORDER
        ):
            return None
        return (
            datetime.fromtimestamp(ctypes_object.epoch/1000),
            ctypes_object.fraction
        )


class DateObject(StandardObject):
    """
    A signed integer number of milliseconds past 1 Jan 1970, aka Epoch
    (8 bytes long integer).

    Represented as a naive datetime.datetime in Python.
    """
    type_code = TC_DATE
    pythonic = datetime
    default = datetime(1970, 1, 1)
    _object_c_type = None

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
    def from_python(cls, value: [date, datetime]):
        if value is None:
            return Null.from_python()
        if type(value) is date:
            value = datetime.combine(value, time())
        data_type = cls.build_c_type()
        data_object = data_type()
        data_object.type_code = int.from_bytes(
            cls.type_code,
            byteorder=PROTOCOL_BYTE_ORDER
        )
        data_object.epoch = int(value.timestamp() * 1000)
        return bytes(data_object)

    @classmethod
    def to_python(cls, ctypes_object, *args, **kwargs):
        if ctypes_object.type_code == int.from_bytes(
            TC_NULL,
            byteorder=PROTOCOL_BYTE_ORDER
        ):
            return None
        return datetime.fromtimestamp(ctypes_object.epoch/1000)


class TimeObject(StandardObject):
    """
    Time of the day as a number of milliseconds since midnight.

    Represented as a datetime.timedelta in Python.
    """
    type_code = TC_TIME
    pythonic = timedelta
    default = timedelta()
    _object_c_type = None

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
    def from_python(cls, value: timedelta):
        if value is None:
            return Null.from_python()
        data_type = cls.build_c_type()
        data_object = data_type()
        data_object.type_code = int.from_bytes(
            cls.type_code,
            byteorder=PROTOCOL_BYTE_ORDER
        )
        data_object.value = int(value.total_seconds() * 1000)
        return bytes(data_object)

    @classmethod
    def to_python(cls, ctypes_object, *args, **kwargs):
        if ctypes_object.type_code == int.from_bytes(
            TC_NULL,
            byteorder=PROTOCOL_BYTE_ORDER
        ):
            return None
        return timedelta(milliseconds=ctypes_object.value)


class EnumObject(StandardObject):
    """
    Two integers used as the ID of the enumeration type, and its value.

    This type itself is useless in Python, but can be used for interoperability
    (using language-specific type serialization is a good way to kill the
    interoperability though), so it represented by tuple(int, int) in Python.
    """
    type_code = TC_ENUM
    _object_c_type = None

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
    def from_python(cls, value: tuple):
        if value is None:
            return Null.from_python()

        data_type = cls.build_c_type()
        data_object = data_type()
        data_object.type_code = int.from_bytes(
            cls.type_code,
            byteorder=PROTOCOL_BYTE_ORDER
        )
        if value is None:
            return Null.from_python(value)
        data_object.type_id, data_object.ordinal = value
        return bytes(data_object)

    @classmethod
    def to_python(cls, ctypes_object, *args, **kwargs):
        if ctypes_object.type_code == int.from_bytes(
            TC_NULL,
            byteorder=PROTOCOL_BYTE_ORDER
        ):
            return None
        return ctypes_object.type_id, ctypes_object.ordinal


class BinaryEnumObject(EnumObject):
    """
    Another way of representing the enum type. Same, but different.
    """
    type_code = TC_BINARY_ENUM


class StandardArray(IgniteDataType):
    """
    Base class for array of primitives. Payload-only.
    """
    standard_type = None
    type_code = None

    @classmethod
    def build_header_class(cls):
        return type(
            cls.__name__+'Header',
            (ctypes.LittleEndianStructure,),
            {
                '_pack_': 1,
                '_fields_': [
                    ('length', ctypes.c_int),
                ],
            }
        )

    @classmethod
    def parse(cls, client: 'Client'):
        header_class = cls.build_header_class()
        buffer = client.recv(ctypes.sizeof(header_class))
        header = header_class.from_buffer_copy(buffer)
        fields = []
        for i in range(header.length):
            c_type, buffer_fragment = cls.standard_type.parse(client)
            buffer += buffer_fragment
            fields.append(('element_{}'.format(i), c_type))

        final_class = type(
            cls.__name__,
            (header_class,),
            {
                '_pack_': 1,
                '_fields_': fields,
            }
        )
        return final_class, buffer

    @classmethod
    def to_python(cls, ctype_object, *args, **kwargs):
        result = []
        for i in range(ctype_object.length):
            result.append(
                cls.standard_type.to_python(
                    getattr(ctype_object, 'element_{}'.format(i)),
                    *args, **kwargs
                )
            )
        return result

    @classmethod
    def from_python(cls, value):
        header_class = cls.build_header_class()
        header = header_class()
        if hasattr(header, 'type_code'):
            header.type_code = int.from_bytes(
                cls.type_code,
                byteorder=PROTOCOL_BYTE_ORDER
            )
        length = len(value)
        header.length = length
        buffer = bytes(header)

        for x in value:
            buffer += cls.standard_type.from_python(x)
        return buffer


class StringArray(StandardArray):
    """
    Array of Pascal-like strings. Payload-only, i.e. no `type_code` field
    in binary representation.

    List(str) in Python.
    """
    standard_type = String


class DecimalArray(StandardArray):
    standard_type = DecimalObject


class UUIDArray(StandardArray):
    standard_type = UUIDObject


class TimestampArray(StandardArray):
    standard_type = TimestampObject


class DateArray(StandardArray):
    standard_type = DateObject


class TimeArray(StandardArray):
    standard_type = TimeObject


class EnumArray(StandardArray):
    standard_type = EnumObject


class StandardArrayObject(StandardArray):
    pythonic = list
    default = []

    @classmethod
    def build_header_class(cls):
        return type(
            cls.__name__+'Header',
            (ctypes.LittleEndianStructure,),
            {
                '_pack_': 1,
                '_fields_': [
                    ('type_code', ctypes.c_byte),
                    ('length', ctypes.c_int),
                ],
            }
        )


class StringArrayObject(StandardArrayObject):
    """ List of strings. """
    standard_type = String
    type_code = TC_STRING_ARRAY


class DecimalArrayObject(StandardArrayObject):
    """ List of decimal.Decimal objects. """
    standard_type = DecimalObject
    type_code = TC_DECIMAL_ARRAY


class UUIDArrayObject(StandardArrayObject):
    """ Translated into Python as a list(uuid.UUID)"""
    standard_type = UUIDObject
    type_code = TC_UUID_ARRAY


class TimestampArrayObject(StandardArrayObject):
    """
    Translated into Python as a list of (datetime.datetime, integer) tuples.
    """
    standard_type = TimestampObject
    type_code = TC_TIMESTAMP_ARRAY


class DateArrayObject(StandardArrayObject):
    """ List of datetime.datetime type values. """
    standard_type = DateObject
    type_code = TC_DATE_ARRAY


class TimeArrayObject(StandardArrayObject):
    """ List of datetime.timedelta type values. """
    standard_type = TimeObject
    type_code = TC_TIME_ARRAY


class EnumArrayObject(StandardArrayObject):
    """
    Array of (int, int) tuples, plus it holds a `type_id` in its header.
    The only `type_id` value of -1 (user type) works from Python perspective.
    """
    standard_type = EnumObject
    type_code = TC_ENUM_ARRAY

    @classmethod
    def build_header_class(cls):
        return type(
            cls.__name__+'Header',
            (ctypes.LittleEndianStructure,),
            {
                '_pack_': 1,
                '_fields_': [
                    ('type_code', ctypes.c_byte),
                    ('type_id', ctypes.c_int),
                    ('length', ctypes.c_int),
                ],
            }
        )

    @classmethod
    def from_python(cls, value):
        type_id, value = value
        header_class = cls.build_header_class()
        header = header_class()
        if hasattr(header, 'type_code'):
            header.type_code = int.from_bytes(
                cls.type_code,
                byteorder=PROTOCOL_BYTE_ORDER
            )
        length = len(value)
        header.length = length
        header.type_id = type_id
        buffer = bytes(header)

        for x in value:
            buffer += cls.standard_type.from_python(x)
        return buffer

    @classmethod
    def to_python(cls, ctype_object, *args, **kwargs):
        type_id = ctype_object.type_id
        return type_id, super().to_python(ctype_object, *args, **kwargs)


class BinaryEnumArrayObject(EnumArrayObject):
    standard_type = BinaryEnumObject


class ObjectArray(EnumArrayObject):
    standard_type = BinaryEnumObject
