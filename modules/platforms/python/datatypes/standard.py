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
from datetime import datetime, timedelta
import uuid

from connection import Connection
from constants import *
from .type_codes import *
from .null_object import Null


__all__ = [
    'String', 'UUIDObject', 'TimestampObject', 'DateObject', 'TimeObject',
    'EnumObject', 'StringArray', 'UUIDArray', 'TimestampArray', 'DateArray',
    'TimeArray', 'EnumArray', 'StringArrayObject', 'UUIDArrayObject',
    'TimestampArrayObject', 'TimeArrayObject', 'DateArrayObject',
    'EnumArrayObject', 'BinaryEnumObject', 'BinaryEnumArrayObject',
    'ObjectArray',
]


class StandardObject:
    tc_type = None

    @classmethod
    def build_c_type(cls):
        raise NotImplementedError('')

    @classmethod
    def parse(cls, conn: Connection):
        tc_type = conn.recv(ctypes.sizeof(ctypes.c_byte))

        if tc_type == TC_NULL:
            return Null.build_c_type(), tc_type

        c_type = cls.build_c_type()
        buffer = tc_type + conn.recv(ctypes.sizeof(c_type) - len(tc_type))
        return c_type, buffer


class String:
    """
    Pascal-style string: `c_int` counter, followed by count*bytes.
    UTF-8-encoded, so that one character may take 1 to 4 bytes.
    """

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
    def parse(cls, conn: Connection):
        tc_type = conn.recv(ctypes.sizeof(ctypes.c_byte))
        # String or Null
        if tc_type == TC_NULL:
            return Null.build_c_type(), tc_type

        buffer = tc_type + conn.recv(ctypes.sizeof(ctypes.c_int))
        length = int.from_bytes(buffer[1:], byteorder=PROTOCOL_BYTE_ORDER)

        data_type = cls.build_c_type(length)
        buffer += conn.recv(ctypes.sizeof(data_type) - len(buffer))

        return data_type, buffer

    @staticmethod
    def to_python(ctype_object):
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
            TC_STRING,
            byteorder=PROTOCOL_BYTE_ORDER
        )
        data_object.length = length
        data_object.data = value
        return bytes(data_object)


class UUIDObject(StandardObject):
    """
    Universally unique identifier (UUID), aka Globally unique identifier
    (GUID). Payload takes up 16 bytes.
    """
    tc_type = TC_UUID

    @classmethod
    def build_c_type(cls):
        return type(
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

    @classmethod
    def from_python(cls, value: uuid.UUID):
        data_type = cls.build_c_type()
        data_object = data_type()
        data_object.type_code = int.from_bytes(
            cls.tc_type,
            byteorder=PROTOCOL_BYTE_ORDER
        )
        for i, byte in enumerate(bytearray(value.bytes)):
            data_object.value[i] = byte
        return bytes(data_object)

    @classmethod
    def to_python(cls, ctypes_object):
        if ctypes_object.type_code == int.from_bytes(
            TC_NULL,
            byteorder=PROTOCOL_BYTE_ORDER
        ):
            return None
        return uuid.UUID(bytes=bytes(ctypes_object.value))


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
    tc_type = TC_TIMESTAMP

    @classmethod
    def build_c_type(cls):
        return type(
            cls.__name__,
            (ctypes.LittleEndianStructure,),
            {
                '_pack_': 1,
                '_fields_': [
                    ('type_code', ctypes.c_byte),
                    ('epoch', ctypes.c_long),
                    ('fraction', ctypes.c_int),
                ],
            }
        )

    @classmethod
    def from_python(cls, value: tuple):
        if value is None:
            return Null.from_python()
        data_type = cls.build_c_type()
        data_object = data_type()
        data_object.type_code = int.from_bytes(
            cls.tc_type,
            byteorder=PROTOCOL_BYTE_ORDER
        )
        data_object.epoch = int(value[0].timestamp() * 1000)
        data_object.fraction = value[1]
        return bytes(data_object)

    @classmethod
    def to_python(cls, ctypes_object):
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
    tc_type = TC_DATE

    @classmethod
    def build_c_type(cls):
        return type(
            cls.__name__,
            (ctypes.LittleEndianStructure,),
            {
                '_pack_': 1,
                '_fields_': [
                    ('type_code', ctypes.c_byte),
                    ('epoch', ctypes.c_long),
                ],
            }
        )

    @classmethod
    def from_python(cls, value: datetime):
        if value is None:
            return Null.from_python()
        data_type = cls.build_c_type()
        data_object = data_type()
        data_object.type_code = int.from_bytes(
            cls.tc_type,
            byteorder=PROTOCOL_BYTE_ORDER
        )
        data_object.epoch = int(value.timestamp() * 1000)
        return bytes(data_object)

    @classmethod
    def to_python(cls, ctypes_object):
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
    tc_type = TC_TIME

    @classmethod
    def build_c_type(cls):
        return type(
            cls.__name__,
            (ctypes.LittleEndianStructure,),
            {
                '_pack_': 1,
                '_fields_': [
                    ('type_code', ctypes.c_byte),
                    ('value', ctypes.c_long),
                ],
            }
        )

    @classmethod
    def from_python(cls, value: timedelta):
        if value is None:
            return Null.from_python()
        data_type = cls.build_c_type()
        data_object = data_type()
        data_object.type_code = int.from_bytes(
            cls.tc_type,
            byteorder=PROTOCOL_BYTE_ORDER
        )
        data_object.value = int(value.total_seconds() * 1000)
        return bytes(data_object)

    @classmethod
    def to_python(cls, ctypes_object):
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
    tc_type = TC_ENUM

    @classmethod
    def build_c_type(cls):
        return type(
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

    @classmethod
    def from_python(cls, value: tuple):
        if value is None:
            return Null.from_python()
        data_type = cls.build_c_type()
        data_object = data_type()
        data_object.type_code = int.from_bytes(
            cls.tc_type,
            byteorder=PROTOCOL_BYTE_ORDER
        )
        if value is None:
            return Null.from_python(value)
        data_object.type_id, data_object.ordinal = value
        return bytes(data_object)

    @classmethod
    def to_python(cls, ctypes_object):
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
    tc_type = TC_BINARY_ENUM


class StandardArray:
    """
    Base class for array of primitives. Payload-only.
    """
    standard_type = None
    tc_type = None

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
    def parse(cls, conn: Connection):
        header_class = cls.build_header_class()
        buffer = conn.recv(ctypes.sizeof(header_class))
        header = header_class.from_buffer_copy(buffer)
        fields = []
        for i in range(header.length):
            c_type, buffer_fragment = cls.standard_type.parse(conn)
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
    def to_python(cls, ctype_object):
        result = []
        for i in range(ctype_object.length):
            result.append(
                cls.standard_type.to_python(
                    getattr(ctype_object, 'element_{}'.format(i))
                )
            )
        return result

    @classmethod
    def from_python(cls, value):
        header_class = cls.build_header_class()
        header = header_class()
        if hasattr(header, 'type_code'):
            header.type_code = int.from_bytes(
                cls.tc_type,
                byteorder=PROTOCOL_BYTE_ORDER
            )
        length = len(value)
        header.length = length
        buffer = bytes(header)

        for x in value:
            buffer += cls.standard_type.from_python(x)
        return buffer


class StringArray:
    """
    Array of Pascal-like strings. Payload-only, i.e. no `type_code` field
    in binary representation.

    List(str) in Python.
    """

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
    def parse(cls, conn: Connection):
        header_class = cls.build_header_class()
        buffer = conn.recv(ctypes.sizeof(header_class))
        header = header_class.from_buffer_copy(buffer)
        fields = []
        for i in range(header.length):
            c_type, buffer_fragment = String.parse(conn)
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
    def to_python(cls, ctype_object):
        result = []
        for i in range(ctype_object.length):
            result.append(
                String.to_python(
                    getattr(ctype_object, 'element_{}'.format(i))
                )
            )
        return result

    @classmethod
    def from_python(cls, value):
        header_class = cls.build_header_class()
        header = header_class()
        if hasattr(header, 'type_code'):
            header.type_code = int.from_bytes(
                TC_STRING_ARRAY,
                byteorder=PROTOCOL_BYTE_ORDER
            )
        header.length = len(value)
        buffer = bytes(header)

        for string in value:
            buffer += String.from_python(string)
        return buffer


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


class UUIDArrayObject(StandardArrayObject):
    """ Translated into Python as a list(uuid.UUID)"""
    standard_type = UUIDObject
    tc_type = TC_UUID_ARRAY


class TimestampArrayObject(StandardArrayObject):
    """
    Translated into Python as a list of (datetime.datetime, integer) tuples.
    """
    standard_type = TimestampObject
    tc_type = TC_TIMESTAMP_ARRAY


class DateArrayObject(StandardArrayObject):
    """ List of datetime.datetime type values. """
    standard_type = DateObject
    tc_type = TC_DATE_ARRAY


class TimeArrayObject(StandardArrayObject):
    """ List of datetime.timedelta type values. """
    standard_type = TimeObject
    tc_type = TC_TIME_ARRAY


class EnumArrayObject(StandardArrayObject):
    """
    Array of (int, int) tuples, plus it holds a `type_id` in its header.
    The only `type_id` value of -1 (user type) works from Python perspective.
    """
    standard_type = EnumObject
    tc_type = TC_ENUM_ARRAY

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
                cls.tc_type,
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
    def to_python(cls, ctype_object):
        type_id = ctype_object.type_id
        return type_id, super().to_python(ctype_object)


class BinaryEnumArrayObject(EnumArrayObject):
    standard_type = BinaryEnumObject


class ObjectArray(EnumArrayObject):
    standard_type = BinaryEnumObject


class StringArrayObject(StringArray):
    """
    Array of Pascal-like strings. Have `type_code`.

    List(str) in Python.
    """

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
