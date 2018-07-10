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

from collections import OrderedDict
import ctypes
import decimal
from datetime import date, datetime, timedelta
from importlib import import_module
import uuid

import attr

from pyignite.connection import Connection, PrefetchConnection
from pyignite.constants import *
from pyignite.exceptions import ParseError
from pyignite.utils import is_hinted, is_iterable
from .primitive import Int
from .primitive_arrays import *
from .primitive_objects import *
from .standard import *
from .null_object import *
from .type_codes import *


__all__ = [
    'AnyDataObject', 'AnyDataArray', 'Map',
    'ObjectArrayObject', 'CollectionObject', 'MapObject',
    'WrappedDataObject', 'BinaryObject',
]


def tc_map(key: bytes):
    return {
        TC_NULL: Null,

        TC_BYTE: ByteObject,
        TC_SHORT: ShortObject,
        TC_INT: IntObject,
        TC_LONG: LongObject,
        TC_FLOAT: FloatObject,
        TC_DOUBLE: DoubleObject,
        TC_CHAR: CharObject,
        TC_BOOL: BoolObject,

        TC_UUID: UUIDObject,
        TC_DATE: DateObject,
        TC_TIMESTAMP: TimestampObject,
        TC_TIME: TimeObject,
        TC_ENUM: EnumObject,
        TC_BINARY_ENUM: BinaryEnumObject,

        TC_BYTE_ARRAY: ByteArrayObject,
        TC_SHORT_ARRAY: ShortArrayObject,
        TC_INT_ARRAY: IntArrayObject,
        TC_LONG_ARRAY: LongArrayObject,
        TC_FLOAT_ARRAY: FloatArrayObject,
        TC_DOUBLE_ARRAY: DoubleArrayObject,
        TC_CHAR_ARRAY: CharArrayObject,
        TC_BOOL_ARRAY: BoolArrayObject,

        TC_UUID_ARRAY: UUIDArrayObject,
        TC_DATE_ARRAY: DateArrayObject,
        TC_TIMESTAMP_ARRAY: TimestampArrayObject,
        TC_TIME_ARRAY: TimeArrayObject,
        TC_ENUM_ARRAY: EnumArrayObject,

        TC_STRING: String,
        TC_STRING_ARRAY: StringArrayObject,
        TC_DECIMAL: DecimalObject,
        TC_DECIMAL_ARRAY: DecimalArrayObject,

        TC_OBJECT_ARRAY: ObjectArrayObject,
        TC_COLLECTION: CollectionObject,
        TC_MAP: MapObject,

        TC_COMPLEX_OBJECT: BinaryObject,
        TC_ARRAY_WRAPPED_OBJECTS: WrappedDataObject,
    }[key]


class AnyDataObject:
    """
    Not an actual Ignite type, but contains a guesswork
    on serializing Python data or parsing an unknown Ignite data object.
    """

    @staticmethod
    def get_subtype(iterable, allow_none=False):
        # arrays of these types can contain Null objects
        object_array_python_types = [
            str,
            datetime,
            timedelta,
            decimal.Decimal,
            uuid.UUID,
        ]

        iterator = iter(iterable)
        type_first = type(None)
        try:
            while isinstance(None, type_first):
                type_first = type(next(iterator))
        except StopIteration:
            raise TypeError(
                'Can not represent an empty iterable '
                'or an iterable of `NoneType` in Ignite type.'
            )

        if type_first in object_array_python_types:
            allow_none = True

        # if an iterable contains items of more than one non-nullable type,
        # return None
        if all([
            isinstance(x, type_first)
            or ((x is None) and allow_none) for x in iterator
        ]):
            return type_first

    @classmethod
    def parse(cls, conn: Connection):
        type_code = conn.recv(ctypes.sizeof(ctypes.c_byte))
        try:
            data_class = tc_map(type_code)
        except KeyError:
            raise ParseError('Unknown type code: `{}`'.format(type_code))
        return data_class.parse(PrefetchConnection(conn, prefetch=type_code))

    @classmethod
    def to_python(cls, ctype_object):
        type_code = ctype_object.type_code.to_bytes(
            ctypes.sizeof(ctypes.c_byte),
            byteorder=PROTOCOL_BYTE_ORDER
        )
        data_class = tc_map(type_code)
        return data_class.to_python(ctype_object)

    @classmethod
    def from_python(cls, value):
        python_map = {
            int: LongObject,
            float: DoubleObject,
            str: String,
            bytes: String,
            bool: BoolObject,
            type(None): Null,
            uuid.UUID: UUIDObject,
            datetime: DateObject,
            date: DateObject,
            timedelta: TimeObject,
            decimal.Decimal: DecimalObject,
        }

        python_array_map = {
            int: LongArrayObject,
            float: DoubleArrayObject,
            str: StringArrayObject,
            bytes: StringArrayObject,
            bool: BoolArrayObject,
            uuid.UUID: UUIDArrayObject,
            datetime: DateArrayObject,
            date: DateArrayObject,
            timedelta: TimeArrayObject,
            decimal.Decimal: DecimalArrayObject,
        }

        value_type = type(value)
        if is_iterable(value) and value_type is not str:
            value_subtype = cls.get_subtype(value)
            if value_subtype in python_array_map:
                return python_array_map[value_subtype].from_python(value)

            # a little heuristics (order may be important)
            if all([
                value_subtype is None,
                len(value) == 2,
                isinstance(value[0], int),
                isinstance(value[1], dict),
            ]):
                return MapObject.from_python(value)

            if all([
                value_subtype is None,
                len(value) == 2,
                isinstance(value[0], int),
                is_iterable(value[1]),
            ]):
                return ObjectArrayObject.from_python(value)

            raise TypeError(
                'Type `array of {}` is invalid'.format(value_subtype)
            )

        if value_type in python_map:
            return python_map[value_type].from_python(value)
        raise TypeError(
            'Type `{}` is invalid.'.format(value_type)
        )


@attr.s
class AnyDataArray(AnyDataObject):
    """
    Sequence of AnyDataObjects, payload-only.
    """
    counter_type = attr.ib(default=ctypes.c_int)

    def build_header(self):
        return type(
            self.__class__.__name__+'Header',
            (ctypes.LittleEndianStructure,),
            {
                '_pack_': 1,
                '_fields_': [
                    ('length', self.counter_type),
                ],
            }
        )

    def parse(self, conn: Connection):
        header_class = self.build_header()
        buffer = conn.recv(ctypes.sizeof(header_class))
        header = header_class.from_buffer_copy(buffer)
        fields = []

        for i in range(header.length):
            c_type, buffer_fragment = super().parse(conn)
            buffer += buffer_fragment
            fields.append(('element_{}'.format(i), c_type))

        final_class = type(
            self.__class__.__name__,
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
                super().to_python(
                    getattr(ctype_object, 'element_{}'.format(i))
                )
            )
        return result

    def from_python(self, value):
        header_class = self.build_header()
        header = header_class()

        try:
            length = len(value)
        except TypeError:
            value = [value]
            length = 1
        header.length = length
        buffer = bytes(header)

        for x in value:
            if is_hinted(x):
                buffer += x[1].from_python(x[0])
            else:
                buffer += super().from_python(x)
        return buffer


class ObjectArrayObject(AnyDataObject):
    """
    Array of objects of any type. Its Python representation is
    tuple(type_id, iterable of any type).
    """
    type_code = TC_OBJECT_ARRAY
    type_or_id_name = 'type_id'

    @classmethod
    def build_header(cls):
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
    def parse(cls, conn: Connection):
        header_class = cls.build_header()
        buffer = conn.recv(ctypes.sizeof(header_class))
        header = header_class.from_buffer_copy(buffer)
        fields = []

        for i in range(header.length):
            c_type, buffer_fragment = super().parse(conn)
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
                super().to_python(
                    getattr(ctype_object, 'element_{}'.format(i))
                )
            )
        return getattr(ctype_object, cls.type_or_id_name), result

    @classmethod
    def from_python(cls, value):
        type_or_id, value = value
        header_class = cls.build_header()
        header = header_class()
        header.type_code = int.from_bytes(
            cls.type_code,
            byteorder=PROTOCOL_BYTE_ORDER
        )
        try:
            length = len(value)
        except TypeError:
            value = [value]
            length = 1
        header.length = length
        setattr(header, cls.type_or_id_name, type_or_id)
        buffer = bytes(header)

        for x in value:
            buffer += super().from_python(x)
        return buffer


class WrappedDataObject:
    """
    One or more binary objects can be wrapped in an array. This allows reading,
    storing, passing and writing objects efficiently without understanding
    their contents, performing simple byte copy.

    Python representation: tuple(payload: bytes, offset: integer). Offset
    points to the root object of the array.
    """
    type_code = TC_ARRAY_WRAPPED_OBJECTS

    @classmethod
    def build_header(cls):
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

    @classmethod
    def parse(cls, conn: Connection):
        header_class = cls.build_header()
        buffer = conn.recv(ctypes.sizeof(header_class))
        header = header_class.from_buffer_copy(buffer)

        final_class = type(
            cls.__name__,
            (header_class,),
            {
                '_pack_': 1,
                '_fields_': [
                    ('payload', ctypes.c_byte*header.length),
                    ('offset', ctypes.c_int),
                ],
            }
        )
        buffer += conn.recv(
            ctypes.sizeof(final_class) - ctypes.sizeof(header_class)
        )
        return final_class, buffer

    @classmethod
    def to_python(cls, ctype_object):
        return bytes(ctype_object.payload), ctype_object.offset

    @classmethod
    def from_python(cls, value):
        payload, offset = value
        header_class = cls.build_header()
        header = header_class()
        header.type_code = int.from_bytes(
            cls.type_code,
            byteorder=PROTOCOL_BYTE_ORDER
        )
        length = len(value)
        header.length = length
        return bytes(header) + payload + Int.from_python(offset)


class CollectionObject(ObjectArrayObject):
    """
    Just like object array, but contains deserialization type hint instead of
    type id. This hint is also useless in Python, because the list type along
    covers all the use cases.

    Also represented as tuple(type_id, iterable of any type) in Python.
    """
    type_code = TC_COLLECTION
    type_or_id_name = 'type'

    @classmethod
    def build_header(cls):
        return type(
            cls.__name__+'Header',
            (ctypes.LittleEndianStructure,),
            {
                '_pack_': 1,
                '_fields_': [
                    ('type_code', ctypes.c_byte),
                    ('length', ctypes.c_int),
                    ('type', ctypes.c_byte),
                ],
            }
        )


class Map:
    """
    Dictionary type, payload-only.

    Ignite does not track the order of key-value pairs in its caches, hence
    the ordinary Python dict type, not the collections.OrderedDict.
    """
    HASH_MAP = 1
    LINKED_HASH_MAP = 2

    @classmethod
    def build_header(cls):
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
        header_class = cls.build_header()
        buffer = conn.recv(ctypes.sizeof(header_class))
        header = header_class.from_buffer_copy(buffer)
        fields = []

        for i in range(header.length << 1):
            c_type, buffer_fragment = AnyDataObject.parse(conn)
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
        map_type = getattr(ctype_object, 'type', cls.HASH_MAP)
        result = OrderedDict() if map_type == cls.LINKED_HASH_MAP else {}

        for i in range(0, ctype_object.length << 1, 2):
            k = AnyDataObject.to_python(
                    getattr(ctype_object, 'element_{}'.format(i))
                )
            v = AnyDataObject.to_python(
                    getattr(ctype_object, 'element_{}'.format(i + 1))
                )
            result[k] = v
        return result

    @classmethod
    def from_python(cls, value, type_id=None):
        header_class = cls.build_header()
        header = header_class()
        length = len(value)
        header.length = length
        if hasattr(header, 'type_code'):
            header.type_code = int.from_bytes(
                cls.type_code,
                byteorder=PROTOCOL_BYTE_ORDER
            )
        if hasattr(header, 'type'):
            header.type = type_id
        buffer = bytes(header)

        for k, v in value.items():
            if is_hinted(k):
                buffer += k[1].from_python(k[0])
            else:
                buffer += AnyDataObject.from_python(k)
            if is_hinted(v):
                buffer += v[1].from_python(v[0])
            else:
                buffer += AnyDataObject.from_python(v)
        return buffer


class MapObject(Map):
    """
    This is a dictionary type. Type conversion hint can be a `HASH_MAP`
    (ordinary dict) or `LINKED_HASH_MAP` (collections.OrderedDict).

    Keys and values in map are independent data objects, but `count`
    counts pairs. Very annoying.
    """
    type_code = TC_MAP

    @classmethod
    def build_header(cls):
        return type(
            cls.__name__+'Header',
            (ctypes.LittleEndianStructure,),
            {
                '_pack_': 1,
                '_fields_': [
                    ('type_code', ctypes.c_byte),
                    ('length', ctypes.c_int),
                    ('type', ctypes.c_byte),
                ],
            }
        )

    @classmethod
    def to_python(cls, ctype_object):
        return ctype_object.type, super().to_python(ctype_object)

    @classmethod
    def from_python(cls, value):
        type_id, value = value
        return super().from_python(value, type_id)


class BinaryObject:
    type_code = TC_COMPLEX_OBJECT

    USER_TYPE = 0x0001
    HAS_SCHEMA = 0x0002
    HAS_RAW_DATA = 0x0004
    OFFSET_ONE_BYTE = 0x0008
    OFFSET_TWO_BYTES = 0x0010
    COMPACT_FOOTER = 0x0020

    @classmethod
    def build_header(cls):
        return type(
            cls.__name__,
            (ctypes.LittleEndianStructure,),
            {
                '_pack_': 1,
                '_fields_': [
                    ('type_code', ctypes.c_byte),
                    ('version', ctypes.c_byte),
                    ('flags', ctypes.c_short),
                    ('type_id', ctypes.c_int),
                    ('hash_code', ctypes.c_int),
                    ('length', ctypes.c_int),
                    ('schema_id', ctypes.c_int),
                    ('schema_offset', ctypes.c_int),
                ],
            }
        )

    @classmethod
    def offset_c_type(cls, flags: int):
        if flags & cls.OFFSET_ONE_BYTE:
            return ctypes.c_byte
        if flags & cls.OFFSET_TWO_BYTES:
            return ctypes.c_short
        return ctypes.c_int

    @staticmethod
    def get_fields(conn: Connection, type_id: int) -> list:
        from pyignite.api import get_binary_type

        # get field names from outer space
        temp_conn = conn.clone()
        result = get_binary_type(temp_conn, type_id)
        temp_conn.close()
        return [
            (x['field_name'], tc_map(bytes([x['type_id']])))
            for x in result.value['binary_fields']
        ]

    @classmethod
    def parse(cls, conn: Connection):
        from pyignite.datatypes.cache_config import Struct

        header_class = cls.build_header()
        buffer = conn.recv(ctypes.sizeof(header_class))
        header = header_class.from_buffer_copy(buffer)

        # TODO: valid only on compact schema approach
        fields = cls.get_fields(conn, header.type_id)
        object_fields_struct = Struct(fields)
        object_fields, object_fields_buffer = object_fields_struct.parse(conn)
        buffer += object_fields_buffer
        final_class_fields = [('object_fields', object_fields)]

        if header.flags & cls.HAS_SCHEMA:
            schema = cls.offset_c_type(header.flags) * len(fields)
            buffer += conn.recv(ctypes.sizeof(schema))
            final_class_fields.append(('schema', schema))

        final_class = type(
            cls.__name__,
            (header_class,),
            {
                '_pack_': 1,
                '_fields_': final_class_fields,
            }
        )
        return final_class, buffer

    @classmethod
    def to_python(cls, ctype_object):
        result = {
            'version': ctype_object.version,
            'type_id': ctype_object.type_id,
            'hash_code': ctype_object.hash_code,
            'schema_id': ctype_object.schema_id,
            'fields': {},
        }
        # hack, but robust
        for field_name, field_c_type in ctype_object.object_fields._fields_:
            module = import_module(field_c_type.__module__)
            object_class = getattr(module, field_c_type.__name__)
            result['fields'][field_name] = object_class.to_python(
                getattr(ctype_object.object_fields, field_name)
            )
        return result

    @classmethod
    def from_python(cls, value: dict):
        from pyignite.datatypes.cache_config import Struct

        # prepare header
        header_class = cls.build_header()
        header = header_class()
        header.type_code = int.from_bytes(
            cls.type_code,
            byteorder=PROTOCOL_BYTE_ORDER
        )
        # TODO: compact footer & no raw data is the only supported variant
        header.flags = cls.USER_TYPE | cls.HAS_SCHEMA | cls.COMPACT_FOOTER
        header.version = value['version']
        header.type_id = value['type_id']
        header.hash_code = value['hash_code']
        header.schema_id = value['schema_id']
        # create fields
        field_data = {}
        field_types = []
        offsets = []
        for field_name, field_value_or_pair in value['fields'].items():
            field_name = field_name.upper()

            if is_hinted(field_value_or_pair):
                field_value, field_type = field_value_or_pair
            else:
                field_value = field_value_or_pair
                field_type = AnyDataObject

            field_data[field_name] = field_value
            field_types.append((field_name, field_type))

        field_struct = Struct(field_types)
        field_buffer, field_data_class = field_struct.from_python(field_data)
        # calculate offsets & create footer
        for _, field_c_type in field_data_class._fields_:
            offsets.append(
                max(offsets, default=0) + ctypes.sizeof(field_c_type)
            )
        if max(offsets, default=0) < 255:
            header.flags |= cls.OFFSET_ONE_BYTE
        elif max(offsets) < 65535:
            header.flags |= cls.OFFSET_TWO_BYTES
        schema_class = cls.offset_c_type(header.flags) * len(offsets)
        schema = schema_class()
        for i, offset in enumerate(offsets):
            schema[i] = offset
        # calculate size
        header.schema_offset = ctypes.sizeof(header_class) + len(field_buffer)
        header.length = header.schema_offset + ctypes.sizeof(schema_class)

        return bytes(header) + field_buffer + bytes(schema)
