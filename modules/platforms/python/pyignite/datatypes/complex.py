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
import inspect

from pyignite.constants import *
from pyignite.exceptions import ParseError
from pyignite.utils import entity_id, hashcode, is_hinted
from .internal import AnyDataObject
from .type_codes import *


__all__ = [
    'Map', 'ObjectArrayObject', 'CollectionObject', 'MapObject',
    'WrappedDataObject', 'BinaryObject',
]


class ObjectArrayObject:
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
    def parse(cls, client: 'Client'):
        header_class = cls.build_header()
        buffer = client.recv(ctypes.sizeof(header_class))
        header = header_class.from_buffer_copy(buffer)
        fields = []

        for i in range(header.length):
            c_type, buffer_fragment = AnyDataObject.parse(client)
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
                AnyDataObject.to_python(
                    getattr(ctype_object, 'element_{}'.format(i)),
                    *args, **kwargs
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
            buffer += AnyDataObject.from_python(x)
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
    def parse(cls, client: 'Client'):
        header_class = cls.build_header()
        buffer = client.recv(ctypes.sizeof(header_class))
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
        buffer += client.recv(
            ctypes.sizeof(final_class) - ctypes.sizeof(header_class)
        )
        return final_class, buffer

    @classmethod
    def to_python(cls, ctype_object, *args, **kwargs):
        return bytes(ctype_object.payload), ctype_object.offset

    @classmethod
    def from_python(cls, value):
        raise ParseError('Send unwrapped data.')


class CollectionObject(ObjectArrayObject):
    """
    Just like object array, but contains deserialization type hint instead of
    type id. This hint is also useless in Python, because the list type along
    covers all the use cases.

    Also represented as tuple(type_id, iterable of any type) in Python.
    """
    type_code = TC_COLLECTION
    type_or_id_name = 'type'
    pythonic = list
    default = []

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
    def parse(cls, client: 'Client'):
        header_class = cls.build_header()
        buffer = client.recv(ctypes.sizeof(header_class))
        header = header_class.from_buffer_copy(buffer)
        fields = []

        for i in range(header.length << 1):
            c_type, buffer_fragment = AnyDataObject.parse(client)
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
        map_type = getattr(ctype_object, 'type', cls.HASH_MAP)
        result = OrderedDict() if map_type == cls.LINKED_HASH_MAP else {}

        for i in range(0, ctype_object.length << 1, 2):
            k = AnyDataObject.to_python(
                    getattr(ctype_object, 'element_{}'.format(i)),
                    *args, **kwargs
                )
            v = AnyDataObject.to_python(
                    getattr(ctype_object, 'element_{}'.format(i + 1)),
                    *args, **kwargs
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
    pythonic = dict
    default = {}

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
    def to_python(cls, ctype_object, *args, **kwargs):
        return ctype_object.type, super().to_python(
            ctype_object, *args, **kwargs
        )

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
            return ctypes.c_ubyte
        if flags & cls.OFFSET_TWO_BYTES:
            return ctypes.c_uint16
        return ctypes.c_uint

    @classmethod
    def schema_type(cls, flags: int):
        if flags & cls.COMPACT_FOOTER:
            return cls.offset_c_type(flags)
        return type(
            'SchemaElement',
            (ctypes.LittleEndianStructure,),
            {
                '_pack_': 1,
                '_fields_': [
                    ('field_id', ctypes.c_int),
                    ('offset', cls.offset_c_type(flags)),
                ],
            },
        )

    @staticmethod
    def get_dataclass(client: 'Client', header) -> OrderedDict:
        # get field names from outer space
        temp_conn = client.clone()
        result = temp_conn.query_binary_type(header.type_id, header.schema_id)
        temp_conn.close()
        if not result:
            raise ParseError('Binary type is not registered')
        return result

    @classmethod
    def parse(cls, client: 'Client'):
        from pyignite.datatypes import Struct

        header_class = cls.build_header()
        buffer = client.recv(ctypes.sizeof(header_class))
        header = header_class.from_buffer_copy(buffer)

        # ignore full schema, always retrieve fields' types and order
        # from complex types registry
        data_class = cls.get_dataclass(client, header)
        fields = data_class.schema.items()
        object_fields_struct = Struct(fields)
        object_fields, object_fields_buffer = object_fields_struct.parse(client)
        buffer += object_fields_buffer
        final_class_fields = [('object_fields', object_fields)]

        if header.flags & cls.HAS_SCHEMA:
            schema = cls.schema_type(header.flags) * len(fields)
            buffer += client.recv(ctypes.sizeof(schema))
            final_class_fields.append(('schema', schema))

        final_class = type(
            cls.__name__,
            (header_class,),
            {
                '_pack_': 1,
                '_fields_': final_class_fields,
            }
        )
        # register schema encoding approach
        client.compact_footer = bool(header.flags & cls.COMPACT_FOOTER)
        return final_class, buffer

    @classmethod
    def to_python(cls, ctype_object, client: 'Client'=None, *args, **kwargs):

        if not client:
            raise ParseError(
                'Can not query binary type {}'.format(ctype_object.type_id)
            )

        data_class = client.query_binary_type(
            ctype_object.type_id,
            ctype_object.schema_id
        )
        result = data_class()

        result.version = ctype_object.version
        for field_name, field_type in data_class.schema.items():
            setattr(
                result, field_name, field_type.to_python(
                    getattr(ctype_object.object_fields, field_name),
                    client, *args, **kwargs
                )
            )
        return result

    @classmethod
    def from_python(cls, value: object):

        def find_client():
            """
            A nice hack. Extracts the nearest `Client` instance from the
            call stack.
            """
            from pyignite import Client

            frame = None
            try:
                for rec in inspect.stack()[2:]:
                    frame = rec[0]
                    code = frame.f_code
                    for varname in code.co_varnames:
                        suspect = frame.f_locals[varname]
                        if isinstance(suspect, Client):
                            return suspect
            finally:
                del frame

        compact_footer = True  # this is actually used
        client = find_client()
        if client:
            # if no client can be found, the class of the `value` is discarded
            # and the new dataclass is automatically registered later on
            client.register_binary_type(value.__class__)
            compact_footer = client.compact_footer
        else:
            raise Warning(
                'Can not register binary type {}'.format(value.type_name)
            )

        # prepare header
        header_class = cls.build_header()
        header = header_class()
        header.type_code = int.from_bytes(
            cls.type_code,
            byteorder=PROTOCOL_BYTE_ORDER
        )

        header.flags = cls.USER_TYPE | cls.HAS_SCHEMA
        if compact_footer:
            header.flags |= cls.COMPACT_FOOTER
        header.version = value.version
        header.type_id = value.type_id
        header.schema_id = value.schema_id

        # create fields and calculate offsets
        field_buffer = b''
        offsets = [ctypes.sizeof(header_class)]
        schema_items = list(value.schema.items())
        for field_name, field_type in schema_items:
            partial_buffer = field_type.from_python(
                getattr(
                    value, field_name, getattr(field_type, 'default', None)
                )
            )
            offsets.append(max(offsets) + len(partial_buffer))
            field_buffer += partial_buffer

        offsets = offsets[:-1]

        # create footer
        if max(offsets, default=0) < 255:
            header.flags |= cls.OFFSET_ONE_BYTE
        elif max(offsets) < 65535:
            header.flags |= cls.OFFSET_TWO_BYTES
        schema_class = cls.schema_type(header.flags) * len(offsets)
        schema = schema_class()
        if compact_footer:
            for i, offset in enumerate(offsets):
                schema[i] = offset
        else:
            for i, offset in enumerate(offsets):
                schema[i].field_id = entity_id(schema_items[i][0])
                schema[i].offset = offset
        # calculate size and hash code
        header.schema_offset = ctypes.sizeof(header_class) + len(field_buffer)
        header.length = header.schema_offset + ctypes.sizeof(schema_class)
        header.hash_code = hashcode(field_buffer + bytes(schema))

        return bytes(header) + field_buffer + bytes(schema)
