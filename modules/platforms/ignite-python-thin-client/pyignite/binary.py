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

"""
:class:`~pyignite.binary.GenericObjectMeta` is a metaclass used to create
classes, which objects serve as a native Python values for Ignite Complex
object data type. You can use this metaclass with your existing classes
to save and restore their selected attributes and properties to/from
Ignite caches. It is also used internally by `pyignite` to create simple
data classes “on the fly” when retrieving arbitrary Complex objects.

You can get the examples of using Complex objects in the
:ref:`complex_object_usage` section of `pyignite` documentation.
"""

from collections import OrderedDict
import ctypes
from io import SEEK_CUR
from typing import Any

import attr

from .constants import PROTOCOL_BYTE_ORDER
from .datatypes import (
    Null, ByteObject, ShortObject, IntObject, LongObject, FloatObject, DoubleObject, CharObject, BoolObject, UUIDObject,
    DateObject, TimestampObject, TimeObject, EnumObject, BinaryEnumObject, ByteArrayObject, ShortArrayObject,
    IntArrayObject, LongArrayObject, FloatArrayObject, DoubleArrayObject, CharArrayObject, BoolArrayObject,
    UUIDArrayObject, DateArrayObject, TimestampArrayObject, TimeArrayObject, EnumArrayObject, String, StringArrayObject,
    DecimalObject, DecimalArrayObject, ObjectArrayObject, CollectionObject, MapObject, BinaryObject, WrappedDataObject
)
from .datatypes.base import IgniteDataTypeProps
from .exceptions import ParseError
from .utils import entity_id, schema_id


ALLOWED_FIELD_TYPES = [
    Null, ByteObject, ShortObject, IntObject, LongObject, FloatObject,
    DoubleObject, CharObject, BoolObject, UUIDObject, DateObject,
    TimestampObject, TimeObject, EnumObject, BinaryEnumObject,
    ByteArrayObject, ShortArrayObject, IntArrayObject, LongArrayObject,
    FloatArrayObject, DoubleArrayObject, CharArrayObject, BoolArrayObject,
    UUIDArrayObject, DateArrayObject, TimestampArrayObject,
    TimeArrayObject, EnumArrayObject, String, StringArrayObject,
    DecimalObject, DecimalArrayObject, ObjectArrayObject, CollectionObject,
    MapObject, BinaryObject, WrappedDataObject,
]


class GenericObjectProps(IgniteDataTypeProps):
    """
    This class is mixed both to metaclass and to resulting class to make class
    properties universally available. You should not subclass it directly.
    """
    @property
    def schema(self) -> OrderedDict:
        """ Binary object schema. """
        return self._schema.copy()

    @property
    def schema_id(self) -> int:
        """ Binary object schema ID. """
        return schema_id(self._schema)

    def __new__(cls, *args, **kwargs) -> Any:
        # allow all items in Binary Object schema to be populated as optional
        # arguments to `__init__()` with sensible defaults.
        if not attr.has(cls):
            attributes = {
                k: attr.ib(type=getattr(v, 'pythonic', type(None)), default=getattr(v, 'default', None))
                for k, v in cls.schema.items()
            }

            attributes.update({'version': attr.ib(type=int, default=1)})
            cls = attr.s(cls, these=attributes)
        # skip parameters
        return super().__new__(cls)


class GenericObjectPropsMeta(type, GenericObjectProps):
    pass


class GenericObjectMeta(GenericObjectPropsMeta):
    """
    Complex (or Binary) Object metaclass. It is aimed to help user create
    classes, which objects could serve as a pythonic representation of the
    :class:`~pyignite.datatypes.complex.BinaryObject` Ignite data type.
    """
    _schema = None
    _type_name = None
    version = None

    def __new__(
        mcs: Any, name: str, base_classes: tuple, namespace: dict, **kwargs
    ) -> Any:
        """ Sort out class creation arguments. """

        result = super().__new__(
            mcs, name, (GenericObjectProps, ) + base_classes, namespace
        )

        def _from_python(self, stream, save_to_buf=False):
            """
            Method for building binary representation of the Generic object
            and calculating a hashcode from it.

            :param self: Generic object instance,
            :param stream: BinaryStream
            :param save_to_buf: Optional. If True, save serialized data to buffer.
            """
            initial_pos = stream.tell()
            header, header_class = write_header(self, stream)

            offsets = [ctypes.sizeof(header_class)]
            schema_items = list(self.schema.items())
            for field_name, field_type in schema_items:
                val = getattr(self, field_name, getattr(field_type, 'default', None))
                field_start_pos = stream.tell()
                field_type.from_python(stream, val)
                offsets.append(max(offsets) + stream.tell() - field_start_pos)

            write_footer(self, stream, header, header_class, schema_items, offsets, initial_pos, save_to_buf)

        async def _from_python_async(self, stream, save_to_buf=False):
            """
            Async version of _from_python
            """
            initial_pos = stream.tell()
            header, header_class = write_header(self, stream)

            offsets = [ctypes.sizeof(header_class)]
            schema_items = list(self.schema.items())
            for field_name, field_type in schema_items:
                val = getattr(self, field_name, getattr(field_type, 'default', None))
                field_start_pos = stream.tell()
                await field_type.from_python_async(stream, val)
                offsets.append(max(offsets) + stream.tell() - field_start_pos)

            write_footer(self, stream, header, header_class, schema_items, offsets, initial_pos, save_to_buf)

        def write_header(obj, stream):
            header_class = BinaryObject.get_header_class()
            header = header_class()
            header.type_code = int.from_bytes(
                BinaryObject.type_code,
                byteorder=PROTOCOL_BYTE_ORDER
            )
            header.flags = BinaryObject.USER_TYPE | BinaryObject.HAS_SCHEMA
            if stream.compact_footer:
                header.flags |= BinaryObject.COMPACT_FOOTER
            header.version = obj.version
            header.type_id = obj.type_id
            header.schema_id = obj.schema_id

            stream.seek(ctypes.sizeof(header_class), SEEK_CUR)

            return header, header_class

        def write_footer(obj, stream, header, header_class, schema_items, offsets, initial_pos, save_to_buf):
            offsets = offsets[:-1]
            header_len = ctypes.sizeof(header_class)

            # create footer
            if max(offsets, default=0) < 255:
                header.flags |= BinaryObject.OFFSET_ONE_BYTE
            elif max(offsets) < 65535:
                header.flags |= BinaryObject.OFFSET_TWO_BYTES

            schema_class = BinaryObject.schema_type(header.flags) * len(offsets)
            schema = schema_class()

            if stream.compact_footer:
                for i, offset in enumerate(offsets):
                    schema[i] = offset
            else:
                for i, offset in enumerate(offsets):
                    schema[i].field_id = entity_id(schema_items[i][0])
                    schema[i].offset = offset

            # calculate size and hash code
            fields_data_len = stream.tell() - initial_pos - header_len
            header.schema_offset = fields_data_len + header_len
            header.length = header.schema_offset + ctypes.sizeof(schema_class)
            header.hash_code = stream.hashcode(initial_pos + header_len, fields_data_len)

            stream.seek(initial_pos)
            stream.write(header)
            stream.seek(initial_pos + header.schema_offset)
            stream.write(schema)

            if save_to_buf:
                obj._buffer = stream.slice(initial_pos, stream.tell() - initial_pos)
            obj._hashcode = header.hash_code

        def _setattr(self, attr_name: str, attr_value: Any):
            # reset binary representation, if any field is changed
            if attr_name in self._schema.keys():
                self._buffer = None
                self._hashcode = None

            # `super()` is really need these parameters
            super(result, self).__setattr__(attr_name, attr_value)

        setattr(result, _from_python.__name__, _from_python)
        setattr(result, _from_python_async.__name__, _from_python_async)
        setattr(result, '__setattr__', _setattr)
        setattr(result, '_buffer', None)
        setattr(result, '_hashcode', None)
        return result

    @staticmethod
    def _validate_schema(schema: dict):
        for field_type in schema.values():
            if field_type not in ALLOWED_FIELD_TYPES:
                raise ParseError(
                    'Wrong binary field type: {}'.format(field_type)
                )

    def __init__(
        cls, name: str, base_classes: tuple, namespace: dict,
        type_name: str = None, schema: OrderedDict = None, **kwargs
    ):
        """
        Initializes binary object class.

        :param type_name: (optional) binary object name. Defaults to class
         name,
        :param schema: (optional) a dict of field names: field types,
        :raise: ParseError if one or more binary field types
         did not recognized.
        """
        cls._type_name = type_name or cls.__name__
        cls._type_id = entity_id(cls._type_name)
        schema = schema or OrderedDict()
        cls._validate_schema(schema)
        cls._schema = schema
        super().__init__(name, base_classes, namespace)
