#
# Copyright 2019 GridGain Systems, Inc. and Contributors.
#
# Licensed under the GridGain Community Edition License (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""
:class:`~pygridgain.binary.GenericObjectMeta` is a metaclass used to create
classes, which objects serve as a native Python values for GridGain Complex
object data type. You can use this metaclass with your existing classes
to save and restore their selected attributes and properties to/from
GridGain caches. It is also used internally by `pygridgain` to create simple
data classes “on the fly” when retrieving arbitrary Complex objects.

You can get the examples of using Complex objects in the
:ref:`complex_object_usage` section of `pygridgain` documentation.
"""

from collections import OrderedDict
import ctypes
from typing import Any

import attr

from pygridgain.constants import *
from .datatypes import *
from .datatypes.base import GridGainDataTypeProps
from .exceptions import ParseError
from .utils import entity_id, hashcode, schema_id


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


class GenericObjectProps(GridGainDataTypeProps):
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
        attributes = {
            k: attr.ib(
                type=getattr(v, 'pythonic', type(None)),
                default=getattr(v, 'default', None),
            ) for k, v in cls.schema.items()
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
    :class:`~pygridgain.datatypes.complex.BinaryObject` GridGain data type.
    """
    _schema = None
    _type_name = None
    version = None

    def __new__(
        mcs: Any, name: str, base_classes: tuple, namespace: dict, **kwargs
    ) -> Any:
        """ Sort out class creation arguments. """

        result = super().__new__(
            mcs, name, (GenericObjectProps, )+base_classes, namespace
        )

        def _build(self, client: 'Client' = None) -> int:
            """
            Method for building binary representation of the Generic object
            and calculating a hashcode from it.

            :param self: Generic object instance,
            :param client: (optional) connection to GridGain cluster,
            """
            if client is None:
                compact_footer = True
            else:
                compact_footer = client.compact_footer

            # prepare header
            header_class = BinaryObject.build_header()
            header = header_class()
            header.type_code = int.from_bytes(
                BinaryObject.type_code,
                byteorder=PROTOCOL_BYTE_ORDER
            )
            header.flags = BinaryObject.USER_TYPE | BinaryObject.HAS_SCHEMA
            if compact_footer:
                header.flags |= BinaryObject.COMPACT_FOOTER
            header.version = self.version
            header.type_id = self.type_id
            header.schema_id = self.schema_id

            # create fields and calculate offsets
            offsets = [ctypes.sizeof(header_class)]
            field_buffer = bytearray()
            schema_items = list(self.schema.items())
            for field_name, field_type in schema_items:
                partial_buffer = field_type.from_python(
                    getattr(
                        self, field_name, getattr(field_type, 'default', None)
                    )
                )
                offsets.append(max(offsets) + len(partial_buffer))
                field_buffer += partial_buffer

            offsets = offsets[:-1]

            # create footer
            if max(offsets, default=0) < 255:
                header.flags |= BinaryObject.OFFSET_ONE_BYTE
            elif max(offsets) < 65535:
                header.flags |= BinaryObject.OFFSET_TWO_BYTES
            schema_class = (
                BinaryObject.schema_type(header.flags)
                * len(offsets)
            )
            schema = schema_class()
            if compact_footer:
                for i, offset in enumerate(offsets):
                    schema[i] = offset
            else:
                for i, offset in enumerate(offsets):
                    schema[i].field_id = entity_id(schema_items[i][0])
                    schema[i].offset = offset

            # calculate size and hash code
            header.schema_offset = (
                ctypes.sizeof(header_class)
                + len(field_buffer)
            )
            header.length = header.schema_offset + ctypes.sizeof(schema_class)
            header.hash_code = hashcode(field_buffer + bytes(schema))

            # reuse the results
            self._buffer = bytes(header) + field_buffer + bytes(schema)
            self._hashcode = header.hash_code

        def _setattr(self, attr_name: str, attr_value: Any):
            # reset binary representation, if any field is changed
            if attr_name in self._schema.keys():
                self._buffer = None
                self._hashcode = None

            # `super()` is really need these parameters
            super(result, self).__setattr__(attr_name, attr_value)

        setattr(result, _build.__name__, _build)
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
