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
import asyncio
from io import SEEK_CUR

import attr
from collections import OrderedDict
import ctypes

from pyignite.connection.protocol_context import ProtocolContext
from pyignite.constants import RHF_TOPOLOGY_CHANGED, RHF_ERROR
from pyignite.datatypes import AnyDataObject, Bool, Int, Long, String, StringArray, Struct
from pyignite.datatypes.binary import body_struct, enum_struct, schema_struct
from pyignite.queries.op_codes import OP_SUCCESS
from pyignite.stream import READ_BACKWARD


class StatusFlagResponseHeader(ctypes.LittleEndianStructure):
    _pack_ = 1
    _fields_ = [
        ('length', ctypes.c_int),
        ('query_id', ctypes.c_longlong),
        ('flags', ctypes.c_short)
    ]


class ResponseHeader(ctypes.LittleEndianStructure):
    _pack_ = 1
    _fields_ = [
        ('length', ctypes.c_int),
        ('query_id', ctypes.c_longlong),
        ('status_code', ctypes.c_int)
    ]


@attr.s
class Response:
    following = attr.ib(type=list, factory=list)
    protocol_context = attr.ib(type=type(ProtocolContext), default=None)
    _response_class_name = 'Response'

    def __attrs_post_init__(self):
        # replace None with empty list
        self.following = self.following or []

    def __parse_header(self, stream):
        init_pos = stream.tell()

        if self.protocol_context.is_status_flags_supported():
            header_class = StatusFlagResponseHeader
        else:
            header_class = ResponseHeader

        header_len = ctypes.sizeof(header_class)
        header = stream.read_ctype(header_class)
        stream.seek(header_len, SEEK_CUR)

        fields = []
        has_error = False
        if self.protocol_context.is_status_flags_supported():
            if header.flags & RHF_TOPOLOGY_CHANGED:
                fields = [
                    ('affinity_version', ctypes.c_longlong),
                    ('affinity_minor', ctypes.c_int),
                ]

            if header.flags & RHF_ERROR:
                fields.append(('status_code', ctypes.c_int))
                has_error = True
        else:
            has_error = header.status_code != OP_SUCCESS

        if fields:
            stream.seek(sum(ctypes.sizeof(c_type) for _, c_type in fields), SEEK_CUR)

        if has_error:
            msg_type = String.parse(stream)
            fields.append(('error_message', msg_type))

        return not has_error, init_pos, header_class, fields

    def __build_response_class(self, stream, init_pos, header_class, fields):
        response_class = type(
            self._response_class_name,
            (header_class,),
            {
                '_pack_': 1,
                '_fields_': fields,
            }
        )

        stream.seek(init_pos + ctypes.sizeof(response_class))
        return response_class

    def parse(self, stream):
        success, init_pos, header_class, fields = self.__parse_header(stream)
        if success:
            self._parse_success(stream, fields)

        return self.__build_response_class(stream, init_pos, header_class, fields)

    async def parse_async(self, stream):
        success, init_pos, header_class, fields = self.__parse_header(stream)
        if success:
            await self._parse_success_async(stream, fields)

        return self.__build_response_class(stream, init_pos, header_class, fields)

    def _parse_success(self, stream, fields: list):
        for name, ignite_type in self.following:
            c_type = ignite_type.parse(stream)
            fields.append((name, c_type))

    async def _parse_success_async(self, stream, fields: list):
        for name, ignite_type in self.following:
            c_type = await ignite_type.parse_async(stream)
            fields.append((name, c_type))

    def to_python(self, ctypes_object, **kwargs):
        if not self.following:
            return None

        result = OrderedDict()
        for name, c_type in self.following:
            result[name] = c_type.to_python(getattr(ctypes_object, name), **kwargs)

        return result

    async def to_python_async(self, ctypes_object, **kwargs):
        if not self.following:
            return None

        values = await asyncio.gather(
            *[c_type.to_python_async(getattr(ctypes_object, name), **kwargs) for name, c_type in self.following]
        )

        return OrderedDict([(name, values[i]) for i, (name, _) in enumerate(self.following)])


@attr.s
class SQLResponse(Response):
    """
    The response class of SQL functions is special in the way the row-column
    data is counted in it. Basically, Ignite thin client API is following a
    “counter right before the counted objects” rule in most of its parts.
    SQL ops are breaking this rule.
    """
    include_field_names = attr.ib(type=bool, default=False)
    has_cursor = attr.ib(type=bool, default=False)
    _response_class_name = 'SQLResponse'

    def fields_or_field_count(self):
        if self.include_field_names:
            return 'fields', StringArray
        return 'field_count', Int

    def _parse_success(self, stream, fields: list):
        body_struct = self.__create_body_struct()
        body_class = body_struct.parse(stream)
        body = stream.read_ctype(body_class, direction=READ_BACKWARD)

        data_fields, field_count = [], self.__get_fields_count(body)
        for i in range(body.row_count):
            row_fields = []
            for j in range(field_count):
                field_class = AnyDataObject.parse(stream)
                row_fields.append(('column_{}'.format(j), field_class))

            self.__row_post_process(i, row_fields, data_fields)

        self.__body_class_post_process(body_class, fields, data_fields)

    async def _parse_success_async(self, stream, fields: list):
        body_struct = self.__create_body_struct()
        body_class = await body_struct.parse_async(stream)
        body = stream.read_ctype(body_class, direction=READ_BACKWARD)

        data_fields, field_count = [], self.__get_fields_count(body)
        for i in range(body.row_count):
            row_fields = []
            for j in range(field_count):
                field_class = await AnyDataObject.parse_async(stream)
                row_fields.append(('column_{}'.format(j), field_class))

            self.__row_post_process(i, row_fields, data_fields)

        self.__body_class_post_process(body_class, fields, data_fields)

    def __create_body_struct(self):
        following = [self.fields_or_field_count(), ('row_count', Int)]
        if self.has_cursor:
            following.insert(0, ('cursor', Long))
        return Struct(following)

    def __get_fields_count(self, body):
        if self.include_field_names:
            return body.fields.length
        return body.field_count

    @staticmethod
    def __row_post_process(idx, row_fields, data_fields):
        row_class = type(
            'SQLResponseRow',
            (ctypes.LittleEndianStructure,),
            {
                '_pack_': 1,
                '_fields_': row_fields,
            }
        )
        data_fields.append((f'row_{idx}', row_class))

    @staticmethod
    def __body_class_post_process(body_class, fields, data_fields):
        data_class = type(
            'SQLResponseData',
            (ctypes.LittleEndianStructure,),
            {
                '_pack_': 1,
                '_fields_': data_fields,
            }
        )
        fields += body_class._fields_ + [
            ('data', data_class),
            ('more', ctypes.c_byte),
        ]

    def to_python(self, ctypes_object, **kwargs):
        if getattr(ctypes_object, 'status_code', 0) == 0:
            result = self.__to_python_result_header(ctypes_object, **kwargs)

            for row_item in ctypes_object.data._fields_:
                row_name = row_item[0]
                row_object = getattr(ctypes_object.data, row_name)
                row = []
                for col_item in row_object._fields_:
                    col_name = col_item[0]
                    col_object = getattr(row_object, col_name)
                    row.append(AnyDataObject.to_python(col_object, **kwargs))
                result['data'].append(row)
            return result

    async def to_python_async(self, ctypes_object, **kwargs):
        if getattr(ctypes_object, 'status_code', 0) == 0:
            result = self.__to_python_result_header(ctypes_object, **kwargs)

            data_coro = []
            for row_item in ctypes_object.data._fields_:
                row_name = row_item[0]
                row_object = getattr(ctypes_object.data, row_name)
                row_coro = []
                for col_item in row_object._fields_:
                    col_name = col_item[0]
                    col_object = getattr(row_object, col_name)
                    row_coro.append(AnyDataObject.to_python_async(col_object, **kwargs))

                data_coro.append(asyncio.gather(*row_coro))

            result['data'] = await asyncio.gather(*data_coro)
            return result

    @staticmethod
    def __to_python_result_header(ctypes_object, *args, **kwargs):
        result = {
            'more': Bool.to_python(ctypes_object.more, *args, **kwargs),
            'data': [],
        }
        if hasattr(ctypes_object, 'fields'):
            result['fields'] = StringArray.to_python(ctypes_object.fields, *args, **kwargs)
        else:
            result['field_count'] = Int.to_python(ctypes_object.field_count, *args, **kwargs)

        if hasattr(ctypes_object, 'cursor'):
            result['cursor'] = Long.to_python(ctypes_object.cursor, *args, **kwargs)
        return result


class BinaryTypeResponse(Response):
    _response_class_name = 'GetBinaryTypeResponse'

    def _parse_success(self, stream, fields: list):
        type_exists = self.__process_type_exists(stream, fields)

        if type_exists.value:
            resp_body_type = body_struct.parse(stream)
            fields.append(('body', resp_body_type))
            resp_body = stream.read_ctype(resp_body_type, direction=READ_BACKWARD)
            if resp_body.is_enum:
                resp_enum = enum_struct.parse(stream)
                fields.append(('enums', resp_enum))

            resp_schema_type = schema_struct.parse(stream)
            fields.append(('schema', resp_schema_type))

    async def _parse_success_async(self, stream, fields: list):
        type_exists = self.__process_type_exists(stream, fields)

        if type_exists.value:
            resp_body_type = await body_struct.parse_async(stream)
            fields.append(('body', resp_body_type))
            resp_body = stream.read_ctype(resp_body_type, direction=READ_BACKWARD)
            if resp_body.is_enum:
                resp_enum = await enum_struct.parse_async(stream)
                fields.append(('enums', resp_enum))

            resp_schema_type = await schema_struct.parse_async(stream)
            fields.append(('schema', resp_schema_type))

    @staticmethod
    def __process_type_exists(stream, fields):
        fields.append(('type_exists', ctypes.c_byte))
        type_exists = stream.read_ctype(ctypes.c_byte)
        stream.seek(ctypes.sizeof(ctypes.c_byte), SEEK_CUR)

        return type_exists

    def to_python(self, ctypes_object, **kwargs):
        if getattr(ctypes_object, 'status_code', 0) == 0:
            result = {
                'type_exists': Bool.to_python(ctypes_object.type_exists)
            }

            if hasattr(ctypes_object, 'body'):
                result.update(body_struct.to_python(ctypes_object.body))

            if hasattr(ctypes_object, 'enums'):
                result['enums'] = enum_struct.to_python(ctypes_object.enums)

            if hasattr(ctypes_object, 'schema'):
                result['schema'] = {
                    x['schema_id']: [
                        z['schema_field_id'] for z in x['schema_fields']
                    ]
                    for x in schema_struct.to_python(ctypes_object.schema)
                }
            return result

    async def to_python_async(self, ctypes_object, **kwargs):
        return self.to_python(ctypes_object, **kwargs)
