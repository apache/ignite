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

from typing import Union

from pyignite.connection import Connection, AioConnection
from pyignite.constants import PROTOCOL_BYTE_ORDER
from pyignite.datatypes.binary import enum_struct, schema_struct, binary_fields_struct
from pyignite.datatypes import String, Int, Bool
from pyignite.queries import Query, query_perform
from pyignite.queries.op_codes import OP_GET_BINARY_TYPE, OP_PUT_BINARY_TYPE
from pyignite.utils import entity_id, schema_id
from .result import APIResult
from ..queries.response import BinaryTypeResponse


def get_binary_type(conn: 'Connection', binary_type: Union[str, int]) -> APIResult:
    """
    Gets the binary type information by type ID.

    :param conn: connection to Ignite server,
    :param binary_type: binary type name or ID,
    :return: API result data object.
    """
    return __get_binary_type(conn, binary_type)


async def get_binary_type_async(conn: 'AioConnection', binary_type: Union[str, int]) -> APIResult:
    """
    Async version of get_binary_type.
    """
    return await __get_binary_type(conn, binary_type)


def __get_binary_type(conn, binary_type):
    query_struct = Query(
        OP_GET_BINARY_TYPE,
        [
            ('type_id', Int),
        ],
        response_type=BinaryTypeResponse
    )

    return query_perform(query_struct, conn, query_params={
        'type_id': entity_id(binary_type),
    })


def put_binary_type(connection: 'Connection', type_name: str, affinity_key_field: str = None,
                    is_enum=False, schema: dict = None) -> APIResult:
    """
    Registers binary type information in cluster.

    :param connection: connection to Ignite server,
    :param type_name: name of the data type being registered,
    :param affinity_key_field: (optional) name of the affinity key field,
    :param is_enum: (optional) register enum if True, binary object otherwise.
     Defaults to False,
    :param schema: (optional) when register enum, pass a dict of enumerated
     parameter names as keys and an integers as values. When register binary
     type, pass a dict of field names: field types. Binary type with no fields
     is OK,
    :return: API result data object.
    """
    return __put_binary_type(connection, type_name, affinity_key_field, is_enum, schema)


async def put_binary_type_async(connection: 'AioConnection', type_name: str, affinity_key_field: str = None,
                                is_enum=False, schema: dict = None, query_id=None) -> APIResult:
    """
    Async version of put_binary_type.
    """
    return await __put_binary_type(connection, type_name, affinity_key_field, is_enum, schema)


def __post_process_put_binary(type_id):
    def internal(result):
        if result.status == 0:
            result.value = {
                'type_id': type_id,
                'schema_id': schema_id,
            }
        return result
    return internal


def __put_binary_type(connection, type_name, affinity_key_field, is_enum, schema):
    # prepare data
    if schema is None:
        schema = {}
    type_id = entity_id(type_name)
    data = {
        'type_name': type_name,
        'type_id': type_id,
        'affinity_key_field': affinity_key_field,
        'binary_fields': [],
        'is_enum': is_enum,
        'schema': [],
    }
    s_id = None
    if is_enum:
        data['enums'] = []
        for literal, ordinal in schema.items():
            data['enums'].append({
                'literal': literal,
                'type_id': ordinal,
            })
    else:
        # assemble schema and calculate schema ID in one go
        s_id = schema_id(schema)
        for field_name, data_type in schema.items():
            # TODO: check for allowed data types
            field_id = entity_id(field_name)
            data['binary_fields'].append({
                'field_name': field_name,
                'type_id': int.from_bytes(
                    data_type.type_code,
                    byteorder=PROTOCOL_BYTE_ORDER
                ),
                'field_id': field_id,
            })

    data['schema'].append({
        'schema_id': s_id,
        'schema_fields': [
            {'schema_field_id': entity_id(x)} for x in schema
        ],
    })

    # do query
    if is_enum:
        query_struct = Query(
            OP_PUT_BINARY_TYPE,
            [
                ('type_id', Int),
                ('type_name', String),
                ('affinity_key_field', String),
                ('binary_fields', binary_fields_struct),
                ('is_enum', Bool),
                ('enums', enum_struct),
                ('schema', schema_struct),
            ]
        )
    else:
        query_struct = Query(
            OP_PUT_BINARY_TYPE,
            [
                ('type_id', Int),
                ('type_name', String),
                ('affinity_key_field', String),
                ('binary_fields', binary_fields_struct),
                ('is_enum', Bool),
                ('schema', schema_struct),
            ]
        )
    return query_perform(query_struct, connection, query_params=data,
                         post_process_fun=__post_process_put_binary(type_id))
