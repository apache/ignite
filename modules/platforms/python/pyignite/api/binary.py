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

from pyignite.connection import Connection
from pyignite.datatypes.cache_config import Struct, StructArray
from pyignite.datatypes.primitive import Int, Bool
from pyignite.datatypes.standard import String
from pyignite.queries import Query, Response
from pyignite.queries.op_codes import *
from .result import APIResult


def get_binary_type(conn: Connection, type_id: int, query_id=None):
    """
    Gets the binary type information by type ID.

    :param conn: connection to Ignite server,
    :param type_id: Java-style hash code of the type name,
    :return: API result data object.
    """

    class GetBinaryTypeQuery(Query):
        op_code = OP_GET_BINARY_TYPE

    query_struct = GetBinaryTypeQuery([
        ('type_id', Int),
    ], query_id=query_id)

    _, send_buffer = query_struct.from_python({
        'type_id': type_id,
    })
    conn.send(send_buffer)

    response_head_struct = Response([
        ('type_exists', Bool),
    ])
    response_head_type, recv_buffer = response_head_struct.parse(conn)
    response_head = response_head_type.from_buffer_copy(recv_buffer)
    response_parts = []
    if response_head.type_exists:
        body_struct = Struct([
            ('type_id', Int),
            ('type_name', String),
            ('affinity_key_field', String),
            ('binary_fields', StructArray([
                ('field_name', String),
                ('type_id', Int),
                ('field_id', Int),
            ])),
            ('is_enum', Bool),
        ])
        resp_body_type, resp_body_buffer = body_struct.parse(conn)
        response_parts.append(('body', resp_body_type))
        resp_body = resp_body_type.from_buffer_copy(resp_body_buffer)
        recv_buffer += resp_body_buffer
        if resp_body.is_enum:
            enum_struct = StructArray([
                ('literal', String),
                ('type_id', Int),
            ])
            resp_enum, resp_enum_buffer = enum_struct.parse(conn)
            response_parts.append(('enums', resp_enum))
            recv_buffer += resp_enum_buffer
        schema_struct = StructArray([
            ('schema_id', Int),
            ('schema_fields', StructArray([
                ('schema_field_id', Int),
            ])),
        ])
        resp_schema_type, resp_schema_buffer = schema_struct.parse(conn)
        response_parts.append(('schema', resp_schema_type))
        recv_buffer += resp_schema_buffer

    response_class = type(
        'GetBinaryTypeResponse',
        (response_head_type,),
        {
            '_pack_': 1,
            '_fields_': response_parts,
        }
    )
    response = response_class.from_buffer_copy(recv_buffer)
    result = APIResult(response)
    if result.status != 0:
        return result
    result.value = {
        'type_exists': response.type_exists
    }
    if hasattr(response, 'body'):
        result.value.update(body_struct.to_python(response.body))
    if hasattr(response, 'enums'):
        result.value['enums'] = enum_struct.to_python(response.enums)
    if hasattr(response, 'schema'):
        result.value['schema'] = schema_struct.to_python(response.schema)
    return result
