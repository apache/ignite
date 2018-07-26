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

from pyignite.datatypes import BinaryObject, DoubleObject, IntObject, String

from pyignite.api import (
    cache_get_or_create, cache_create_with_config,
    cache_put, put_binary_type, sql_fields,
)
from pyignite.connection import Connection
from pyignite.datatypes.prop_codes import *
from pyignite.utils import hashcode

conn = Connection()
conn.connect('127.0.0.1', 10800)

cache_get_or_create(conn, 'PUBLIC')

cache_create_with_config(conn, {
        PROP_NAME: 'SQL_PUBLIC_STUDENT',
        PROP_SQL_SCHEMA: 'PUBLIC',
        PROP_QUERY_ENTITIES: [
            {
                'table_name': 'Student'.upper(),
                'key_field_name': 'SID',
                'key_type_name': 'java.lang.Integer',
                'field_name_aliases': [],
                'query_fields': [
                    {
                        'name': 'SID',
                        'type_name': 'java.lang.Integer',
                        'is_key_field': True,
                        'is_notnull_constraint_field': True,
                        'default_value': None,
                    },
                    {
                        'name': 'NAME',
                        'type_name': 'java.lang.String',
                        'is_key_field': False,
                        'is_notnull_constraint_field': False,
                        'default_value': None,
                    },
                    {
                        'name': 'LOGIN',
                        'type_name': 'java.lang.String',
                        'is_key_field': False,
                        'is_notnull_constraint_field': False,
                        'default_value': None,
                    },
                    {
                        'name': 'AGE',
                        'type_name': 'java.lang.Integer',
                        'is_key_field': False,
                        'is_notnull_constraint_field': False,
                        'default_value': None,
                    },
                    {
                        'name': 'GPA',
                        'type_name': 'java.math.Double',
                        'is_key_field': False,
                        'is_notnull_constraint_field': False,
                        'default_value': None,
                    },
                ],
                'query_indexes': [],
                'value_type_name': 'SQL_PUBLIC_STUDENT_TYPE',
                'value_field_name': None,
            },
        ],
    })

result = put_binary_type(
    conn,
    'SQL_PUBLIC_STUDENT_TYPE',
    schema={
        'NAME': String,
        'LOGIN': String,
        'AGE': IntObject,
        'GPA': DoubleObject,
    }
)

type_id = result.value['type_id']
schema_id = result.value['schema_id']

cache_put(
    conn,
    hashcode('SQL_PUBLIC_STUDENT'),
    key=1,
    key_hint=IntObject,
    value={
        'version': 1,
        'type_id': type_id,
        'schema_id': schema_id,
        'fields': {
            'LOGIN': 'jdoe',
            'NAME': 'John Doe',
            'AGE': (17, IntObject),
            'GPA': 4.25,
        },
    },
    value_hint=BinaryObject,
)

result = sql_fields(
    conn,
    hashcode('PUBLIC'),
    'SELECT * FROM Student',
    1,
    include_field_names=True,
)
print(result.value)

# {
#     'more': False,
#     'data': [
#         [1, 'John Doe', 'jdoe', 17, 4.25]
#     ],
#     'fields': ['SID', 'NAME', 'LOGIN', 'AGE', 'GPA'],
#     'cursor': 1
# }
