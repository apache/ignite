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
from decimal import Decimal

from pyignite.api import (
    hashcode, sql_fields, cache_create, scan, cache_create_with_config,
    cache_get_configuration, put_binary_type, get_binary_type,
    cache_put, cache_destroy,
)
from pyignite.datatypes.complex import BinaryObject
from pyignite.datatypes.primitive_objects import BoolObject, IntObject
from pyignite.datatypes.standard import DecimalObject, String
from pyignite.datatypes.prop_codes import *


insert_data = [
    [1, True, (42, IntObject), Decimal('2.4'), 'asdf'],
    [2, False, (43, IntObject), Decimal('2.5'), 'zxcvb'],
    [3, True, (44, IntObject), Decimal('2.6'), 'qwerty'],
]

page_size = 100

scheme_name = 'PUBLIC'
scheme_hash_code = hashcode(scheme_name)

table_sql_name = 'AllDataType'
table_cache_name = 'SQL_{}_{}'.format(
    scheme_name,
    table_sql_name.upper(),
)
table_hash_code = hashcode(table_cache_name)

create_query = '''
CREATE TABLE {} (
  test_pk INTEGER(11) PRIMARY KEY,
  test_bool BOOLEAN,
  test_int INTEGER(11),
  test_decimal DECIMAL(11, 5),
  test_str VARCHAR(24),
)
'''.format(table_sql_name)

insert_query = '''
INSERT INTO {} (
  test_pk, test_bool, test_int, test_decimal, test_str
) VALUES (?, ?, ?, ?, ?)'''.format(table_sql_name)

select_query = '''
SELECT (test_pk, test_bool, test_int, test_decimal, test_str) FROM {}
'''.format(table_sql_name)

drop_query = 'DROP TABLE {}'.format(table_sql_name)


def test_sql_read_as_binary(conn):

    cache_create(conn, scheme_name)

    # create table
    result = sql_fields(
        conn,
        scheme_hash_code,
        create_query,
        page_size
    )
    assert result.status == 0, result.message

    # insert some rows
    for line in insert_data:
        result = sql_fields(
            conn,
            scheme_hash_code,
            insert_query,
            page_size,
            query_args=line
        )
        assert result.status == 0, result.message

    result = scan(conn, table_hash_code, 100)
    assert result.status == 0, result.message

    # now `data` is a dict of table rows with primary index column as a key
    # and other columns as a value, represented as a BinaryObject
    # wrapped in WrappedDataObject
    for key, value in result.value['data'].items():
        # we can't automagically unwind the contents of the WrappedDataObject
        # the same way we do for Map or Collection, we got bytes instead
        buffer, offset = value
        # offset is 0 in this example, but that's not granted
        mock_conn = conn.make_buffered(buffer)
        mock_conn.pos = offset

        data_class, data_bytes = BinaryObject.parse(mock_conn)
        value = BinaryObject.to_python(data_class.from_buffer_copy(data_bytes))
        assert buffer == data_bytes
        assert len(value['fields']) == 4

    result = cache_get_configuration(conn, table_hash_code)
    assert result.status == 0, result.message

    # cleanup
    result = sql_fields(conn, scheme_hash_code, drop_query, page_size)
    assert result.status == 0, result.message

    result = cache_destroy(conn, scheme_hash_code)
    assert result.status == 0, result.message


def test_sql_write_as_binary(conn):

    cache_create(conn, scheme_name)

    # configure cache as an SQL table
    type_name = table_cache_name

    result = cache_create_with_config(conn, {
        PROP_NAME: table_cache_name,
        PROP_SQL_SCHEMA: scheme_name,
        PROP_QUERY_ENTITIES: [
            {
                'table_name': table_sql_name.upper(),
                'key_field_name': 'TEST_PK',
                'key_type_name': 'java.lang.Integer',
                'field_name_aliases': [],
                'query_fields': [
                    {
                        'name': 'TEST_PK',
                        'type_name': 'java.lang.Integer',
                        'is_key_field': True,
                        'is_notnull_constraint_field': True,
                        'default_value': None,
                    },
                    {
                        'name': 'TEST_BOOL',
                        'type_name': 'java.lang.Boolean',
                        'is_key_field': False,
                        'is_notnull_constraint_field': False,
                        'default_value': None,
                    },
                    {
                        'name': 'TEST_STR',
                        'type_name': 'java.lang.String',
                        'is_key_field': False,
                        'is_notnull_constraint_field': False,
                        'default_value': None,
                    },
                    {
                        'name': 'TEST_INT',
                        'type_name': 'java.lang.Integer',
                        'is_key_field': False,
                        'is_notnull_constraint_field': False,
                        'default_value': None,
                    },
                    {
                        'name': 'TEST_DECIMAL',
                        'type_name': 'java.math.BigDecimal',
                        'is_key_field': False,
                        'is_notnull_constraint_field': False,
                        'default_value': None,
                    },
                ],
                'query_indexes': [],
                'value_type_name': type_name,
                'value_field_name': None,
            },
        ],
    })
    assert result.status == 0, result.message

    result = cache_get_configuration(conn, table_hash_code)
    assert result.status == 0, result.message

    # register binary type
    result = put_binary_type(
        conn,
        type_name,
        schema=OrderedDict([
            ('TEST_BOOL', BoolObject),
            ('TEST_STR', String),
            ('TEST_INT', IntObject),
            ('TEST_DECIMAL', DecimalObject),
        ])
    )
    assert result.status == 0, result.message

    sql_type_id = result.value['type_id']
    schema_id = result.value['schema_id']

    # recheck
    result = get_binary_type(conn, sql_type_id)
    assert result.status == 0, result.message
    assert schema_id == result.value['schema'][0]['schema_id'], (
        'Client-side schema ID calculation is incorrect'
    )

    # insert rows as k-v
    for row in insert_data:
        result = cache_put(
            conn,
            table_hash_code,
            key=row[0],
            key_hint=IntObject,
            value={
                'version': 1,
                'type_id': sql_type_id,
                'schema_id': schema_id,
                'fields': OrderedDict([
                    ('TEST_BOOL', row[1]),
                    ('TEST_STR', row[4]),
                    ('TEST_INT', row[2]),
                    ('TEST_DECIMAL', row[3]),
                ]),
            },
            value_hint=BinaryObject,
        )
        assert result.status == 0, result.message

    result = scan(conn, table_hash_code, 100)
    assert result.status == 0, result.message

    # read rows as SQL
    result = sql_fields(
        conn,
        scheme_hash_code,
        select_query,
        100,
        include_field_names=True,
    )
    assert result.status == 0, result.message
    assert len(result.value['data']) == len(insert_data)

    # cleanup
    result = cache_destroy(conn, table_hash_code)
    assert result.status == 0, result.message

    result = cache_destroy(conn, scheme_hash_code)
    assert result.status == 0, result.message
