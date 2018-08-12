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
    sql_fields, cache_create, scan, cache_create_with_config,
    cache_get_configuration, put_binary_type, get_binary_type,
    cache_get, cache_put, cache_destroy,
)
from pyignite.datatypes import (
    BinaryObject, BoolObject, IntObject, DecimalObject, LongObject, String,
)
from pyignite.datatypes.prop_codes import *
from pyignite.utils import unwrap_binary


insert_data = [
    [1, True, (42, IntObject), Decimal('2.4'), 'asdf'],
    [2, False, (43, IntObject), Decimal('2.5'), 'zxcvb'],
    [3, True, (44, IntObject), Decimal('2.6'), 'qwerty'],
]

page_size = 100

scheme_name = 'PUBLIC'

table_sql_name = 'AllDataType'
table_cache_name = 'SQL_{}_{}'.format(
    scheme_name,
    table_sql_name.upper(),
)

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


def test_sql_read_as_binary(client):

    cache_create(client, scheme_name)

    # create table
    result = sql_fields(
        client,
        scheme_name,
        create_query,
        page_size
    )
    assert result.status == 0, result.message

    # insert some rows
    for line in insert_data:
        result = sql_fields(
            client,
            scheme_name,
            insert_query,
            page_size,
            query_args=line
        )
        assert result.status == 0, result.message

    result = scan(client, table_cache_name, 100)
    assert result.status == 0, result.message

    # now `data` is a dict of table rows with primary index column as a key
    # and other columns as a value, represented as a BinaryObject
    # wrapped in WrappedDataObject
    for key, value in result.value['data'].items():
        # we can't automagically unwind the contents of the WrappedDataObject
        # the same way we do for Map or Collection, we got bytes instead
        binary_obj = unwrap_binary(client, value)
        assert len(binary_obj['fields']) == 4

    result = cache_get_configuration(client, table_cache_name)
    assert result.status == 0, result.message

    # cleanup
    result = sql_fields(client, scheme_name, drop_query, page_size)
    assert result.status == 0, result.message

    result = cache_destroy(client, scheme_name)
    assert result.status == 0, result.message


def test_sql_write_as_binary(client):

    cache_create(client, scheme_name)

    # configure cache as an SQL table
    type_name = table_cache_name

    result = cache_create_with_config(client, {
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

    result = cache_get_configuration(client, table_cache_name)
    assert result.status == 0, result.message

    # register binary type
    result = put_binary_type(
        client,
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
    result = get_binary_type(client, sql_type_id)
    assert result.status == 0, result.message
    assert schema_id in result.value['schema'], (
        'Client-side schema ID calculation is incorrect'
    )

    # insert rows as k-v
    for row in insert_data:
        result = cache_put(
            client,
            table_cache_name,
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

    result = scan(client, table_cache_name, 100)
    assert result.status == 0, result.message

    # read rows as SQL
    result = sql_fields(
        client,
        scheme_name,
        select_query,
        100,
        include_field_names=True,
    )
    assert result.status == 0, result.message
    assert len(result.value['data']) == len(insert_data)

    # cleanup
    result = cache_destroy(client, table_cache_name)
    assert result.status == 0, result.message

    result = cache_destroy(client, scheme_name)
    assert result.status == 0, result.message


def test_nested_binary_objects(client):

    cache_create(client, 'nested_binary')

    inner_schema = OrderedDict([
        ('inner_int', LongObject),
        ('inner_str', String),
    ])

    outer_schema = OrderedDict([
        ('outer_int', LongObject),
        ('nested_binary', BinaryObject),
        ('outer_str', String),
    ])

    result = put_binary_type(client, 'InnerType', schema=inner_schema)
    inner_type_id = result.value['type_id']
    inner_schema_id = result.value['schema_id']

    result = put_binary_type(client, 'OuterType', schema=outer_schema)
    outer_type_id = result.value['type_id']
    outer_schema_id = result.value['schema_id']

    result = cache_put(
        client,
        'nested_binary',
        1,
        {
            'version': 1,
            'type_id': outer_type_id,
            'schema_id': outer_schema_id,
            'fields': OrderedDict([
                ('outer_int', 42),
                ('nested_binary', (
                    {
                        'version': 1,
                        'type_id': inner_type_id,
                        'schema_id': inner_schema_id,
                        'fields': OrderedDict([
                            ('inner_int', 24),
                            ('inner_str', 'World'),
                        ]),
                    }, BinaryObject)),
                ('outer_str', 'Hello'),
            ])
        },
        value_hint=BinaryObject,
    )
    assert result.status == 0, result.message

    result = cache_get(client, 'nested_binary', 1)
    assert result.status == 0, result.message

    data = unwrap_binary(client, result.value, recurse=False)['fields']
    assert data['outer_str'] == 'Hello'
    assert data['outer_int'] == 42

    inner_data = data['nested_binary']['fields']
    assert inner_data['inner_str'] == 'World'
    assert inner_data['inner_int'] == 24

    cache_destroy(client, 'nested_binary')


def test_add_schema_to_binary_object(client):

    cache_create(client, 'migrate_binary')

    original_schema = OrderedDict([
        ('test_str', String),
        ('test_int', LongObject),
        ('test_bool', BoolObject),
    ])
    result = put_binary_type(client, 'MyBinaryType', schema=original_schema)
    assert result.status == 0, result.message
    type_id = result.value['type_id']
    original_schema_id = result.value['schema_id']

    result = cache_put(
        client,
        'migrate_binary',
        1,
        {
            'version': 1,
            'type_id': type_id,
            'schema_id': original_schema_id,
            'fields': OrderedDict([
                ('test_str', 'Hello World'),
                ('test_int', 42),
                ('test_bool', True),
            ]),
        },
        value_hint=BinaryObject,
    )
    assert result.status == 0, result.message

    modified_schema = original_schema.copy()
    modified_schema['test_decimal'] = DecimalObject
    del modified_schema['test_bool']

    result = put_binary_type(client, 'MyBinaryType', schema=modified_schema)
    assert result.status == 0, result.message
    modified_schema_id = result.value['schema_id']
    assert result.value['type_id'] == type_id

    assert original_schema_id != modified_schema_id

    result = cache_put(
        client,
        'migrate_binary',
        2,
        {
            'version': 1,
            'type_id': type_id,
            'schema_id': modified_schema_id,
            'fields': OrderedDict([
                ('test_str', 'Hello World'),
                ('test_int', 42),
                ('test_decimal', Decimal('3.45')),
            ]),
        },
        value_hint=BinaryObject,
    )
    assert result.status == 0, result.message

    result = cache_get(client, 'migrate_binary', 2)
    assert result.status == 0, result.message
    data = unwrap_binary(client, result.value)['fields']
    assert len(data) == 3
    assert 'test_str' in data
    assert 'test_decimal' in data
    assert 'test_bool' not in data

    cache_destroy(client, 'migrate_binary')
