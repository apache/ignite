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

from decimal import Decimal

from pyignite.api import (
    hashcode, sql_fields, cache_create, scan, get_binary_type,
)
from pyignite.datatypes.complex import BinaryObject
from pyignite.connection import BufferedConnection


page_size = 100

scheme_name = 'PUBLIC'
scheme_hash_code = hashcode('PUBLIC')

table_sql_name = 'AllDataType'
table_cache_name = 'SQL_{}_{}'.format(
    scheme_name,
    table_sql_name.upper(),
)
table_hash_code = hashcode(table_cache_name)

create_query = '''
CREATE TABLE {} (
  test_pk INTEGER(11) PRIMARY KEY,
  test_bool BOOLEAN DEFAULT TRUE,
  test_int INTEGER(11),
  test_decimal DECIMAL(11, 5),
  test_str VARCHAR(24) DEFAULT '' NOT NULL,
)
'''.format(table_sql_name)

insert_query = '''
INSERT INTO {} (
  test_pk, test_decimal, test_int, test_str
) VALUES (?, ?, ?, ?)'''.format(table_sql_name)

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
    insert_data = [
        [1, Decimal('2.4'), 42, 'asdf'],
        [2, Decimal('2.5'), 43, 'zxcvb'],
        [3, Decimal('2.6'), 44, 'qwerty'],
    ]

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

    # cleanup
    result = sql_fields(conn, scheme_hash_code, drop_query, page_size)
    assert result.status == 0, result.message
