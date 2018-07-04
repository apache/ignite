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

from pyignite.api import (
    hashcode, sql_fields, cache_create, cache_get_configuration,
    cache_get_names, cache_get, scan,
    get_binary_type,
)


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
  test_decimal DECIMAL(11, 5),
  test_str VARCHAR(24) DEFAULT '' NOT NULL,
)
'''.format(table_sql_name)

insert_query = '''
INSERT INTO {} (
  test_pk, test_decimal, test_str
) VALUES (2, 2.5, 'qwertyasdf1230-=[]')'''.format(table_sql_name)

drop_query = 'DROP TABLE {}'.format(table_sql_name)


def test_sql_types_creation(conn):

    cache_create(conn, scheme_name)

    result = sql_fields(
        conn,
        scheme_hash_code,
        create_query,
        page_size
    )

    assert result.status == 0, result.message

    result = cache_get_configuration(conn, table_hash_code)

    assert result.status == 0, result.message

    result = cache_get_names(conn)
    assert result.status == 0

    result = sql_fields(
        conn,
        scheme_hash_code,
        insert_query,
        page_size
    )
    assert result.status == 0

    result = scan(conn, table_hash_code, 100)
    # assert result.status == 0
    pass

    result = sql_fields(conn, scheme_hash_code, drop_query, page_size)
    assert result.status == 0, result.message
