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
    sql_fields, sql_fields_cursor_get_page, hashcode, cache_create,
)


def test_sql(conn):

    cache_create(conn, 'PUBLIC')

    initial_data = [
        ('John', 'Doe', 5),
        ('Jane', 'Roe', 4),
        ('Joe', 'Bloggs', 4),
        ('Richard', 'Public', 3),
        ('Negidius', 'Numerius', 3),
    ]

    create_query = '''CREATE TABLE Student (
    id INT(11) PRIMARY KEY,
    first_name CHAR(24),
    last_name CHAR(32),
    grade INT(11))'''

    insert_query = '''INSERT INTO Student(id, first_name, last_name, grade)
    VALUES (?, ?, ?, ?)'''

    select_query = 'SELECT (id, first_name, last_name, grade) FROM Student'

    drop_query = 'DROP TABLE Student'

    page_size = 4

    result = sql_fields(
        conn,
        hashcode('PUBLIC'),
        create_query,
        page_size,
        include_field_names=True
    )
    assert result.status == 0

    for i, data_line in enumerate(initial_data, start=1):
        fname, lname, grade = data_line
        result = sql_fields(
            conn,
            hashcode('PUBLIC'),
            insert_query,
            page_size,
            query_args=[i, fname, lname, grade],
            include_field_names=True
        )
        assert result.status == 0

    result = sql_fields(
        conn,
        hashcode('PUBLIC'),
        select_query,
        page_size,
        include_field_names=True
    )
    assert result.status == 0
    assert len(result.value['data']) == page_size
    assert result.value['more'] is True

    cursor = result.value['cursor']

    result = sql_fields_cursor_get_page(conn, cursor)
    assert result.status == 0
    assert len(result.value['data']) == len(initial_data) - page_size
    assert result.value['more'] is False

    # cleanup
    result = sql_fields(conn, hashcode('PUBLIC'), drop_query, page_size)
    assert result.status == 0
