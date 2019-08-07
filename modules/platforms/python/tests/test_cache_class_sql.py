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

import pytest


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

select_query = 'SELECT id, first_name, last_name, grade FROM Student'

drop_query = 'DROP TABLE Student IF EXISTS'


@pytest.mark.parametrize('page_size', range(1, 6, 2))
def test_sql_fields(client, page_size):

    client.sql(drop_query, page_size)

    result = client.sql(create_query, page_size)
    assert next(result)[0] == 0

    for i, data_line in enumerate(initial_data, start=1):
        fname, lname, grade = data_line
        result = client.sql(
            insert_query,
            page_size,
            query_args=[i, fname, lname, grade]
        )
        assert next(result)[0] == 1

    result = client.sql(
        select_query,
        page_size,
        include_field_names=True,
    )
    field_names = next(result)
    assert set(field_names) == {'ID', 'FIRST_NAME', 'LAST_NAME', 'GRADE'}

    data = list(result)
    assert len(data) == 5
    for row in data:
        assert len(row) == 4

    client.sql(drop_query, page_size)


@pytest.mark.parametrize('page_size', range(1, 6, 2))
def test_sql(client, page_size):

    client.sql(drop_query, page_size)

    result = client.sql(create_query, page_size)
    assert next(result)[0] == 0

    for i, data_line in enumerate(initial_data, start=1):
        fname, lname, grade = data_line
        result = client.sql(
            insert_query,
            page_size,
            query_args=[i, fname, lname, grade]
        )
        assert next(result)[0] == 1

    student = client.get_or_create_cache('SQL_PUBLIC_STUDENT')
    result = student.select_row('TRUE', page_size)
    for k, v in result:
        assert k in range(1, 6)
        assert v.FIRST_NAME in [
            'John',
            'Jane',
            'Joe',
            'Richard',
            'Negidius',
        ]

    client.sql(drop_query, page_size)
