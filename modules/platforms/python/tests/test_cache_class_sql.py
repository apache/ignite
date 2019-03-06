#                   GridGain Community Edition Licensing
#                   Copyright 2019 GridGain Systems, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
# Restriction; you may not use this file except in compliance with the License. You may obtain a
# copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the
# License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the specific language governing permissions
# and limitations under the License.
#
# Commons Clause Restriction
#
# The Software is provided to you by the Licensor under the License, as defined below, subject to
# the following condition.
#
# Without limiting other conditions in the License, the grant of rights under the License will not
# include, and the License does not grant to you, the right to Sell the Software.
# For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
# under the License to provide to third parties, for a fee or other consideration (including without
# limitation fees for hosting or consulting/ support services related to the Software), a product or
# service whose value derives, entirely or substantially, from the functionality of the Software.
# Any license notice or attribution required by the License must also include this Commons Clause
# License Condition notice.
#
# For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
# the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
# Edition software provided with this notice.

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
