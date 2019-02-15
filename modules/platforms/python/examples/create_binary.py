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

from collections import OrderedDict

from pyignite import Client, GenericObjectMeta
from pyignite.datatypes import DoubleObject, IntObject, String
from pyignite.datatypes.prop_codes import *

client = Client()
client.connect('127.0.0.1', 10800)

student_cache = client.create_cache({
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
                    },
                    {
                        'name': 'NAME',
                        'type_name': 'java.lang.String',
                    },
                    {
                        'name': 'LOGIN',
                        'type_name': 'java.lang.String',
                    },
                    {
                        'name': 'AGE',
                        'type_name': 'java.lang.Integer',
                    },
                    {
                        'name': 'GPA',
                        'type_name': 'java.math.Double',
                    },
                ],
                'query_indexes': [],
                'value_type_name': 'SQL_PUBLIC_STUDENT_TYPE',
                'value_field_name': None,
            },
        ],
    })


class Student(
    metaclass=GenericObjectMeta,
    type_name='SQL_PUBLIC_STUDENT_TYPE',
    schema=OrderedDict([
        ('NAME', String),
        ('LOGIN', String),
        ('AGE', IntObject),
        ('GPA', DoubleObject),
    ])
):
    pass


student_cache.put(
    1,
    Student(LOGIN='jdoe', NAME='John Doe', AGE=17, GPA=4.25),
    key_hint=IntObject
)

result = client.sql(
    r'SELECT * FROM Student',
    include_field_names=True
)
print(next(result))
# ['SID', 'NAME', 'LOGIN', 'AGE', 'GPA']

print(*result)
# [1, 'John Doe', 'jdoe', 17, 4.25]

# DROP_QUERY = 'DROP TABLE Student'
# client.sql(DROP_QUERY)
#
# pyignite.exceptions.SQLError: class org.apache.ignite.IgniteCheckedException:
# Only cache created with CREATE TABLE may be removed with DROP TABLE
# [cacheName=SQL_PUBLIC_STUDENT]

student_cache.destroy()
client.close()
