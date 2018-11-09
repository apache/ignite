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
