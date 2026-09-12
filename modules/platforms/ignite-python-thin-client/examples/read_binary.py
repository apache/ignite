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

from pprint import pprint

from helpers.converters import obj_to_dict
from helpers.sql_helper import TableNames, Query, TestData
from pyignite import Client
from pyignite.datatypes.prop_codes import PROP_NAME, PROP_QUERY_ENTITIES

# establish connection
client = Client()
with client.connect('127.0.0.1', 10800):
    # create tables
    for query in [
        Query.COUNTRY_CREATE_TABLE,
        Query.CITY_CREATE_TABLE,
        Query.LANGUAGE_CREATE_TABLE,
    ]:
        client.sql(query)

    # create indices
    for query in [Query.CITY_CREATE_INDEX, Query.LANGUAGE_CREATE_INDEX]:
        client.sql(query)

    # load data
    for row in TestData.COUNTRY:
        client.sql(Query.COUNTRY_INSERT, query_args=row)

    for row in TestData.CITY:
        client.sql(Query.CITY_INSERT, query_args=row)

    for row in TestData.LANGUAGE:
        client.sql(Query.LANGUAGE_INSERT, query_args=row)

    # examine the storage
    result = client.get_cache_names()
    pprint(result)
    # ['SQL_PUBLIC_CITY', 'SQL_PUBLIC_COUNTRY', 'SQL_PUBLIC_COUNTRYLANGUAGE']

    city_cache = client.get_or_create_cache('SQL_PUBLIC_CITY')
    pprint(city_cache.settings[PROP_NAME])
    # 'SQL_PUBLIC_CITY'

    pprint(city_cache.settings[PROP_QUERY_ENTITIES])
    # [{'field_name_aliases': [{'alias': 'DISTRICT', 'field_name': 'DISTRICT'},
    #                              {'alias': 'POPULATION', 'field_name': 'POPULATION'},
    #                              {'alias': 'COUNTRYCODE', 'field_name': 'COUNTRYCODE'},
    #                              {'alias': 'ID', 'field_name': 'ID'},
    #                              {'alias': 'NAME', 'field_name': 'NAME'}],
    #       'key_field_name': None,
    #       'key_type_name': 'SQL_PUBLIC_CITY_081f37cc8ac72b10f08ab1273b744497_KEY',
    #       'query_fields': [{'default_value': None,
    #                         'is_key_field': True,
    #                         'is_notnull_constraint_field': False,
    #                         'name': 'ID',
    #                         'precision': -1,
    #                         'scale': -1,
    #                         'type_name': 'java.lang.Integer'},
    #                        {'default_value': None,
    #                         'is_key_field': False,
    #                         'is_notnull_constraint_field': False,
    #                         'name': 'NAME',
    #                         'precision': 35,
    #                         'scale': -1,
    #                         'type_name': 'java.lang.String'},
    #                        {'default_value': None,
    #                         'is_key_field': True,
    #                         'is_notnull_constraint_field': False,
    #                         'name': 'COUNTRYCODE',
    #                         'precision': 3,
    #                         'scale': -1,
    #                         'type_name': 'java.lang.String'},
    #                        {'default_value': None,
    #                         'is_key_field': False,
    #                         'is_notnull_constraint_field': False,
    #                         'name': 'DISTRICT',
    #                         'precision': 20,
    #                         'scale': -1,
    #                         'type_name': 'java.lang.String'},
    #                        {'default_value': None,
    #                         'is_key_field': False,
    #                         'is_notnull_constraint_field': False,
    #                         'name': 'POPULATION',
    #                         'precision': -1,
    #                         'scale': -1,
    #                         'type_name': 'java.lang.Integer'}],
    #       'query_indexes': [],
    #       'table_name': 'CITY',
    #       'value_field_name': None,
    #       'value_type_name': 'SQL_PUBLIC_CITY_081f37cc8ac72b10f08ab1273b744497'}]

    print('-' * 20)
    with city_cache.scan() as cursor:
        for line in next(cursor):
            pprint(obj_to_dict(line))
    # {'COUNTRYCODE': 'USA',
    #  'ID': 3793,
    #  'type_name': 'SQL_PUBLIC_CITY_081f37cc8ac72b10f08ab1273b744497_KEY'}
    # {'DISTRICT': 'New York',
    #  'NAME': 'New York',
    #  'POPULATION': 8008278,
    #  'type_name': 'SQL_PUBLIC_CITY_081f37cc8ac72b10f08ab1273b744497'}

    print('-' * 20)
    with client.sql('SELECT _KEY, _VAL FROM CITY WHERE ID = ?', query_args=[1890]) as cursor:
        for line in next(cursor):
            pprint(obj_to_dict(line))
    # {'COUNTRYCODE': 'CHN',
    #  'ID': 1890,
    #  'type_name': 'SQL_PUBLIC_CITY_081f37cc8ac72b10f08ab1273b744497_KEY'}
    # {'DISTRICT': 'Shanghai',
    #  'NAME': 'Shanghai',
    #  'POPULATION': 9696300,
    #  'type_name': 'SQL_PUBLIC_CITY_081f37cc8ac72b10f08ab1273b744497'}

    # clean up
    for table_name in TableNames:
        result = client.sql(Query.DROP_TABLE.format(table_name.value))
