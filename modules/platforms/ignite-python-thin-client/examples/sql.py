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

from helpers.sql_helper import TableNames, Query, TestData
from pyignite import Client

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

    # 10 most populated cities (with pagination)
    with client.sql('SELECT name, population FROM City ORDER BY population DESC LIMIT 10') as cursor:
        print('Most 10 populated cities:')
        for row in cursor:
            print(row)
    # Most 10 populated cities:
    # ['Mumbai (Bombay)', 10500000]
    # ['Shanghai', 9696300]
    # ['New York', 8008278]
    # ['Peking', 7472000]
    # ['Delhi', 7206704]
    # ['Chongqing', 6351600]
    # ['Tianjin', 5286800]
    # ['Calcutta [Kolkata]', 4399819]
    # ['Wuhan', 4344600]
    # ['Harbin', 4289800]
    print('-' * 20)
    # 10 most populated cities in 3 countries (with pagination and header row)
    MOST_POPULATED_IN_3_COUNTRIES = '''
    SELECT country.name as country_name, city.name as city_name, MAX(city.population) AS max_pop FROM country
        JOIN city ON city.countrycode = country.code
        WHERE country.code IN ('USA','IND','CHN')
        GROUP BY country.name, city.name ORDER BY max_pop DESC LIMIT 10
    '''

    with client.sql(MOST_POPULATED_IN_3_COUNTRIES, include_field_names=True) as cursor:
        print('Most 10 populated cities in USA, India and China:')
        table_str_pattern = '{:15}\t| {:20}\t| {}'
        print(table_str_pattern.format(*next(cursor)))
        print('*' * 50)
        for row in cursor:
            print(table_str_pattern.format(*row))
    # Most 10 populated cities in USA, India and China:
    # COUNTRY_NAME   	| CITY_NAME           	| MAX_POP
    # **************************************************
    # India          	| Mumbai (Bombay)     	| 10500000
    # China          	| Shanghai            	| 9696300
    # United States  	| New York            	| 8008278
    # China          	| Peking              	| 7472000
    # India          	| Delhi               	| 7206704
    # China          	| Chongqing           	| 6351600
    # China          	| Tianjin             	| 5286800
    # India          	| Calcutta [Kolkata]  	| 4399819
    # China          	| Wuhan               	| 4344600
    # China          	| Harbin              	| 4289800
    print('-' * 20)

    # Show city info
    with client.sql('SELECT * FROM City WHERE id = ?', query_args=[3802], include_field_names=True) as cursor:
        field_names = next(cursor)
        field = list(*cursor)
        print('City info:')
        for field_name, field_value in zip(field_names * len(field), field):
            print(f'{field_name}: {field_value}')
    # City info:
    # ID: 3802
    # NAME: Detroit
    # COUNTRYCODE: USA
    # DISTRICT: Michigan
    # POPULATION: 951270

    # Clean up
    for table_name in TableNames:
        result = client.sql(Query.DROP_TABLE.format(table_name.value))
