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
    hashcode, cache_get_or_create, sql_fields, cache_get_names,
    cache_get_configuration, get_binary_type, scan,
)
from pyignite.connection import Connection
from pyignite.utils import unwrap_binary

PAGE_SIZE = 5

SCHEMA_NAME = 'PUBLIC'

COUNTRY_TABLE_NAME = 'Country'

CITY_TABLE_NAME = 'City'

LANGUAGE_TABLE_NAME = 'CountryLanguage'

COUNTRY_CREATE_TABLE_QUERY = '''CREATE TABLE Country (
    Code CHAR(3) PRIMARY KEY,
    Name CHAR(52),
    Continent CHAR(50),
    Region CHAR(26),
    SurfaceArea DECIMAL(10,2),
    IndepYear SMALLINT(6),
    Population INT(11),
    LifeExpectancy DECIMAL(3,1),
    GNP DECIMAL(10,2),
    GNPOld DECIMAL(10,2),
    LocalName CHAR(45),
    GovernmentForm CHAR(45),
    HeadOfState CHAR(60),
    Capital INT(11),
    Code2 CHAR(2)
)'''

COUNTRY_INSERT_QUERY = '''INSERT INTO Country(
    Code, Name, Continent, Region,
    SurfaceArea, IndepYear, Population,
    LifeExpectancy, GNP, GNPOld,
    LocalName, GovernmentForm, HeadOfState,
    Capital, Code2
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''

CITY_CREATE_TABLE_QUERY = '''CREATE TABLE City (
    ID INT(11),
    Name CHAR(35),
    CountryCode CHAR(3),
    District CHAR(20),
    Population INT(11),
    PRIMARY KEY (ID, CountryCode)
) WITH "affinityKey=CountryCode"'''

CITY_CREATE_INDEX = '''
CREATE INDEX idx_country_code ON city (CountryCode)'''

CITY_INSERT_QUERY = '''INSERT INTO City(
    ID, Name, CountryCode, District, Population
) VALUES (?, ?, ?, ?, ?)'''

LANGUAGE_CREATE_TABLE_QUERY = '''CREATE TABLE CountryLanguage (
    CountryCode CHAR(3),
    Language CHAR(30),
    IsOfficial CHAR(2),
    Percentage DECIMAL(4,1),
    PRIMARY KEY (CountryCode, Language)
) WITH "affinityKey=CountryCode"'''

LANGUAGE_CREATE_INDEX = '''
CREATE INDEX idx_lang_country_code ON CountryLanguage (CountryCode)'''

LANGUAGE_INSERT_QUERY = '''INSERT INTO CountryLanguage(
    CountryCode, Language, IsOfficial, Percentage
) VALUES (?, ?, ?, ?)'''

DROP_TABLE_QUERY = '''DROP TABLE {}'''

COUNTRY_DATA = [
    [
        'USA', 'United States', 'North America', 'North America',
        Decimal('9363520.00'), 1776, 278357000,
        Decimal('77.1'), Decimal('8510700.00'), Decimal('8110900.00'),
        'United States', 'Federal Republic', 'George W. Bush',
        3813, 'US',
    ],
    [
        'IND', 'India', 'Asia', 'Southern and Central Asia',
        Decimal('3287263.00'), 1947, 1013662000,
        Decimal('62.5'), Decimal('447114.00'), Decimal('430572.00'),
        'Bharat/India', 'Federal Republic', 'Kocheril Raman Narayanan',
        1109, 'IN',
    ],
    [
        'CHN', 'China', 'Asia', 'Eastern Asia',
        Decimal('9572900.00'), -1523, 1277558000,
        Decimal('71.4'), Decimal('982268.00'), Decimal('917719.00'),
        'Zhongquo', 'PeoplesRepublic', 'Jiang Zemin',
        1891, 'CN',
    ],
]

CITY_DATA = [
    [3793, 'New York', 'USA', 'New York', 8008278],
    [3794, 'Los Angeles', 'USA', 'California', 3694820],
    [3795, 'Chicago', 'USA', 'Illinois', 2896016],
    [3796, 'Houston', 'USA', 'Texas', 1953631],
    [3797, 'Philadelphia', 'USA', 'Pennsylvania', 1517550],
    [3798, 'Phoenix', 'USA', 'Arizona', 1321045],
    [3799, 'San Diego', 'USA', 'California', 1223400],
    [3800, 'Dallas', 'USA', 'Texas', 1188580],
    [3801, 'San Antonio', 'USA', 'Texas', 1144646],
    [3802, 'Detroit', 'USA', 'Michigan', 951270],
    [3803, 'San Jose', 'USA', 'California', 894943],
    [3804, 'Indianapolis', 'USA', 'Indiana', 791926],
    [3805, 'San Francisco', 'USA', 'California', 776733],
    [1024, 'Mumbai (Bombay)', 'IND', 'Maharashtra', 10500000],
    [1025, 'Delhi', 'IND', 'Delhi', 7206704],
    [1026, 'Calcutta [Kolkata]', 'IND', 'West Bengali', 4399819],
    [1027, 'Chennai (Madras)', 'IND', 'Tamil Nadu', 3841396],
    [1028, 'Hyderabad', 'IND', 'Andhra Pradesh', 2964638],
    [1029, 'Ahmedabad', 'IND', 'Gujarat', 2876710],
    [1030, 'Bangalore', 'IND', 'Karnataka', 2660088],
    [1031, 'Kanpur', 'IND', 'Uttar Pradesh', 1874409],
    [1032, 'Nagpur', 'IND', 'Maharashtra', 1624752],
    [1033, 'Lucknow', 'IND', 'Uttar Pradesh', 1619115],
    [1034, 'Pune', 'IND', 'Maharashtra', 1566651],
    [1035, 'Surat', 'IND', 'Gujarat', 1498817],
    [1036, 'Jaipur', 'IND', 'Rajasthan', 1458483],
    [1890, 'Shanghai', 'CHN', 'Shanghai', 9696300],
    [1891, 'Peking', 'CHN', 'Peking', 7472000],
    [1892, 'Chongqing', 'CHN', 'Chongqing', 6351600],
    [1893, 'Tianjin', 'CHN', 'Tianjin', 5286800],
    [1894, 'Wuhan', 'CHN', 'Hubei', 4344600],
    [1895, 'Harbin', 'CHN', 'Heilongjiang', 4289800],
    [1896, 'Shenyang', 'CHN', 'Liaoning', 4265200],
    [1897, 'Kanton [Guangzhou]', 'CHN', 'Guangdong', 4256300],
    [1898, 'Chengdu', 'CHN', 'Sichuan', 3361500],
    [1899, 'Nanking [Nanjing]', 'CHN', 'Jiangsu', 2870300],
    [1900, 'Changchun', 'CHN', 'Jilin', 2812000],
    [1901, 'Xi´an', 'CHN', 'Shaanxi', 2761400],
    [1902, 'Dalian', 'CHN', 'Liaoning', 2697000],
    [1903, 'Qingdao', 'CHN', 'Shandong', 2596000],
    [1904, 'Jinan', 'CHN', 'Shandong', 2278100],
    [1905, 'Hangzhou', 'CHN', 'Zhejiang', 2190500],
    [1906, 'Zhengzhou', 'CHN', 'Henan', 2107200],
]

LANGUAGE_DATA = [
    ['USA', 'Chinese', False, Decimal('0.6')],
    ['USA', 'English', True, Decimal('86.2')],
    ['USA', 'French', False, Decimal('0.7')],
    ['USA', 'German', False, Decimal('0.7')],
    ['USA', 'Italian', False, Decimal('0.6')],
    ['USA', 'Japanese', False, Decimal('0.2')],
    ['USA', 'Korean', False, Decimal('0.3')],
    ['USA', 'Polish', False, Decimal('0.3')],
    ['USA', 'Portuguese', False, Decimal('0.2')],
    ['USA', 'Spanish', False, Decimal('7.5')],
    ['USA', 'Tagalog', False, Decimal('0.4')],
    ['USA', 'Vietnamese', False, Decimal('0.2')],
    ['IND', 'Asami', False, Decimal('1.5')],
    ['IND', 'Bengali', False, Decimal('8.2')],
    ['IND', 'Gujarati', False, Decimal('4.8')],
    ['IND', 'Hindi', True, Decimal('39.9')],
    ['IND', 'Kannada', False, Decimal('3.9')],
    ['IND', 'Malajalam', False, Decimal('3.6')],
    ['IND', 'Marathi', False, Decimal('7.4')],
    ['IND', 'Orija', False, Decimal('3.3')],
    ['IND', 'Punjabi', False, Decimal('2.8')],
    ['IND', 'Tamil', False, Decimal('6.3')],
    ['IND', 'Telugu', False, Decimal('7.8')],
    ['IND', 'Urdu', False, Decimal('5.1')],
    ['CHN', 'Chinese', True, Decimal('92.0')],
    ['CHN', 'Dong', False, Decimal('0.2')],
    ['CHN', 'Hui', False, Decimal('0.8')],
    ['CHN', 'Mantšu', False, Decimal('0.9')],
    ['CHN', 'Miao', False, Decimal('0.7')],
    ['CHN', 'Mongolian', False, Decimal('0.4')],
    ['CHN', 'Puyi', False, Decimal('0.2')],
    ['CHN', 'Tibetan', False, Decimal('0.4')],
    ['CHN', 'Tujia', False, Decimal('0.5')],
    ['CHN', 'Uighur', False, Decimal('0.6')],
    ['CHN', 'Yi', False, Decimal('0.6')],
    ['CHN', 'Zhuang', False, Decimal('1.4')],
]

# establish connection
conn = Connection()
conn.connect('127.0.0.1', 10800)

# create schema
cache_get_or_create(conn, SCHEMA_NAME)

# create tables
for query in [
    COUNTRY_CREATE_TABLE_QUERY,
    CITY_CREATE_TABLE_QUERY,
    LANGUAGE_CREATE_TABLE_QUERY,
]:
    sql_fields(conn, hashcode(SCHEMA_NAME), query, PAGE_SIZE)

# create indices
for query in [CITY_CREATE_INDEX, LANGUAGE_CREATE_INDEX]:
    sql_fields(conn, hashcode(SCHEMA_NAME), query, PAGE_SIZE)

# load data
for row in COUNTRY_DATA:
    sql_fields(
        conn,
        hashcode(SCHEMA_NAME),
        COUNTRY_INSERT_QUERY,
        PAGE_SIZE,
        query_args=row,
    )

for row in CITY_DATA:
    sql_fields(
        conn,
        hashcode(SCHEMA_NAME),
        CITY_INSERT_QUERY,
        PAGE_SIZE,
        query_args=row,
    )

for row in LANGUAGE_DATA:
    sql_fields(
        conn,
        hashcode(SCHEMA_NAME),
        LANGUAGE_INSERT_QUERY,
        PAGE_SIZE,
        query_args=row,
    )

# examine the storage
result = cache_get_names(conn)
print(result.value)

# [
#     'SQL_PUBLIC_CITY',
#     'SQL_PUBLIC_COUNTRY',
#     'PUBLIC',
#     'SQL_PUBLIC_COUNTRYLANGUAGE'
# ]

result = cache_get_configuration(conn, hashcode('SQL_PUBLIC_CITY'))
print(dict(result.value))

# {
#     'name': 'SQL_PUBLIC_CITY',
#     'sql_schema': 'PUBLIC',
#     'cache_key_configuration': [
#         {
#             'type_name': 'SQL_PUBLIC_CITY_9ac8e17a_2f99_45b7_958e_06da32882e9d_KEY',
#             'affinity_key_field_name': 'COUNTRYCODE'
#         }
#     ],
#     'query_entities': [
#         {
#             'key_type_name': 'SQL_PUBLIC_CITY_9ac8e17a_2f99_45b7_958e_06da32882e9d_KEY',
#             'value_type_name': 'SQL_PUBLIC_CITY_9ac8e17a_2f99_45b7_958e_06da32882e9d',
#             'table_name': 'CITY',
#             'query_fields': [
#                 ...
#             ],
#             'field_name_aliases': [
#                 ...
#             ],
#             'query_indexes': []
#         }
#     ]
# }

key_binary_type_name = result.value['query_entities'][0]['key_type_name']
key_binary_type_id = hashcode(key_binary_type_name.lower())

value_binary_type_name = result.value['query_entities'][0]['value_type_name']
value_binary_type_id = hashcode(value_binary_type_name.lower())

print(key_binary_type_id, value_binary_type_id)

# -996482981 -1295865797

result = get_binary_type(conn, key_binary_type_id)
print(result.value['type_exists'])

# True

result = get_binary_type(conn, value_binary_type_id)
print(result.value['type_exists'])

# True

print(result.value)

# {
#     'type_exists': True,
#     'type_id': -1295865797,
#     'type_name': 'SQL_PUBLIC_CITY_9ac8e17a_2f99_45b7_958e_06da32882e9d',
#     'affinity_key_field': None,
#     'binary_fields': [
#         {'field_name': 'NAME', 'type_id': 9, 'field_id': 3373707},
#         {'field_name': 'DISTRICT', 'type_id': 9, 'field_id': 288961422},
#         {'field_name': 'POPULATION', 'type_id': 3, 'field_id': -2023558323}
#     ],
#     'is_enum': False,
#     'schema': [
#         {
#             'schema_id': 275495165,
#             'schema_fields': [
#                 {'schema_field_id': 3373707},
#                 {'schema_field_id': 288961422},
#                 {'schema_field_id': -2023558323}
#             ]
#         }
#     ]
# }

result = scan(conn, hashcode('SQL_PUBLIC_CITY'), 1)
print(result.value['data'])

# {
#     (b'… Some binary data…', 0): (b'… Some more binary data…', 0)
# }

wrapped_value = list(result.value['data'].values())[0]
binary_obj = unwrap_binary(conn, wrapped_value)
print(binary_obj)

# {
#     'version': 1,
#     'type_id': -1295865797,
#     'hash_code': 819840247,
#     'schema_id': 275495165,
#     'fields': {
#         'NAME': 'Shanghai',
#         'DISTRICT': 'Shanghai',
#         'POPULATION': 9696300
#     }
# }
