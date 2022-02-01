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

#tag::example-block[]
from pyignite import Client

client = Client()
client.connect('127.0.0.1', 10800)

CITY_CREATE_TABLE_QUERY = '''CREATE TABLE City (
    ID INT(11),
    Name CHAR(35),
    CountryCode CHAR(3),
    District CHAR(20),
    Population INT(11),
    PRIMARY KEY (ID, CountryCode)
) WITH "affinityKey=CountryCode"'''

client.sql(CITY_CREATE_TABLE_QUERY)

CITY_CREATE_INDEX = '''CREATE INDEX idx_country_code ON city (CountryCode)'''

client.sql(CITY_CREATE_INDEX)

CITY_INSERT_QUERY = '''INSERT INTO City(
    ID, Name, CountryCode, District, Population
) VALUES (?, ?, ?, ?, ?)'''

CITY_DATA = [
    [3793, 'New York', 'USA', 'New York', 8008278],
    [3794, 'Los Angeles', 'USA', 'California', 3694820],
    [3795, 'Chicago', 'USA', 'Illinois', 2896016],
    [3796, 'Houston', 'USA', 'Texas', 1953631],
    [3797, 'Philadelphia', 'USA', 'Pennsylvania', 1517550],
    [3798, 'Phoenix', 'USA', 'Arizona', 1321045],
    [3799, 'San Diego', 'USA', 'California', 1223400],
    [3800, 'Dallas', 'USA', 'Texas', 1188580],
]

for row in CITY_DATA:
    client.sql(CITY_INSERT_QUERY, query_args=row)

CITY_SELECT_QUERY = "SELECT * FROM City"

cities = client.sql(CITY_SELECT_QUERY)
for city in cities:
    print(*city)

#tag::field-names[]
field_names = client.sql(CITY_SELECT_QUERY, include_field_names=True).__next__()
print(field_names)
#end::field-names[]

#end::example-block[]
