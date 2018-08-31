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
from pyignite.datatypes import *


class Person(metaclass=GenericObjectMeta, schema=OrderedDict([
    ('first_name', String),
    ('last_name', String),
    ('age', IntObject),
])):
    pass


client = Client()
client.connect('localhost', 10800)

person_cache = client.get_or_create_cache('person')

person_cache.put(
    1, Person(first_name='Ivan', last_name='Ivanov', age=33)
)

person = person_cache.get(1)
print(person.__class__.__name__)
# Person

print(person.__class__ is Person)
# True if `Person` was registered automatically (on writing)
# or manually (using `client.register_binary_type()` method).
# False otherwise

print(person)
# Person(first_name='Ivan', last_name='Ivanov', age=33, version=1)

client.register_binary_type(Person)

Person = person.__class__
