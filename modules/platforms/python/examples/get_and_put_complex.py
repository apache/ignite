#
# Copyright 2019 GridGain Systems, Inc. and Contributors.
#
# Licensed under the GridGain Community Edition License (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
from collections import OrderedDict

from pygridgain import Client
from pygridgain.datatypes import (
    CollectionObject, MapObject, ObjectArrayObject,
)


client = Client()
client.connect('127.0.0.1', 10800)

my_cache = client.get_or_create_cache('my cache')

value = OrderedDict([(1, 'test'), ('key', 2.0)])

# saving ordered dictionary
type_id = MapObject.LINKED_HASH_MAP
my_cache.put('my dict', (type_id, value))
result = my_cache.get('my dict')
print(result)  # (2, OrderedDict([(1, 'test'), ('key', 2.0)]))

# saving unordered dictionary
type_id = MapObject.HASH_MAP
my_cache.put('my dict', (type_id, value))
result = my_cache.get('my dict')
print(result)  # (1, {'key': 2.0, 1: 'test'})

type_id = CollectionObject.LINKED_LIST
value = [1, '2', 3.0]

my_cache.put('my list', (type_id, value))

result = my_cache.get('my list')
print(result)  # (2, [1, '2', 3.0])

type_id = CollectionObject.HASH_SET
value = [4, 4, 'test', 5.6]

my_cache.put('my set', (type_id, value))

result = my_cache.get('my set')
print(result)  # (3, [5.6, 4, 'test'])

type_id = ObjectArrayObject.OBJECT
value = [7, '8', 9.0]

my_cache.put(
    'my array of objects',
    (type_id, value),
    value_hint=ObjectArrayObject  # this hint is mandatory!
)
result = my_cache.get('my array of objects')
print(result)  # (-1, [7, '8', 9.0])
