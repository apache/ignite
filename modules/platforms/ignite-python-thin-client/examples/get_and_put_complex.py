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

from pyignite import Client
from pyignite.datatypes import CollectionObject, MapObject, ObjectArrayObject


client = Client()
with client.connect('127.0.0.1', 10800):
    my_cache = client.get_or_create_cache('my cache')

    value = {1: 'test', 'key': 2.0}

    # saving ordered dictionary
    type_id = MapObject.LINKED_HASH_MAP
    my_cache.put('my dict', (type_id, value))
    result = my_cache.get('my dict')
    print(result)  # (2, {1: 'test', 'key': 2.0})

    # saving unordered dictionary
    type_id = MapObject.HASH_MAP
    my_cache.put('my dict', (type_id, value))
    result = my_cache.get('my dict')
    print(result)  # (1, {1: 'test', 'key': 2.0})

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

    my_cache.destroy()
