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

client = Client()
client.connect('127.0.0.1', 10800)

my_cache = client.create_cache('my cache')

my_cache.put('my key', 42)

result = my_cache.get('my key')
print(result)  # 42

result = my_cache.get('non-existent key')
print(result)  # None

result = my_cache.get_all([
    'my key',
    'non-existent key',
    'other-key',
])
print(result)  # {'my key': 42}

my_cache.clear_key('my key')

my_cache.destroy()
client.close()
