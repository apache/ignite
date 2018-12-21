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

my_cache.put_all({'key_{}'.format(v): v for v in range(20)})
# {
#     'key_0': 0,
#     'key_1': 1,
#     'key_2': 2,
#     ... 20 elements in total...
#     'key_18': 18,
#     'key_19': 19
# }

result = my_cache.scan()
for k, v in result:
    print(k, v)
# 'key_17' 17
# 'key_10' 10
# 'key_6' 6,
# ... 20 elements in total...
# 'key_16' 16
# 'key_12' 12

result = my_cache.scan()
print(dict(result))
# {
#     'key_17': 17,
#     'key_10': 10,
#     'key_6': 6,
#     ... 20 elements in total...
#     'key_16': 16,
#     'key_12': 12
# }

my_cache.destroy()
client.close()
