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


def test_cache_create(conn):
    cache = conn.create_cache('my_oop_cache')
    assert cache.name == 'my_oop_cache'
    cache.destroy()


def test_cache_get_put(conn):
    cache = conn.create_cache('my_oop_cache')
    cache.put('my_key', 42)
    result = cache.get('my_key')
    assert result, 42
    cache.destroy()
