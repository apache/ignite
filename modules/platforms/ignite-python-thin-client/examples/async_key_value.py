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

import asyncio
from pprint import pprint

from pyignite import AioClient


async def main():
    # Create client and connect.
    client = AioClient()
    async with client.connect('127.0.0.1', 10800):
        # Create cache
        cache = await client.get_or_create_cache('test_async_cache')

        # Load data concurrently.
        await asyncio.gather(
            *[cache.put(f'key_{i}', f'value_{i}') for i in range(0, 20)]
        )

        # Key-value queries.
        print(await cache.get('key_10'))
        # value_10
        pprint(await cache.get_all([f'key_{i}' for i in range(0, 10)]))
        # {'key_0': 'value_0',
        #  'key_1': 'value_1',
        #  'key_2': 'value_2',
        #  'key_3': 'value_3',
        #  'key_4': 'value_4',
        #  'key_5': 'value_5',
        #  'key_6': 'value_6',
        #  'key_7': 'value_7',
        #  'key_8': 'value_8',
        #  'key_9': 'value_9'}

        # Scan query.
        async with cache.scan() as cursor:
            async for k, v in cursor:
                print(f'key = {k}, value = {v}')
        # key = key_42, value = value_42
        # key = key_43, value = value_43
        # key = key_40, value = value_40
        # key = key_41, value = value_41
        # key = key_37, value = value_37
        # key = key_51, value = value_51
        # key = key_20, value = value_20
        # ......

        # Clean up.
        await cache.destroy()


if __name__ == '__main__':
    asyncio.run(main())
