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
import time
from datetime import timedelta

from pyignite import Client, AioClient
from pyignite.datatypes import ExpiryPolicy
from pyignite.datatypes.prop_codes import PROP_NAME, PROP_EXPIRY_POLICY
from pyignite.exceptions import NotSupportedByClusterError


def sync_actions():
    print("Running sync ExpiryPolicy example.")

    client = Client()
    with client.connect('127.0.0.1', 10800):
        print("Create cache with expiry policy.")
        try:
            ttl_cache = client.create_cache({
                PROP_NAME: 'test',
                PROP_EXPIRY_POLICY: ExpiryPolicy(create=timedelta(seconds=1.0))
            })
        except NotSupportedByClusterError:
            print("'ExpiryPolicy' API is not supported by cluster. Finishing...")
            return

        try:
            ttl_cache.put(1, 1)
            time.sleep(0.5)
            print(f"key = {1}, value = {ttl_cache.get(1)}")
            # key = 1, value = 1
            time.sleep(1.2)
            print(f"key = {1}, value = {ttl_cache.get(1)}")
            # key = 1, value = None
        finally:
            ttl_cache.destroy()

        print("Create simple Cache and set TTL through `with_expire_policy`")
        simple_cache = client.create_cache('test')
        try:
            ttl_cache = simple_cache.with_expire_policy(access=timedelta(seconds=1.0))
            ttl_cache.put(1, 1)
            time.sleep(0.5)
            print(f"key = {1}, value = {ttl_cache.get(1)}")
            # key = 1, value = 1
            time.sleep(1.7)
            print(f"key = {1}, value = {ttl_cache.get(1)}")
            # key = 1, value = None
        finally:
            simple_cache.destroy()


async def async_actions():
    print("Running async ExpiryPolicy example.")

    client = AioClient()
    async with client.connect('127.0.0.1', 10800):
        print("Create cache with expiry policy.")
        try:
            ttl_cache = await client.create_cache({
                PROP_NAME: 'test',
                PROP_EXPIRY_POLICY: ExpiryPolicy(create=timedelta(seconds=1.0))
            })
        except NotSupportedByClusterError:
            print("'ExpiryPolicy' API is not supported by cluster. Finishing...")
            return

        try:
            await ttl_cache.put(1, 1)
            await asyncio.sleep(0.5)
            value = await ttl_cache.get(1)
            print(f"key = {1}, value = {value}")
            # key = 1, value = 1
            await asyncio.sleep(1.2)
            value = await ttl_cache.get(1)
            print(f"key = {1}, value = {value}")
            # key = 1, value = None
        finally:
            await ttl_cache.destroy()

        print("Create simple Cache and set TTL through `with_expire_policy`")
        simple_cache = await client.create_cache('test')
        try:
            ttl_cache = simple_cache.with_expire_policy(access=timedelta(seconds=1.0))
            await ttl_cache.put(1, 1)
            await asyncio.sleep(0.5)
            value = await ttl_cache.get(1)
            print(f"key = {1}, value = {value}")
            # key = 1, value = 1
            await asyncio.sleep(1.7)
            value = await ttl_cache.get(1)
            print(f"key = {1}, value = {value}")
            # key = 1, value = None
        finally:
            await simple_cache.destroy()


if __name__ == '__main__':
    sync_actions()
    print('-' * 20)
    asyncio.run(async_actions())

# Running sync ExpiryPolicy example.
# Create cache with expiry policy.
# key = 1, value = 1
# key = 1, value = None
# Create simple Cache and set TTL through `with_expire_policy`
# key = 1, value = 1
# key = 1, value = None
# --------------------
# Running async ExpiryPolicy example.
# Create cache with expiry policy.
# key = 1, value = 1
# key = 1, value = None
# Create simple Cache and set TTL through `with_expire_policy`
# key = 1, value = 1
# key = 1, value = None
