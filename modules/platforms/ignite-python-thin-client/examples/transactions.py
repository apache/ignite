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
import sys
import time

from pyignite import AioClient, Client
from pyignite.datatypes import TransactionIsolation, TransactionConcurrency
from pyignite.datatypes.cache_config import CacheAtomicityMode
from pyignite.datatypes.prop_codes import PROP_CACHE_ATOMICITY_MODE, PROP_NAME
from pyignite.exceptions import CacheError


async def async_example():
    client = AioClient()
    async with client.connect('127.0.0.1', 10800):
        cache = await client.get_or_create_cache({
            PROP_NAME: 'tx_cache',
            PROP_CACHE_ATOMICITY_MODE: CacheAtomicityMode.TRANSACTIONAL
        })

        # starting transaction
        key = 1
        async with client.tx_start(
                isolation=TransactionIsolation.REPEATABLE_READ,
                concurrency=TransactionConcurrency.PESSIMISTIC
        ) as tx:
            await cache.put(key, 'success')
            await tx.commit()

        # key=1 value=success
        val = await cache.get(key)
        print(f"key={key} value={val}")

        # rollback transaction.
        try:
            async with client.tx_start(
                    isolation=TransactionIsolation.REPEATABLE_READ,
                    concurrency=TransactionConcurrency.PESSIMISTIC
            ):
                await cache.put(key, 'fail')
                raise RuntimeError('test')
        except RuntimeError:
            pass

        # key=1 value=success
        val = await cache.get(key)
        print(f"key={key} value={val}")

        # rollback transaction on timeout.
        try:
            async with client.tx_start(timeout=1000, label='long-tx') as tx:
                await cache.put(key, 'fail')
                await asyncio.sleep(2.0)
                await tx.commit()
        except CacheError as e:
            # Cache transaction timed out: GridNearTxLocal[...timeout=1000, ... label=long-tx]
            print(e)

        # key=1 value=success
        val = await cache.get(key)
        print(f"key={key} value={val}")

        # destroy cache
        await cache.destroy()


def sync_example():
    client = Client()
    with client.connect('127.0.0.1', 10800):
        cache = client.get_or_create_cache({
            PROP_NAME: 'tx_cache',
            PROP_CACHE_ATOMICITY_MODE: CacheAtomicityMode.TRANSACTIONAL
        })

        # starting transaction
        key = 1
        with client.tx_start(
                isolation=TransactionIsolation.REPEATABLE_READ,
                concurrency=TransactionConcurrency.PESSIMISTIC
        ) as tx:
            cache.put(key, 'success')
            tx.commit()

        # key=1 value=success
        print(f"key={key} value={cache.get(key)}")

        # rollback transaction.
        try:
            with client.tx_start(
                    isolation=TransactionIsolation.REPEATABLE_READ,
                    concurrency=TransactionConcurrency.PESSIMISTIC
            ):
                cache.put(key, 'fail')
                raise RuntimeError('test')
        except RuntimeError:
            pass

        # key=1 value=success
        print(f"key={key} value={cache.get(key)}")

        # rollback transaction on timeout.
        try:
            with client.tx_start(timeout=1000, label='long-tx') as tx:
                cache.put(key, 'fail')
                time.sleep(2.0)
                tx.commit()
        except CacheError as e:
            # Cache transaction timed out: GridNearTxLocal[...timeout=1000, ... label=long-tx]
            print(e)

        # key=1 value=success
        print(f"key={key} value={cache.get(key)}")

        # destroy cache
        cache.destroy()


def check_is_transactions_supported():
    client = Client()
    with client.connect('127.0.0.1', 10800):
        if not client.protocol_context.is_transactions_supported():
            print("'Transactions' API is not supported by cluster. Finishing...")
            exit(0)


if __name__ == '__main__':
    check_is_transactions_supported()

    print("Starting sync example")
    sync_example()

    if sys.version_info >= (3, 7):
        print("Starting async example")
        asyncio.run(async_example())
