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

import contextvars

import attr

from pyignite.datatypes import Byte, String, Long, Int, Bool
from pyignite.exceptions import CacheError
from pyignite.queries import Query, query_perform
from pyignite.queries.op_codes import OP_TX_START, OP_TX_END

__CURRENT_TX = contextvars.ContextVar('current_tx', default=None)


def get_tx_id():
    ctx = __CURRENT_TX.get() if __CURRENT_TX else None
    return ctx.tx_id if ctx else None


def get_tx_connection():
    ctx = __CURRENT_TX.get() if __CURRENT_TX else None
    return ctx.conn if ctx else None


@attr.s
class TransactionContext:
    tx_id = attr.ib(type=int, default=None)
    conn = attr.ib(default=None)


def tx_start(conn, concurrency, isolation, timeout: int = 0, label: str = None):
    result = __tx_start(conn, concurrency, isolation, timeout, label)
    return __tx_start_post_process(result, conn)


async def tx_start_async(conn, concurrency, isolation, timeout: int = 0, label: str = None):
    result = await __tx_start(conn, concurrency, isolation, timeout, label)
    return __tx_start_post_process(result, conn)


def __tx_start(conn, concurrency, isolation, timeout, label):
    query_struct = Query(
        OP_TX_START,
        [
            ('concurrency', Byte),
            ('isolation', Byte),
            ('timeout', Long),
            ('label', String)
        ]
    )
    return query_perform(
        query_struct, conn,
        query_params={
            'concurrency': concurrency,
            'isolation': isolation,
            'timeout': timeout,
            'label': label
        },
        response_config=[
            ('tx_id', Int)
        ]
    )


def tx_end(tx_id, committed):
    ctx = __CURRENT_TX.get()

    if not ctx or ctx.tx_id != tx_id:
        raise CacheError("Cannot commit transaction from different thread or coroutine")

    try:
        return __tx_end(ctx.conn, tx_id, committed)
    finally:
        __CURRENT_TX.set(None)


async def tx_end_async(tx_id, committed):
    ctx = __CURRENT_TX.get()

    if not ctx or ctx.tx_id != tx_id:
        raise CacheError("Cannot commit transaction from different thread or coroutine")

    try:
        return await __tx_end(ctx.conn, tx_id, committed)
    finally:
        __CURRENT_TX.set(None)


def __tx_end(conn, tx_id, committed):
    query_struct = Query(
        OP_TX_END,
        [
            ('tx_id', Int),
            ('committed', Bool)
        ],
    )
    return query_perform(
        query_struct, conn,
        query_params={
            'tx_id': tx_id,
            'committed': committed
        }
    )


def __tx_start_post_process(result, conn):
    if result.status == 0:
        tx_id = result.value['tx_id']
        __CURRENT_TX.set(TransactionContext(tx_id, conn))
        result.value = tx_id
    return result
