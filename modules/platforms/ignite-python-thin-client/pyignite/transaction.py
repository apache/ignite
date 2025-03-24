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

from enum import IntEnum
from typing import Union, Type

from pyignite.api.tx_api import tx_end, tx_start, tx_end_async, tx_start_async
from pyignite.datatypes import TransactionIsolation, TransactionConcurrency
from pyignite.exceptions import CacheError
from pyignite.utils import status_to_exception


def _validate_int_enum_param(value: Union[int, IntEnum], cls: Type[IntEnum]):
    if value not in set(v.value for v in cls):  # Use this trick to disable warning on python 3.7
        raise ValueError(f'{value} not in {cls}')
    return value


def _validate_timeout(value):
    if not isinstance(value, int) or value < 0:
        raise ValueError(f'Timeout value should be a positive integer, {value} passed instead')
    return value


def _validate_label(value):
    if value and not isinstance(value, str):
        raise ValueError(f'Label should be str, {type(value)} passed instead')
    return value


class _BaseTransaction:
    def __init__(self, client, concurrency=TransactionConcurrency.PESSIMISTIC,
                 isolation=TransactionIsolation.REPEATABLE_READ, timeout=0, label=None):
        self.client = client
        self.concurrency = _validate_int_enum_param(concurrency, TransactionConcurrency)
        self.isolation = _validate_int_enum_param(isolation, TransactionIsolation)
        self.timeout = _validate_timeout(timeout)
        self.label, self.closed = _validate_label(label), False


class Transaction(_BaseTransaction):
    """
    Thin client transaction.
    """
    def __init__(self, client, concurrency=TransactionConcurrency.PESSIMISTIC,
                 isolation=TransactionIsolation.REPEATABLE_READ, timeout=0, label=None):
        super().__init__(client, concurrency, isolation, timeout, label)
        self.tx_id = self.__start_tx()

    def commit(self) -> None:
        """
        Commit transaction.
        """
        if not self.closed:
            self.closed = True
            return self.__end_tx(True)

    def rollback(self) -> None:
        """
        Rollback transaction.
        """
        self.close()

    def close(self) -> None:
        """
        Close transaction.
        """
        if not self.closed:
            self.closed = True
            return self.__end_tx(False)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @status_to_exception(CacheError)
    def __start_tx(self):
        conn = self.client.random_node
        return tx_start(conn, self.concurrency, self.isolation, self.timeout, self.label)

    @status_to_exception(CacheError)
    def __end_tx(self, committed):
        return tx_end(self.tx_id, committed)


class AioTransaction(_BaseTransaction):
    """
    Async thin client transaction.
    """
    def __init__(self, client, concurrency=TransactionConcurrency.PESSIMISTIC,
                 isolation=TransactionIsolation.REPEATABLE_READ, timeout=0, label=None):
        super().__init__(client, concurrency, isolation, timeout, label)

    def __await__(self):
        return (yield from self.__aenter__().__await__())

    async def commit(self) -> None:
        """
        Commit transaction.
        """
        if not self.closed:
            self.closed = True
            return await self.__end_tx(True)

    async def rollback(self) -> None:
        """
        Rollback transaction.
        """
        await self.close()

    async def close(self) -> None:
        """
        Close transaction.
        """
        if not self.closed:
            self.closed = True
            return await self.__end_tx(False)

    async def __aenter__(self):
        self.tx_id = await self.__start_tx()
        self.closed = False
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    @status_to_exception(CacheError)
    async def __start_tx(self):
        conn = await self.client.random_node()
        return await tx_start_async(conn, self.concurrency, self.isolation, self.timeout, self.label)

    @status_to_exception(CacheError)
    async def __end_tx(self, committed):
        return await tx_end_async(self.tx_id, committed)
