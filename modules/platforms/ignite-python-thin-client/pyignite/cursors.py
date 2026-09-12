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

"""
This module contains sync and async cursors for different types of queries.
"""

import asyncio

from pyignite.api import (
    scan, scan_cursor_get_page, resource_close, scan_async, scan_cursor_get_page_async, resource_close_async, sql,
    sql_cursor_get_page, sql_fields, sql_fields_cursor_get_page, sql_fields_cursor_get_page_async, sql_fields_async
)
from pyignite.exceptions import CacheError, SQLError


__all__ = ['ScanCursor', 'SqlCursor', 'SqlFieldsCursor', 'AioScanCursor', 'AioSqlFieldsCursor']


class BaseCursorMixin:
    @property
    def connection(self):
        """
        Ignite cluster connection.
        """
        return getattr(self, '_conn', None)

    @connection.setter
    def connection(self, value):
        setattr(self, '_conn', value)

    @property
    def cursor_id(self):
        """
        Cursor id.
        """
        return getattr(self, '_cursor_id', None)

    @cursor_id.setter
    def cursor_id(self, value):
        setattr(self, '_cursor_id', value)

    @property
    def more(self):
        """
        Whether cursor has more values.
        """
        return getattr(self, '_more', None)

    @more.setter
    def more(self, value):
        setattr(self, '_more', value)

    @property
    def cache_info(self):
        """
        Cache id.
        """
        return getattr(self, '_cache_info', None)

    @cache_info.setter
    def cache_info(self, value):
        setattr(self, '_cache_info', value)

    @property
    def client(self):
        """
        Apache Ignite client.
        """
        return getattr(self, '_client', None)

    @client.setter
    def client(self, value):
        setattr(self, '_client', value)

    @property
    def data(self):
        """
        Current fetched data.
        """
        return getattr(self, '_data', None)

    @data.setter
    def data(self, value):
        setattr(self, '_data', value)


class CursorMixin(BaseCursorMixin):
    def __enter__(self):
        return self

    def __iter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        """
        Close cursor.
        """
        if self.connection and self.cursor_id and self.more:
            resource_close(self.connection, self.cursor_id)


class AioCursorMixin(BaseCursorMixin):
    def __await__(self):
        return (yield from self.__aenter__().__await__())

    def __aiter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def close(self):
        """
        Close cursor.
        """
        if self.connection and self.cursor_id and self.more:
            await resource_close_async(self.connection, self.cursor_id)


class AbstractScanCursor:
    def __init__(self, client, cache_info, page_size, partitions, local):
        self.client = client
        self.cache_info = cache_info
        self._page_size = page_size
        self._partitions = partitions
        self._local = local

    def _finalize_init(self, result):
        if result.status != 0:
            raise CacheError(result.message)

        self.cursor_id, self.more = result.value['cursor'], result.value['more']
        self.data = iter(result.value['data'].items())

    def _process_page_response(self, result):
        if result.status != 0:
            raise CacheError(result.message)

        self.data, self.more = iter(result.value['data'].items()), result.value['more']


class ScanCursor(AbstractScanCursor, CursorMixin):
    """
    Synchronous scan cursor.
    """
    def __init__(self, client, cache_info, page_size, partitions, local):
        """
        :param client: Synchronous Apache Ignite client.
        :param cache_info: Cache meta info.
        :param page_size: page size.
        :param partitions: number of partitions to query (negative to query entire cache).
        :param local: pass True if this query should be executed on local node only.
        """
        super().__init__(client, cache_info, page_size, partitions, local)

        self.connection = self.client.random_node
        result = scan(self.connection, self.cache_info, self._page_size, self._partitions, self._local)
        self._finalize_init(result)

    def __next__(self):
        if not self.data:
            raise StopIteration

        try:
            k, v = next(self.data)
        except StopIteration:
            if self.more:
                self._process_page_response(scan_cursor_get_page(self.connection, self.cursor_id))
                k, v = next(self.data)
            else:
                raise StopIteration

        return self.client.unwrap_binary(k), self.client.unwrap_binary(v)


class AioScanCursor(AbstractScanCursor, AioCursorMixin):
    """
    Asynchronous scan query cursor.
    """
    def __init__(self, client, cache_info, page_size, partitions, local):
        """
        :param client: Asynchronous Apache Ignite client.
        :param cache_info: Cache meta info.
        :param page_size: page size.
        :param partitions: number of partitions to query (negative to query entire cache).
        :param local: pass True if this query should be executed on local node only.
        """
        super().__init__(client, cache_info, page_size, partitions, local)

    async def __aenter__(self):
        if not self.connection:
            self.connection = await self.client.random_node()
            result = await scan_async(self.connection, self.cache_info, self._page_size, self._partitions, self._local)
            self._finalize_init(result)
        return self

    async def __anext__(self):
        if not self.connection:
            raise CacheError("Using uninitialized cursor, initialize it using async with expression.")

        if not self.data:
            raise StopAsyncIteration

        try:
            k, v = next(self.data)
        except StopIteration:
            if self.more:
                self._process_page_response(await scan_cursor_get_page_async(self.connection, self.cursor_id))
                try:
                    k, v = next(self.data)
                except StopIteration:
                    raise StopAsyncIteration
            else:
                raise StopAsyncIteration

        return await asyncio.gather(
            *[self.client.unwrap_binary(k), self.client.unwrap_binary(v)]
        )


class SqlCursor(CursorMixin):
    """
    Synchronous SQL query cursor.
    """
    def __init__(self, client, cache_info, *args, **kwargs):
        """
        :param client: Synchronous Apache Ignite client.
        :param cache_info: Cache meta info.
        """
        self.client = client
        self.cache_info = cache_info
        self.connection = self.client.random_node
        result = sql(self.connection, self.cache_info, *args, **kwargs)
        if result.status != 0:
            raise SQLError(result.message)

        self.cursor_id, self.more = result.value['cursor'], result.value['more']
        self.data = iter(result.value['data'].items())

    def __next__(self):
        if not self.data:
            raise StopIteration

        try:
            k, v = next(self.data)
        except StopIteration:
            if self.more:
                result = sql_cursor_get_page(self.connection, self.cursor_id)
                if result.status != 0:
                    raise SQLError(result.message)
                self.data, self.more = iter(result.value['data'].items()), result.value['more']

                k, v = next(self.data)
            else:
                raise StopIteration

        return self.client.unwrap_binary(k), self.client.unwrap_binary(v)


class AbstractSqlFieldsCursor:
    def __init__(self, client, cache_info):
        self.client = client
        self.cache_info = cache_info

    def _finalize_init(self, result):
        if result.status != 0:
            raise SQLError(result.message)

        self.cursor_id, self.more = result.value['cursor'], result.value['more']
        self.data = iter(result.value['data'])
        self._field_names = result.value.get('fields', None)
        if self._field_names:
            self._field_count = len(self._field_names)
        else:
            self._field_count = result.value['field_count']


class SqlFieldsCursor(AbstractSqlFieldsCursor, CursorMixin):
    """
    Synchronous SQL fields query cursor.
    """
    def __init__(self, client, cache_info, *args, **kwargs):
        """
        :param client: Synchronous Apache Ignite client.
        :param cache_info: Cache meta info.
        """
        super().__init__(client, cache_info)
        self.connection = self.client.random_node
        self._finalize_init(sql_fields(self.connection, self.cache_info, *args, **kwargs))

    def __next__(self):
        if not self.data:
            raise StopIteration

        if self._field_names:
            result = self._field_names
            self._field_names = None
            return result

        try:
            row = next(self.data)
        except StopIteration:
            if self.more:
                result = sql_fields_cursor_get_page(self.connection, self.cursor_id, self._field_count)
                if result.status != 0:
                    raise SQLError(result.message)

                self.data, self.more = iter(result.value['data']), result.value['more']

                row = next(self.data)
            else:
                raise StopIteration

        return [self.client.unwrap_binary(v) for v in row]


class AioSqlFieldsCursor(AbstractSqlFieldsCursor, AioCursorMixin):
    """
    Asynchronous SQL fields query cursor.
    """
    def __init__(self, client, cache_info, *args, **kwargs):
        """
        :param client: Synchronous Apache Ignite client.
        :param cache_info: Cache meta info.
        """
        super().__init__(client, cache_info)
        self._params = (args, kwargs)

    async def __aenter__(self):
        await self._initialize(*self._params[0], *self._params[1])
        return self

    async def __anext__(self):
        if not self.connection:
            raise SQLError("Attempting to use uninitialized aio cursor, please await on it or use with expression.")

        if not self.data:
            raise StopAsyncIteration

        if self._field_names:
            result = self._field_names
            self._field_names = None
            return result

        try:
            row = next(self.data)
        except StopIteration:
            if self.more:
                result = await sql_fields_cursor_get_page_async(self.connection, self.cursor_id, self._field_count)
                if result.status != 0:
                    raise SQLError(result.message)

                self.data, self.more = iter(result.value['data']), result.value['more']
                try:
                    row = next(self.data)
                except StopIteration:
                    raise StopAsyncIteration
            else:
                raise StopAsyncIteration

        return await asyncio.gather(*[self.client.unwrap_binary(v) for v in row])

    async def _initialize(self, *args, **kwargs):
        if self.connection and self.cursor_id:
            return

        self.connection = await self.client.random_node()
        self._finalize_init(await sql_fields_async(self.connection, self.cache_info, *args, **kwargs))
