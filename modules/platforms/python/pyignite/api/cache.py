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

from typing import Any, Iterable, Optional, Union

from pyignite.connection import Connection
from pyignite.datatypes import prop_codes
from pyignite.exceptions import (
    CacheCreationError, CacheError, ParameterError, SQLError,
)
from pyignite.utils import (
    cache_id, is_wrapped, status_to_exception, unwrap_binary,
)
from .cache_config import (
    cache_create, cache_create_with_config,
    cache_get_or_create, cache_get_or_create_with_config,
    cache_destroy, cache_get_configuration,
)
from .key_value import (
    cache_get, cache_put, cache_get_all, cache_put_all, cache_replace,
    cache_clear, cache_clear_key, cache_clear_keys,
    cache_contains_key, cache_contains_keys,
    cache_get_and_put, cache_get_and_put_if_absent, cache_put_if_absent,
    cache_get_and_remove, cache_get_and_replace,
    cache_remove_key, cache_remove_keys, cache_remove_all,
    cache_remove_if_equals, cache_replace_if_equals, cache_get_size,
)
from .sql import scan, scan_cursor_get_page, sql, sql_cursor_get_page


PROP_CODES = set([
    getattr(prop_codes, x)
    for x in dir(prop_codes)
    if x.startswith('PROP_')
])
CACHE_CREATE_FUNCS = {
    True: {
        True: cache_get_or_create_with_config,
        False: cache_create_with_config,
    },
    False: {
        True: cache_get_or_create,
        False: cache_create,
    },
}


class Cache:
    """
    Ignite cache abstraction. Can be obtained by calling
    `Connection.create_cache` or `Connection.get_or_create_cache` methods.
    """
    _cache_id = None
    _name = None
    _conn = None
    _settings = None

    @staticmethod
    def validate_settings(settings: Union[str, dict]=None):
        if any([
            not settings,
            type(settings) not in (str, dict),
            type(settings) is dict and prop_codes.PROP_NAME not in settings,
        ]):
            raise ParameterError('You should supply at least cache name')

        if all([
            type(settings) is dict,
            not set(settings).issubset(PROP_CODES),
        ]):
            raise ParameterError('One or more settings was not recognized')

    def __init__(
        self, conn: Connection, settings: Union[str, dict]=None,
        with_get: bool=False
    ):
        self._conn = conn
        self.validate_settings(settings)
        if type(settings) == str:
            self._name = settings
        else:
            self._name = settings[prop_codes.PROP_NAME]

        func = CACHE_CREATE_FUNCS[type(settings) is dict][with_get]
        result = func(conn, settings)
        if result.status != 0:
            raise CacheCreationError(result.message)

        self._cache_id = cache_id(self._name)

    @property
    def settings(self) -> Optional[dict]:
        if self._settings is None:
            config_result = cache_get_configuration(self._conn, self._cache_id)
            self._settings = config_result.value

        return self._settings

    @property
    def name(self) -> str:
        if self._name is None:
            self._name = self.settings[prop_codes.PROP_NAME]

        return self._name

    @property
    def conn(self) -> Connection:
        return self._conn

    @property
    def cache_id(self) -> int:
        return self._cache_id

    def process_binary(self, value: Any) -> Any:
        """
        Detects and recursively unwraps Binary Object.

        :param value: anything that could be a Binary Object,
        :return: the result of the Binary Object unwrapping with all other data
         left intact.
        """
        if is_wrapped(value):
            return unwrap_binary(self._conn, value)
        return value

    @status_to_exception(CacheError)
    def destroy(self):
        return cache_destroy(self._conn, self._cache_id)

    @status_to_exception(CacheError)
    def get(self, key, key_hint: object=None) -> Any:
        result = cache_get(self._conn, self._cache_id, key, key_hint=key_hint)
        result.value = self.process_binary(result.value)
        return result

    @status_to_exception(CacheError)
    def put(self, key, value, key_hint: object=None, value_hint: object=None):
        return cache_put(
            self._conn, self._cache_id, key, value,
            key_hint=key_hint, value_hint=value_hint
        )

    @status_to_exception(CacheError)
    def get_all(self, keys: list):
        result = cache_get_all(self._conn, self._cache_id, keys)
        if result.value:
            for key, value in result.value.items():
                result.value[key] = self.process_binary(value)
        return result

    @status_to_exception(CacheError)
    def put_all(self, pairs: dict):
        return cache_put_all(self._conn, self._cache_id, pairs)

    @status_to_exception(CacheError)
    def replace(
        self, key, value, key_hint: object=None, value_hint: object=None
    ):
        result = cache_replace(
            self._conn, self._cache_id, key, value,
            key_hint=key_hint, value_hint=value_hint
        )
        result.value = self.process_binary(result.value)
        return result

    @status_to_exception(CacheError)
    def clear(self, keys: Optional[list]=None):
        if keys:
            return cache_clear_keys(self._conn, self._cache_id, keys)
        else:
            return cache_clear(self._conn, self._cache_id)

    @status_to_exception(CacheError)
    def clear_key(self, key, key_hint: object=None):
        return cache_clear_key(
            self._conn, self._cache_id, key, key_hint=key_hint
        )

    @status_to_exception(CacheError)
    def contains_key(self, key, key_hint=None):
        return cache_contains_key(
            self._conn, self._cache_id, key, key_hint=key_hint
        )

    @status_to_exception(CacheError)
    def contains_keys(self, keys: Iterable):
        return cache_contains_keys(self._conn, self._cache_id, keys)

    @status_to_exception(CacheError)
    def get_and_put(self, key, value, key_hint=None, value_hint=None):
        result = cache_get_and_put(
            self._conn, self._cache_id, key, value, key_hint, value_hint
        )
        result.value = self.process_binary(result.value)
        return result

    @status_to_exception(CacheError)
    def get_and_put_if_absent(
        self, key, value, key_hint=None, value_hint=None
    ):
        result = cache_get_and_put_if_absent(
            self._conn, self._cache_id, key, value, key_hint, value_hint
        )
        result.value = self.process_binary(result.value)
        return result

    @status_to_exception(CacheError)
    def put_if_absent(self, key, value, key_hint=None, value_hint=None):
        return cache_put_if_absent(
            self._conn, self._cache_id, key, value, key_hint, value_hint
        )

    @status_to_exception(CacheError)
    def get_and_remove(self, key, key_hint=None):
        result = cache_get_and_remove(
            self._conn, self._cache_id, key, key_hint
        )
        result.value = self.process_binary(result.value)
        return result

    @status_to_exception(CacheError)
    def get_and_replace(self, key, value, key_hint=None, value_hint=None):
        result = cache_get_and_replace(
            self._conn, self._cache_id, key, value, key_hint, value_hint
        )
        result.value = self.process_binary(result.value)
        return result

    @status_to_exception(CacheError)
    def remove_key(self, key, key_hint=None):
        return cache_remove_key(self._conn, self._cache_id, key, key_hint)

    @status_to_exception(CacheError)
    def remove_keys(self, keys):
        return cache_remove_keys(self._conn, self._cache_id, keys)

    @status_to_exception(CacheError)
    def remove_all(self):
        return cache_remove_all(self._conn, self._cache_id)

    @status_to_exception(CacheError)
    def remove_if_equals(self, key, sample, key_hint=None, sample_hint=None):
        return cache_remove_if_equals(
            self._conn, self._cache_id, key, sample, key_hint, sample_hint
        )

    @status_to_exception(CacheError)
    def replace_if_equals(
        self, key, sample, value,
        key_hint=None, sample_hint=None, value_hint=None
    ):
        return cache_replace_if_equals(
            self._conn, self._cache_id, key, sample, value,
            key_hint, sample_hint, value_hint
        )

    @status_to_exception(CacheError)
    def get_size(self, peek_modes=0):
        return cache_get_size(self._conn, self._cache_id, peek_modes)

    def scan(self, page_size: int=1, partitions: int=-1, local: bool=False):
        """
        Returns all key-value pairs from the cache, similar to `get_all`, but
        with internal pagination, which is slower, but safer.

        :param page_size: (optional) page size. Default size is 1 (slowest
         and safest),
        :param partitions: (optional) number of partitions to query
         (negative to query entire cache),
        :param local: (optional) pass True if this query should be executed
         on local node only. Defaults to False,
        :return: generator with key-value pairs.
        """
        result = scan(self._conn, self._cache_id, page_size, partitions, local)
        if result.status != 0:
            raise CacheError(result.message)

        cursor = result.value['cursor']
        for k, v in result.value['data'].items():
            yield k, v

        while result.value['more']:
            result = scan_cursor_get_page(self._conn, cursor)
            if result.status != 0:
                raise CacheError(result.message)

            for k, v in result.value['data'].items():
                yield k, v

    def sql(
        self, query_str: str, page_size: int=1,
        query_args: Optional[list]=None, distributed_joins: bool=False,
        replicated_only: bool=False, local: bool=False, timeout: int=0
    ):
        """
        Executes a simplified SQL SELECT query over data stored in the cache.
        The query returns the whole record (key and value).

        :param query_str: SQL query string,
        :param page_size: cursor page size,
        :param query_args: (optional) query arguments,
        :param distributed_joins: (optional) distributed joins. Defaults
         to False,
        :param replicated_only: (optional) whether query contains only
         replicated tables or not. Defaults to False,
        :param local: (optional) pass True if this query should be executed
         on local node only. Defaults to False,
        :param timeout: (optional) non-negative timeout value in ms. Zero
         disables timeout (default),
        :return: generator with key-value pairs.
        """
        def generate_result(value):
            cursor = value['cursor']
            more = value['more']
            for k, v in value['data'].items():
                k = self.process_binary(k)
                v = self.process_binary(v)
                yield k, v

            while more:
                inner_result = sql_cursor_get_page(self._conn, cursor)
                if result.status != 0:
                    raise SQLError(result.message)
                more = inner_result.value['more']
                for k, v in inner_result.value['data'].items():
                    k = self.process_binary(k)
                    v = self.process_binary(v)
                    yield k, v

        type_name = self.settings[
            prop_codes.PROP_QUERY_ENTITIES
        ][0]['value_type_name']
        if not type_name:
            raise SQLError('Value type is unknown')
        result = sql(
            self._conn,
            self._cache_id,
            type_name,
            query_str,
            page_size,
            query_args,
            distributed_joins,
            replicated_only,
            local,
            timeout
        )
        if result.status != 0:
            raise SQLError(result.message)

        return generate_result(result.value)
