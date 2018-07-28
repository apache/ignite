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

from typing import Any, Type, Optional, Union

from pyignite.connection import Connection
from pyignite.datatypes import prop_codes
from pyignite.exceptions import (
    CacheCreationError, CacheError, ParameterError,
)
from pyignite.utils import cache_id
from .cache_config import (
    cache_create, cache_create_with_config,
    cache_get_or_create, cache_get_or_create_with_config,
    cache_destroy,
)
from .key_value import (
    cache_get, cache_put, cache_get_all, cache_put_all, cache_replace,
)


PROP_CODES = set([getattr(prop_codes, x) for x in dir(prop_codes)])
CACHE_CREATE_FUNCS = {
    True: {
        True: cache_get_or_create,
        False: cache_create,
    },
    False: {
        True: cache_get_or_create_with_config,
        False: cache_create_with_config,
    },
}


def status_to_exception(exc: Type[Exception]):
    """
    Converts erroneous status code with error message to an exception
    of the given class.

    :param exc: the class of exception to raise,
    :return: decorator.
    """
    def ste_decorator(fn):
        def ste_wrapper(*args, **kwargs):
            result = fn(*args, **kwargs)
            if result.status != 0:
                raise exc(result.message)
            return result.value
        return ste_wrapper
    return ste_decorator


class Cache:
    """
    Ignite cache abstraction. Can be obtained by calling
    `Connection.create_cache` or `Connection.get_or_create_cache` methods.
    """
    _cache_id = None
    _name = None
    _conn = None

    @staticmethod
    def validate_settings(settings: Union[str, dict]=None):
        if any([
            not settings,
            type(settings) not in (str, dict),
            type(settings) is dict and prop_codes.PROP_NAME not in settings,
        ]):
            raise ParameterError('You should supply at least cache name')

        if not set(settings).issubset(PROP_CODES):
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
            self._settings = settings

        func = CACHE_CREATE_FUNCS[type(settings) is dict][with_get]
        result = func(conn, settings)
        if result.status != 0:
            raise CacheCreationError(result.message)

        self._cache_id = cache_id(self._name)

    @property
    def settings(self) -> Optional[dict]:
        return self._settings

    @property
    def name(self) -> str:
        return self._name

    @property
    def cache_id(self) -> int:
        return self._cache_id

    @status_to_exception(CacheError)
    def destroy(self):
        return cache_destroy(self._conn, self._cache_id)

    def get(self, key, key_hint: object=None) -> Any:
        result = cache_get(self._conn, self._cache_id, key, key_hint=key_hint)
        return result.value

    @status_to_exception(CacheError)
    def put(self, key, value, key_hint: object=None, value_hint: object=None):
        return cache_put(
            self._conn, self._cache_id, key, value,
            key_hint=key_hint, value_hint=value_hint
        )

    @status_to_exception(CacheError)
    def get_all(self, keys: list):
        return cache_get_all(self._conn, self._cache_id, keys)

    @status_to_exception(CacheError)
    def put_all(self, pairs: dict):
        return cache_put_all(self._conn, self._cache_id, pairs)

    @status_to_exception(CacheError)
    def replace(self, key, value, key_hint=None, value_hint=None):
        return cache_replace(
            self._conn, self._cache_id, key, value,
            key_hint=key_hint, value_hint=value_hint
        )
