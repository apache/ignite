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
This module contains classes and functions, that let you communicate
with Apache Ignite cluster node by the means of Ignite binary client protocol.

To start the communication, you may connect to the node of their choice
by instantiating the `Connection` object with proper connection parameters,
like host name, and port number. Connection wraps TCP socket handling,
as well as Ignite protocol handshaking.

The whole storage room of Ignite cluster is split up onto named structures,
called caches. You may access the particular cache, creating the hash code
of its name with `hashcode()` function.

Apache Ignite binary API contains a rich set of functions to control
the lifecycle of caches. These functions resides in the `cache_config` module.

On successful connection, you may query the data on Ignite server, using
either key-value subset of functions, in the way similar to Redis/memcached
APIs, or SQL subset of functions, or both. Those are `key_value` and `sql`
modules for you.
"""

from .cache_config import (
    cache_create,
    cache_get_names,
    cache_get_or_create,
    cache_destroy,
    cache_get_configuration,
    cache_create_with_config,
    cache_get_or_create_with_config,
)
from .key_value import (
    cache_get,
    cache_put,
    cache_get_all,
    cache_put_all,
    cache_contains_key,
    cache_contains_keys,
    cache_get_and_put,
    cache_get_and_replace,
    cache_get_and_remove,
    cache_put_if_absent,
    cache_get_and_put_if_absent,
    cache_replace,
    cache_replace_if_equals,
    cache_clear,
    cache_clear_key,
    cache_clear_keys,
    cache_remove_key,
    cache_remove_if_equals,
    cache_remove_keys,
    cache_remove_all,
    cache_get_size,
)
from .sql import (
    scan,
    scan_cursor_get_page,
    sql,
    sql_cursor_get_page,
    sql_fields,
    sql_fields_cursor_get_page,
    resource_close,
)
from .binary import (
    get_binary_type,
    put_binary_type,
)
from .result import APIResult
