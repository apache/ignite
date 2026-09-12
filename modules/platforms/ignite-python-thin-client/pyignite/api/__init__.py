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
This module contains functions, that are (more or less) directly mapped to
Apache Ignite binary protocol operations. Read more:

https://ignite.apache.org/docs/latest/binary-client-protocol/binary-client-protocol#client-operations

When the binary client protocol changes, these functions also change. For
stable end user API see :mod:`pyignite.client` module.
"""

# flake8: noqa

from .affinity import (
    cache_get_node_partitions, cache_get_node_partitions_async,
)
from .cache_config import (
    cache_create, cache_create_async,
    cache_get_names, cache_get_names_async,
    cache_get_or_create, cache_get_or_create_async,
    cache_destroy, cache_destroy_async,
    cache_get_configuration, cache_get_configuration_async,
    cache_create_with_config, cache_create_with_config_async,
    cache_get_or_create_with_config, cache_get_or_create_with_config_async,
)
from .key_value import (
    cache_get, cache_get_async,
    cache_put, cache_put_async,
    cache_get_all, cache_get_all_async,
    cache_put_all, cache_put_all_async,
    cache_contains_key, cache_contains_key_async,
    cache_contains_keys, cache_contains_keys_async,
    cache_get_and_put, cache_get_and_put_async,
    cache_get_and_replace, cache_get_and_replace_async,
    cache_get_and_remove, cache_get_and_remove_async,
    cache_put_if_absent, cache_put_if_absent_async,
    cache_get_and_put_if_absent, cache_get_and_put_if_absent_async,
    cache_replace, cache_replace_async,
    cache_replace_if_equals, cache_replace_if_equals_async,
    cache_clear, cache_clear_async,
    cache_clear_key, cache_clear_key_async,
    cache_clear_keys, cache_clear_keys_async,
    cache_remove_key, cache_remove_key_async,
    cache_remove_if_equals, cache_remove_if_equals_async,
    cache_remove_keys, cache_remove_keys_async,
    cache_remove_all, cache_remove_all_async,
    cache_get_size, cache_get_size_async,
    cache_local_peek, cache_local_peek_async,
)
from .sql import (
    scan, scan_async,
    scan_cursor_get_page, scan_cursor_get_page_async,
    sql,
    sql_cursor_get_page,
    sql_fields, sql_fields_async,
    sql_fields_cursor_get_page, sql_fields_cursor_get_page_async,
    resource_close, resource_close_async
)
from .binary import (
    get_binary_type, get_binary_type_async,
    put_binary_type, put_binary_type_async
)
from .result import APIResult
