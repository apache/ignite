#
# Copyright 2019 GridGain Systems, Inc. and Contributors.
#
# Licensed under the GridGain Community Edition License (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""
This module contains functions, that are (more or less) directly mapped to
GridGain binary protocol operations. Read more:

https://apacheignite.readme.io/docs/binary-client-protocol#section-client-operations

When the binary client protocol changes, these functions also change. For
stable end user API see :mod:`pygridgain.client` module.
"""

from .affinity import (
    cache_get_node_partitions,
)
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
    cache_local_peek,
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
