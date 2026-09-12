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

from typing import Iterable, Union

from pyignite.connection import AioConnection, Connection
from pyignite.datatypes import Bool, Int, Long, UUIDObject
from pyignite.datatypes.internal import StructArray, Conditional, Struct
from pyignite.queries import Query, query_perform
from pyignite.queries.op_codes import OP_CACHE_PARTITIONS
from pyignite.utils import is_iterable
from .result import APIResult


cache_ids = StructArray([
    ('cache_id', Int),
])

cache_config = StructArray([
    ('key_type_id', Int),
    ('affinity_key_field_id', Int),
])

node_partitions = StructArray([
    ('partition_id', Int),
])

node_mapping = StructArray([
    ('node_uuid', UUIDObject),
    ('node_partitions', node_partitions)
])

cache_mapping = StructArray([
    ('cache_id', Int),
    ('cache_config', cache_config),
])

empty_cache_mapping = StructArray([
    ('cache_id', Int)
])

empty_node_mapping = Struct([])

partition_mapping = StructArray([
    ('is_applicable', Bool),

    ('cache_mapping', Conditional(['is_applicable'],
                                  lambda ctx: ctx['is_applicable'] and ctx['is_applicable'].value == 1,
                                  lambda ctx: ctx['is_applicable'],
                                  cache_mapping, empty_cache_mapping)),

    ('node_mapping', Conditional(['is_applicable'],
                                 lambda ctx: ctx['is_applicable'] and ctx['is_applicable'].value == 1,
                                 lambda ctx: ctx['is_applicable'],
                                 node_mapping, empty_node_mapping)),
])


def cache_get_node_partitions(conn: 'Connection', caches: Union[int, Iterable[int]]) -> APIResult:
    """
    Gets partition mapping for an Ignite cache or a number of caches. See
    “IEP-23: Best Effort Affinity for thin clients”.

    :param conn: connection to Ignite server,
    :param caches: cache ID(s) the mapping is provided for
    :return: API result data object.
    """
    return __cache_get_node_partitions(conn, caches)


async def cache_get_node_partitions_async(conn: 'AioConnection', caches: Union[int, Iterable[int]]) -> APIResult:
    """
    Async version of cache_get_node_partitions.
    """
    return await __cache_get_node_partitions(conn, caches)


def __post_process_partitions(result):
    if result.status == 0:
        # tidying up the result
        value = {
            'version': (
                result.value['version_major'],
                result.value['version_minor']
            ),
            'partition_mapping': {},
        }
        for partition_map in result.value['partition_mapping']:
            is_applicable = partition_map['is_applicable']

            node_mapping = None
            if is_applicable:
                node_mapping = {
                    p['node_uuid']: set(x['partition_id'] for x in p['node_partitions'])
                    for p in partition_map['node_mapping']
                }

            for cache_info in partition_map['cache_mapping']:
                cache_id = cache_info['cache_id']

                cache_partition_mapping = {
                    'is_applicable': is_applicable,
                }

                parts = 0
                if is_applicable:
                    cache_partition_mapping['cache_config'] = {
                        a['key_type_id']: a['affinity_key_field_id']
                        for a in cache_info['cache_config']
                    }
                    cache_partition_mapping['node_mapping'] = node_mapping

                    parts = sum(len(p) for p in cache_partition_mapping['node_mapping'].values())

                cache_partition_mapping['number_of_partitions'] = parts

                value['partition_mapping'][cache_id] = cache_partition_mapping
        result.value = value
    return result


def __cache_get_node_partitions(conn, caches):
    query_struct = Query(
        OP_CACHE_PARTITIONS,
        [
            ('cache_ids', cache_ids),
        ]
    )
    if not is_iterable(caches):
        caches = [caches]

    return query_perform(
        query_struct,
        conn,
        query_params={
            'cache_ids': [{'cache_id': cache} for cache in caches],
        },
        response_config=[
            ('version_major', Long),
            ('version_minor', Int),
            ('partition_mapping', partition_mapping),
        ],
        post_process_fun=__post_process_partitions
    )
