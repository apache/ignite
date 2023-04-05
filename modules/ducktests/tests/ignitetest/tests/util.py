# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Common utils.
"""
import time
from typing import NamedTuple

from ignitetest.services.ignite_app import IgniteApplicationService

DEFAULT_DATA_REGION_SZ = 1 << 30


class DataGenerationParams(NamedTuple):
    """
    Data generation parameters.
    See org.apache.ignite.internal.ducktest.tests.DataGenerationApplication in java code.
    """
    backups: int = 1
    cache_count: int = 1
    entry_count: int = 15_000
    entry_size: int = 50_000
    preloaders: int = 1
    index_count: int = 0
    transactional: bool = False

    @property
    def data_region_max_size(self):
        """
        Max size for DataRegionConfiguration.
        """
        return max(self.cache_count * self.entry_count * self.entry_size * (self.backups + 1), DEFAULT_DATA_REGION_SZ) \


    @property
    def entry_count_per_preloader(self):
        """
        Entry count per preloader.
        """
        return int(self.entry_count / self.preloaders)


def load_data(context, config, data_gen_params: DataGenerationParams, timeout=3600):
    """
    Puts entry_count of key-value pairs of entry_size bytes to cache_count caches.
    :param context: Test context.
    :param config: Ignite configuration.
    :param data_gen_params: Data generation parameters.
    :param timeout: Timeout in seconds for application finished.
    :return: Load applications.
    """
    assert data_gen_params.preloaders > 0
    assert data_gen_params.cache_count > 0
    assert data_gen_params.entry_count > 0
    assert data_gen_params.entry_size > 0

    apps = []

    def start_app(_from, _to):
        app = IgniteApplicationService(
            context,
            config=config,
            java_class_name="org.apache.ignite.internal.ducktest.tests.DataGenerationApplication",
            params={
                "backups": data_gen_params.backups,
                "cacheCount": data_gen_params.cache_count,
                "entrySize": data_gen_params.entry_size,
                "from": _from,
                "to": _to,
                "indexCount": data_gen_params.index_count,
                "transactional": data_gen_params.transactional
            },
            shutdown_timeout_sec=timeout)
        app.start()

        apps.append(app)

    count = data_gen_params.entry_count_per_preloader
    end = 0

    for _ in range(data_gen_params.preloaders - 1):
        start = end
        end += count
        start_app(start, end)

    start_app(end, data_gen_params.entry_count)

    return apps


def preload_data(context, config, data_gen_params: DataGenerationParams, timeout=3600):
    """
    Puts entry_count of key-value pairs of entry_size bytes to cache_count caches and awaits the load finished.
    :param context: Test context.
    :param config: Ignite configuration.
    :param data_gen_params: Data generation parameters.
    :param timeout: Timeout in seconds for application finished.
    :return: Time taken for data preloading.
    """

    apps = load_data(context, config, data_gen_params, timeout)

    for app in apps:
        app.await_stopped()

    return (max(map(lambda app: app.get_finish_time(), apps)) -
            min(map(lambda app: app.get_init_time(), apps))).total_seconds()


def current_millis():
    return round(time.time() * 1000)
