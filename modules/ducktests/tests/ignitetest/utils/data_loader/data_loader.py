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

from typing import NamedTuple

from ignitetest.services.ignite import IgniteService
from ignitetest.services.ignite_app import IgniteApplicationService
from ignitetest.services.utils.ignite_configuration.discovery import from_ignite_cluster

DEFAULT_DATA_REGION_SZ = 1 << 30


class DataLoadParams(NamedTuple):
    """
    Preload parameters
    """
    backups: int = 1
    cache_count: int = 1
    entry_count: int = 15_000
    entry_size: int = 50_000
    preloaders: int = 1
    jvm_opts: list = None

    @property
    def data_region_max_size(self):
        """
        Max size for DataRegionConfiguration.
        """
        return max(self.cache_count * self.entry_count * self.entry_size * (self.backups + 1), DEFAULT_DATA_REGION_SZ)


class DataLoader:
    def __init__(self, test_context, data_load_params: DataLoadParams):
        assert data_load_params.preloaders > 0
        assert data_load_params.cache_count > 0
        assert data_load_params.entry_count > 0
        assert data_load_params.entry_size > 0

        self.data_load_params = data_load_params
        self.test_context = test_context
        self.apps = []

    def load_data(self, ignites: IgniteService, from_key=None, to_key=None, timeout=3600):
        """
        Puts entry_count of key-value pairs of entry_size bytes to cache_count caches.
        :param ignites: Ignite cluster to load data.
        :param from_key: Insert starting this key value.
        :param to_key: Insert until this key value - last inserted entry has the (to_key - 1) key value.
        :param timeout: Timeout in seconds for application finished.
        :return: Time taken for data preloading.
        """

        config = ignites.config._replace(client_mode=True, discovery_spi=from_ignite_cluster(ignites))
        if len(self.apps) == 0:
            self.__create_apps(config)

        if not from_key:
            from_key = 0
        if not to_key:
            to_key = self.data_load_params.entry_count

        count = int((to_key - from_key) / self.data_load_params.preloaders)
        end = from_key

        for _app in self.apps:
            start = end
            end += count
            if end > to_key:
                end = to_key
            self.__start_app(_app, start, end, timeout)

        for _app in self.apps:
            _app.await_stopped()

        return (max(map(lambda _app: _app.get_finish_time(), self.apps)) -
                min(map(lambda _app: _app.get_init_time(), self.apps))).total_seconds()

    def __create_apps(self, config):
        for _ in range(self.data_load_params.preloaders):
            _app = IgniteApplicationService(
                self.test_context,
                config=config,
                java_class_name="org.apache.ignite.internal.ducktest.tests.data_generation.DataGenerationApplication",
                jvm_opts=self.data_load_params.jvm_opts
            )
            self.apps.append(_app)

    def __start_app(self, _app, _from, _to, timeout):
        _app.params = {
            "backups": self.data_load_params.backups,
            "cacheCount": self.data_load_params.cache_count,
            "entrySize": self.data_load_params.entry_size,
            "from": _from,
            "to": _to
        }
        _app.shutdown_timeout_sec = timeout
        _app.jvm_opts = self.data_load_params.jvm_opts

        _app.start_async()
