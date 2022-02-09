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

from statistics import mean, median
from typing import NamedTuple

from ignitetest.services.ignite import IgniteService
from ignitetest.services.ignite_app import IgniteApplicationService
from ignitetest.services.utils.ignite_configuration import IgniteConfiguration, DataStorageConfiguration
from ignitetest.services.utils.ignite_configuration.data_storage import DataRegionConfiguration
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
    threads: int = 1
    jvm_opts: list = None
    persistent: bool = True

    def data_region_max_size(self, node_count):
        """
        Max size for DataRegionConfiguration.
        """
        return int(max(self.cache_count * self.entry_count * (self.entry_size / 0.6) * (self.backups + 1) / node_count,
                       DEFAULT_DATA_REGION_SZ))


class DataLoader:
    def __init__(self, test_context, data_load_params: DataLoadParams):
        assert data_load_params.preloaders > 0
        assert data_load_params.cache_count > 0
        assert data_load_params.entry_count > 0
        assert data_load_params.entry_size > 0

        self.data_load_params = data_load_params
        self.test_context = test_context
        self.apps = []

    def start_ignite(self, config: IgniteConfiguration, node_count=None, jvm_opts=None) -> IgniteService:
        """
        Start IgniteService:

        :param config: Ignite configuration
        :param node_count: Number of nodes to start. If not passed start on all nodes minus number of preloaders
        :param jvm_opts:
        :return: IgniteService.
        """

        if node_count is None:
            node_count = len(self.test_context.cluster) - self.data_load_params.preloaders

        if self.data_load_params.persistent:
            data_storage = DataStorageConfiguration(
                checkpoint_threads=self.data_load_params.threads,
                default=DataRegionConfiguration(
                    persistent=True,
                    max_size=self.data_load_params.data_region_max_size(node_count),
                    init_size=self.data_load_params.data_region_max_size(node_count)
                )
            )
        else:
            data_storage = DataStorageConfiguration(
                default=DataRegionConfiguration(max_size=self.data_load_params.data_region_max_size(node_count))
            )

        node_config = config._replace(data_storage=data_storage)

        ignites = IgniteService(self.test_context, config=node_config,
                                num_nodes=node_count,
                                jvm_opts=jvm_opts)
        ignites.start()

        return ignites

    def load_data(self, ignites: IgniteService, from_key=None, to_key=None, timeout=3600):
        """
        Puts entry_count of key-value pairs of entry_size bytes to cache_count caches.
        :param ignites: Ignite cluster to load data.
        :param from_key: Insert starting this key value.
        :param to_key: Insert until this key value - last inserted entry has the (to_key - 1) key value.
        :param timeout: Timeout in seconds for application finished.
        :return: Time taken for data preloading.
        """

        config = ignites.config._replace(
            client_mode=True,
            discovery_spi=from_ignite_cluster(ignites),
            data_streamer_thread_pool_size=self.data_load_params.threads)
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

    def get_summary_report(self):
        assert len(self.apps) > 0
        heap_values = list(map(lambda _app: int(_app.extract_result("PEAK_HEAP_MEMORY")), self.apps))
        duration_values = list(map(lambda _app: (_app.get_finish_time() - _app.get_init_time()).total_seconds(),
                                   self.apps))
        return {
            "duration": {
                "max": max(duration_values),
                "min": min(duration_values),
                "total": (max(map(lambda _app: _app.get_finish_time(), self.apps)) -
                          min(map(lambda _app: _app.get_init_time(), self.apps))).total_seconds(),
                "mean": mean(duration_values),
                "median": median(duration_values)

            },
            "heap": {
                "max": sizeof_fmt(max(heap_values)),
                "min": sizeof_fmt(min(heap_values)),
                "mean": sizeof_fmt(mean(heap_values)),
                "median": sizeof_fmt(median(heap_values))
            }
        }

    def __create_apps(self, config):
        for _ in range(self.data_load_params.preloaders):
            _app = IgniteApplicationService(
                self.test_context,
                config=config,
                java_class_name="org.apache.ignite.internal.ducktest.tests.data_generation.DataGenerationApplication",
                jvm_opts=self.data_load_params.jvm_opts
            )
            _app.log_level = "DEBUG"
            self.apps.append(_app)

    def __start_app(self, _app, _from, _to, timeout):
        _app.params = {
            "backups": self.data_load_params.backups,
            "cacheCount": self.data_load_params.cache_count,
            "entrySize": self.data_load_params.entry_size,
            "threads": self.data_load_params.threads,
            "preloaders": self.data_load_params.preloaders,
            "timeoutSecs": timeout,
            "from": _from,
            "to": _to
        }
        _app.shutdown_timeout_sec = timeout
        _app.jvm_opts = self.data_load_params.jvm_opts

        _app.start_async()


def sizeof_fmt(num, suffix="B"):
    for unit in ["", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"]:
        if abs(num) < 1024.0:
            return f"{num:3.1f}{unit}{suffix}"
        num /= 1024.0
    return f"{num:.1f}Yi{suffix}"
