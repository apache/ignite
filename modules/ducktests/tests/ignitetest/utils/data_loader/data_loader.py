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
import threading
import uuid
from statistics import mean, median
from typing import NamedTuple

from ignitetest.services.ignite import IgniteService
from ignitetest.services.ignite_app import IgniteApplicationService
from ignitetest.services.utils.ignite_configuration.discovery import from_ignite_cluster
from ignitetest.services.utils.ignite_configuration.data_storage import DEFAULT_MIN_DATA_REGION_SIZE

MAX_DATA_REGION_SIZE_KEY = "max_data_region_size"


class DataLoadParams(NamedTuple):
    """
    Preload parameters
    """
    backups: int = 1
    cache_count: int = 1
    cache_name_template: str = None
    cache_names: list = None
    entry_count: int = 15_000
    entry_size: int = 50_000
    preloaders: int = 1
    threads: int = 1
    jvm_opts: list = None

    @property
    def data_size(self):
        """
        Max size for DataRegionConfiguration.
        """
        return int(self.cache_count * self.entry_count * self.entry_size * (self.backups + 1) * 1.5)


class DataLoader:
    def __init__(self, test_context, data_load_params: DataLoadParams):
        assert data_load_params.preloaders > 0
        assert data_load_params.cache_count > 0 or len(data_load_params.cache_names) > 0
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

        token = str(uuid.uuid4())
        workers = []
        for _app in self.apps:
            start = end
            end += count
            if end > to_key:
                end = to_key
            # self.__start_app(_app, start, end, token, timeout)
            worker = threading.Thread(
                name="preloader-starter-worker-" + str(start),
                target=self.__start_app,
                args=(_app, start, end, token, timeout)
            )
            worker.daemon = True
            worker.start()

            workers.append(worker)

        for worker in workers:
            worker.join()

        for _app in self.apps:
            _app.await_stopped()

        return (max(map(lambda _app: _app.get_finish_time(), self.apps)) -
                min(map(lambda _app: _app.get_init_time(), self.apps))).total_seconds()

    def free(self):
        for _app in self.apps:
            _app.free()

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

    def __start_app(self, _app, _from, _to, token, timeout):
        _app.params = {
            "backups": self.data_load_params.backups,
            "cacheCount": self.data_load_params.cache_count,
            "cacheNameTemplate": self.data_load_params.cache_name_template,
            "cacheNames": self.data_load_params.cache_names,
            "entrySize": self.data_load_params.entry_size,
            "threads": self.data_load_params.threads,
            "preloaders": self.data_load_params.preloaders,
            "preloadersToken": token,
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


def data_region_size(test, required_size):
    return min(
        max(required_size, DEFAULT_MIN_DATA_REGION_SIZE),
        test._global_int(MAX_DATA_REGION_SIZE_KEY, 1 << 30)
    )

