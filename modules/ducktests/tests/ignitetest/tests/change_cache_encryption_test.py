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
This module contains PME free switch tests.
"""
import multiprocessing

from ducktape.mark._mark import parametrize
from ducktape.mark.resource import cluster

from ignitetest.services.ignite import IgniteService
from ignitetest.services.ignite_app import IgniteApplicationService
from ignitetest.services.utils.control_utility import ControlUtility
from ignitetest.services.utils.ignite_configuration import IgniteConfiguration, DataStorageConfiguration
from ignitetest.services.utils.ignite_configuration.cache import CacheConfiguration
from ignitetest.services.utils.ignite_configuration.data_storage import DataRegionConfiguration
from ignitetest.services.utils.ignite_configuration.discovery import from_ignite_cluster
from ignitetest.utils.ignite_test import IgniteTest
from ignitetest.utils.version import DEV_BRANCH


class ChangeCacheEnryptionTest(IgniteTest):
    """
    Tests background encryption with and without load.
    """
    NUM_NODES = 3
    MAX_AWAIT_SEC = 900
    CACHE_NAME = "test-cache"
    THREADS = multiprocessing.cpu_count()
    DATAGEN_TIME = 99
    LOAD_TIME = 33

    @cluster(num_nodes=NUM_NODES + 2)
    @parametrize(gen_time=DATAGEN_TIME, gen_threads=THREADS, load_time = LOAD_TIME, load_threads=THREADS)
    def test_encrypt11(self, gen_time, gen_threads, load_time, load_threads):
        return self._perform_load(True, gen_time, gen_threads, load_time, load_threads)

    @cluster(num_nodes=NUM_NODES + 2)
    @parametrize(gen_time=DATAGEN_TIME, gen_threads=THREADS, load_time = LOAD_TIME, load_threads=THREADS)
    def test_idle11(self, gen_time, gen_threads, load_time, load_threads):
        return self._perform_load(False, gen_time, gen_threads, load_time, load_threads)

    @cluster(num_nodes=NUM_NODES + 2)
    @parametrize(gen_time=DATAGEN_TIME, gen_threads=THREADS / 2, load_time = LOAD_TIME, load_threads=THREADS)
    def test_encrypt21(self, gen_time, gen_threads, load_time, load_threads):
        return self._perform_load(True, gen_time, gen_threads, load_time, load_threads)

    @cluster(num_nodes=NUM_NODES + 2)
    @parametrize(gen_time=DATAGEN_TIME, gen_threads=THREADS / 2, load_time = LOAD_TIME, load_threads=THREADS)
    def test_idle21(self, gen_time, gen_threads, load_time, load_threads):
        return self._perform_load(False, gen_time, gen_threads, load_time, load_threads)

    @cluster(num_nodes=NUM_NODES + 2)
    @parametrize(gen_time=DATAGEN_TIME, gen_threads=THREADS / 2, load_time = LOAD_TIME, load_threads=THREADS / 2)
    def test_encrypt22(self, gen_time, gen_threads, load_time, load_threads):
        return self._perform_load(True, gen_time, gen_threads, load_time, load_threads)

    @cluster(num_nodes=NUM_NODES + 2)
    @parametrize(gen_time=DATAGEN_TIME, gen_threads=THREADS / 2, load_time = LOAD_TIME, load_threads=THREADS / 2)
    def test_idle22(self, gen_time, gen_threads, load_time, load_threads):
        return self._perform_load(False, gen_time, gen_threads, load_time, load_threads)

    @cluster(num_nodes=NUM_NODES + 2)
    @parametrize(gen_time=DATAGEN_TIME, gen_threads=THREADS / 4, load_time = LOAD_TIME, load_threads=THREADS / 2)
    def test_encrypt42(self, gen_time, gen_threads, load_time, load_threads):
        return self._perform_load(True, gen_time, gen_threads, load_time, load_threads)

    @cluster(num_nodes=NUM_NODES + 2)
    @parametrize(gen_time=DATAGEN_TIME, gen_threads=THREADS / 4, load_time = LOAD_TIME, load_threads=THREADS / 2)
    def test_idle42(self, gen_time, gen_threads, load_time, load_threads):
        return self._perform_load(False, gen_time, gen_threads, load_time, load_threads)

    @cluster(num_nodes=NUM_NODES + 2)
    @parametrize(gen_time=DATAGEN_TIME, gen_threads=THREADS / 4, load_time = LOAD_TIME, load_threads=THREADS / 4)
    def test_encrypt44(self, gen_time, gen_threads, load_time, load_threads):
        return self._perform_load(True, gen_time, gen_threads, load_time, load_threads)

    @cluster(num_nodes=NUM_NODES + 2)
    @parametrize(gen_time=DATAGEN_TIME, gen_threads=THREADS / 4, load_time = LOAD_TIME, load_threads=THREADS / 4)
    def test_idle44(self, gen_time, gen_threads, load_time, load_threads):
        return self._perform_load(False, gen_time, gen_threads, load_time, load_threads)

    def _perform_load(self, encrypt, generate_time, gen_threads, load_time, load_threads):
        """
        Tests TDE encryption cache key rotation latency.
        """
        data = {}

        config = IgniteConfiguration(
            cluster_state="INACTIVE",
            version=DEV_BRANCH,
            caches=[CacheConfiguration(
                name=self.CACHE_NAME,
                backups=2,
                atomicity_mode='TRANSACTIONAL',
                encryption_enabled=True)
            ],
            data_storage=DataStorageConfiguration(
                checkpoint_frequency=10 * 1000,
                default=DataRegionConfiguration(name='persistent', persistent=True)
            )
        )

        ignites = IgniteService(self.test_context, config, num_nodes=self.NUM_NODES)

        ignites.start()

        client_config = config._replace(client_mode=True,
                                        discovery_spi=from_ignite_cluster(ignites, slice(0, self.NUM_NODES - 1)))

        control_utility = ControlUtility(ignites, self.test_context)

        control_utility.activate()

        data_generator = IgniteApplicationService(self.test_context, client_config,
                                                  java_class_name="org.apache.ignite.internal.ducktest.tests.DataGenerationApplication",
                                                  params={"cache_name": self.CACHE_NAME,
                                         "duration": generate_time,
                                         "threads_count": gen_threads},
                                                  timeout_sec=self.MAX_AWAIT_SEC)

        data_generator.run()

        data["Cache size"] = data_generator.extract_result("CACHE_SIZE")

        if encrypt:
            control_utility.change_cache_key(self.CACHE_NAME)

        load_generator = IgniteApplicationService(
            self.test_context,
            client_config,
            java_class_name="org.apache.ignite.internal.ducktest.tests.LoadGeneratorApplication",
            params={"cache_name": self.CACHE_NAME, "warmup": 100, "range": 10_000_000, "threads": load_threads, "duration": load_time})

        load_generator.run()

        # time.sleep(load_time)

        if encrypt:
            ignites.await_event("Cache group reencryption is finished", self.MAX_AWAIT_SEC, from_the_beginning=True)

        # load_generator.stop()

        data["Worst latency (ms)"] = load_generator.extract_result("WORST_LATENCY")
        data["Streamed txs"] = load_generator.extract_result("STREAMED")
        data["Avg operation duration (ms)"] = load_generator.extract_result("AVG_OPERATION")

        return data