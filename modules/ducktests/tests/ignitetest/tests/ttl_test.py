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
Module contains TTL tests.
"""
import time

from ducktape.mark import defaults

from ignitetest.services.ignite import IgniteService
from ignitetest.services.ignite_app import IgniteApplicationService
from ignitetest.services.utils.control_utility import ControlUtility
from ignitetest.services.utils.ignite_configuration import IgniteConfiguration
from ignitetest.utils import cluster, ignite_versions
from ignitetest.utils.ignite_test import IgniteTest
from ignitetest.utils.version import IgniteVersion, DEV_BRANCH, LATEST

CACHE_NAME = "atomic-ttl"
JAVA_CLIENT_CLASS_NAME = "org.apache.ignite.internal.ducktest.tests.ttl.ScanQueryTest"


class TtlTest(IgniteTest):
    @cluster(num_nodes=14)
    @ignite_versions(str(DEV_BRANCH), str(LATEST))
    @defaults(nodes=[2], entry_count=[100_000], entry_size=[10], ttl=["touched", "accessed"], pacing=[500])
    def scan_test(self, ignite_version, entry_count, nodes=2, entry_size=10, ttl="touched", pacing=1000):
        """
        Test ScanQuery for assessed and touched ttl.
        """
        ignite = IgniteService(self.test_context, IgniteConfiguration(version=IgniteVersion(ignite_version)),
                               num_nodes=nodes, jvm_opts=["-Xmx5g", "-Xms5g"])
        ignite.start()

        control_sh = ControlUtility(ignite)
        control_sh.activate()

        client_cfg = ignite.config._replace(client_mode=True)

        client = IgniteApplicationService(
            self.test_context,
            client_cfg,
            java_class_name=JAVA_CLIENT_CLASS_NAME,
            num_nodes=1,
            params={
                "cache_name": CACHE_NAME,
                "entry_count": entry_count,
                "entry_size": entry_size,
                "pacing": pacing,
                "ttl": ttl
            }
        )

        client.start()

        time.sleep(120)

        client.stop()

        online_ignite_nodes = [n for n in control_sh.cluster_state().baseline if n.state == "ONLINE"]

        assert len(online_ignite_nodes) == nodes
