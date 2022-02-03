#  Licensed to the Apache Software Foundation (ASF) under one or more
#  contributor license agreements.  See the NOTICE file distributed with
#  this work for additional information regarding copyright ownership.
#  The ASF licenses this file to You under the Apache License, Version 2.0
#  (the "License"); you may not use this file except in compliance with
#  the License.  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.


"""
This module contains consistency check/repair tests.
"""

from ducktape.errors import TimeoutError

from ignitetest.services.ignite_app import IgniteApplicationService
from ignitetest.services.ignite_execution_exception import IgniteExecutionException
from ignitetest.services.utils.control_utility import ControlUtility
from ignitetest.services.utils.ignite_configuration import IgniteConfiguration
from ignitetest.utils import cluster, ignite_versions
from ignitetest.utils.ignite_test import IgniteTest
from ignitetest.utils.version import DEV_BRANCH, IgniteVersion


class ConsistencyTest(IgniteTest):
    """
    Consistency test.
    """
    CACHE_NAME = "TEST"

    PROPERTIES = """
        <property name="includeEventTypes">
            <util:constant static-field="org.apache.ignite.events.EventType.EVT_CONSISTENCY_VIOLATION"/>
        </property>
        """

    @cluster(num_nodes=2)
    @ignite_versions(str(DEV_BRANCH))
    def test_logging(self, ignite_version):
        """
        Tests logging goes to the correct file (consistency.log) when default AI config used.
        """
        cfg_filename = "ignite-default-log4j.xml"

        ignites = IgniteApplicationService(
            self.test_context,
            IgniteConfiguration(
                version=IgniteVersion(ignite_version),
                cluster_state="INACTIVE",
                properties=self.PROPERTIES,
                log4j_config=cfg_filename  # default AI config (will be generated below)
            ),
            java_class_name="org.apache.ignite.internal.ducktest.tests.control_utility.InconsistentNodeApplication",
            params={
                "cacheName": self.CACHE_NAME,
                "amount": 1024,
                "parts": 1,
                "tx": False
            },
            startup_timeout_sec=180,
            num_nodes=len(self.test_context.cluster))

        for node in ignites.nodes:  # copying default AI config with log path replacement
            ignites.init_persistent(node)

            cfg_file = f"{ignites.config_dir}/{cfg_filename}"

            ignites.exec_command(node, f"cp {ignites.home_dir}/config/ignite-log4j.xml {cfg_file}")

            orig = "${IGNITE_HOME}/work/log".replace('/', '\\/')
            fixed = ignites.log_dir.replace('/', '\\/')

            ignites.exec_command(node, f"sed -i 's/{orig}/{fixed}/g' {cfg_file}")

        ignites.start()

        control_utility = ControlUtility(ignites)

        control_utility.activate()

        ignites.await_event("APPLICATION_STREAMING_FINISHED", 60, from_the_beginning=True)

        try:
            control_utility.idle_verify()  # making sure we have broken data
            raise IgniteExecutionException("Fail.")
        except AssertionError:
            pass

        control_utility.check_consistency(f"repair --cache {self.CACHE_NAME} --partition 0 --strategy LWW")  # checking/repairing

        message = "Cache consistency violations recorded."

        ignites.await_event(message, 60, from_the_beginning=True, log_file="consistency.log")

        try:
            ignites.await_event(message, 10, from_the_beginning=True)
            raise IgniteExecutionException("Fail.")
        except TimeoutError:
            pass
