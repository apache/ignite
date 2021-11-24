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
This module contains smoke tests that checks that ducktape works as expected
"""
import os

from ducktape.mark import matrix

from ignitetest.services.ignite import IgniteService
from ignitetest.services.ignite_app import IgniteApplicationService
from ignitetest.services.ignite_execution_exception import IgniteExecutionException
from ignitetest.services.utils.ignite_configuration import IgniteConfiguration, IgniteClientConfiguration
from ignitetest.services.utils.ignite_configuration.discovery import from_ignite_cluster
from ignitetest.utils import ignite_versions, cluster
from ignitetest.utils.ignite_test import IgniteTest
from ignitetest.utils.version import DEV_BRANCH, IgniteVersion


class SelfTest(IgniteTest):
    """
    Self test
    """

    @cluster(num_nodes=1)
    @ignite_versions(str(DEV_BRANCH))
    def test_assertion_convertion(self, ignite_version):
        """
        Test to make sure Java assertions are converted to python exceptions
        """
        server_configuration = IgniteConfiguration(version=IgniteVersion(ignite_version))

        app = IgniteApplicationService(
            self.test_context,
            server_configuration,
            java_class_name="org.apache.ignite.internal.ducktest.tests.smoke_test.AssertionApplication")

        try:
            app.start()
        except IgniteExecutionException as ex:
            assert str(ex) == "Java application execution failed. java.lang.AssertionError"
        else:
            app.stop()
            assert False

    @cluster(num_nodes=4)
    @ignite_versions(str(DEV_BRANCH))
    def test_simple_services_start_stop(self, ignite_version):
        """
        Tests plain services start and stop (termitation vs self-terination).
        """
        ignites = IgniteService(self.test_context, IgniteConfiguration(version=IgniteVersion(ignite_version)),
                                num_nodes=1)

        ignites.start()

        client = IgniteService(self.test_context, IgniteClientConfiguration(version=IgniteVersion(ignite_version)),
                               num_nodes=1)

        client.start()

        node1 = IgniteApplicationService(
            self.test_context,
            IgniteClientConfiguration(version=IgniteVersion(ignite_version),
                                      discovery_spi=from_ignite_cluster(ignites)),
            java_class_name="org.apache.ignite.internal.ducktest.tests.self_test.TestKillableApplication",
            startup_timeout_sec=180)

        node2 = IgniteApplicationService(
            self.test_context,
            IgniteClientConfiguration(version=IgniteVersion(ignite_version),
                                      discovery_spi=from_ignite_cluster(ignites)),
            java_class_name="org.apache.ignite.internal.ducktest.tests.self_test.TestSelfKillableApplication",
            startup_timeout_sec=180)

        node1.start()

        node2.run()

        node1.stop()

        client.stop()

        ignites.stop()

    @cluster(num_nodes=1)
    @ignite_versions(str(DEV_BRANCH))
    def test_logs_rotation(self, ignite_version):
        """
        Test logs rotation after ignite service restart.
        """
        def get_log_lines_count(service, filename):
            node = service.nodes[0]
            log_file = os.path.join(service.log_dir, filename)
            log_cnt = list(node.account.ssh_capture(f'cat {log_file} | wc -l', callback=int))[0]
            return log_cnt

        def get_logs_count(service):
            node = service.nodes[0]
            return list(node.account.ssh_capture(f'ls {service.log_dir}/ignite.log* | wc -l', callback=int))[0]

        ignites = IgniteService(self.test_context, IgniteConfiguration(version=IgniteVersion(ignite_version)),
                                num_nodes=1)

        ignites.start()

        num_restarts = 6
        for i in range(num_restarts - 1):
            ignites.stop()

            old_cnt = get_log_lines_count(ignites, "ignite.log")
            assert old_cnt > 0

            ignites.start(clean=False)

            new_cnt = get_log_lines_count(ignites, "ignite.log")
            assert new_cnt > 0

            # check that there is no new entry in rotated file
            assert old_cnt == get_log_lines_count(ignites, f"ignite.log.{i + 1}")

        assert get_logs_count(ignites) == num_restarts

    @cluster(num_nodes=1)
    @ignite_versions(str(DEV_BRANCH))
    @matrix(is_ignite_service=[True, False])
    def test_config_add_to_result(self, ignite_version, is_ignite_service):
        """
        Test that the config file is in config directory
        and Service.logs contains the config directory to add to the result.
        """
        ignite_cfg = IgniteConfiguration(version=IgniteVersion(ignite_version))

        if is_ignite_service:
            ignite = IgniteService(self.test_context, ignite_cfg, num_nodes=1)
        else:
            ignite = IgniteApplicationService(
                self.test_context, ignite_cfg,
                java_class_name="org.apache.ignite.internal.ducktest.tests.self_test.TestKillableApplication")

        ignite.start()

        assert ignite.logs.get('config').get('path') == ignite.config_dir

        assert ignite.config_file.startswith(ignite.config_dir)

        ignite.nodes[0].account.ssh(f'ls {ignite.config_dir} | grep {os.path.basename(ignite.config_file)}')
        ignite.nodes[0].account.ssh(f'ls {ignite.config_dir} | grep {os.path.basename(ignite.log_config_file)}')

        ignite.stop()
