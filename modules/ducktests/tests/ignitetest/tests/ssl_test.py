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
This module contains ssl tests
"""
import os
import time

from ignitetest.services.ignite import IgniteService
from ignitetest.services.ignite_app import IgniteApplicationService
from ignitetest.services.ignite_execution_exception import IgniteExecutionException
from ignitetest.services.utils.ignite_configuration import IgniteConfiguration, IgniteClientConfiguration, \
    SslContextFactory
from ignitetest.services.utils.ignite_configuration.discovery import from_ignite_cluster
from ignitetest.utils import ignite_versions, cluster
from ignitetest.utils.ignite_test import IgniteTest
from ignitetest.utils.version import DEV_BRANCH, IgniteVersion, LATEST_2_7, LATEST_2_8, LATEST_2_9


# pylint: disable=W0223
class SslTest(IgniteTest):
    """
    Ssl test
    """

    @cluster(num_nodes=2)
    @ignite_versions(str(DEV_BRANCH))
    def test_ssl_connection(self, ignite_version):
        """
        Ssl test
        """
        ssl_context_factory = SslContextFactory()
        server_configuration = IgniteConfiguration(version=IgniteVersion(ignite_version),
                                                   ssl_context_factory=ssl_context_factory)

        ignite = IgniteService(self.test_context, server_configuration, num_nodes=2,
                               startup_timeout_sec=180)

        ssl_context_factory.key_store_file_path = ignite.get_cert_path("server.jks")
        ssl_context_factory.trust_store_file_path = ignite.get_cert_path("truststore.jks")

        print(self.test_context)
        ignite.start()

        time.sleep(300*5)

        ignite.stop()
