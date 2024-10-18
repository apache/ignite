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
This module contains client queries tests.
"""
from ducktape.mark import matrix

from ignitetest.services.ignite import IgniteService
from ignitetest.services.ignite_app import IgniteApplicationService
from ignitetest.services.utils.ignite_configuration import IgniteConfiguration, IgniteThinClientConfiguration
from ignitetest.services.utils.ignite_spec import IgniteNodeSpec
from ignitetest.services.utils.ssl.client_connector_configuration import ClientConnectorConfiguration
from ignitetest.utils import cluster, ignite_versions
from ignitetest.utils.ignite_test import IgniteTest
from ignitetest.utils.version import DEV_BRANCH, IgniteVersion


class ThinClientQueryTest(IgniteTest):
    """
    cluster - cluster size.
    JAVA_CLIENT_CLASS_NAME - running classname.
    to use with ssl enabled:
    export GLOBALS='{"ssl":{"enabled":true}}' .
    """
    @cluster(num_nodes=3)
    @ignite_versions(str(DEV_BRANCH), version_prefix="server_version")
    @matrix(filter=[False, True])
    def test_thin_client_index_query(self, server_version, filter):
        """
        Thin client IndexQuery test.
        :param server_version Ignite node version.
        :param filter Whether to use filter for queries.
        """

        server_config = IgniteConfiguration(version=IgniteVersion(server_version),
                                            client_connector_configuration=ClientConnectorConfiguration())

        ignite = IgniteService(self.test_context, server_config, 2)

        if not filter:
            ignite.spec = IgniteNodeSpecExcludeDucktests(service=ignite)

        addresses = [ignite.nodes[0].account.hostname + ":" + str(server_config.client_connector_configuration.port)]

        cls = "org.apache.ignite.internal.ducktest.tests.thin_client_query_test.ThinClientQueryTestApplication"

        thin_clients = IgniteApplicationService(self.test_context,
                                                IgniteThinClientConfiguration(
                                                    addresses=addresses,
                                                    version=IgniteVersion(str(DEV_BRANCH))),
                                                java_class_name=cls,
                                                num_nodes=1,
                                                params={"filter": filter})

        ignite.start()
        thin_clients.run()
        ignite.stop()


class IgniteNodeSpecExcludeDucktests(IgniteNodeSpec):
    """
    Ignite node specification that excludes module 'ducktests' from classpath.
    """
    def modules(self):
        """
        Exclude module from preparing USER_LIBS environment variable.
        """
        modules = super().modules()

        modules.remove("ducktests")

        return modules

    def envs(self):
        """
        Skip the module target directory while building classpath.
        """
        envs = super().envs()

        if envs.get("EXCLUDE_MODULES") is not None:
            envs["EXCLUDE_MODULES"] = envs["EXCLUDE_MODULES"] + ",ducktests"
        else:
            envs["EXCLUDE_MODULES"] = "ducktests"

        return envs
