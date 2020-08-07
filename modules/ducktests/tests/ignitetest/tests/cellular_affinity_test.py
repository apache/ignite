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
This module contains Cellular Affinity tests.
"""

from ducktape.mark import parametrize
from ducktape.mark.resource import cluster
from jinja2 import Template

from ignitetest.services.ignite import IgniteService
from ignitetest.services.ignite_app import IgniteApplicationService
from ignitetest.tests.utils.ignite_test import IgniteTest
from ignitetest.tests.utils.version import DEV_BRANCH, IgniteVersion


# pylint: disable=W0223
class CellularAffinity(IgniteTest):
    """
    Tests Cellular Affinity scenarios.
    """
    NUM_NODES = 3

    CONFIG_TEMPLATE = """
            <property name="cacheConfiguration">
                <list>
                    <bean class="org.apache.ignite.configuration.CacheConfiguration">
                        <property name="affinity">
                            <bean class="org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction">
                                <property name="affinityBackupFilter">
                                    <bean class="org.apache.ignite.internal.ducktest.tests.cellular_affinity_test.CellularAffinityBackupFilter">
                                        <constructor-arg value="CELL"/>
                                    </bean>
                                </property>
                            </bean>
                        </property>
                        <property name="name" value="test-cache"/>
                        <property name="backups" value="{{ nodes }}"/> 
                    </bean>
                </list>
            </property>
        """

    @staticmethod
    def properties():
        """
        :return: Configuration properties.
        """
        return Template(CellularAffinity.CONFIG_TEMPLATE) \
            .render(nodes=CellularAffinity.NUM_NODES)  # bigger than cell capacity (to handle single cell useless test)

    def __init__(self, test_context):
        super(CellularAffinity, self).__init__(test_context=test_context)

    def setUp(self):
        pass

    def teardown(self):
        pass

    @cluster(num_nodes=NUM_NODES * 3 + 1)
    @parametrize(version=str(DEV_BRANCH))
    def test(self, version):
        """
        Test Cellular Affinity scenario (partition distribution).
        """
        ignite_version = IgniteVersion(version)

        ignites1 = IgniteService(
            self.test_context,
            num_nodes=CellularAffinity.NUM_NODES,
            version=ignite_version,
            properties=self.properties(),
            jvm_opts=['-DCELL=1'])

        ignites1.start()

        ignites2 = IgniteService(
            self.test_context,
            num_nodes=CellularAffinity.NUM_NODES,
            version=ignite_version,
            properties=self.properties(),
            jvm_opts=['-DCELL=2'])

        ignites2.start()

        ignites3 = IgniteService(
            self.test_context,
            num_nodes=CellularAffinity.NUM_NODES,
            version=ignite_version,
            properties=self.properties(),
            jvm_opts=['-DCELL=XXX', '-DRANDOM=42'])

        ignites3.start()

        checker = IgniteApplicationService(
            self.test_context,
            java_class_name="org.apache.ignite.internal.ducktest.tests.cellular_affinity_test.DistributionChecker",
            params={"cacheName": "test-cache", "attr": "CELL", "nodesPerCell": self.NUM_NODES},
            version=ignite_version)

        checker.start()
