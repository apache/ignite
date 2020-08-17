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
from ignitetest.utils.ignite_test import IgniteTest
from ignitetest.utils.version import DEV_BRANCH


# pylint: disable=W0223
class CellularAffinity(IgniteTest):
    """
    Tests Cellular Affinity scenarios.
    """
    NUM_NODES = 3

    ATTRIBUTE = "CELL"

    CACHE_NAME = "test-cache"

    CONFIG_TEMPLATE = """
            <property name="cacheConfiguration">
                <list>
                    <bean class="org.apache.ignite.configuration.CacheConfiguration">
                        <property name="affinity">
                            <bean class="org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction">
                                <property name="affinityBackupFilter">
                                    <bean class="org.apache.ignite.internal.ducktest.tests.cellular_affinity_test.CellularAffinityBackupFilter">
                                        <constructor-arg value="{{ attr }}"/>
                                    </bean>
                                </property>
                            </bean>
                        </property>
                        <property name="name" value="{{ cacheName }}"/>
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
            .render(nodes=CellularAffinity.NUM_NODES,  # bigger than cell capacity (to handle single cell useless test)
                    attr=CellularAffinity.ATTRIBUTE,
                    cacheName=CellularAffinity.CACHE_NAME)

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
        self.start_cell(version, ['-D' + CellularAffinity.ATTRIBUTE + '=1'])
        self.start_cell(version, ['-D' + CellularAffinity.ATTRIBUTE + '=2'])
        self.start_cell(version, ['-D' + CellularAffinity.ATTRIBUTE + '=XXX', '-DRANDOM=42'])

        checker = IgniteApplicationService(
            self.test_context,
            java_class_name="org.apache.ignite.internal.ducktest.tests.cellular_affinity_test.DistributionChecker",
            params={"cacheName": CellularAffinity.CACHE_NAME,
                    "attr": CellularAffinity.ATTRIBUTE,
                    "nodesPerCell": self.NUM_NODES},
            version=version)

        checker.run()

    def start_cell(self, ignite_version, jvm_opts):
        """
        Starts cell.
        """
        ignites = IgniteService(
            self.test_context,
            num_nodes=CellularAffinity.NUM_NODES,
            version=ignite_version,
            properties=self.properties(),
            jvm_opts=jvm_opts)

        ignites.start()
