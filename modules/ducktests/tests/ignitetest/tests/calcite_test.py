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
This module contains tests for calcite engine
"""
from ignitetest.services.ignite_app import IgniteCustomApplicationService
from ignitetest.services.utils.ignite_configuration import CustomApplicationConfiguration
from ignitetest.utils import cluster, ignite_versions
from ignitetest.utils.ignite_test import IgniteTest
from ignitetest.utils.version import DEV_BRANCH, IgniteVersion


class CalciteTest(IgniteTest):
    """
    Calcite engine tests
    """
    @cluster(num_nodes=1)
    @ignite_versions(str(DEV_BRANCH))
    def test_std_sql_operators(self, ignite_version):
        """
        This test validates that classpath contains all the required libraries that used by calcite engine.
        """

        modules = ["calcite", "core"]

        app = IgniteCustomApplicationService(
            self.test_context,
            config=CustomApplicationConfiguration(version=IgniteVersion(ignite_version)),
            modules=modules,
            main_java_class="org.apache.ignite.internal.ducktest.tests.calcite.CalciteTestingApplication",
            num_nodes=1)

        app.start()

        app.await_stopped()
