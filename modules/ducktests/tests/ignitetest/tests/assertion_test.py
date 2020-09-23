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

from ducktape.mark.resource import cluster

from ignitetest.services.ignite_app import IgniteApplicationService
from ignitetest.services.utils.ignite_configuration import IgniteConfiguration
from ignitetest.utils import ignite_versions
from ignitetest.utils.ignite_test import IgniteTest
from ignitetest.utils.version import DEV_BRANCH, IgniteVersion

# pylint: disable=W0223
class SmokeSelfTest(IgniteTest):
    """
    Self test implementations
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
        except Exception as ex:
            assert str(ex) == "Java application execution failed. java.lang.AssertionError"
        else:
            app.stop()
            assert False
