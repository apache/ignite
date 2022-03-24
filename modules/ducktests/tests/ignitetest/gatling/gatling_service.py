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
This module contains class to start Gatling test.
"""
import os

from ducktape.mark.resource import cluster
from ducktape.tests.test import TestContext

from ignitetest.services.ignite_app import IgniteApplicationService
from ignitetest.services.utils.ignite_configuration import IgniteConfiguration
from ignitetest.services.utils.path import get_home_dir, PathAware
from ignitetest.utils import ignite_versions
from ignitetest.utils.ignite_test import IgniteTest
from ignitetest.utils.version import DEV_BRANCH, IgniteVersion


# pylint: disable=W0223
class GatlingService(IgniteApplicationService, PathAware):
    """
    The base class to start the Gatling application.
    """
    GATLING_CLASS_NAME = "org.apache.ignite.gatling.GatlingStarter"

    # pylint: disable=R0913
    def __init__(self, context, config, simulation_class_name, num_nodes=1, java_class_name=GATLING_CLASS_NAME,
                 startup_timeout_sec=180, shutdown_timeout_sec=180, jvm_opts=None, full_jvm_opts=None):
        super().__init__(context, config, java_class_name, num_nodes, startup_timeout_sec, shutdown_timeout_sec,
                         modules=["gatling"], jvm_opts=jvm_opts, full_jvm_opts=full_jvm_opts)

        self.params = {
            "resourcesDirectory": self.gatling_resources(),
            "resultsDirectory": self.gatling_logs(),
            "binariesDirectory": self.gatling_binaries(),
            "simulationClass": simulation_class_name
        }

    @property
    def globals(self):
        return self.context.globals

    def init_logs_attribute(self):
        super().init_logs_attribute()

        self.logs["gatling_logs"] = {
            "path": self.gatling_logs(),
            "collect_default": True
        }

    def gatling_logs(self):
        return os.path.join(self.persistent_root, "gatling", "results")

    def gatling_resources(self):
        return os.path.join(get_home_dir(self.install_root, str(DEV_BRANCH)), "modules", "gatling", "src",
                            "test", "resources")

    def gatling_binaries(self):
        return os.path.join(get_home_dir(self.install_root, str(DEV_BRANCH)), "modules", "gatling", "target",
                            "test-classes")
