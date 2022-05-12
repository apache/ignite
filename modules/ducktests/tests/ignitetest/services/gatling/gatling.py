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

import os

from ignitetest.services.ignite_app import IgniteApplicationService
from ignitetest.services.gatling import GatlingConfiguration

SERVICE_JAVA_CLASS_NAME = "org.apache.ignite.internal.ducktest.tests.gatling.GatlingRunnerApplication"

DEFAULT_GATLING_CONF = "gatling.conf.j2"
DEFAULT_GATLING_AKKA_CONF = "gatling-akka.conf.j2"


class GatlingService(IgniteApplicationService):
    def __init__(self, context, config, simulation_class_name, num_nodes=1, startup_timeout_sec=60,
                 shutdown_timeout_sec=60, modules=None, jvm_opts=None, merge_with_default=True):
        super().__init__(
            context, config, SERVICE_JAVA_CLASS_NAME, num_nodes, {"simulation": simulation_class_name},
            startup_timeout_sec, shutdown_timeout_sec, modules, jvm_opts=jvm_opts, merge_with_default=merge_with_default
        )

    def _prepare_configs(self, node):
        super()._prepare_configs(node)

        config = GatlingConfiguration(core_directory_results=self.log_dir)

        config_file = self.render(DEFAULT_GATLING_CONF, settings=config, data_dir=self.work_dir)
        node.account.create_file(os.path.join(self.config_dir, "gatling.conf"), config_file)

        config_file = self.render(DEFAULT_GATLING_AKKA_CONF, settings=config, data_dir=self.work_dir)
        node.account.create_file(os.path.join(self.config_dir, "gatling-akka.conf"), config_file)
