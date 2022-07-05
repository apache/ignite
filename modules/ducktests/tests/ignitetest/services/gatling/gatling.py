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
import re
import stat
from typing import NamedTuple

from ignitetest.services.ignite_app import IgniteApplicationService

SERVICE_JAVA_CLASS_NAME = "org.apache.ignite.internal.ducktest.gatling.GatlingRunnerApplication"

DEFAULT_GATLING_CONF = "gatling.conf.j2"
DEFAULT_GATLING_LOG_CONF = "logback.xml.j2"
DEFAULT_GATLING_AKKA_CONF = "gatling-akka.conf.j2"


class GatlingConfiguration(NamedTuple):
    """
    Settings for the Gatling service.
    """
    core_directory_results: str


class GatlingService(IgniteApplicationService):
    """
    Gatling service.
    """
    def __init__(self, context, client_config, params=None,
                 num_nodes=1, startup_timeout_sec=60, shutdown_timeout_sec=60, modules=None,
                 jvm_opts=None, merge_with_default=True):
        super().__init__(
            context, client_config, SERVICE_JAVA_CLASS_NAME, num_nodes,
            params, startup_timeout_sec, shutdown_timeout_sec,
            extend_with(modules, "gatling-plugin"), jvm_opts=jvm_opts, merge_with_default=merge_with_default
        )
        if params is None:
            self.params = {}
        self.report_generated = False

    def _prepare_configs(self, node):
        super()._prepare_configs(node)

        config = GatlingConfiguration(core_directory_results=self.log_dir)

        config_file = self.render(DEFAULT_GATLING_CONF, settings=config, data_dir=self.work_dir)
        node.account.create_file(os.path.join(self.config_dir, "gatling.conf"), config_file)

        config_file = self.render(DEFAULT_GATLING_AKKA_CONF, settings=config, data_dir=self.work_dir)
        node.account.create_file(os.path.join(self.config_dir, "gatling-akka.conf"), config_file)

        config_file = self.render(DEFAULT_GATLING_LOG_CONF, service=self, settings=config, data_dir=self.work_dir)
        node.account.create_file(os.path.join(self.config_dir, "logback.xml"), config_file)

    def run_with_timeout(self, timeout_sec):
        """Run with specified timeout."""
        self.start()
        self.wait(timeout_sec=timeout_sec)
        self.stop()

    def generate_report(self, directory="gatling-full-report"):
        """
        Generates html report for the most recent run results
        :param directory: Directory to save report in
        """
        main_node = self.nodes[0]
        main_node.account.sftp_client.mkdir(os.path.join(self.log_dir, directory))
        for node in self.nodes:
            files = node.account.sftp_client.listdir(self.log_dir)
            files.sort(reverse=True)
            for filename in files:
                if stat.S_ISDIR(node.account.sftp_client.stat(self.log_dir).st_mode) \
                        and re.match("^gatling-report-.*$", filename):
                    node.account.copy_between(os.path.join(self.log_dir, filename, "simulation.log"),
                                              os.path.join(self.log_dir, directory,
                                                           "%s-%s.log" % (filename, node.name)),
                                              main_node)
                    break

        save_params = self.params
        self.params = {"reportsOnly": os.path.join(self.log_dir, directory)}
        self.start_node(main_node)
        self.await_stopped()
        self.params = save_params


def extend_with(modules, new_module):
    return [new_module] if modules is None else modules.append(new_module)
