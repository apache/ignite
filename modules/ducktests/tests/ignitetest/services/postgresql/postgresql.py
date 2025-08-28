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
This module contains classes and utilities to start PostgreSql cluster for testing CDC.
"""

import os.path
from distutils.version import LooseVersion

from ignitetest.services.utils.ducktests_service import DucktestsService
from ignitetest.services.utils.path_aware import PathAware
from ignitetest.services.utils.wait import wait_until
from ignitetest.services.utils.logs import monitor_log


class PostgresSettings:
    """
    Settings for PostgreSQL nodes.
    """
    def __init__(self, **kwargs):
        self.port = kwargs.get("port", 5432)
        self.data_dir = kwargs.get("data_dir", "/tmp/pgdata")
        self.log_filename = kwargs.get("log_filename", "postgresql.log")

        version = kwargs.get("version")
        if version:
            if isinstance(version, str):
                version = LooseVersion(version)
            self.version = version
        else:
            self.version = LooseVersion("15.4")


class PostgresService(DucktestsService, PathAware):
    """
    PostgreSQL service.
    """
    LOG_FILENAME = "postgresql.log"

    def __init__(self, context, num_nodes, settings=PostgresSettings(), start_timeout_sec=30):
        super().__init__(context, num_nodes)
        self.settings = settings
        self.start_timeout_sec = start_timeout_sec
        self.init_logs_attribute()

    @property
    def product(self):
        return f"postgresql-{self.settings.version}"

    @property
    def globals(self):
        return self.context.globals

    @property
    def log_file(self):
        return path.join(self.log_dir, self.LOG_FILENAME)

    def start(self, **kwargs):
        self.start_async(**kwargs)
        self.await_started()

    def start_async(self, **kwargs):
        super().start(**kwargs)

    def await_started(self):
        self.logger.info("Waiting for PostgreSQL nodes...")

        for node in self.nodes:
            self.await_ready(node, self.start_timeout_sec)

        self.logger.info("PostgreSQL cluster is up.")

    def start_node(self, node, **kwargs):
        idx = self.idx(node)
        self.logger.info("Starting PostgreSQL node %d on %s", idx, node.account.hostname)

        # Init database if not already
        node.account.ssh(f"rm -rf {self.settings.data_dir}; "
                         f"/opt/postgres-{self.settings.version}/bin/initdb -D {self.settings.data_dir}")

        # Start postgres
        start_cmd = (
            f"nohup /opt/postgres-{self.settings.version}/bin/postgres "
            f"-D {self.settings.data_dir} "
            f"-p {self.settings.port} "
            f"> {self.log_file} 2>&1 &"
        )
        node.account.ssh(start_cmd)

    def await_ready(self, node, timeout):
        with monitor_log(node, self.log_file, from_the_beginning=True) as monitor:
            monitor.wait_until(
                "database system is ready to accept connections",
                timeout_sec=timeout,
                err_msg=f"PostgreSQL not ready on {node.account.hostname}"
            )

    def pids(self, node):
        return node.account.ssh_capture(
            f"pgrep -f '/opt/postgres-{self.settings.version}/bin/postgres.*-D {self.settings.data_dir}'",
            allow_fail=True
        )

    def alive(self, node):
        return len(self.pids(node)) > 0

    def stop_node(self, node, force_stop=False, **kwargs):
        idx = self.idx(node)
        self.logger.info("Stopping PostgreSQL node %d on %s", idx, node.account.hostname)
        if force_stop:
            node.account.kill_process("postgres", allow_fail=True)
        else:
            node.account.ssh(f"/opt/postgres-{self.settings.version}/bin/pg_ctl "
                             f"-D {self.settings.data_dir} -m fast stop")

    def clean_node(self, node, **kwargs):
        super().clean_node(node, **kwargs)

        self.logger.info("Cleaning PostgreSQL node %d on %s", self.idx(node), node.account.hostname)
        node.account.ssh(f"rm -rf -- {self.settings.data_dir}", allow_fail=False)
