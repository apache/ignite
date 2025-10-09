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
import re
from distutils.version import LooseVersion

from ducktape.cluster.remoteaccount import RemoteCommandError
from ducktape.utils.util import wait_until

from ignitetest.services.utils.ducktests_service import DucktestsService
from ignitetest.services.utils.ignite_spec import envs_to_exports
from ignitetest.services.utils.log_utils import monitor_log
from ignitetest.services.utils.path import PathAware


class PostgresSettings:
    """
    Settings for PostgreSQL nodes.
    """

    def __init__(self, **kwargs):
        self.port = kwargs.get("port", 5432)

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
        return "%s-%s" % ("postgres", self.settings.version)

    @property
    def globals(self):
        return self.context.globals

    @property
    def log_config_file(self):
        raise RuntimeError("Not implemented!")

    @property
    def config_file(self):
        return os.path.join(self.work_dir, "postgresql.conf")

    def init_persistent(self, node):
        _PERMISSIONS = int('750', 8)

        node.account.mkdirs(f"{self.persistent_root} {self.work_dir} {self.log_dir}", _PERMISSIONS)

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
        self.logger.info(f"Initializing PostgreSQL data directory {self.work_dir}")

        self.__init_db_dir(node)

        self.logger.info(f"Creating PostgreSQL configuration")

        config_file = self.render(
            'postgresql.conf.j2',
            settings=self.settings,
            log_dir=self.log_dir,
            data_dir=self.work_dir
        )

        node.account.create_file(self.config_file, config_file)

        self.logger.info(f"Created PostgreSQL configuration: {config_file}")

        self.logger.info(f"Starting PostgreSQL node {self.idx(node)} on {node.account.hostname}")

        start_cmd = (
            f"nohup {os.path.join(self.home_dir, 'bin', 'postgres')} "
            f"-D {self.work_dir} "
            f"-p {self.settings.port} "
            f"> {self.log_file} 2>&1 &"
        )

        node.account.ssh(start_cmd)

    def __init_db_dir(self, node):
        self.init_persistent(node)

        # node.account.ssh(f"{envs_to_exports(self.envs())}") TODO: check the param on start

        init_db_cmd = f"{os.path.join(self.home_dir, 'bin', 'initdb')} -D {self.work_dir}"

        raw_output = (node.account.ssh_capture(init_db_cmd, allow_fail=True))

        code, output = self.__parse_init_db_output(raw_output)

        self.logger.debug(f"Output of command {init_db_cmd} on node {node.name}, exited with code {code}, is {output}")

        if code != 0:
            raise RemoteCommandError(node.account, init_db_cmd, code, output)

    @staticmethod
    def __parse_init_db_output(raw_output):
        exit_code = raw_output.channel_file.channel.recv_exit_status()
        output = "".join(raw_output)

        match = re.compile("Success").search(output)

        if match:
            return int(match.group(1)), output

        return exit_code, output

    def envs(self):
        """
        :return: environment set.
        """
        return {
            'LD_LIBRARY_PATH': f"{os.path.join(self.home_dir)}/lib:$LD_LIBRARY_PATH"
        }

    def wait_node(self, node, timeout_sec=20):
        wait_until(lambda: not self.alive(node), timeout_sec=timeout_sec)

        return not self.alive(node)

    def await_ready(self, node, timeout):
        """
        Await PostgreSQL nodes
        :param node:  PostgreSQL service node.
        :param timeout: Wait timeout.
        """
        with monitor_log(node, self.log_file, from_the_beginning=True) as monitor:
            monitor.wait_until(
                "database system is ready to accept connections",
                timeout_sec=timeout,
                err_msg=f"PostgreSQL not ready on {node.account.hostname}"
            )

    @property
    def log_file(self):
        return os.path.join(self.log_dir, self.LOG_FILENAME)

    def pids(self, node):
        return node.account.ssh_capture(
            f"pgrep -f '{os.path.join(self.home_dir, 'bin', 'postgres')}.*-D {self.work_dir}'",
            allow_fail=True
        )

    def alive(self, node):
        return len(self.pids(node)) > 0

    def stop_node(self, node, force_stop=False, **kwargs):
        self.logger.info(f"Stopping PostgreSQL node {self.idx(node)} on {node.account.hostname}")

        node.account.kill_process(
            f"{os.path.join(self.home_dir, 'bin', 'postgres')}",
            clean_shutdown=not force_stop,
            allow_fail=False
        )

    def clean_node(self, node, **kwargs):
        super().clean_node(node, **kwargs)

        self.logger.info(f"Cleaning PostgreSQL node {self.idx(node)} on {node.account.hostname}")

        node.account.ssh(f"rm -rf -- {self.persistent_root}", allow_fail=False)
