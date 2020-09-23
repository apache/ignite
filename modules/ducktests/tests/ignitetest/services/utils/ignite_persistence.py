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
This module contains classes that represent persistent artifacts of tests
"""

import os

from ignitetest.services.utils.config_template import IgniteLoggerConfigTemplate


class PersistenceAware:
    """
    This class contains basic persistence artifacts
    """
    # Root directory for persistent output
    PERSISTENT_ROOT = "/mnt/service"
    STDOUT_STDERR_CAPTURE = os.path.join(PERSISTENT_ROOT, "console.log")

    logs = {
        "console_log": {
            "path": STDOUT_STDERR_CAPTURE,
            "collect_default": True
        }
    }

    def init_persistent(self, node):
        """
        Init persistent directory.
        :param node: Service node.
        """
        node.account.mkdirs(self.PERSISTENT_ROOT)


class IgnitePersistenceAware(PersistenceAware):
    """
    This class contains Ignite persistence artifacts
    """
    WORK_DIR = os.path.join(PersistenceAware.PERSISTENT_ROOT, "work")
    SNAPSHOT = os.path.join(WORK_DIR, "snapshots")
    CONFIG_FILE = os.path.join(PersistenceAware.PERSISTENT_ROOT, "ignite-config.xml")
    LOG4J_CONFIG_FILE = os.path.join(PersistenceAware.PERSISTENT_ROOT, "ignite-log4j.xml")

    def __getattribute__(self, item):
        if item == 'logs':
            return PersistenceAware.logs

        return super().__getattribute__(item)

    def init_persistent(self, node):
        """
        Init persistent directory.
        :param node: Ignite service node.
        """
        super().init_persistent(node)

        logger_config = IgniteLoggerConfigTemplate().render(work_dir=self.WORK_DIR)
        node.account.create_file(self.LOG4J_CONFIG_FILE, logger_config)
