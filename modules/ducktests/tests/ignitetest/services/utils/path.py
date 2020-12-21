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
from abc import abstractmethod

from ignitetest.services.utils.config_template import IgniteLoggerConfigTemplate


class PathAware:
    """
    Basic class for path configs.
    """
    def init_persistent(self, node):
        """
        Init persistent directory.
        :param node: Service node.
        """
        node.account.mkdirs(self.persistent_root)
        node.account.mkdirs(self.temp_dir)

    @property
    @abstractmethod
    def log_path(self):
        pass

    @property
    @abstractmethod
    def config_file(self):
        pass

    @property
    @abstractmethod
    def log4j_config_file(self):
        pass

    @property
    def temp_dir(self):
        return os.path.join(self.persistent_root, "tmp")

    @property
    def persistent_root(self):
        return self.context.globals.get("persistent_root", "/mnt/service")

    @property
    def install_root(self):
        return self.context.globals.get("install_root", "/opt")


class IgnitePathAware(PathAware):
    """
    This class contains Ignite path configs.
    """
    def init_persistent(self, node):
        """
        Init persistent directory.
        :param node: Ignite service node.
        """
        super().init_persistent(node)

        logger_config = IgniteLoggerConfigTemplate().render(work_dir=self.work_dir)
        node.account.create_file(self.log4j_config_file, logger_config)

        setattr(self, 'logs', {
            "console_log": {
                "path": self.log_path,
                "collect_default": True
            }
        })

    @property
    def config_file(self):
        return os.path.join(self.persistent_root, "ignite-config.xml")

    @property
    def log4j_config_file(self):
        return os.path.join(self.persistent_root, "ignite-log4j.xml")

    @property
    def log_path(self):
        return os.path.join(self.persistent_root, "console.log")

    @property
    def work_dir(self):
        return os.path.join(self.persistent_root, "work")


    def script(self, script_name):
        """
        :param script_name: name of Ignite script
        :return: absolute path to the specified script
        """
        return os.path.join(self.home, "bin", script_name)
