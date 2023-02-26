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
from abc import abstractmethod, ABCMeta

from ignitetest.utils.version import DEV_BRANCH


def get_home_dir(install_root, product):
    """
    Get path to binary release (home) directory.
    """
    return os.path.join(install_root, product)


def get_module_path(project_dir, module_name, is_dev):
    """
    Get absolute path to the specified module.
    """
    if is_dev:
        module_path = os.path.join("modules", module_name, "target")
    else:
        module_path = os.path.join("libs", "optional", "ignite-%s" % module_name)

    return os.path.join(project_dir, module_path)


def get_shared_root_path(test_globals):
    """
    Get path to shared root directory.
    """
    return os.path.join(test_globals.get("persistent_root", "/mnt/service"), "shared")


class PathAware:
    """
    Basic class for path configs.
    """
    def init_persistent(self, node):
        """
        Init persistent directory.
        :param node: Service node.
        """
        node.account.mkdirs(
            f"{self.persistent_root} {self.temp_dir} {self.work_dir} {self.log_dir} {self.config_dir} {self.jfr_dir}")

    def init_logs_attribute(self):
        """
        Initialize logs attribute for collecting logs by ducktape.
        After changing to property based logs, will be removed.
        """
        setattr(self, 'logs', {
            "log": {
                "path": self.log_dir,
                "collect_default": True
            },
            "config": {
                "path": self.config_dir,
                "collect_default": True
            },
            "shared": {
                "path": self.shared_root,
                "collect_default": True
            },
            "jfr": {
                "path": self.jfr_dir,
                "collect_default": True
            }
        })

    @property
    @abstractmethod
    def config_file(self):
        """
        :return: path to project configuration file
        """

    @property
    @abstractmethod
    def log_config_file(self):
        """
        :return: path to logger configuration file
        """

    @property
    def work_dir(self):
        """
        :return: path to work directory
        """
        return os.path.join(self.persistent_root, "work")

    @property
    def config_dir(self):
        """
        :return: path to config directory
        """
        return os.path.join(self.persistent_root, "config")

    @property
    def jfr_dir(self):
        """
        :return: path to jfr directory
        """
        return os.path.join(self.persistent_root, "jfr")

    @property
    def log_dir(self):
        """
        :return: path to log directory
        """
        return os.path.join(self.persistent_root, "logs")

    @property
    def shared_root(self):
        """
        :return: path to directory with shared files - same files on all nodes
        """
        return get_shared_root_path(self.globals)

    @property
    @abstractmethod
    def product(self):
        """
        :return: Represents product (folder name), typically project/fork name with version.
        """

    @property
    @abstractmethod
    def globals(self):
        """
        :return: dictionary of globals variable (usually from test context).
        """

    @property
    def home_dir(self):
        """
        :return: path to binary release (home) directory
        """
        return get_home_dir(self.install_root, self.product)

    @property
    def temp_dir(self):
        """
        :return: path to temp directory
        """
        return os.path.join(self.persistent_root, "tmp")

    @property
    def persistent_root(self):
        """
        :return: path to persistent root
        """
        return self.globals.get("persistent_root", "/mnt/service")

    @property
    def install_root(self):
        """
        :return: path to distributive installation root
        """
        return self.globals.get("install_root", "/opt")


class IgnitePathAware(PathAware, metaclass=ABCMeta):
    """
    This class contains Ignite path configs.
    """
    IGNITE_CONFIG_NAME = "ignite-config.xml"

    IGNITE_THIN_CLIENT_CONFIG_NAME = "ignite-thin-config.xml"

    IGNITE_LOG_CONFIG_NAME = "ignite-ducktape-log4j2.xml"

    @property
    def config_file(self):
        """
        :return: path to config file
        """
        return os.path.join(self.config_dir, IgnitePathAware.IGNITE_CONFIG_NAME)

    @property
    def thin_client_config_file(self):
        """
        :return: path to thin client config file
        """
        return os.path.join(self.config_dir, IgnitePathAware.IGNITE_THIN_CLIENT_CONFIG_NAME)

    @property
    def log_config_file(self):
        """
        :return: path to log config file
        """
        return os.path.join(self.config_dir, IgnitePathAware.IGNITE_LOG_CONFIG_NAME)

    @property
    def database_dir(self):
        """
        :return: path to database directory
        """
        return os.path.join(self.work_dir, "db")

    @property
    def wal_dir(self):
        """
        :return: path to wal directory
        """
        return os.path.join(self.database_dir, "wal")

    @property
    def snapshots_dir(self):
        """
        :return: path to snapshots directory
        """
        return os.path.join(self.work_dir, "snapshots")

    @property
    def certificate_dir(self):
        """
        :return: path to the certificate directory.
        """
        return os.path.join(get_home_dir(self.install_root, str(DEV_BRANCH)), "modules", "ducktests", "tests", "certs")

    def script(self, script_name):
        """
        :param script_name: name of Ignite script
        :return: absolute path to the specified script
        """
        return os.path.join(self.home_dir, "bin", script_name)

    def cache_dir(self, consistent_dir, cache_name):
        return os.path.join(self.database_dir, consistent_dir, f'cache-{cache_name}')

    def index_file(self, consistent_dir, cache_name):
        return os.path.join(self.database_dir, consistent_dir, f'cache-{cache_name}', 'index.bin')
