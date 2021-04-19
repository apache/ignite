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
This module contains Spec classes that describes config and command line to start Ignite services
"""

import base64
import importlib
import json
import os
from abc import ABCMeta, abstractmethod

from ignitetest.services.utils.config_template import IgniteClientConfigTemplate, IgniteServerConfigTemplate, \
    IgniteThinClientConfigTemplate
from ignitetest.services.utils.jvm_utils import create_jvm_settings, merge_jvm_settings
from ignitetest.services.utils.path import get_home_dir, get_module_path
from ignitetest.utils.version import DEV_BRANCH


# pylint: disable=R0913
def resolve_spec(service, context, config, main_java_class, start_ignite, thin_client_config=None, **kwargs):
    """
    Resolve Spec classes for IgniteService and IgniteApplicationService
    """
    def _resolve_spec(name, default):
        if name in context.globals:
            fqdn = context.globals[name]
            (module, clazz) = fqdn.rsplit('.', 1)
            module = importlib.import_module(module)
            return getattr(module, clazz)
        return default

    def is_impl(impl):
        classes = map(lambda s: s.__name__, service.__class__.mro())
        impl_filter = list(filter(lambda c: c == impl, classes))
        return len(impl_filter) > 0

    if is_impl("IgniteService"):
        return _resolve_spec("NodeSpec", ApacheIgniteNodeSpec)(path_aware=service, context=context, config=config,
                                                               **kwargs)

    if is_impl("IgniteApplicationService"):
        return _resolve_spec("AppSpec", ApacheIgniteApplicationSpec)(path_aware=service, context=context, config=config,
                                                                     main_java_class=main_java_class,
                                                                     start_ignite=start_ignite,
                                                                     thin_client_config=thin_client_config, **kwargs)

    raise Exception("There is no specification for class %s" % type(service))


class IgniteSpec(metaclass=ABCMeta):
    """
    This class is a basic Spec
    """
    # pylint: disable=R0913
    def __init__(self, path_aware, config, project, thin_client_config=None, jvm_opts=None, full_jvm_opts=None):
        self.project = project
        self.path_aware = path_aware
        self.envs = {}

        if full_jvm_opts:
            self.jvm_opts = full_jvm_opts

            if jvm_opts:
                self._add_jvm_opts(jvm_opts)
        else:
            self.jvm_opts = create_jvm_settings(opts=jvm_opts,
                                                gc_dump_path=os.path.join(path_aware.log_dir, "ignite_gc.log"),
                                                oom_path=os.path.join(path_aware.log_dir, "ignite_out_of_mem.hprof"))
        self.config = config
        self.thin_client_config = thin_client_config
        if self.config:
            self.version = config.version
        else:
            self.version = thin_client_config.version

    @property
    def config_template(self):
        """
        :return: config that service will use to start on a node
        """
        if self.config.client_mode:
            return IgniteClientConfigTemplate()
        return IgniteServerConfigTemplate()

    @property
    def thin_client_config_template(self):
        """
        :return: thin client config that service will use to start on a node
        """
        if self.thin_client_config:
            return IgniteThinClientConfigTemplate()
        return None

    def __home(self, version=None, project=None):
        """
        Get home directory for current spec.
        """
        project = project if project else self.project
        version = version if version else self.version
        return get_home_dir(self.path_aware.install_root, project, version)

    def _module(self, name):
        """
        Get module path for current spec.
        """
        if name == "ducktests":
            return get_module_path(self.__home(DEV_BRANCH, project="ignite"), name, DEV_BRANCH)

        return get_module_path(self.__home(self.version), name, self.version)

    @abstractmethod
    def command(self, node):
        """
        :return: string that represents command to run service on a node
        """

    def _envs(self):
        """
        :return: line with exports env variables: export A=B; export C=D;
        """
        exports = ["export %s=%s" % (key, self.envs[key]) for key in self.envs]
        return "; ".join(exports) + ";"

    def _jvm_opts(self):
        """
        :return: line with extra JVM params for ignite.sh script: -J-Dparam=value -J-ea
        """
        opts = ["-J%s" % o for o in self.jvm_opts]
        return " ".join(opts)

    def _add_jvm_opts(self, opts):
        """Properly adds JVM options to current"""
        self.jvm_opts = merge_jvm_settings(self.jvm_opts, opts)


class IgniteNodeSpec(IgniteSpec):
    """
    Spec to run ignite node
    """
    def command(self, node):
        cmd = "%s %s %s %s 2>&1 | tee -a %s &" % \
              (self._envs(),
               self.path_aware.script("ignite.sh"),
               self._jvm_opts(),
               self.path_aware.config_file,
               node.log_file)

        return cmd


class IgniteApplicationSpec(IgniteSpec):
    """
    Spec to run ignite application
    """
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.args = ""

    def _app_args(self):
        return ",".join(self.args)

    # pylint: disable=W0221
    def command(self, node):
        cmd = "%s %s %s %s 2>&1 | tee -a %s &" % \
              (self._envs(),
               self.path_aware.script("ignite.sh"),
               self._jvm_opts(),
               self._app_args(),
               node.log_file)

        return cmd


class ApacheIgniteNodeSpec(IgniteNodeSpec):
    """
    Implementation IgniteNodeSpec for Apache Ignite project
    """
    def __init__(self, context, modules, **kwargs):
        super().__init__(project=context.globals.get("project", "ignite"), **kwargs)

        libs = (modules or [])
        libs.append("log4j")
        libs = list(map(lambda m: os.path.join(self._module(m), "*"), libs))

        libs.append(os.path.join(self._module("ducktests"), "*"))

        self.envs = {
            'EXCLUDE_TEST_CLASSES': 'true',
            'IGNITE_LOG_DIR': self.path_aware.persistent_root,
            'USER_LIBS': ":".join(libs)
        }

        self._add_jvm_opts(["-DIGNITE_SUCCESS_FILE=" + os.path.join(self.path_aware.persistent_root, "success_file"),
                            "-Dlog4j.configuration=file:" + self.path_aware.log_config_file,
                            "-Dlog4j.configDebug=true"])


class ApacheIgniteApplicationSpec(IgniteApplicationSpec):
    """
    Implementation IgniteApplicationSpec for Apache Ignite project
    """
    # pylint: disable=too-many-arguments
    def __init__(self, context, modules, main_java_class, java_class_name, params, start_ignite, **kwargs):
        super().__init__(project=context.globals.get("project", "ignite"), **kwargs)
        self.context = context

        libs = modules or []
        libs.extend(["log4j"])

        libs = list(map(lambda m: os.path.join(self._module(m), "*"), libs))
        libs.append(os.path.join(self._module("ducktests"), "*"))
        libs.extend(self.__jackson())

        self.envs = {
            "MAIN_CLASS": main_java_class,
            "EXCLUDE_TEST_CLASSES": "true",
            "IGNITE_LOG_DIR": self.path_aware.persistent_root,
            "USER_LIBS": ":".join(libs)
        }

        self._add_jvm_opts(["-DIGNITE_SUCCESS_FILE=" + os.path.join(self.path_aware.persistent_root, "success_file"),
                            "-Dlog4j.configuration=file:" + self.path_aware.log_config_file,
                            "-Dlog4j.configDebug=true",
                            "-DIGNITE_NO_SHUTDOWN_HOOK=true",  # allows to perform operations on app termination.
                            "-Xmx1G",
                            "-ea",
                            "-DIGNITE_ALLOW_ATOMIC_OPS_IN_TX=false"])

        self.args = [
            str(start_ignite.name),
            java_class_name,
            self.path_aware.config_file,
            str(base64.b64encode(json.dumps(params).encode('utf-8')), 'utf-8')
        ]

    def __jackson(self):
        version = self.version
        if not version.is_dev:
            aws = self._module("aws")
            return self.context.cluster.nodes[0].account.ssh_capture(
                "ls -d %s/* | grep jackson | tr '\n' ':' | sed 's/.$//'" % aws)

        return []
