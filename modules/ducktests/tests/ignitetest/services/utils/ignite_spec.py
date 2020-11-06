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
from abc import ABCMeta, abstractmethod

from ignitetest.services.utils.config_template import IgniteClientConfigTemplate, IgniteServerConfigTemplate
from ignitetest.services.utils.ignite_path import IgnitePath
from ignitetest.services.utils.ignite_persistence import IgnitePersistenceAware
from ignitetest.utils.version import DEV_BRANCH


def resolve_spec(service, context, config, **kwargs):
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
        return _resolve_spec("NodeSpec", ApacheIgniteNodeSpec)(config=config, **kwargs)

    if is_impl("IgniteApplicationService"):
        return _resolve_spec("AppSpec", ApacheIgniteApplicationSpec)(context=context, config=config, **kwargs)

    raise Exception("There is no specification for class %s" % type(service))


class IgniteSpec(metaclass=ABCMeta):
    """
    This class is a basic Spec
    """
    def __init__(self, config, project, jvm_opts):
        self.version = config.version
        self.path = IgnitePath(self.version, project)
        self.envs = {}
        self.jvm_opts = jvm_opts or []
        self.config = config

    @property
    def config_template(self):
        """
        :return: config that service will use to start on a node
        """
        if self.config.client_mode:
            return IgniteClientConfigTemplate()
        return IgniteServerConfigTemplate()

    @property
    @abstractmethod
    def command(self):
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


class IgniteNodeSpec(IgniteSpec, IgnitePersistenceAware):
    """
    Spec to run ignite node
    """
    @property
    def command(self):
        cmd = "%s %s %s %s 2>&1 | tee -a %s &" % \
              (self._envs(),
               self.path.script("ignite.sh"),
               self._jvm_opts(),
               self.CONFIG_FILE,
               self.STDOUT_STDERR_CAPTURE)

        return cmd


class IgniteApplicationSpec(IgniteSpec, IgnitePersistenceAware):
    """
    Spec to run ignite application
    """
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.args = ""

    def _app_args(self):
        return ",".join(self.args)

    @property
    def command(self):
        cmd = "%s %s %s %s 2>&1 | tee -a %s &" % \
              (self._envs(),
               self.path.script("ignite.sh"),
               self._jvm_opts(),
               self._app_args(),
               self.STDOUT_STDERR_CAPTURE)

        return cmd


class ApacheIgniteNodeSpec(IgniteNodeSpec, IgnitePersistenceAware):
    """
    Implementation IgniteNodeSpec for Apache Ignite project
    """
    def __init__(self, modules, **kwargs):
        super().__init__(project="ignite", **kwargs)

        libs = (modules or [])
        libs.append("log4j")
        libs = list(map(lambda m: self.path.module(m) + "/*", libs))

        libs.append(IgnitePath(DEV_BRANCH).module("ducktests") + "/*")

        self.envs = {
            'EXCLUDE_TEST_CLASSES': 'true',
            'IGNITE_LOG_DIR': self.PERSISTENT_ROOT,
            'USER_LIBS': ":".join(libs)
        }

        self.jvm_opts.extend([
            "-DIGNITE_SUCCESS_FILE=" + self.PERSISTENT_ROOT + "/success_file",
            "-Dlog4j.configuration=file:" + self.LOG4J_CONFIG_FILE,
            "-Dlog4j.configDebug=true"
        ])


class ApacheIgniteApplicationSpec(IgniteApplicationSpec, IgnitePersistenceAware):
    """
    Implementation IgniteApplicationSpec for Apache Ignite project
    """
    # pylint: disable=too-many-arguments
    def __init__(self, context, modules, servicejava_class_name, java_class_name, params, start_ignite, **kwargs):
        super().__init__(project="ignite", **kwargs)
        self.context = context

        libs = modules or []
        libs.extend(["log4j"])

        libs = [self.path.module(m) + "/*" for m in libs]
        libs.append(IgnitePath(DEV_BRANCH).module("ducktests") + "/*")
        libs.extend(self.__jackson())

        self.envs = {
            "MAIN_CLASS": servicejava_class_name,
            "EXCLUDE_TEST_CLASSES": "true",
            "IGNITE_LOG_DIR": self.PERSISTENT_ROOT,
            "USER_LIBS": ":".join(libs)
        }

        self.jvm_opts.extend([
            "-DIGNITE_SUCCESS_FILE=" + self.PERSISTENT_ROOT + "/success_file",
            "-Dlog4j.configuration=file:" + self.LOG4J_CONFIG_FILE,
            "-Dlog4j.configDebug=true",
            "-DIGNITE_NO_SHUTDOWN_HOOK=true",  # allows to perform operations on app termination.
            "-Xmx1G",
            "-ea",
            "-DIGNITE_ALLOW_ATOMIC_OPS_IN_TX=false"
        ])

        self.args = [
            str(start_ignite),
            java_class_name,
            self.CONFIG_FILE,
            str(base64.b64encode(json.dumps(params).encode('utf-8')), 'utf-8')
        ]

    def __jackson(self):
        if not self.version.is_dev:
            aws = self.path.module("aws")
            return self.context.cluster.nodes[0].account.ssh_capture(
                "ls -d %s/* | grep jackson | tr '\n' ':' | sed 's/.$//'" % aws)

        return []
