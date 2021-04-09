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
This module contains Copy of Spec classes that describes config and command line to start Ignite services
"""

import base64
import json
import os

from ignitetest.services.utils.ignite_spec import IgniteNodeSpec, IgniteApplicationSpec


class ApacheIgniteNodeSpecCopy(IgniteNodeSpec):
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
                            "-Dlog4j.configDebug=true",
                            "-DRunFromCopyOfSpec=true"])


class ApacheIgniteApplicationSpecCopy(IgniteApplicationSpec):
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
                            "-DIGNITE_ALLOW_ATOMIC_OPS_IN_TX=false",
                            "-DRunFromCopyOfSpec=true"])

        self.args = [
            str(start_ignite),
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
