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
import subprocess
from abc import ABCMeta, abstractmethod

from ignitetest.services.utils import IgniteServiceType
from ignitetest.services.utils.config_template import IgniteClientConfigTemplate, IgniteServerConfigTemplate, \
    IgniteLoggerConfigTemplate, IgniteThinClientConfigTemplate
from ignitetest.services.utils.jvm_utils import create_jvm_settings, merge_jvm_settings
from ignitetest.services.utils.path import get_home_dir, get_module_path, IgnitePathAware
from ignitetest.services.utils.ssl.ssl_params import is_ssl_enabled
from ignitetest.utils.ignite_test import JFR_ENABLED
from ignitetest.utils.version import DEV_BRANCH

SHARED_PREPARED_FILE = ".ignite_prepared"


def resolve_spec(service, **kwargs):
    """
    Resolve Spec classes for IgniteService and IgniteApplicationService
    """

    def _resolve_spec(name, default):
        if name in service.context.globals:
            fqdn = service.context.globals[name]
            (module, clazz) = fqdn.rsplit('.', 1)
            module = importlib.import_module(module)
            return getattr(module, clazz)
        return default

    def is_impl(impl):
        classes = map(lambda s: s.__name__, service.__class__.mro())
        impl_filter = list(filter(lambda c: c == impl, classes))
        return len(impl_filter) > 0

    if is_impl("IgniteService"):
        return _resolve_spec("NodeSpec", IgniteNodeSpec)(service=service, **kwargs)

    if is_impl("IgniteApplicationService"):
        return _resolve_spec("AppSpec", IgniteApplicationSpec)(service=service, **kwargs)

    raise Exception("There is no specification for class %s" % type(service))


def envs_to_exports(envs):
    """
    :return: line with exports env variables: export A=B; export C=D;
    """
    exports = ["export %s=%s" % (key, envs[key]) for key in envs]
    return "; ".join(exports) + ";"


class IgniteSpec(metaclass=ABCMeta):
    """
    This class is a basic Spec
    """

    def __init__(self, service, jvm_opts, full_jvm_opts):
        self.service = service

        if full_jvm_opts:
            self.jvm_opts = full_jvm_opts

            if jvm_opts:
                self._add_jvm_opts(jvm_opts)
        else:
            self.jvm_opts = create_jvm_settings(opts=jvm_opts,
                                                gc_dump_path=os.path.join(service.log_dir, "ignite_gc.log"),
                                                oom_path=os.path.join(service.log_dir, "ignite_out_of_mem.hprof"))

        self._add_jvm_opts(["-DIGNITE_SUCCESS_FILE=" + os.path.join(self.service.persistent_root, "success_file"),
                            "-Dlog4j.configuration=file:" + self.service.log_config_file,
                            "-Dlog4j.configDebug=true"])

        if service.context.globals.get(JFR_ENABLED, False):
            self._add_jvm_opts(["-XX:+UnlockCommercialFeatures",
                                "-XX:+FlightRecorder",
                                "-XX:StartFlightRecording=dumponexit=true," +
                                f"filename={self.service.jfr_dir}/recording.jfr"])

    def config_templates(self):
        """
        :return: config that service will use to start on a node
        """
        if self.service.config.service_type == IgniteServiceType.NONE:
            return []

        config_templates = [(IgnitePathAware.IGNITE_LOG_CONFIG_NAME, IgniteLoggerConfigTemplate())]

        if self.service.config.service_type == IgniteServiceType.NODE:
            config_templates.append((IgnitePathAware.IGNITE_CONFIG_NAME,
                                     IgniteClientConfigTemplate() if self.service.config.client_mode
                                     else IgniteServerConfigTemplate()))
        else:
            config_templates.append((IgnitePathAware.IGNITE_THIN_CLIENT_CONFIG_NAME, IgniteThinClientConfigTemplate()))

        return config_templates

    def extend_config(self, config):
        """
        Extend config with custom variables
        """
        return config

    def __home(self, product=None):
        """
        Get home directory for current spec.
        """
        product = product if product else self.service.product
        return get_home_dir(self.service.install_root, product)

    def _module(self, name):
        """
        Get module path for current spec.
        """
        if name == "ducktests":
            return get_module_path(self.__home(str(DEV_BRANCH)), name, DEV_BRANCH.is_dev)

        return get_module_path(self.__home(), name, self.service.config.version.is_dev)

    @abstractmethod
    def command(self, node):
        """
        :return: string that represents command to run service on a node
        """

    def libs(self):
        """
        :return: libs set.
        """
        libs = self.service.modules or []

        libs.append("log4j")
        libs.append("ducktests")

        return list(map(lambda m: os.path.join(self._module(m), "*"), libs))

    def envs(self):
        """
        :return: environment set.
        """
        return {
            'EXCLUDE_TEST_CLASSES': 'true',
            'IGNITE_LOG_DIR': self.service.persistent_root,
            'USER_LIBS': ":".join(self.libs())
        }

    def config_file_path(self):
        """
        :return: path to project configuration file
        """
        return self.service.config_file

    def is_prepare_shared_files(self, local_dir):
        """
        :return True if we have something to prepare.
        """
        if not is_ssl_enabled(self.service.context.globals) and \
                not (self.service.config.service_type == IgniteServiceType.NODE and self.service.config.ssl_params):
            self.service.logger.debug("Ssl disabled. Nothing to generate.")
            return False

        if os.path.isfile(os.path.join(local_dir, SHARED_PREPARED_FILE)):
            self.service.logger.debug("Local shared dir already prepared. Exiting. " + local_dir)
            return False

        return True

    def prepare_shared_files(self, local_dir):
        """
        Prepare files that should be copied on all nodes.
        """
        self.service.logger.debug("Local shared dir not exists. Creating. " + local_dir)
        try:
            os.mkdir(local_dir)
        except FileExistsError:
            self.service.logger.debug("Shared dir already exists, ignoring and continue." + local_dir)

        script_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "..", "certs")

        self._runcmd(f"cp {script_dir}/* {local_dir}")
        self._runcmd(f"chmod a+x {local_dir}/*.sh")
        self._runcmd(f"{local_dir}/mkcerts.sh")

    def _jvm_opts(self):
        """
        :return: line with extra JVM params for ignite.sh script: -J-Dparam=value -J-ea
        """
        opts = ["-J%s" % o for o in self.jvm_opts]
        return " ".join(opts)

    def _add_jvm_opts(self, opts):
        """Properly adds JVM options to current"""
        self.jvm_opts = merge_jvm_settings(self.jvm_opts, opts)

    def _runcmd(self, cmd):
        self.service.logger.debug(cmd)
        proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        stdout, _ = proc.communicate()

        if proc.returncode != 0:
            raise RuntimeError("Command '%s' returned non-zero exit status %d: %s" % (cmd, proc.returncode, stdout))


class IgniteNodeSpec(IgniteSpec):
    """
    Spec to run ignite node
    """

    def command(self, node):
        cmd = "%s %s %s %s 2>&1 | tee -a %s &" % \
              (envs_to_exports(self.envs()),
               self.service.script("ignite.sh"),
               self._jvm_opts(),
               self.config_file_path(),
               os.path.join(self.service.log_dir, "console.log"))

        return cmd


class IgniteApplicationSpec(IgniteSpec):
    """
    Spec to run ignite application
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._add_jvm_opts(["-DIGNITE_NO_SHUTDOWN_HOOK=true",  # allows to perform operations on app termination.
                            "-Xmx1G",
                            "-ea",
                            "-DIGNITE_ALLOW_ATOMIC_OPS_IN_TX=false"])

    def command(self, node):
        args = [
            str(self.service.config.service_type.name),
            self.service.java_class_name,
            self.config_file_path(),
            str(base64.b64encode(json.dumps(self.service.params).encode('utf-8')), 'utf-8')
        ]

        cmd = "%s %s %s %s 2>&1 | tee -a %s &" % \
              (envs_to_exports(self.envs()),
               self.service.script("ignite.sh"),
               self._jvm_opts(),
               ",".join(args),
               os.path.join(self.service.log_dir, "console.log"))

        return cmd

    def config_file_path(self):
        return self.service.config_file if self.service.config.service_type == IgniteServiceType.NODE \
            else self.service.thin_client_config_file

    def libs(self):
        libs = super().libs()
        libs.extend(self.__jackson())

        return libs

    def __jackson(self):
        if not self.service.config.version.is_dev:
            aws = self._module("aws")
            return self.service.context.cluster.nodes[0].account.ssh_capture(
                "ls -d %s/* | grep jackson | tr '\n' ':' | sed 's/.$//'" % aws)

        return []

    def envs(self):
        return {**super().envs(), **{"MAIN_CLASS": self.service.main_java_class}}
