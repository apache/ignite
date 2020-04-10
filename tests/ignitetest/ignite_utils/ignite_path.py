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

import importlib
import os

from ignitetest.version import get_version, IgniteVersion, DEV_BRANCH


"""This module serves a few purposes:

First, it gathers information about path layout in a single place, and second, it
makes the layout of the Ignite installation pluggable, so that users are not forced
to use the layout assumed in the IgnitePathResolver class.
"""

SCRATCH_ROOT = "/mnt"
IGNITE_INSTALL_ROOT = "/opt"

def create_path_resolver(context, project="ignite"):
    """Factory for generating a path resolver class

    This will first check for a fully qualified path resolver classname in context.globals.

    If present, construct a new instance, else default to IgniteSystemTestPathResolver
    """
    assert project is not None

    resolver_fully_qualified_classname = "ignitetest.ignite_utils.ignite_path.IgniteSystemTestPathResolver"
    # Using the fully qualified classname, import the resolver class
    (module_name, resolver_class_name) = resolver_fully_qualified_classname.rsplit('.', 1)
    cluster_mod = importlib.import_module(module_name)
    path_resolver_class = getattr(cluster_mod, resolver_class_name)
    path_resolver = path_resolver_class(context, project)

    return path_resolver


class IgnitePathResolverMixin(object):
    """Mixin to automatically provide pluggable path resolution functionality to any class using it.

    Keep life simple, and don't add a constructor to this class:
    Since use of a mixin entails multiple inheritence, it is *much* simpler to reason about the interaction of this
    class with subclasses if we don't have to worry about method resolution order, constructor signatures etc.
    """

    @property
    def path(self):
        if not hasattr(self, "_path"):
            setattr(self, "_path", create_path_resolver(self.context, "ignite"))
            if hasattr(self.context, "logger") and self.context.logger is not None:
                self.context.logger.debug("Using path resolver %s" % self._path.__class__.__name__)

        return self._path


class IgniteSystemTestPathResolver(object):
    """Path resolver for Ignite system tests which assumes the following layout:

        /opt/ignite-dev          # Current version of Ignite under test
        /opt/ignite-2.7.6        # Example of an older version of Ignite installed from tarball
        /opt/ignite-<version>    # Other previous versions of Ignite
        ...
    """
    def __init__(self, context, project="ignite"):
        self.context = context
        self.project = project

    def home(self, node_or_version=DEV_BRANCH, project=None):
        version = self._version(node_or_version)
        home_dir = project or self.project
        if version is not None:
            home_dir += "-%s" % str(version)

        return os.path.join(IGNITE_INSTALL_ROOT, home_dir)

    def bin(self, node_or_version=DEV_BRANCH, project=None):
        version = self._version(node_or_version)
        return os.path.join(self.home(version, project=project), "bin")

    def script(self, script_name, node_or_version=DEV_BRANCH, project=None):
        version = self._version(node_or_version)
        return os.path.join(self.bin(version, project=project), script_name)

    def jar(self, jar_name, node_or_version=DEV_BRANCH, project=None):
        version = self._version(node_or_version)
        return os.path.join(self.home(version, project=project), JARS[str(version)][jar_name])

    def scratch_space(self, service_instance):
        return os.path.join(SCRATCH_ROOT, service_instance.service_id)

    def _version(self, node_or_version):
        if isinstance(node_or_version, IgniteVersion):
            return node_or_version
        else:
            return get_version(node_or_version)

