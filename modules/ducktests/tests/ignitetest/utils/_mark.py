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
Module contains useful test decorators.
"""

import copy
from collections.abc import Iterable

from ducktape.cluster.cluster_spec import ClusterSpec
from ducktape.mark._mark import Ignore, Mark, _inject

from ignitetest.utils.version import IgniteVersion
from ignitetest.utils.ignite_test import IgniteTestContext


class IgnoreIf(Ignore):
    """
    Ignore test if version or global parameters correspond to condition.
    """
    def __init__(self, condition, variable_name):
        super().__init__()
        self.condition = condition
        self.variable_name = variable_name

    def apply(self, seed_context, context_list):
        assert len(context_list) > 0, "ignore if decorator is not being applied to any test cases"

        for ctx in context_list:
            if self.variable_name in ctx.injected_args:
                version = ctx.injected_args[self.variable_name]
                assert isinstance(version, str), "'%s'n injected args must be a string" % (self.variable_name,)
                ctx.ignore = ctx.ignore or self.condition(IgniteVersion(version), ctx.globals)

        return context_list

    def __eq__(self, other):
        return super().__eq__(other) and self.condition == other.condition


class IgniteVersionParametrize(Mark):
    """
    Parametrize function with ignite_version
    """
    def __init__(self, *args, version_prefix):
        """
        :param args: Can be string, tuple of strings or iterable of them.
        :param version_prefix: prefix for variable to inject into test function.
        """
        self.versions = list(args)
        self.version_prefix = version_prefix

    def apply(self, seed_context, context_list):
        if 'ignite_versions' in seed_context.globals:
            ver = seed_context.globals['ignite_versions']
            if isinstance(ver, str):
                self.versions = [ver]
            elif isinstance(ver, Iterable):
                self.versions = list(ver)
            else:
                raise AssertionError("Expected string or iterable as parameter in ignite_versions, "
                                     "%s of type %s passed" % (ver, type(ver)))

        self.versions = self._inject_global_project(self.versions, seed_context.globals.get("project"))

        new_context_list = []
        if context_list:
            for ctx in context_list:
                for version in self.versions:
                    if self._check_injected(ctx):
                        continue

                    new_context_list.insert(0, self._prepare_new_ctx(version, seed_context, ctx))
        else:
            for version in self.versions:
                new_context_list.insert(0, self._prepare_new_ctx(version, seed_context))

        return new_context_list

    @staticmethod
    def _inject_global_project(version, project):
        if isinstance(version, (list, tuple)):
            return list(map(lambda v: IgniteVersionParametrize._inject_global_project(v, project), version))

        if (version.lower() == "dev" or version[0].isdigit()) and project:
            version = "%s-%s" % (project, version)

        return version

    def _prepare_new_ctx(self, version, seed_context, ctx=None):
        injected_args = dict(ctx.injected_args) if ctx and ctx.injected_args else {}

        if isinstance(version, (list, tuple)) and len(version) >= 2:
            for idx, ver in enumerate(version):
                injected_args["%s_%s" % (self.version_prefix, idx + 1)] = ver
        elif isinstance(version, str):
            injected_args[self.version_prefix] = version
        else:
            raise AssertionError("Expected string or iterable of size greater than 2 as element in ignite_versions,"
                                 "%s of type %s passed" % (version, type(version)))

        injected_fun = _inject(**injected_args)(seed_context.function)

        return seed_context.copy(function=injected_fun, injected_args=injected_args)

    def _check_injected(self, ctx):
        if ctx.injected_args:
            for arg_name in ctx.injected_args.keys():
                if arg_name.startswith(self.version_prefix):
                    return True

        return False

    @property
    def name(self):
        """
        This function should return "PARAMETRIZE" string in order to ducktape's method parametrization works.
        Should be fixed after apropriate patch in ducktape will be merged.
        """
        return "PARAMETRIZE"

    def __eq__(self, other):
        return super().__eq__(other) and self.versions == other.versions


CLUSTER_SPEC_KEYWORD = "cluster_spec"
CLUSTER_SIZE_KEYWORD = "num_nodes"


class ParametrizableClusterMetadata(Mark):
    """Provide a hint about how a given test will use the cluster."""

    def __init__(self, **kwargs):
        self.metadata = copy.copy(kwargs)

    @property
    def name(self):
        return "PARAMETRIZABLE_RESOURCE_HINT_CLUSTER_USE"

    def apply(self, seed_context, context_list):
        assert len(context_list) > 0, "parametrizable cluster use annotation is not being applied to any test cases"

        cluster_size_param = self._extract_cluster_size(seed_context)

        for ctx in context_list:
            if cluster_size_param:
                self._modify_metadata(cluster_size_param)

            if not ctx.cluster_use_metadata:
                ctx.cluster_use_metadata = self.metadata

        return list(map(lambda _ctx: IgniteTestContext.resolve(_ctx), context_list))

    @staticmethod
    def _extract_cluster_size(seed_context):
        cluster_size = seed_context.globals.get('cluster_size')

        if cluster_size is None:
            return None

        res = int(cluster_size) if isinstance(cluster_size, str) else cluster_size

        if not isinstance(res, int):
            raise ValueError(f"Expected string or int param, {cluster_size} of {type(cluster_size)} passed instead.")

        if res <= 0:
            raise ValueError(f"Expected cluster_size greater than 0, {cluster_size} passed instead.")

        return res

    def _modify_metadata(self, new_size):
        cluster_spec = self.metadata.get(CLUSTER_SPEC_KEYWORD)
        cluster_size = self.metadata.get(CLUSTER_SIZE_KEYWORD)

        if cluster_spec is not None and not cluster_spec.empty():
            node_spec = next(iter(cluster_spec))
            self.metadata[CLUSTER_SPEC_KEYWORD] = ClusterSpec.from_nodes([node_spec] * new_size)
        elif cluster_size is not None:
            self.metadata[CLUSTER_SIZE_KEYWORD] = new_size


def ignite_versions(*args, version_prefix="ignite_version"):
    """
    Decorate test function to inject ignite versions. Versions will be overriden by globals "ignite_versions" param.
    :param args: Can be string, tuple of strings or iterable of them.
    :param version_prefix: prefix for variable to inject into test function.
    """
    def parametrizer(func):
        Mark.mark(func, IgniteVersionParametrize(*args, version_prefix=version_prefix))

        return func
    return parametrizer


def ignore_if(condition, *, variable_name='ignite_version'):
    """
    Mark decorated test method as IGNORE if version or global parameters correspond to condition.

    :param condition: function(IgniteVersion, Globals) -> bool
    :param variable_name: version variable name
    """
    def ignorer(func):
        Mark.mark(func, IgnoreIf(condition, variable_name))
        return func

    return ignorer


def cluster(**kwargs):
    """Test method decorator used to provide hints about how the test will use the given cluster.

    :Keywords:

      - ``num_nodes`` provide hint about how many nodes the test will consume
      - ``cluster_spec`` provide hint about how many nodes of each type the test will consume
    """
    def cluster_use_metadata_adder(func):
        def extended_test(self, *args, **kwargs):
            self.test_context.before()
            test_result = func(self, *args, **kwargs)
            return self.test_context.after(test_result)

        extended_test.__dict__.update(**func.__dict__)
        extended_test.__name__ = func.__name__

        Mark.mark(extended_test, ParametrizableClusterMetadata(**kwargs))
        return extended_test

    return cluster_use_metadata_adder
