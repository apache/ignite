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

from collections.abc import Iterable

from ducktape.mark._mark import Ignore, Mark, _inject

from ignitetest.utils.version import IgniteVersion, ALL_VERSIONS_STR


class VersionIf(Ignore):
    """
    Ignore test if version doesn't corresponds to condition.
    """
    def __init__(self, condition, variable_name):
        super().__init__()
        self.condition = condition
        self.variable_name = variable_name

    def apply(self, seed_context, context_list):
        assert len(context_list) > 0, "ignore_if decorator is not being applied to any test cases"

        for ctx in context_list:
            if self.variable_name in ctx.injected_args:
                version = ctx.injected_args[self.variable_name]
                assert isinstance(version, str), "'%s'n injected args must be a string" % (self.variable_name,)
                ctx.ignore = ctx.ignore or not self.condition(IgniteVersion(version))

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


def version_if(condition, variable_name='ignite_version'):
    """
    Mark decorated test method as IGNORE if version doesn't corresponds to condition.

    :param condition: function(IgniteVersion) -> bool
    :param variable_name: version variable name
    """
    def ignorer(func):
        Mark.mark(func, VersionIf(condition, variable_name))
        return func

    return ignorer


def version_with_previous(*args, version_prefix="ignite_version"):
    """
    Decorate test function to inject ignite versions. Versions will be overriden by globals "ignite_versions" param.
    :param args: Can be string, tuple of strings or iterable of them.
    :param version_prefix: prefix for variable to inject into test function.
    """
    res = []
    for v_arg in args:
        for v_all in ALL_VERSIONS_STR:
            if v_all <= v_arg:
                res.append((v_arg, v_all))

    def parametrizer(func):
        Mark.mark(func, IgniteVersionParametrize(*res, version_prefix=version_prefix))

        return func

    return parametrizer
