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
from ducktape.mark._mark import Ignore, Mark

from ignitetest.utils.version import IgniteVersion


class VersionIf(Ignore):
    """
    Ignore test if version doesn't corresponds to condition.
    """
    def __init__(self, condition):
        super(VersionIf, self).__init__()
        self.condition = condition

    def apply(self, seed_context, context_list):
        assert len(context_list) > 0, "ignore_if decorator is not being applied to any test cases"

        for ctx in context_list:
            assert 'version' in ctx.injected_args, "'version' in injected args not present"
            version = ctx.injected_args['version']
            assert isinstance(version, str), "'version' in injected args must be a string"
            ctx.ignore = ctx.ignore or not self.condition(IgniteVersion(version))

        return context_list

    def __eq__(self, other):
        return super(VersionIf, self).__eq__(other) and self.condition == other.condition


def version_if(condition):
    """
    Mark decorated test method as IGNORE if version doesn't corresponds to condition.

    :param condition: function(IgniteVersion) -> bool
    """
    def ignorer(func):
        Mark.mark(func, VersionIf(condition))
        return func

    return ignorer
