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
This module contains various syntaxic sugar for enums
"""

from enum import IntEnum


def constructible(cls):
    """
    If decorate IntEnum subclass with this decorator, following is possible:

    @constructible
    class StopType(IntEnum):
        SIGTERM = 0
        SIGKILL = 1
        DISCONNECT = 2

    stop_type = 1

    with StopType.construct_from(stop_type) as sp:
        if sp is StopType.SIGKILL:
            print('got sigkill')

    stop_type = 'DISCONNECT'

    with StopType.construct_from(stop_type) as sp:
        if sp is StopType.DISCONNECT:
            print('got disconnect')

    """
    assert issubclass(cls, IntEnum)

    members = dict(cls.__members__.items())

    def construct_from(val):
        if isinstance(val, str):
            return members[val]
        return cls.__new__(cls, val)

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        pass

    cls.construct_from = construct_from
    cls.__enter__ = __enter__
    cls.__exit__ = __exit__

    return cls
