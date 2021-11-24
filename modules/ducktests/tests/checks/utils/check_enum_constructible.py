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
Checks IntEnum enhancement.
"""

from enum import IntEnum

import pytest
from ignitetest.utils.enum import constructible


@constructible
class ConnectType(IntEnum):
    """
    Example of IntEnum.
    """
    UDP = 0
    TCP = 1
    HTTP = 2


check_params = []

for name, value in ConnectType.__members__.items():
    check_params.append([name, value])
    check_params.append([int(value), value])
    check_params.append([value, value])


class CheckEnumConstructible:
    """
    Basic test of IntEnum decorated with @constructible.
    """
    @pytest.mark.parametrize(
        ['input_value', 'expected_value'],
        check_params
    )
    def check_construct_from(self, input_value, expected_value):
        """Basic checks."""
        with ConnectType.construct_from(input_value) as conn_type:
            assert conn_type is expected_value

    @pytest.mark.parametrize(
        ['input_value'],
        [[val] for val in [-1, .6, 'test']]
    )
    def check_invalid_input(self, input_value):
        """Check invalid input."""
        with pytest.raises(Exception):
            ConnectType.construct_from(input_value)

    def check_invalid_usage(self):
        """Check invalid type decoration."""
        with pytest.raises(AssertionError):
            class SimpleClass:
                """Cannot be decorated"""

            constructible(SimpleClass)
