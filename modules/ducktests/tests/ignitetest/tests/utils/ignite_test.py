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
This module contains basic ignite test.
"""
from time import monotonic

from ducktape.tests.test import Test


# pylint: disable=W0223
class IgniteTest(Test):
    """
    Basic ignite test.
    """
    def __init__(self, test_context):
        super(IgniteTest, self).__init__(test_context=test_context)

    def stage(self, msg):
        """
        Print stage mark.
        :param msg: Stage mark message.
        """
        self.logger.info("[TEST_STAGE] " + msg)

    @staticmethod
    def monotonic():
        """
        monotonic() -> float

        :return:
            The value (in fractional seconds) of a monotonic clock, i.e. a clock that cannot go backwards.
            The clock is not affected by system clock updates. The reference point of the returned value is undefined,
            so that only the difference between the results of consecutive calls is valid.
        """
        return monotonic()
