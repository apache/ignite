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

from ducktape.cluster.remoteaccount import RemoteCommandError
from ducktape.tests.test import Test

# pylint: disable=W0223
from ignitetest.services.utils.ignite_aware import IgniteAwareService


class IgniteTest(Test):
    """
    Basic ignite test.
    """
    def __init__(self, test_context):
        super().__init__(test_context=test_context)

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

    # pylint: disable=W0212
    def tearDown(self):
        self.logger.debug("Killing all runned IgniteAwareServices to speed-up the tearing down.")

        for service in self.test_context.services._services.values():
            if isinstance(service, IgniteAwareService) and not service.stopped:
                try:
                    service.stop_async(force_stop=True)
                except RemoteCommandError:
                    pass  # Process may be already self-killed on segmentation.

        self.logger.debug("All runned IgniteAwareServices killed.")

        for service in self.test_context.services._services.values():
            if isinstance(service, IgniteAwareService):
                service.await_stopped(force_stop=True)

        self.logger.debug("All IgniteAwareServices checked to be killed.")

        super().tearDown()

    def _global_param(self, param_name, default=None):
        """Reads global parameter passed to the test suite."""
        return self.test_context.globals.get(param_name, default)

    def _global_int(self, param_name, default: int = None):
        """Reads global parameter passed to the test suite and converts to int."""
        return int(self._global_param(param_name, default))
