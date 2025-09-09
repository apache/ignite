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
import importlib
from time import monotonic

from ducktape.cluster.remoteaccount import RemoteCommandError
from ducktape.tests.test import Test, TestContext

from ignitetest.services.utils.ducktests_service import DucktestsService

# globals:
JFR_ENABLED = "jfr_enabled"
IGNITE_TEST_CONTEXT_CLASS_KEY_NAME = "IgniteTestContext"
SAFEPOINT_LOGS_ENABLED = "safepoint_log_enabled"


class IgniteTestContext(TestContext):
    def __init__(self, test_context):
        super().__init__()
        self.__dict__.update(**test_context.__dict__)

    @property
    def available_cluster_size(self):
        return len(self.cluster)

    def before(self):
        pass

    def after(self, test_result):
        return test_result

    @staticmethod
    def resolve(test_context):
        if IGNITE_TEST_CONTEXT_CLASS_KEY_NAME in test_context.globals:
            fqdn = test_context.globals[IGNITE_TEST_CONTEXT_CLASS_KEY_NAME]
            (module, clazz) = fqdn.rsplit('.', 1)
            module = importlib.import_module(module)
            return getattr(module, clazz)(test_context)
        else:
            return IgniteTestContext(test_context)


class IgniteTest(Test):
    """
    Basic ignite test.
    """
    def __init__(self, test_context):
        assert isinstance(test_context, IgniteTestContext), \
            "any IgniteTest MUST BE decorated with the @ignitetest.utils.cluster decorator"

        super().__init__(test_context=test_context)

    @property
    def available_cluster_size(self):
        # noinspection PyUnresolvedReferences
        return self.test_context.available_cluster_size

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

    def tearDown(self):
        # jfr requires graceful shutdown to save the recording.
        if not self.test_context.globals.get(JFR_ENABLED, False):
            self.logger.debug("Killing all runned services to speed-up the tearing down.")

            for service in self.test_context.services._services.values():
                assert isinstance(service, DucktestsService)

                try:
                    service.kill()
                except RemoteCommandError:
                    pass  # Process may be already self-killed on segmentation.

                assert service.stopped

            self.logger.debug("All runned services killed.")

        super().tearDown()

    def _global_param(self, param_name, default=None):
        """Reads global parameter passed to the test suite."""
        return self.test_context.globals.get(param_name, default)

    def _global_int(self, param_name, default: int = None):
        """Reads global parameter passed to the test suite and converts to int."""
        return int(self._global_param(param_name, default))
