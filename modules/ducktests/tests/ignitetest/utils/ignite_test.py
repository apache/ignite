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
from ignitetest.utils.pause import DemoPause

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

        self.__demo_pause = None

        # Stamped here rather than at the first breakpoint: it is what demo breakpoints count
        # their elapsed time from, and what they measure the runner budget against, and both
        # of those mean the start of the test - setup included. This is deliberately
        # conservative: ducktape's kill timer is actually reset by the last client event
        # before a pause, which fires after setup, so the real window is longer - but
        # anchoring at construction needs no hook into the runner client and never
        # overestimates the budget.
        self.__started_at = monotonic()

    def pause(self, name, *describers):
        """
        Holds the scenario at a named demo breakpoint until it is resumed from the host, so
        that the cluster can be shown in exactly this state. Does nothing at all unless the
        `demo_pause` global names this breakpoint - see :mod:`ignitetest.utils.pause`.

        Must be called from the test body: :meth:`tearDown` kills every service, so a
        breakpoint placed after the body would only ever show a dead cluster.

        :param name: Breakpoint name, matched against the `demo_pause` global.
        :param describers: Objects exposing `describe() -> list of str`, each contributing a
               section to the banner shown while paused, on top of the service list every
               breakpoint reports. Any fixture a test drives can implement it.
        """
        if self.__demo_pause is None:
            self.__demo_pause = DemoPause(self.logger, self.test_context.globals, self.test_context.test_name,
                                          started_at=self.__started_at,
                                          runner_timeout_sec=self.__runner_timeout_sec())

        self.__demo_pause.pause(name, describers, self.test_context.services)

    def __runner_timeout_sec(self):
        """
        :return: Ducktape's `--test-runner-timeout` in seconds, None when the session carries
                 none. A breakpoint has to give up inside it, since the runner kills a test
                 client it hears nothing from for that long - see
                 :meth:`ignitetest.utils.pause.DemoPause._budgeted_timeout`.
        """
        session_context = getattr(self.test_context, "session_context", None)
        timeout_ms = getattr(session_context, "test_runner_timeout", None)

        return timeout_ms / 1000 if timeout_ms else None

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
