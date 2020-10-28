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
import os
import random
import string
from time import monotonic

from ducktape.tests.test import Test


# pylint: disable=W0223
class IgniteTest(Test):
    """
    Basic ignite test.
    """
    def __init__(self, test_context):
        super().__init__(test_context=test_context)

        self.tmp_path_root = None

    def setup(self):
        super().setup()

        self.tmp_path_root = os.path.join("/tmp", ''.join(random.choices(string.ascii_letters + string.digits, k=10)),
                                          self.test_context.cls_name)

        self.clear_tmp_dir(True)

    def teardown(self):
        self.clear_tmp_dir()

        super().teardown()

    def clear_tmp_dir(self, recreate=False):
        """Creates temporary directory for current test."""
        for node in self.test_context.cluster.nodes:
            node.account.ssh_client.exec_command("rm -drf " + self.tmp_path_root)

            if recreate:
                node.account.ssh_client.exec_command("mkdir -p " + self.tmp_path_root)

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
