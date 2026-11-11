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
Checks sending a signal to a node's Ignite process.
"""

import signal
from types import SimpleNamespace

from ignitetest.services.utils.ignite_aware import IgniteAwareService


class FakeAccount:
    """
    Remote account that records the signals sent through it.
    """
    def __init__(self):
        self.sent = []

    def signal(self, pid, sig, allow_fail=False):
        """Records the signal."""
        self.sent.append((pid, sig, allow_fail))


def check_signals_every_ignite_process():
    """Each process of the service's main class gets the signal, and a failed send fails the call."""
    node = SimpleNamespace(account=FakeAccount())

    service = SimpleNamespace(main_java_class="Main",
                              pids=lambda n, cls: [11, 12] if n is node and cls == "Main" else [])

    IgniteAwareService.signal_node(service, node, signal.SIGSEGV)

    assert node.account.sent == [(11, signal.SIGSEGV, False), (12, signal.SIGSEGV, False)]
