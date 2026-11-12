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
Checks the check of a service's logs for suspicious lines.
"""

from types import SimpleNamespace

from ignitetest.services.utils.ignite_aware import IgniteAwareService


def check_every_pattern_in_every_log():
    """Each suspicious pattern is looked for in every log of the nodes, including the ones rotated by a restart."""
    calls = []

    service = SimpleNamespace(check_event_absent=lambda pattern, log_file=None: calls.append((pattern, log_file)))

    IgniteAwareService.check_logs_clean(service)

    assert calls == [(pattern, "ignite*.log*") for pattern in (
        "long running transactions", "long running explicit locks", "long running cache futures",
        "Failed to wait for partition map exchange", "Detected lost partitions", "AssertionError")]
