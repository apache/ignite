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

import re
from dataclasses import dataclass
from typing import List


@dataclass(frozen=True)
class IptablesMarkRule:
    destination: str
    mark: int
    packets: int
    bytes: int


_MARK_RE = re.compile(
    r"""
    ^\s*
    (?P<pkts>\d+)\s+
    (?P<bytes>\d+)\S*\s+
    MARK\s+
    .*?
    (?P<dst>\d+\.\d+\.\d+\.\d+)\s+
    MARK\s+set\s+0x(?P<mark>[0-9a-fA-F]+)
    """,
    re.VERBOSE,
)


def parse_iptables_marks(output: str) -> List[IptablesMarkRule]:
    rules = []

    for line in output.splitlines():
        match = _MARK_RE.search(line)
        if not match:
            continue

        rules.append(
            IptablesMarkRule(
                destination=match.group("dst"),
                mark=int(match.group("mark"), 16),
                packets=int(match.group("pkts")),
                bytes=int(match.group("bytes")),
            )
        )

    return rules