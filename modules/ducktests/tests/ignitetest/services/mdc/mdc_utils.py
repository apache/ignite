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
MDC helpers that need no cluster.
"""
from typing import Dict, List, Sequence, Union

DATA_CENTER_ATTR = "IGNITE_DATA_CENTER_ID"
IGNITE_SQL_RETRY_TIMEOUT_ATTR = "IGNITE_SQL_RETRY_TIMEOUT"

IGNITE_SQL_RETRY_TIMEOUT_MS = 1_000


def dc_jvm_opts(dc: str) -> List[str]:
    """
    :return: JVM options assigning a node to the given data center.
    """
    return [f"-D{DATA_CENTER_ATTR}={dc}", f"-D{IGNITE_SQL_RETRY_TIMEOUT_ATTR}={IGNITE_SQL_RETRY_TIMEOUT_MS}"]


def per_dc(value: Union[int, Dict[str, int]], dcs: Sequence[str]) -> Dict[str, int]:
    """
    Normalizes an int-or-dict per-DC count into a dict, e.g. 3 -> {DC1: 3, DC2: 3}.
    """
    return dict(value) if isinstance(value, dict) else {dc: value for dc in dcs}


def assert_cross_dc_distribution_by_attribute(distribution, dc_attr, expected_dcs, owning_only=True,
                                              copies_per_dc=None):
    """
    Asserts that every partition of every cache group has at least one copy in every DC,
    using a node attribute (requested via --user-attributes) as the DC marker.

    :param distribution: CacheDistribution returned by ControlUtility.cache_distribution(),
                         requested with user_attributes=[dc_attr].
    :param dc_attr: Attribute name holding the DC id, e.g. "IGNITE_DATA_CENTER_ID".
    :param expected_dcs: Collection of DC ids that must own a copy of every partition.
    :param owning_only: Count only copies in OWNING state as present.
    :param copies_per_dc: If set, each DC must hold exactly this many copies of every
                          partition - the MdcAffinityBackupFilter guarantee
                          ``(backups + 1) / dcsNum``.
    """
    def dc_of(copy):
        return copy.user_attributes.get(dc_attr)

    _assert_cross_dc(distribution, set(expected_dcs), dc_of, owning_only, copies_per_dc,
                     layout_hint=f"DC attribute: {dc_attr}, expected DCs: {sorted(expected_dcs)}")


def _assert_cross_dc(distribution, expected_dcs, dc_of, owning_only, copies_per_dc, layout_hint):
    violations = []

    for group in distribution.groups.values():
        for part, copies in sorted(group.partitions.items()):
            counted = [c for c in copies if not owning_only or c.state == "OWNING"]

            per_dc_cnt = {dc: 0 for dc in expected_dcs}

            for copy in counted:
                dc = dc_of(copy)

                if dc in per_dc_cnt:
                    per_dc_cnt[dc] += 1

            missing = {dc for dc, cnt in per_dc_cnt.items() if cnt == 0}

            unbalanced = {} if copies_per_dc is None else \
                {dc: cnt for dc, cnt in per_dc_cnt.items() if cnt != copies_per_dc}

            if missing or unbalanced:
                copies_dump = ", ".join(
                    f"{c.node_id}({'P' if c.primary else 'B'},{c.state},dc={dc_of(c)},{c.node_addresses})"
                    for c in copies)

                problems = []

                if missing:
                    problems.append(f"missing DCs={sorted(missing)}")

                if unbalanced:
                    problems.append(f"copies per DC != {copies_per_dc}: {unbalanced}")

                violations.append(f"group={group.name}(id={group.group_id}), partition={part}, "
                                  f"{', '.join(problems)}, copies=[{copies_dump}]")

    assert not violations, \
        "Partition distribution is not cross-DC:\n  " + "\n  ".join(violations) + "\n" + layout_hint
