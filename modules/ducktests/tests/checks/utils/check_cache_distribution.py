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
Checks parsing of 'control.sh --cache distribution' output.

The user attribute values in a row are emitted in the iteration order of the node's
attribute map on the control.sh side, which matches neither the --user-attributes
argument order nor alphabetical order. The header line is printed from the keys of
that same map, so these checks pin down that the names come from the header and not
from the requested list.
"""

import pytest

from ignitetest.services.utils.control_utility import ControlUtility

HEADER = "[groupId,partition,nodeId,primary,state,updateCounter,partitionSize,nodeAddresses"

GROUP = "[next group: id=-1368047377, name=mdc-cache]"


def _parse(output, user_attributes=None):
    # pylint: disable=protected-access
    return ControlUtility._ControlUtility__parse_cache_distribution(output, user_attributes)


def _copies(distribution, group="mdc-cache", partition=0):
    return distribution.groups[group].partitions[partition]


class CheckCacheDistribution:
    """
    Checks the distribution output parser.
    """
    def check_no_user_attributes(self):
        """Without --user-attributes the header has no trailing names and rows carry none."""
        distribution = _parse("\n".join([
            HEADER + "]",
            GROUP,
            "-1368047377,0,a1b2c3d4,P,OWNING,42,100,[127.0.0.1, 10.0.0.1]"
        ]))

        copy = _copies(distribution)[0]

        assert copy.node_id == "a1b2c3d4"
        assert copy.primary
        assert copy.state == "OWNING"
        assert copy.update_counter == 42
        assert copy.partition_size == 100
        assert copy.node_addresses == ["127.0.0.1", "10.0.0.1"]
        assert copy.user_attributes == {}

    def check_single_user_attribute(self):
        """The one-attribute case all current callers use."""
        distribution = _parse("\n".join([
            HEADER + ",IGNITE_DATA_CENTER_ID]",
            GROUP,
            "-1368047377,0,a1b2c3d4,P,OWNING,42,100,[127.0.0.1],DC1",
            "-1368047377,0,e5f6a7b8,B,OWNING,42,100,[10.0.0.1],DC2"
        ]), user_attributes=["IGNITE_DATA_CENTER_ID"])

        primary, backup = _copies(distribution)

        assert primary.user_attributes == {"IGNITE_DATA_CENTER_ID": "DC1"}
        assert backup.user_attributes == {"IGNITE_DATA_CENTER_ID": "DC2"}

    def check_attribute_names_follow_the_header_not_the_request(self):
        """
        The regression the header-driven parse exists for: the map order on the control.sh
        side is neither the requested order nor alphabetical, so zipping the values against
        a sorted request list mis-assigns every value.
        """
        distribution = _parse("\n".join([
            HEADER + ",ZONE,DC]",
            GROUP,
            "-1368047377,0,a1b2c3d4,P,OWNING,42,100,[127.0.0.1],east,DC1"
        ]), user_attributes=["DC", "ZONE"])

        assert _copies(distribution)[0].user_attributes == {"ZONE": "east", "DC": "DC1"}

    def check_missing_attribute_value_keeps_columns_aligned(self):
        """A node lacking an attribute prints an empty field, it does not drop the column."""
        distribution = _parse("\n".join([
            HEADER + ",ZONE,DC]",
            GROUP,
            "-1368047377,0,a1b2c3d4,P,OWNING,42,100,[127.0.0.1],,DC1"
        ]), user_attributes=["DC", "ZONE"])

        assert _copies(distribution)[0].user_attributes == {"ZONE": "", "DC": "DC1"}

    def check_requested_attribute_absent_from_header_fails(self):
        """A silently dropped --user-attributes argument must not parse as success."""
        with pytest.raises(AssertionError, match="missing from the distribution output"):
            _parse("\n".join([
                HEADER + ",DC]",
                GROUP,
                "-1368047377,0,a1b2c3d4,P,OWNING,42,100,[127.0.0.1],DC1"
            ]), user_attributes=["DC", "ZONE"])

    def check_multiple_groups_and_partitions(self):
        """Rows are filed under the group header that precedes them."""
        distribution = _parse("\n".join([
            HEADER + ",DC]",
            GROUP,
            "-1368047377,0,a1b2c3d4,P,OWNING,42,100,[127.0.0.1],DC1",
            "-1368047377,1,e5f6a7b8,P,OWNING,7,50,[10.0.0.1],DC2",
            "[next group: id=42, name=other-cache]",
            "42,0,a1b2c3d4,P,MOVING,1,10,[127.0.0.1],DC1"
        ]), user_attributes=["DC"])

        assert sorted(distribution.groups) == ["mdc-cache", "other-cache"]
        assert sorted(distribution.groups["mdc-cache"].partitions) == [0, 1]

        assert _copies(distribution, partition=1)[0].update_counter == 7
        assert _copies(distribution, "other-cache")[0].state == "MOVING"
