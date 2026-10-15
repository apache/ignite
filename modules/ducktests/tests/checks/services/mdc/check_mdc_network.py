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
Checks the network between the DCs of an MDC cluster.
"""
from itertools import combinations

from ignitetest.services.mdc.mdc_cluster import cross_dc_network, DCS_3


class CheckMdcNetwork:
    """
    Checks the cross-DC impairments.
    """
    def check_the_delay_reaches_every_dc_pair(self, new_mdc):
        """Every pair of DCs gets the delay."""
        store = cross_dc_network(None, new_mdc(dcs=DCS_3), delay_ms=100).network_group_store

        for dc_a, dc_b in combinations(DCS_3, 2):
            assert store.get_config(dc_a, dc_b).delay == "100ms"
