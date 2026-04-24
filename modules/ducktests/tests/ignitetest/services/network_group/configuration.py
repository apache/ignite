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

from dataclasses import dataclass
from typing import Optional, Dict


@dataclass
class CrossNetworkGroupConfiguration:
    """
    Defines the network impairment profile between two network groups.
    """
    delay: Optional[str] = None   # e.g., "100ms"
    loss: Optional[float] = None  # e.g., 0.1 (10%)
    rate: str = "1gbit"           # Default to high-speed interface


class NetworkGroupStore:
    """
    A registry for managing traffic impairments between different network groups.
    """
    def __init__(self):
        self.matrix: Dict[str, Dict[str, CrossNetworkGroupConfiguration]] = {}

    def set_config(self, group_a: str, group_b: str, impairment: CrossNetworkGroupConfiguration):
        """
        Sets bidirectional rules between two network groups.

        Args:
            group_a: The first network group identifier.
            group_b: The second network group identifier.
            impairment: The :class:`CrossNetworkGroupConfiguration` applied to all cross-group traffic directions.
        """
        for src, dst in [(group_a, group_b), (group_b, group_a)]:
            self.matrix.setdefault(src, {})[dst] = impairment

    def get_config(self, src_group: str, dst_group: str) -> Optional[CrossNetworkGroupConfiguration]:
        """
        :return: :class:`CrossNetworkGroupConfiguration` for traffic from src to dst or None if not defined.
        """
        return self.matrix.get(src_group, {}).get(dst_group)