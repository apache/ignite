# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


from enum import IntFlag
from typing import Optional

from pyignite.constants import PROTOCOL_BYTE_ORDER


class BitmaskFeature(IntFlag):
    CLUSTER_API = 1 << 2

    def __bytes__(self) -> bytes:
        """
        Convert feature flags array to bytearray bitmask.

        :return: Bitmask as bytearray.
        """
        full_bytes = self.bit_length() // 8 + 1
        return self.to_bytes(full_bytes, byteorder=PROTOCOL_BYTE_ORDER)

    @staticmethod
    def all_supported() -> 'BitmaskFeature':
        """
        Get all supported features.

        :return: All supported features.
        """
        supported = BitmaskFeature(0)
        for feature in BitmaskFeature:
            supported |= feature
        return supported

    @staticmethod
    def from_array(features_array: bytes) -> Optional['BitmaskFeature']:
        """
        Get features from bytearray.

        :param features_array: Feature bitmask as array,
        :return: Return features.
        """
        if features_array is None:
            return None
        return BitmaskFeature.from_bytes(features_array, byteorder=PROTOCOL_BYTE_ORDER)
