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
Checks JVM settings.
"""

from ignitetest.services.utils.jvm_utils import create_jvm_settings, merge_jvm_settings, DEFAULT_HEAP


class CheckJVMSettings:
    """
    Checks behavior of various tools.
    """

    # pylint: disable=R0201
    def check_list(self):
        """
        Checks list representation of JVM settings.
        """
        jvm_settings = create_jvm_settings()

        assert "-Xms" + DEFAULT_HEAP in jvm_settings
        assert "-Xmx" + DEFAULT_HEAP in jvm_settings

        jvm_settings = merge_jvm_settings(jvm_settings, additionals="-Xms981M -Xmx981M")

        assert "-Xms981M" in jvm_settings
        assert "-Xmx981M" in jvm_settings
        assert "-Xms" + DEFAULT_HEAP not in jvm_settings
        assert "-Xmx" + DEFAULT_HEAP not in jvm_settings

        jvm_settings = merge_jvm_settings(jvm_settings, additionals="-XX:ParallelGCThreads=1024")
        jvm_settings = merge_jvm_settings(jvm_settings, additionals="-xx:ParallelGCThreads=512")

        assert "-XX:ParallelGCThreads=1024" in jvm_settings
        assert "-XX:ParallelGCThreads=512" not in jvm_settings
