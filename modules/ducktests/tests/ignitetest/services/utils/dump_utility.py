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

from ignitetest.services.ignite_app import IgniteApplicationService


class DumpUtility:
    """
    Control the cache dump operations.
    """
    def __init__(self, test_context, cluster):
        self.cluster = cluster

        self.app = IgniteApplicationService(
            test_context,
            cluster.config._replace(client_mode=True),
            java_class_name="org.apache.ignite.internal.ducktest.tests.dump.DumpUtility"
        )

    def create(self, dump_name):
        """
        Create cache dump.
        :param dump_name: Name of the dump.
        """
        self.app.params = {
            "cmd": "create",
            "dumpName": dump_name
        }

        self.app.start(clean=False)

        self.app.wait()

        dump_create_time_ms = self.app.extract_result("DUMP_CREATE_TIME_MS")

        return int(dump_create_time_ms) if dump_create_time_ms != "" else None
