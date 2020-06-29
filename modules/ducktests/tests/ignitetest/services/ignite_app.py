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

from ducktape.services.service import Service

from ignitetest.services.utils.ignite_aware_app import IgniteAwareApplicationService
from ignitetest.version import DEV_BRANCH

"""
The Ignite application service allows to perform custom logic writen on java.
"""


class IgniteApplicationService(IgniteAwareApplicationService):
    def __init__(self, context, java_class_name, version=DEV_BRANCH, properties="", params="", timeout_sec=60):
        IgniteAwareApplicationService.__init__(
            self, context, java_class_name, version, properties, params, timeout_sec,
            service_java_class_name="org.apache.ignite.internal.ducktest.utils.IgniteApplicationService")

    def start(self):
        Service.start(self)

        self.logger.info("Waiting for Ignite Application (%s) to start..." % self.java_class_name)

        self.await_event("Topology snapshot", self.timeout_sec, from_the_beginning=True)
        self.await_event("IGNITE_APPLICATION_INITIALIZED", self.timeout_sec, from_the_beginning=True)
