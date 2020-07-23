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

from ignitetest.services.utils.ignite_aware_app import IgniteAwareApplicationService
from ignitetest.tests.utils.version import DEV_BRANCH

"""
The Ignite application service allows to perform custom logic writen on java.
"""


class IgniteApplicationService(IgniteAwareApplicationService):
    service_java_class_name = "org.apache.ignite.internal.ducktest.utils.IgniteApplicationService"

    def __init__(self, context, java_class_name, version=DEV_BRANCH, properties="", params="", timeout_sec=60):
        super(IgniteApplicationService, self).__init__(context, java_class_name, version, properties, params,
                                                       timeout_sec, self.service_java_class_name)
