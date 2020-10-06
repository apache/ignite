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
This module contains the base class to build Ignite aware application written on java.
"""

# pylint: disable=R0901
from ignitetest.services.base_app import BaseIgniteService


class MultiNodeService(BaseIgniteService):
    """
    Ignite node service.
    """
    SERVICE_CLASS = "org.apache.ignite.internal.ducktest.utils.IgniteAwareApplicationService"

    # pylint: disable=R0913
    def __init__(self, context, config, java_class_name, num_nodes, params="", timeout_sec=60, modules=None,
                 servicejava_class_name=SERVICE_CLASS, jvm_opts=None):
        super().__init__(context, config, num_nodes=num_nodes, modules=modules,
                         servicejava_class_name=servicejava_class_name,
                         java_class_name=java_class_name, params=params, jvm_opts=jvm_opts, start_ignite=True)
