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
This module contains class to start ignite cluster node.
"""

from ignitetest.services.utils.ignite_aware import IgniteAwareService


class IgniteService(IgniteAwareService):
    """
    Ignite node service.
    """
    APP_SERVICE_CLASS = "org.apache.ignite.startup.cmdline.CommandLineStartup"

    def __init__(self, context, config, num_nodes, jvm_opts=None, merge_with_default=True, startup_timeout_sec=60,
                 shutdown_timeout_sec=60, modules=None, main_java_class=APP_SERVICE_CLASS):
        super().__init__(context, config, num_nodes, startup_timeout_sec, shutdown_timeout_sec, main_java_class,
                         modules, jvm_opts=jvm_opts, merge_with_default=merge_with_default)
