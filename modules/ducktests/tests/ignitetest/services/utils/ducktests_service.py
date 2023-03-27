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
Base service for all services.
"""

from abc import ABCMeta

from ducktape.services.service import Service


class DucktestsService(Service, metaclass=ABCMeta):
    """DucktestsService provides common semantic for all services."""

    def __init__(self, context, num_nodes=None, cluster_spec=None, **kwargs):
        super().__init__(context, num_nodes, cluster_spec, **kwargs)

        self.stopped = False

    def start(self, **kwargs):
        self.stopped = False

        super().start(**kwargs)

    def stop(self, force_stop=False, **kwargs):
        if self.stopped:
            return

        self.stopped = True

        super().stop(force_stop=force_stop, **kwargs)

    def stop_node(self, node, force_stop=False, **kwargs):
        super().stop_node(node, force_stop=force_stop, **kwargs)

    def kill(self):
        """
        Kills the service.
        """
        self.stop(force_stop=True)

    def clean_node(self, node, **kwargs):
        pass
