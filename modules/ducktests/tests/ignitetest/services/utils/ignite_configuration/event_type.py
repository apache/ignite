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
# limitations under the License

from enum import Enum, auto


class EventType(Enum):
    """
    Helper for includeEventTypes property in XML configuration
    """

    EVT_CACHE_STARTED = auto()
    EVT_CACHE_STOPPED = auto()
    EVT_CHECKPOINT_SAVED = auto()
    EVT_CLUSTER_SNAPSHOT_FAILED = auto()
    EVT_CLUSTER_SNAPSHOT_FINISHED = auto()
    EVT_CLUSTER_SNAPSHOT_STARTED = auto()
    EVT_CLUSTER_SNAPSHOT_RESTORE_FAILED = auto()
    EVT_CLUSTER_SNAPSHOT_RESTORE_FINISHED = auto()
    EVT_CLUSTER_SNAPSHOT_RESTORE_STARTED = auto()
    EVT_CONSISTENCY_VIOLATION = auto()
    EVT_NODE_JOINED = auto()
    EVT_NODE_LEFT = auto()
    EVT_NODE_FAILED = auto()
    EVT_NODE_VALIDATION_FAILED = auto()
    EVT_SQL_QUERY_EXECUTION = auto()

    def __str__(self):
        return '#{{T(org.apache.ignite.events.EventType).{}}}'.format(self._name_)

    def __repr__(self):
        return self.__str__()
