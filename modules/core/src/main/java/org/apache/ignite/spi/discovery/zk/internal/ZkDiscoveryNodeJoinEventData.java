/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.spi.discovery.zk.internal;

import java.util.UUID;
import org.apache.ignite.events.EventType;

/**
 *
 */
class ZkDiscoveryNodeJoinEventData extends ZkDiscoveryEventData {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    final int joinedInternalId;

    /** */
    final UUID nodeId;

    /** */
    transient ZkJoiningNodeData joiningNodeData;

    /**
     * @param evtId Event ID.
     * @param topVer Topology version.
     * @param nodeId Joined node ID.
     * @param joinedInternalId Joined node internal ID.
     */
    ZkDiscoveryNodeJoinEventData(long evtId, long topVer, UUID nodeId, int joinedInternalId) {
        super(evtId, EventType.EVT_NODE_JOINED, topVer);

        this.nodeId = nodeId;
        this.joinedInternalId = joinedInternalId;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "NodeJoinEventData [topVer=" + topologyVersion() + ", node=" + nodeId + ']';
    }
}
