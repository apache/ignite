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

import java.io.Serializable;
import java.util.Collection;
import java.util.Set;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_JOINED;
import static org.apache.ignite.internal.events.DiscoveryCustomEvent.EVT_DISCOVERY_CUSTOM_EVT;

/**
 *
 */
abstract class ZkDiscoveryEventData implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private final long evtId;

    /** */
    private final int evtType;

    /** */
    private final long topVer;

    /** */
    private transient Set<Integer> remainingAcks;

    /**
     * @param evtType Event type.
     * @param topVer Topology version.
     */
    ZkDiscoveryEventData(long evtId, int evtType, long topVer) {
        assert evtType == EVT_NODE_JOINED || evtType == EVT_NODE_FAILED || evtType == EVT_DISCOVERY_CUSTOM_EVT : evtType;

        this.evtId = evtId;
        this.evtType = evtType;
        this.topVer = topVer;
    }

    void remainingAcks(Collection<ZookeeperClusterNode> nodes) {
        assert remainingAcks == null : this;

        remainingAcks = U.newHashSet(nodes.size());

        for (ZookeeperClusterNode node : nodes) {
            if (!node.isLocal() && node.order() <= topVer)
                remainingAcks.add(node.internalId());
        }
    }

    boolean allAcksReceived() {
        return remainingAcks.isEmpty();
    }

    boolean onAckReceived(Integer nodeInternalId, long ackEvtId) {
        assert remainingAcks != null;

        if (ackEvtId >= evtId)
            remainingAcks.remove(nodeInternalId);

        return remainingAcks.isEmpty();
    }

    boolean onNodeFail(ZookeeperClusterNode node) {
        assert remainingAcks != null : this;

        remainingAcks.remove(node.internalId());

        return remainingAcks.isEmpty();
    }

    long eventId() {
        return evtId;
    }

    int eventType() {
        return evtType;
    }

    long topologyVersion() {
        return topVer;
    }
}
