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
import java.util.TreeMap;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

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

    /** */
    int flags;

    /**
     * @param evtId Event ID.
     * @param evtType Event type.
     * @param topVer Topology version.
     */
    ZkDiscoveryEventData(long evtId, int evtType, long topVer) {
        assert evtType == EVT_NODE_JOINED || evtType == EVT_NODE_FAILED || evtType == EVT_DISCOVERY_CUSTOM_EVT : evtType;

        this.evtId = evtId;
        this.evtType = evtType;
        this.topVer = topVer;
    }

    /**
     * @param nodes Current nodes in topology.
     */
    void initRemainingAcks(Collection<ZookeeperClusterNode> nodes) {
        assert remainingAcks == null : this;

        remainingAcks = U.newHashSet(nodes.size());

        for (ZookeeperClusterNode node : nodes) {
            if (!node.isLocal() && node.order() <= topVer) {
                boolean add = remainingAcks.add(node.internalId());

                assert add : node;
            }
        }
    }

    /**
     * @param node Node.
     */
    void addRemainingAck(ZookeeperClusterNode node) {
        assert node.order() <= topVer : node;

        boolean add = remainingAcks.add(node.internalId());

        assert add : node;
    }

    /**
     * @return {@code True} if all nodes processed event.
     */
    boolean allAcksReceived() {
        return remainingAcks.isEmpty();
    }

    /**
     * @return Remaining acks.
     */
    Set<Integer> remainingAcks() {
        return remainingAcks;
    }

    /**
     * @param nodeInternalId Node ID.
     * @param ackEvtId Last event ID processed on node.
     * @return {@code True} if all nodes processed event.
     */
    boolean onAckReceived(Integer nodeInternalId, long ackEvtId) {
        assert remainingAcks != null;

        if (ackEvtId >= evtId)
            remainingAcks.remove(nodeInternalId);

        return remainingAcks.isEmpty();
    }

    /**
     * @param node Failed node.
     * @return {@code True} if all nodes processed event.
     */
    boolean onNodeFail(ZookeeperClusterNode node) {
        assert remainingAcks != null : this;

        remainingAcks.remove(node.internalId());

        return remainingAcks.isEmpty();
    }

    /**
     * @param flag Flag mask.
     * @return {@code True} if flag set.
     */
    boolean flagSet(int flag) {
        return (flags & flag) == flag;
    }

    /**
     * @return Event ID.
     */
    long eventId() {
        return evtId;
    }

    /**
     * @return Event type.
     */
    int eventType() {
        return evtType;
    }

    /**
     * @return Event topology version.
     */
    long topologyVersion() {
        return topVer;
    }
}
