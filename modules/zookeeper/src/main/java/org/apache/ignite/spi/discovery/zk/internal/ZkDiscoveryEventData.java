/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.spi.discovery.zk.internal;

import java.io.Serializable;
import java.util.Collection;
import java.util.Set;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 *
 */
abstract class ZkDiscoveryEventData implements Serializable {
    /** */
    static final byte ZK_EVT_NODE_JOIN = 1;

    /** */
    static final byte ZK_EVT_NODE_FAILED = 2;

    /** */
    static final byte ZK_EVT_CUSTOM_EVT = 3;

    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private final long evtId;

    /** */
    private final byte evtType;

    /** */
    private final long topVer;

    /** */
    private transient Set<Long> remainingAcks;

    /** */
    int flags;

    /**
     * @param evtId Event ID.
     * @param evtType Event type.
     * @param topVer Topology version.
     */
    ZkDiscoveryEventData(long evtId, byte evtType, long topVer) {
        assert evtType == ZK_EVT_NODE_JOIN || evtType == ZK_EVT_NODE_FAILED || evtType == ZK_EVT_CUSTOM_EVT : evtType;

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
    Set<Long> remainingAcks() {
        return remainingAcks;
    }

    /**
     * @param nodeInternalId Node ID.
     * @param ackEvtId Last event ID processed on node.
     * @return {@code True} if all nodes processed event.
     */
    boolean onAckReceived(Long nodeInternalId, long ackEvtId) {
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
    byte eventType() {
        return evtType;
    }

    /**
     * @return Event topology version.
     */
    long topologyVersion() {
        return topVer;
    }
}
