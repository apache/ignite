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
import java.util.TreeMap;
import java.util.UUID;

import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
class ZkDiscoveryEventsData implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Unique cluster ID (generated when first node in cluster starts). */
    final UUID clusterId;

    /** Internal order of last processed custom event. */
    long procCustEvt = -1;

    /** Event ID counter. */
    long evtIdGen;

    /** Current topology version. */
    long topVer;

    /** Max node internal order in cluster. */
    long maxInternalOrder;

    /** Cluster start time (recorded when first node in cluster starts). */
    final long clusterStartTime;

    /** Events to process. */
    final TreeMap<Long, ZkDiscoveryEventData> evts;

    /** ID of current active communication error resolve process. */
    private UUID commErrFutId;

    /**
     * @param clusterStartTime Start time of first node in cluster.
     * @return Events.
     */
    static ZkDiscoveryEventsData createForNewCluster(long clusterStartTime) {
        return new ZkDiscoveryEventsData(
            UUID.randomUUID(),
            clusterStartTime,
            1L,
            new TreeMap<Long, ZkDiscoveryEventData>()
        );
    }

    /**
     * @param clusterId Cluster ID.
     * @param topVer Current topology version.
     * @param clusterStartTime Cluster start time.
     * @param evts Events history.
     */
    private ZkDiscoveryEventsData(
        UUID clusterId,
        long clusterStartTime,
        long topVer,
        TreeMap<Long, ZkDiscoveryEventData> evts)
    {
        this.clusterId = clusterId;
        this.clusterStartTime = clusterStartTime;
        this.topVer = topVer;
        this.evts = evts;
    }

    /**
     * @param node Joined node.
     */
    void onNodeJoin(ZookeeperClusterNode node) {
        if (node.internalId() > maxInternalOrder)
            maxInternalOrder = node.internalId();
    }

    /**
     * @return Future ID.
     */
    @Nullable UUID communicationErrorResolveFutureId() {
        return commErrFutId;
    }

    /**
     * @param id Future ID.
     */
     void communicationErrorResolveFutureId(@Nullable UUID id) {
        commErrFutId = id;
    }

    /**
     * @param nodes Current nodes in topology (these nodes should ack that event processed).
     * @param evt Event.
     */
    void addEvent(Collection<ZookeeperClusterNode> nodes, ZkDiscoveryEventData evt) {
        Object old = evts.put(evt.eventId(), evt);

        assert old == null : old;

        evt.initRemainingAcks(nodes);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ZkDiscoveryEventsData.class, this,
            "topVer", topVer,
            "evtIdGen", evtIdGen,
            "procCustEvt", procCustEvt,
            "evts", evts);
    }
}
