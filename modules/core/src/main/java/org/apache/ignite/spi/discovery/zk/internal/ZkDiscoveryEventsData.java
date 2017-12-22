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
import java.util.TreeMap;
import java.util.UUID;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
class ZkDiscoveryEventsData implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    int procCustEvt = -1;

    /** */
    long evtIdGen;

    /** */
    long topVer;

    /** */
    long maxInternalOrder;

    /** */
    final long startInternalOrder;

    /** */
    final long gridStartTime;

    /** */
    final TreeMap<Long, ZkDiscoveryEventData> evts;

    /** */
    private UUID commErrFutId;

    /**
     * @param startInternalOrder First
     * @param topVer Current topology version.
     * @param gridStartTime Cluster start time.
     * @param evts Events history.
     */
    ZkDiscoveryEventsData(
        long startInternalOrder,
        long gridStartTime,
        long topVer,
        TreeMap<Long, ZkDiscoveryEventData> evts)
    {
        this.startInternalOrder = startInternalOrder;
        this.gridStartTime = gridStartTime;
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
}
