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
    long gridStartTime;

    /** */
    TreeMap<Long, ZkDiscoveryEventData> evts;

    /**
     * @param topVer Current topology version.
     * @param gridStartTime Cluster start time.
     * @param evts Events history.
     */
    ZkDiscoveryEventsData(long gridStartTime, long topVer, TreeMap<Long, ZkDiscoveryEventData> evts) {
        this.gridStartTime = gridStartTime;
        this.topVer = topVer;
        this.evts = evts;
    }

    /**
     * @param nodes Current nodes in topology (these nodes should ack that event processed).
     * @param evt Event.
     * @param alives Optional alives nodes for additional filtering.
     */
    void addEvent(Collection<ZookeeperClusterNode> nodes,
        ZkDiscoveryEventData evt,
        @Nullable TreeMap<Integer, String> alives)
    {
        Object old = evts.put(evt.eventId(), evt);

        assert old == null : old;

        evt.initRemainingAcks(nodes, alives);
    }
}
