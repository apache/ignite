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

package org.apache.ignite.internal.processors.cache.distributed.dht.preloader;

import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionTopology;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionTopologyImpl;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.MOVING;

/**
 * Partition to node assignments.
 */
public class GridDhtPreloaderAssignments extends ConcurrentHashMap<ClusterNode, GridDhtPartitionDemandMessage> {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private final GridDhtPartitionExchangeId exchangeId;

    /** */
    private final AffinityTopologyVersion topVer;

    /** */
    private boolean cancelled;

    /** Some of owned by affinity partitions were changed state to moving. */
    private final boolean affinityReassign;

    /**
     * @param exchangeId Exchange ID.
     * @param topVer Last join order.
     */
    public GridDhtPreloaderAssignments(
        GridDhtPartitionExchangeId exchangeId,
        AffinityTopologyVersion topVer,
        boolean affinityReassign
    ) {
        assert exchangeId != null;
        assert topVer.topologyVersion() > 0 : topVer;

        this.exchangeId = exchangeId;
        this.topVer = topVer;
        this.affinityReassign = affinityReassign;
    }

    /**
     * @return True if partitions were reassigned.
     */
    public boolean affinityReassign() {
        return affinityReassign;
    }

    /**
     * @return {@code True} if assignments creation was cancelled.
     */
    public boolean cancelled() {
        return cancelled;
    }

    /**
     * @param cancelled {@code True} if assignments creation was cancelled.
     */
    public void cancelled(boolean cancelled) {
        this.cancelled = cancelled;
    }

    /**
     * @return Exchange future.
     */
    GridDhtPartitionExchangeId exchangeId() {
        return exchangeId;
    }

    /**
     * @return Topology version based on last {@link GridDhtPartitionTopologyImpl#readyTopVer}.
     */
    public AffinityTopologyVersion topologyVersion() {
        return topVer;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtPreloaderAssignments.class, this, "super", super.toString());
    }

    /**
     * Retains only moving partitions for the current topology.
     *
     * @param top Topology.
     */
    public void retainMoving(GridDhtPartitionTopology top) {
        Iterator<Entry<ClusterNode, GridDhtPartitionDemandMessage>> it = entrySet().iterator();

        while (it.hasNext()) {
            Entry<ClusterNode, GridDhtPartitionDemandMessage> mapping = it.next();

            GridDhtPartitionDemandMessage val = mapping.getValue();

            IgniteDhtDemandedPartitionsMap cntrMap = val.partitions();

            CachePartitionPartialCountersMap curHistMap = cntrMap.historicalMap();
            CachePartitionPartialCountersMap newHistMap = null;

            if (!curHistMap.isEmpty()) {
                int moving = 0;

                // Fast-path check.
                for (int i = 0; i < curHistMap.size(); i++) {
                    int partId = curHistMap.partitionAt(i);

                    if (top.localPartition(partId).state() == MOVING)
                        moving++;
                }

                if (moving != curHistMap.size()) {
                    newHistMap = new CachePartitionPartialCountersMap(moving);

                    for (int i = 0; i < curHistMap.size(); i++) {
                        int partId = curHistMap.partitionAt(i);
                        long initUpdCntr = curHistMap.initialUpdateCounterAt(i);
                        long updCntr = curHistMap.updateCounterAt(i);

                        if (top.localPartition(partId).state() == MOVING)
                            newHistMap.add(partId, initUpdCntr, updCntr);
                    }
                }
            }

            Set<Integer> curFullSet = cntrMap.fullSet();
            Set<Integer> newFullSet = null;

            if (!curFullSet.isEmpty()) {
                int moving = 0;

                // Fast-path check.
                for (Integer partId : curFullSet) {
                    if (top.localPartition(partId).state() == MOVING)
                        moving++;
                }

                if (moving != curFullSet.size()) {
                    newFullSet = U.newHashSet(moving);

                    for (Integer partId : curFullSet) {
                        if (top.localPartition(partId).state() == MOVING)
                            newFullSet.add(partId);
                    }
                }
            }

            if (newHistMap != null || newFullSet != null) {
                if (newHistMap == null)
                    newHistMap = curHistMap;

                if (newFullSet == null)
                    newFullSet = curFullSet;

                IgniteDhtDemandedPartitionsMap newMap = new IgniteDhtDemandedPartitionsMap(newHistMap, newFullSet);

                if (newMap.isEmpty())
                    it.remove();
                else
                    mapping.setValue(val.withNewPartitionsMap(newMap));
            }
        }
    }
}
