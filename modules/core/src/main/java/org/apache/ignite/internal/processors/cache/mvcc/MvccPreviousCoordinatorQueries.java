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

package org.apache.ignite.internal.processors.cache.mvcc;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.util.GridLongList;
import org.apache.ignite.internal.util.typedef.F;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.mvcc.MvccQueryTracker.MVCC_TRACKER_ID_NA;

/**
 *
 */
class MvccPreviousCoordinatorQueries {
    /** */
    private volatile boolean prevQueriesDone;

    /** Map of nodes to active {@link MvccQueryTracker} IDs list. */
    private final ConcurrentHashMap<UUID, GridLongList> activeQueries = new ConcurrentHashMap<>();

    /** */
    private final ConcurrentHashMap<UUID, GridLongList> rcvdAcks = new ConcurrentHashMap<>();

    /** */
    private Set<UUID> rcvd;

    /** */
    private Set<UUID> waitNodes;

    /** */
    private boolean initDone;

    /**
     * @param nodeQueries Active queries map.
     * @param discoCache Discovery data.
     * @param mgr Discovery manager.
     */
    void init(Map<UUID, GridLongList> nodeQueries, DiscoCache discoCache, GridDiscoveryManager mgr) {
        synchronized (this) {
            assert !initDone;
            assert waitNodes == null;

            waitNodes = new HashSet<>();

            for (ClusterNode node : discoCache.allNodes()) {
                if ((nodeQueries == null || !nodeQueries.containsKey(node.id())) &&
                    mgr.alive(node) &&
                    !F.contains(rcvd, node.id()))
                    waitNodes.add(node.id());
            }

            initDone = waitNodes.isEmpty();

            if (nodeQueries != null) {
                for (Map.Entry<UUID, GridLongList> e : nodeQueries.entrySet())
                    mergeToActiveQueries(e.getKey(), e.getValue());
            }

            if (initDone && !prevQueriesDone)
                prevQueriesDone = activeQueries.isEmpty() && rcvdAcks.isEmpty();
        }
    }

    /**
     * @return {@code True} if all queries on
     */
    boolean previousQueriesDone() {
        return prevQueriesDone;
    }

    /**
     * Merges current node active queries with the given ones.
     *
     * @param nodeId Node ID.
     * @param nodeTrackers Active query trackers started on node.
     */
    private void mergeToActiveQueries(UUID nodeId, GridLongList nodeTrackers) {
        if (nodeTrackers == null || nodeTrackers.isEmpty() || prevQueriesDone)
            return;

        GridLongList currTrackers = activeQueries.get(nodeId);

        if (currTrackers == null)
            activeQueries.put(nodeId, currTrackers = nodeTrackers);
        else
            currTrackers.addAll(nodeTrackers);

        assert currTrackers == null || currTrackers.distinct();

        // Check if there were any acks had been arrived before.
        GridLongList currAcks = rcvdAcks.get(nodeId);

        if (currTrackers != null && currAcks != null && !currAcks.isEmpty()) {
            GridLongList foundAcks = new GridLongList();

            // Find those acks if any.
            for (int i = 0; i < currAcks.size(); i++) {
                long trackerId = currAcks.get(i);

                if (currTrackers.contains(trackerId))
                    foundAcks.add(trackerId);
            }

            // Remove those acks from current trackers.
            for (int i = 0; i < foundAcks.size(); i++) {
                long trackerId = foundAcks.get(i);

                currTrackers.removeValue(0, trackerId);
                currAcks.removeValue(0, trackerId);
            }

            if (currTrackers.isEmpty())
                activeQueries.remove(nodeId);

            if (currAcks.isEmpty())
                rcvdAcks.remove(nodeId);
        }

        if (initDone && !prevQueriesDone)
            prevQueriesDone = activeQueries.isEmpty() && rcvdAcks.isEmpty();
    }

    /**
     * @param nodeId Node ID.
     * @param nodeTrackers  Active query trackers started on node.
     */
    void addNodeActiveQueries(UUID nodeId, @Nullable GridLongList nodeTrackers) {
        synchronized (this) {
            if (initDone)
                return;

            if (waitNodes == null) {
                if (rcvd == null)
                    rcvd = new HashSet<>();

                rcvd.add(nodeId);
            }
            else {
                waitNodes.remove(nodeId);

                initDone = waitNodes.isEmpty();
            }

            mergeToActiveQueries(nodeId, nodeTrackers);

            if (initDone && !prevQueriesDone)
                prevQueriesDone = activeQueries.isEmpty() && rcvdAcks.isEmpty();
        }
    }

    /**
     * @param nodeId Failed node ID.
     */
    void onNodeFailed(UUID nodeId) {
        synchronized (this) {
            if (waitNodes != null) {
                waitNodes.remove(nodeId);

                initDone = waitNodes.isEmpty();
            }

            if (initDone && !prevQueriesDone && activeQueries.remove(nodeId) != null)
                prevQueriesDone = activeQueries.isEmpty() && rcvdAcks.isEmpty();
        }
    }

    /**
     * @param nodeId Node ID.
     * @param qryTrackerId Query tracker Id.
     */
    void onQueryDone(UUID nodeId, long qryTrackerId) {
        if (qryTrackerId == MVCC_TRACKER_ID_NA)
            return;

        synchronized (this) {
            GridLongList nodeTrackers = activeQueries.get(nodeId);

            if (nodeTrackers == null || (nodeTrackers.removeValue(0, qryTrackerId) == -1)) {
                GridLongList nodeAcks = rcvdAcks.get(nodeId);

                if (nodeAcks == null)
                    rcvdAcks.put(nodeId, nodeAcks = new GridLongList());

                // We received qry done ack before the active qry message. Need to save it.
                nodeAcks.add(qryTrackerId);
            }

            if (nodeTrackers != null && nodeTrackers.isEmpty())
                activeQueries.remove(nodeId);

            if (initDone && !prevQueriesDone)
                prevQueriesDone = activeQueries.isEmpty() && rcvdAcks.isEmpty();
        }
    }
}
