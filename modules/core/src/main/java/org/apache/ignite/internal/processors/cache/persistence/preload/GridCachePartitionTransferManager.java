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
package org.apache.ignite.internal.processors.cache.persistence.preload;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteFeatures;
import org.apache.ignite.internal.processors.cache.GridCacheSharedManagerAdapter;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionDemandMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPreloaderAssignments;
import org.apache.ignite.internal.util.GridIntList;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 *
 */
public class GridCachePartitionTransferManager extends GridCacheSharedManagerAdapter {
    /** */

    /**
     * @param assignMap A generated cache assignments in a cut of cache group [grpId, [nodeId, parts]].
     * @param grpOrderMap A map of caches grouped by #getRebalanceOrder() [orderNo, grpIds].
     * @return A map of cache assignments in a cut of cache group order number, grouped by nodeId.
     * Filtered by supported feature of rebalance with partition files enabled.
     */
    public static NavigableMap<Integer, Map<UUID, Map<Integer, GridIntList>>> splitNodeCacheAssignments(
        Map<Integer, GridDhtPreloaderAssignments> assignMap,
        Map<Integer, List<Integer>> grpOrderMap
    ) {
        NavigableMap<Integer, Map<UUID, Map<Integer, GridIntList>>> result = new TreeMap<>();

        for (Map.Entry<Integer, List<Integer>> grpOrderEntry : grpOrderMap.entrySet()) {
            int orderNo = grpOrderEntry.getKey();
            List<Integer> grps = grpOrderEntry.getValue();

            for (int grpId : grps) {
                GridDhtPreloaderAssignments grpAssigns = assignMap.get(grpId);

                if (isPersistenceRebalanceSupported(grpAssigns)) {
                    for (Map.Entry<ClusterNode, GridDhtPartitionDemandMessage> nodeMsgEntry : grpAssigns.entrySet()) {
                        Map<UUID, Map<Integer, GridIntList>> splitByUuid =
                            result.getOrDefault(orderNo, new HashMap<>());

                        Map<Integer, GridIntList> splitByGrpId =
                            splitByUuid.getOrDefault(nodeMsgEntry.getKey().id(), new HashMap<>());

                        Set<Integer> fullParts = nodeMsgEntry.getValue().partitions().fullSet();

                        for (int partId : fullParts) {
                            GridIntList grpParts = splitByGrpId.getOrDefault(grpId, new GridIntList());

                            grpParts.add(partId);
                        }
                    }
                }
            }
        }

        return result;
    }

    /**
     * @param assigns A generated cache assignments in a cut of cache group [grpId, [nodeId, parts]].
     * @return {@code True} if cache might be rebalanced by sending cache partition files.
     */
    public static boolean isPersistenceRebalanceSupported(GridDhtPreloaderAssignments assigns) {
        return IgniteFeatures.allNodesSupports(assigns.keySet(), IgniteFeatures.CACHE_PARTITION_FILE_REBALANCE);
    }

    /** {@inheritDoc} */
    @Override protected void start0() throws IgniteCheckedException {

    }

    /** {@inheritDoc} */
    @Override protected void stop0(boolean cancel) {

    }

    /**
     * @param nodeAssigns A map of cache assignments grouped by nodeId to request.
     * @param force {@code true} if must cancel previous rebalance.
     * @param rebalanceId Current rebalance id.
     * @param next A task to build the chained rebalance process.
     */
    public void addNodeAssignments(
        Map<UUID, Map<Integer, GridIntList>> nodeAssigns,
        boolean force,
        int rebalanceId,
        Runnable next
    ) {

    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCachePartitionTransferManager.class, this);
    }
}
