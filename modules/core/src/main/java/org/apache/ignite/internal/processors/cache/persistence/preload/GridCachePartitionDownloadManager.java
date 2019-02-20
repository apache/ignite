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
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteFeatures;
import org.apache.ignite.internal.managers.communication.GridIoChannelListener;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedManagerAdapter;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionDemandMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPreloaderAssignments;
import org.apache.ignite.internal.util.GridIntList;
import org.apache.ignite.internal.util.nio.channel.IgniteSocketChannel;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;

import static org.apache.ignite.internal.processors.cache.persistence.preload.GridCachePartitionUploadManager.persistenceRebalanceApplicable;
import static org.apache.ignite.internal.processors.cache.persistence.preload.GridCachePartitionUploadManager.rebalanceThreadTopic;

/**
 *
 */
public class GridCachePartitionDownloadManager extends GridCacheSharedManagerAdapter {
    /** */
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    /**
     * @param ctx The kernal context.
     */
    public GridCachePartitionDownloadManager(GridKernalContext ctx) {
        assert CU.isPersistenceEnabled(ctx.config());

    }

    /**
     * @param assigns A generated cache assignments in a cut of cache group [grpId, [nodeId, parts]].
     * @param grp The corresponding to assignments cache group context.
     * @return {@code True} if cache might be rebalanced by sending cache partition files.
     */
    static boolean groupRebalancedByPartitions(CacheGroupContext grp, GridDhtPreloaderAssignments assigns) {
        return grp.persistenceEnabled() && IgniteFeatures.allNodesSupports(assigns.keySet(),
            IgniteFeatures.CACHE_PARTITION_FILE_REBALANCE);
    }

    /** {@inheritDoc} */
    @Override protected void start0() throws IgniteCheckedException {
        if (persistenceRebalanceApplicable(cctx)) {
            for (int cnt = 0; cnt < cctx.gridConfig().getRebalanceThreadPoolSize(); cnt++) {
                final int topicId = cnt;

                // Register channel listeners for each rebalance thread.
                cctx.gridIO().addChannelListener(rebalanceThreadTopic(topicId), new GridIoChannelListener() {
                    @Override public void onChannelCreated(IgniteSocketChannel channel) {
                        if (lock.readLock().tryLock())
                            return;

                        try {
                            onChannelCreated0(topicId, channel);
                        }
                        finally {
                            lock.readLock().unlock();
                        }
                    }
                });
            }
        }
    }

    /** {@inheritDoc} */
    @Override protected void stop0(boolean cancel) {
        for (int cnt = 0; cnt < cctx.gridConfig().getRebalanceThreadPoolSize(); cnt++)
            cctx.gridIO().removeChannelListener(rebalanceThreadTopic(cnt), null);
    }

    /**
     * @param topicId The index of rebalance pool thread.
     * @param channel A blocking socket channel to handle rebalance partitions.
     */
    private void onChannelCreated0(int topicId, IgniteSocketChannel channel) {

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

    /**
     * @param assignMap A generated cache assignments in a cut of cache group [grpId, [nodeId, parts]].
     * @param grpOrderMap A map of caches grouped by #getRebalanceOrder() [orderNo, grpIds].
     * @return A map of cache assignments in a cut of cache group order number, grouped by nodeId.
     * Filtered by supported feature of rebalance with partition files enabled.
     */
    public NavigableMap<Integer, Map<UUID, Map<Integer, GridIntList>>> splitNodeCacheAssignments(
        Map<Integer, GridDhtPreloaderAssignments> assignMap,
        Map<Integer, List<Integer>> grpOrderMap
    ) {
        NavigableMap<Integer, Map<UUID, Map<Integer, GridIntList>>> result = new TreeMap<>();

        for (Map.Entry<Integer, List<Integer>> grpOrderEntry : grpOrderMap.entrySet()) {
            int orderNo = grpOrderEntry.getKey();
            List<Integer> grps = grpOrderEntry.getValue();

            for (int grpId : grps) {
                GridDhtPreloaderAssignments grpAssigns = assignMap.get(grpId);

                CacheGroupContext grp = cctx.cache().cacheGroup(grpId);

                if (groupRebalancedByPartitions(grp, grpAssigns)) {
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

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCachePartitionDownloadManager.class, this);
    }
}
