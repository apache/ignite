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

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteFeatures;
import org.apache.ignite.internal.managers.communication.GridIoChannelListener;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedManagerAdapter;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionDemandMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPreloaderAssignments;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.FileTransferManager;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.meta.PartitionFileMetaInfo;
import org.apache.ignite.internal.util.GridIntList;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.nio.channel.IgniteSocketChannel;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.configuration.CacheConfiguration.DFLT_REBALANCE_TIMEOUT;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.SYSTEM_POOL;
import static org.apache.ignite.internal.processors.cache.persistence.preload.GridCachePartitionUploadManager.persistenceRebalanceApplicable;
import static org.apache.ignite.internal.processors.cache.persistence.preload.GridCachePartitionUploadManager.rebalanceThreadTopic;

/**
 *
 */
public class GridCachePartitionDownloadManager extends GridCacheSharedManagerAdapter {
    /** The default factory to provide IO oprations over downloading files. */
    private static final FileIOFactory dfltIoFactory = new RandomAccessFileIOFactory();

    /** */
    private final ConcurrentMap<UUID, RebalanceDownloadFuture> futMap = new ConcurrentHashMap<>();

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
                    @Override public void onChannelCreated(UUID nodeId, IgniteSocketChannel channel) {
                        if (lock.readLock().tryLock())
                            return;

                        try {
                            onChannelCreated0(topicId, nodeId, channel);
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
     * @param nodeId The remote node id.
     * @param channel A blocking socket channel to handle rebalance partitions.
     */
    private void onChannelCreated0(int topicId, UUID nodeId, IgniteSocketChannel channel) {
        U.log(log, "Handle channel created event [topicId=" + topicId + ", channel=" + channel + ']');

        RebalanceDownloadFuture fut0 = futMap.get(nodeId);

        try {
            for (Integer partId : Arrays.asList(1, 2)) {
                FileTransferManager<PartitionFileMetaInfo> ioDownloader =
                    new FileTransferManager<>(cctx.kernalContext(), channel.channel(), dfltIoFactory);

                PartitionFileMetaInfo meta;

                ioDownloader.readFileMetaInfo(meta = new PartitionFileMetaInfo());

                File partFile = new File("");

                ioDownloader.readFile(partFile, meta.getSize());
            }
        }
        catch (IOException | IgniteCheckedException e) {
            U.error(log, "Error of handling channel creation event", e);
        }
    }

    /**
     * @param nodeAssignsMap A map of cache assignments grouped by nodeId to request.
     * @param force {@code true} if must cancel previous rebalance.
     * @param rebalanceId Current rebalance id.
     * @param next A task to build the chained rebalance process.
     */
    public void addNodeAssignments(
        Map<ClusterNode, Map<Integer, GridIntList>> nodeAssignsMap,
        AffinityTopologyVersion topVer,
        boolean force,
        int rebalanceId,
        Runnable next
    ) {
        int totalStripes = cctx.gridConfig().getRebalanceThreadPoolSize();

        // Start from the only first stripe.
        int topicId = 1;

        Map.Entry<ClusterNode, Map<Integer, GridIntList>> nodeAssigns = nodeAssignsMap.entrySet().iterator().next();

        RebalanceDownloadFuture downloadFut = futMap.get(nodeAssigns.getKey().id());

        try {
            GridPartitionsCopyDemandMessage msg0 = new GridPartitionsCopyDemandMessage(rebalanceId, topVer,
                nodeAssigns.getValue());

            cctx.gridIO().sendOrderedMessage(nodeAssigns.getKey(), rebalanceThreadTopic(topicId),
                msg0, SYSTEM_POOL, DFLT_REBALANCE_TIMEOUT, false);
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Error sending partition copy demand message", e);
        }
    }

    /**
     * @param assignMap A generated cache assignments in a cut of cache group [grpId, [nodeId, parts]].
     * @param grpOrderMap A map of caches grouped by #getRebalanceOrder() [orderNo, grpIds].
     * @return A map of cache assignments in a cut of cache group order number, grouped by nodeId.
     * Filtered by supported feature of rebalance with partition files enabled.
     */
    public NavigableMap<Integer, Map<ClusterNode, Map<Integer, GridIntList>>> splitNodeCacheAssignments(
        Map<Integer, GridDhtPreloaderAssignments> assignMap,
        Map<Integer, List<Integer>> grpOrderMap
    ) {
        NavigableMap<Integer, Map<ClusterNode, Map<Integer, GridIntList>>> result = new TreeMap<>();

        for (Map.Entry<Integer, List<Integer>> grpOrderEntry : grpOrderMap.entrySet()) {
            int orderNo = grpOrderEntry.getKey();
            List<Integer> grps = grpOrderEntry.getValue();

            for (int grpId : grps) {
                GridDhtPreloaderAssignments grpAssigns = assignMap.get(grpId);

                CacheGroupContext grp = cctx.cache().cacheGroup(grpId);

                if (groupRebalancedByPartitions(grp, grpAssigns)) {
                    for (Map.Entry<ClusterNode, GridDhtPartitionDemandMessage> nodeMsgEntry : grpAssigns.entrySet()) {
                        // TODO will not work, need putIfAbsent
                        Map<ClusterNode, Map<Integer, GridIntList>> splitByUuid =
                            result.getOrDefault(orderNo, new HashMap<>());

                        Map<Integer, GridIntList> splitByGrpId =
                            splitByUuid.getOrDefault(nodeMsgEntry.getKey(), new HashMap<>());

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

    /** */
    private static class RebalanceDownloadFuture extends GridFutureAdapter<Boolean> {
        /** */
        private UUID nodeId;

        /** */
        private Map<Integer, GridIntList> nodeAssigns;

        /**
         * @param nodeId The remote nodeId.
         * @param nodeAssigns Map of assignments to request from remote.
         */
        public RebalanceDownloadFuture(UUID nodeId,
            Map<Integer, GridIntList> nodeAssigns) {
            this.nodeId = nodeId;
            this.nodeAssigns = nodeAssigns;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(RebalanceDownloadFuture.class, this);
        }
    }
}
