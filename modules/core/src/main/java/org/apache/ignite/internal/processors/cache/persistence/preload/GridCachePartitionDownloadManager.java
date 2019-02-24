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
import java.nio.ByteBuffer;
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
import org.apache.ignite.internal.pagemem.store.PageStore;
import org.apache.ignite.internal.processors.affinity.AffinityAssignment;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedManagerAdapter;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionDemandMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPreloaderAssignments;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStore;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.file.FileTransferManager;
import org.apache.ignite.internal.processors.cache.persistence.file.meta.PartitionFileMetaInfo;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.util.GridIntList;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.nio.channel.IgniteSocketChannel;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.configuration.CacheConfiguration.DFLT_REBALANCE_TIMEOUT;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.SYSTEM_POOL;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.MOVING;
import static org.apache.ignite.internal.processors.cache.persistence.preload.GridCachePartitionUploadManager.persistenceRebalanceApplicable;
import static org.apache.ignite.internal.processors.cache.persistence.preload.GridCachePartitionUploadManager.rebalanceThreadTopic;

/**
 *
 */
public class GridCachePartitionDownloadManager extends GridCacheSharedManagerAdapter {
    /** */
    private final ConcurrentMap<UUID, RebalanceDownloadFuture> futMap = new ConcurrentHashMap<>();

    /** */
    private FilePageStoreManager filePageStore;

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
        assert cctx.pageStore() instanceof FilePageStoreManager;

        filePageStore = (FilePageStoreManager)cctx.pageStore();

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

        if (fut0 == null || fut0.isDone())
            return;

        FileTransferManager<PartitionFileMetaInfo> source = null;

        int totalParts = fut0.nodeAssigns.values().stream()
            .mapToInt(GridIntList::size)
            .sum();

        AffinityTopologyVersion topVer = fut0.topVer;

        try {
            source = new FileTransferManager<>(cctx.kernalContext(), channel.channel());

            PartitionFileMetaInfo meta;

            for (int i = 0; i < totalParts; i++) {
                // Start processing original partition file.
                source.readMetaInto(meta = new PartitionFileMetaInfo());

                assert meta.getType() == 0 : meta;

                int grpId = meta.getGrpId();
                int partId = meta.getPartId();

                CacheGroupContext grp = cctx.cache().cacheGroup(grpId);
                AffinityAssignment aff = grp.affinity().cachedAffinity(topVer);

                if (aff.get(partId).contains(cctx.localNode())) {
                    GridDhtLocalPartition part = grp.topology().localPartition(partId);

                    assert part != null;

                    if (part.state() == MOVING) {
                        boolean reserved = part.reserve();

                        assert reserved : "Failed to reserve partition [igniteInstanceName=" +
                            cctx.igniteInstanceName() + ", grp=" + grp.cacheOrGroupName() + ", part=" + part + ']';

                        part.lock();

                        try {
                            FilePageStore store = (FilePageStore)filePageStore.getStore(grpId, partId);

                            File cfgFile = new File(store.getFileAbsolutePath());

                            // Skip the file header and first pageId with meta.
                            // Will restore meta pageId on merge delta file phase.
                            assert store.size() <= meta.getSize() : "Trim zero bytes from the end of partition";

                            source.readInto(cfgFile, store.headerSize() + store.getPageSize(), meta.getSize());

                            // Start processing delta file.
                            source.readMetaInto(meta = new PartitionFileMetaInfo());

                            assert meta.getType() == 1 : meta;

                            applyPartitionDeltaPages(source, store, meta.getSize());

                            fut0.markProcessed(grpId, partId);

                            // Validate partition

                            // Rebuild indexes by partition

                            // Own partition
                        }
                        finally {
                            part.unlock();
                        }
                    }
                    else {
                        if (log.isDebugEnabled()) {
                            log.debug("Skipping partition (state is not MOVING) " +
                                "[grpId=" + grpId + ", partId=" + partId + ", topicId=" + topicId +
                                ", nodeId=" + nodeId + ']');
                        }
                    }
                }
            }

            fut0.onDone(true);
        }
        catch (IOException | IgniteCheckedException e) {
            U.error(log, "Error of handling channel creation event", e);

            fut0.onDone(e);
        }
        finally {
            U.closeQuiet(source);
        }
    }

    /**
     * @param ftMgr The manager handles channel.
     * @param store Cache partition store.
     * @param size Expected size of bytes in channel.
     * @throws IgniteCheckedException If fails.
     */
    private void applyPartitionDeltaPages(
        FileTransferManager<PartitionFileMetaInfo> ftMgr,
        PageStore store,
        long size
    ) throws IgniteCheckedException {
        ByteBuffer pageBuff = ByteBuffer.allocate(store.getPageSize());

        long readed;
        long position = 0;

        while ((readed = ftMgr.readInto(pageBuff)) > 0 && position < size) {
            position += readed;

            pageBuff.flip();

            long pageId = PageIO.getPageId(pageBuff);
            long pageOffset = store.pageOffset(pageId);

            if (log.isDebugEnabled())
                log.debug("Page delta [pageId=" + pageId +
                    ", pageOffset=" + pageOffset +
                    ", partSize=" + store.size() +
                    ", skipped=" + (pageOffset >= store.size()) +
                    ", position=" + position +
                    ", size=" + size + ']');

            pageBuff.rewind();

            assert pageOffset < store.size();

            store.write(pageId, pageBuff, Integer.MAX_VALUE, false);

            pageBuff.clear();
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

        UUID nodeId = nodeAssigns.getKey().id();

        RebalanceDownloadFuture downloadFut = futMap.get(nodeId);

        if (downloadFut == null || !downloadFut.isDone()) {
            downloadFut.cancel();

            downloadFut = new RebalanceDownloadFuture(nodeId, nodeAssigns.getValue(), topVer);
        }

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

        /** */
        private AffinityTopologyVersion topVer;

        /** */
        private Map<Integer, GridIntList> remaining;

        /**
         * @param nodeId The remote nodeId.
         * @param nodeAssigns Map of assignments to request from remote.
         */
        public RebalanceDownloadFuture(
            UUID nodeId,
            Map<Integer, GridIntList> nodeAssigns,
            AffinityTopologyVersion topVer
        ) {
            this.nodeId = nodeId;
            this.nodeAssigns = nodeAssigns;
            this.topVer = topVer;

            this.remaining = U.newHashMap(nodeAssigns.size());

            for (Map.Entry<Integer, GridIntList> grpPartEntry : nodeAssigns.entrySet())
                remaining.putIfAbsent(grpPartEntry.getKey(), grpPartEntry.getValue().copy());
        }

        /** {@inheritDoc} */
        @Override public boolean cancel() {
            return onCancelled();
        }

        /**
         * @param grpId Cache group id to search.
         * @param partId Cache partition to remove;
         * @throws IgniteCheckedException If fails.
         */
        public synchronized void markProcessed(int grpId, int partId) throws IgniteCheckedException {
            GridIntList parts = remaining.get(grpId);

            if (parts == null)
                throw new IgniteCheckedException("Partition index incorrect [grpId=" + grpId + ", partId=" + partId + ']');

            int partIdx = parts.removeValue(0, partId);

            assert partIdx >= 0;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(RebalanceDownloadFuture.class, this);
        }
    }
}
