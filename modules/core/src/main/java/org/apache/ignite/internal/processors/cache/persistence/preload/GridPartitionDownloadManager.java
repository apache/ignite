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
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.managers.communication.GridIoChannelListener;
import org.apache.ignite.internal.pagemem.store.PageStore;
import org.apache.ignite.internal.processors.affinity.AffinityAssignment;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
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
import org.apache.ignite.internal.util.lang.IgniteInClosureX;
import org.apache.ignite.internal.util.nio.channel.IgniteSocketChannel;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.configuration.CacheConfiguration.DFLT_REBALANCE_TIMEOUT;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.SYSTEM_POOL;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.MOVING;
import static org.apache.ignite.internal.processors.cache.persistence.preload.GridPartitionUploadManager.persistenceRebalanceApplicable;
import static org.apache.ignite.internal.processors.cache.persistence.preload.IgniteCachePreloadSharedManager.rebalanceThreadTopic;
import static org.apache.ignite.internal.util.GridIntList.getAsIntList;

/**
 *
 */
public class GridPartitionDownloadManager {
    /** */
    private static final Runnable NO_OP = () -> {};

    /** */
    private GridCacheSharedContext<?, ?> cctx;

    /** */
    private final IgniteLogger log;

    /** */
    private final ConcurrentMap<UUID, RebalanceDownloadFuture> futMap = new ConcurrentHashMap<>();

    /** */
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    /** */
    private FilePageStoreManager filePageStore;

    /** */
    private volatile RebalanceDownloadFuture headFut = new RebalanceDownloadFuture();

    /**
     * @param ktx Kernal context to process.
     */
    public GridPartitionDownloadManager(GridKernalContext ktx) {
        assert CU.isPersistenceEnabled(ktx.config());

        log = ktx.log(getClass());
    }

    /** */
    public void start0(GridCacheSharedContext<?, ?> cctx) throws IgniteCheckedException {
        assert cctx.pageStore() instanceof FilePageStoreManager : cctx.pageStore();

        this.cctx = cctx;

        filePageStore = (FilePageStoreManager)cctx.pageStore();

        if (persistenceRebalanceApplicable(cctx)) {
            // Register channel listeners for the rebalance thread.
            cctx.gridIO().addChannelListener(rebalanceThreadTopic(), new GridIoChannelListener() {
                @Override public void onChannelCreated(UUID nodeId, IgniteSocketChannel channel) {
                    final RebalanceDownloadFuture fut0 = futMap.get(nodeId);

                    if (fut0 == null || fut0.isComplete())
                        return;

                    lock.readLock().lock();

                    try {
                        onChannelCreated0(nodeId, channel, fut0);
                    }
                    finally {
                        lock.readLock().unlock();
                    }
                }
            });
        }
    }

    /** */
    public void stop0(boolean cancel) {
        lock.writeLock().lock();

        try {
            for (RebalanceDownloadFuture rebFut : futMap.values())
                rebFut.cancel();

            futMap.clear();

            cctx.gridIO().removeChannelListener(rebalanceThreadTopic(), null);
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * @param nodeId The remote node id.
     * @param channel A blocking socket channel to handle rebalance partitions.
     * @param rebFut The future of assignments handling.
     */
    private void onChannelCreated0(
        UUID nodeId,
        IgniteSocketChannel channel,
        RebalanceDownloadFuture rebFut
    ) {
        assert rebFut.nodeId.equals(nodeId);

        if (rebFut.isComplete())
            return;

        U.log(log, "Channel created. Start handling partition files [channel=" + channel + ']');

        FileTransferManager<PartitionFileMetaInfo> source = null;

        int totalParts = rebFut.nodeAssigns.values().stream()
            .mapToInt(GridIntList::size)
            .sum();

        AffinityTopologyVersion topVer = rebFut.topVer;
        Integer grpId = null;
        Integer partId = null;

        try {
            source = new FileTransferManager<>(cctx.kernalContext(), channel.channel(), rebFut);

            PartitionFileMetaInfo meta;

            for (int i = 0; i < totalParts && !rebFut.isComplete(); i++) {
                // Start processing original partition file.
                source.readMetaInto(meta = new PartitionFileMetaInfo());

                assert meta.getType() == 0 : meta;

                U.log(log, "Partition meta received from source: " + meta);

                grpId = meta.getGrpId();
                partId = meta.getPartId();

                CacheGroupContext grp = cctx.cache().cacheGroup(grpId);
                AffinityAssignment aff = grp.affinity().cachedAffinity(topVer);

                // WAL should be enabled for rebalancing cache groups by partition files
                // to provide recovery guaranties over switching from temp-WAL to the original
                // partition file by flushing a special WAL-record.
                assert grp.localWalEnabled() : "WAL must be enabled to rebalance via files: " + grp;

                if (aff.get(partId).contains(cctx.localNode())) {
                    GridDhtLocalPartition part = grp.topology().localPartition(partId, topVer, true);

                    assert part != null;

                    if (part.state() == MOVING) {
                        boolean reserved = part.reserve();

                        assert reserved : "Failed to reserve partition [igniteInstanceName=" +
                            cctx.igniteInstanceName() + ", grp=" + grp.cacheOrGroupName() + ", part=" + part + ']';

                        part.lock();

                        try {
                            FilePageStore store = (FilePageStore)filePageStore.getStore(grpId, partId);

                            File cfgFile = new File(store.getFileAbsolutePath());

                            assert store.size() <= meta.getSize() : "Trim zero bytes from the end of partition";

                            U.log(log, "Start receiving partition file: " + cfgFile.getName());

                            // TODO Skip the file header and first pageId with meta.
                            // Will restore meta pageId on merge delta file phase, if it exists
                            source.readInto(cfgFile, 0, meta.getSize());

                            U.log(log, "Partition file uptated succusfully: " + cfgFile.getName());

                            // Start processing delta file.
                            source.readMetaInto(meta = new PartitionFileMetaInfo());

                            U.log(log, "Received meta pages: " + meta);

                            assert meta.getType() == 1 : meta;

                            applyPartitionDeltaPages(source, store, meta.getSize());

                            U.log(log, "Partition delta pages applied successfully");

                            // TODO Validate CRC partition

                            // TODO Rebuild indexes by partition

                            // TODO Owning partition here, but must own on switch from temp-WAl
                            // There is no need to check grp.localWalEnabled() as for the partition
                            // file transfer process it has no meaning. We always apply this partiton
                            // without any records to the WAL.
                            boolean isOwned = grp.topology().own(part);

                            assert isOwned : "Partition must be owned: " + part;

                            // TODO Send EVT_CACHE_REBALANCE_PART_LOADED

                            rebFut.markPartitionDone(grpId, partId);

                            U.log(log, "The partition file have been processed successfully [" +
                                "nodeId=" + cctx.localNodeId() + ", grpId=" + grpId +
                                ", partId=" + partId + ", state=" + part.state().name() +
                                ", cfgFile=" + cfgFile.getName() + ']');
                        }
                        finally {
                            part.unlock();
                            part.release();
                        }
                    }
                    else {
                        if (log.isDebugEnabled()) {
                            log.debug("Skipping partition (state is not MOVING) " +
                                "[grpId=" + grpId + ", partId=" + partId + ", nodeId=" + nodeId + ']');
                        }
                    }
                }
            }

            rebFut.onCompleteSuccess();
        }
        catch (IOException | IgniteCheckedException e) {
            U.error(log, "An error during receiving binary data from channel: " + channel, e);

            rebFut.onDone(new IgniteCheckedException("Error with downloading binary data from remote node " +
                "[grpId=" + grpId + ", partId=" + partId + ", nodeId=" + nodeId + ']', e));
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
        // There is no delta file to apply.
        if (size <= 0)
            return;

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
     * @param assignsMap The map of cache groups assignments to process.
     * @return The map of cache assignments <tt>[group_order, [node, [group_id, partitions]]]</tt>
     */
    private NavigableMap<Integer, Map<ClusterNode, Map<Integer, GridIntList>>> sliceNodeCacheAssignments(
        Map<Integer, GridDhtPreloaderAssignments> assignsMap
    ) {
        NavigableMap<Integer, Map<ClusterNode, Map<Integer, GridIntList>>> result = new TreeMap<>();

        for (Map.Entry<Integer, GridDhtPreloaderAssignments> grpEntry : assignsMap.entrySet()) {
            int grpId = grpEntry.getKey();
            CacheGroupContext grp = cctx.cache().cacheGroup(grpId);

            if (cctx.preloadMgr().rebalanceByPartitionSupported(grp, grpEntry.getValue())) {
                int grpOrderNo = grp.config().getRebalanceOrder();

                result.putIfAbsent(grpOrderNo, new HashMap<>());

                for (Map.Entry<ClusterNode, GridDhtPartitionDemandMessage> grpAssigns : grpEntry.getValue().entrySet()) {
                    ClusterNode node = grpAssigns.getKey();

                    result.get(grpOrderNo).putIfAbsent(node, new HashMap<>());

                    GridIntList intParts = getAsIntList(grpAssigns.getValue().partitions().fullSet());

                    if (!intParts.isEmpty())
                        result.get(grpOrderNo).get(node).putIfAbsent(grpId, intParts);
                }
            }
        }

        return result;
    }

    /**
     * @param assignsMap A map of cache assignments grouped by grpId.
     * @param force {@code true} if must cancel previous rebalance.
     * @param rebalanceId Current rebalance id.
     * @return Runnable to execute the chain.
     */
    public Runnable addNodeAssignments(
        Map<Integer, GridDhtPreloaderAssignments> assignsMap,
        AffinityTopologyVersion topVer,
        boolean force,
        long rebalanceId
    ) {
        NavigableMap<Integer, Map<ClusterNode, Map<Integer, GridIntList>>> nodeOrderAssignsMap =
            sliceNodeCacheAssignments(assignsMap);

        if (nodeOrderAssignsMap.isEmpty())
            return NO_OP;

        // Start new rebalance session.
        final RebalanceDownloadFuture headFut0 = headFut;

        if (!headFut0.isDone())
            headFut0.cancel();

        // TODO Start eviction.
        // Assume that the partition tag will be changed on eviction process finished,
        // so we will have no additional writes (via writeInternal method) to current
        // MOVING partition if checkpoint thread occures. So the current partition file
        // can be easily replaced with the new one received from the socket.

        lock.writeLock().lock();

        try {
            RebalanceDownloadFuture rqFut = null;
            Runnable rq = NO_OP;

            U.log(log, "Prepare the chain to demand assignments: " + nodeOrderAssignsMap);

            // Clear the previous rebalance futures if exists.
            futMap.clear();

            for (Map<ClusterNode, Map<Integer, GridIntList>> descNodeMap : nodeOrderAssignsMap.descendingMap().values()) {
                for (Map.Entry<ClusterNode, Map<Integer, GridIntList>> assignEntry : descNodeMap.entrySet()) {
                    RebalanceDownloadFuture rebFut = new RebalanceDownloadFuture(assignEntry.getKey().id(), rebalanceId,
                        assignEntry.getValue(), topVer);

                    final Runnable nextRq0 = rq;
                    final RebalanceDownloadFuture rqFut0 = rqFut;

                    if (rqFut0 == null)
                        headFut = rebFut; // The first seen rebalance node.
                    else {
                        rebFut.listen(f -> {
                            try {
                                if (f.get()) // Not cancelled.
                                    nextRq0.run();
                            }
                            catch (IgniteCheckedException e) {
                                rqFut0.onDone(e);
                            }
                        });
                    }

                    rq = requestNodePartitions(assignEntry.getKey(), rebFut);
                    rqFut = rebFut;
                }
            }

            headFut.listen(new IgniteInClosureX<IgniteInternalFuture<Boolean>>() {
                @Override public void applyx(IgniteInternalFuture<Boolean> fut0) throws IgniteCheckedException {
                    if (fut0.get())
                        U.log(log, "The final persistence rebalance future is done [result=" + fut0.isDone() + ']');
                }
            });

            return rq;
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * @param node Clustre node to send inital demand message to.
     * @param rebFut The future to handle demand request.
     */
    private Runnable requestNodePartitions(
        ClusterNode node,
        RebalanceDownloadFuture rebFut
    ) {
        return new Runnable() {
            @Override public void run() {
                if (rebFut.isComplete())
                    return;

                U.log(log, "Start partitions preloading [from=" + node.id() + ", fut=" + rebFut + ']');

                try {
                    GridPartitionCopyDemandMessage msg0 = new GridPartitionCopyDemandMessage(rebFut.rebalanceId,
                        rebFut.topVer, rebFut.nodeAssigns);

                    futMap.put(node.id(), rebFut);

                    cctx.gridIO().sendOrderedMessage(node, rebalanceThreadTopic(),
                        msg0, SYSTEM_POOL, DFLT_REBALANCE_TIMEOUT, false);
                }
                catch (IgniteCheckedException e) {
                    U.error(log, "Error sending request for demanded cache partitions", e);

                    rebFut.onDone(e);

                    futMap.remove(node.id());
                }
            }
        };
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridPartitionDownloadManager.class, this);
    }

    /** */
    private class RebalanceDownloadFuture extends GridFutureAdapter<Boolean> {
        /** */
        private UUID nodeId;

        /** */
        private long rebalanceId;

        /** */
        @GridToStringInclude
        private Map<Integer, GridIntList> nodeAssigns;

        /** */
        private AffinityTopologyVersion topVer;

        /** */
        private Map<Integer, GridIntList> remaining;

        /**
         * Default constructor for the dummy future.
         */
        public RebalanceDownloadFuture() {
            onDone();
        }

        /**
         * @param nodeId The remote nodeId.
         * @param nodeAssigns Map of assignments to request from remote.
         */
        public RebalanceDownloadFuture(
            UUID nodeId,
            long rebalanceId,
            Map<Integer, GridIntList> nodeAssigns,
            AffinityTopologyVersion topVer
        ) {
            this.nodeId = nodeId;
            this.rebalanceId = rebalanceId;
            this.nodeAssigns = nodeAssigns;
            this.topVer = topVer;

            remaining = U.newHashMap(nodeAssigns.size());

            for (Map.Entry<Integer, GridIntList> grpPartEntry : nodeAssigns.entrySet())
                remaining.putIfAbsent(grpPartEntry.getKey(), grpPartEntry.getValue().copy());
        }

        /** {@inheritDoc} */
        @Override public boolean cancel() {
            return onCancelled();
        }

        /** */
        public synchronized void onCompleteSuccess() {
            assert remaining.isEmpty();

            U.log(log, "Partitions have been scheduled to resend. Files have been transferred " +
                "[from=" + nodeId + ", to=" + cctx.localNodeId() + ']');

            // Late affinity assignment
            cctx.exchange().scheduleResendPartitions();

            onDone(true);
        }

        /**
         * @return {@code True} if current future cannot be processed.
         */
        public boolean isComplete() {
            return isCancelled() || isFailed() || isDone();
        }

        /**
         * @param grpId Cache group id to search.
         * @param partId Cache partition to remove;
         * @throws IgniteCheckedException If fails.
         */
        public synchronized void markPartitionDone(int grpId, int partId) throws IgniteCheckedException {
            GridIntList parts = remaining.get(grpId);

            if (parts == null)
                throw new IgniteCheckedException("Partition index incorrect [grpId=" + grpId + ", partId=" + partId + ']');

            int partIdx = parts.removeValue(0, partId);

            assert partIdx >= 0;

            if (parts.isEmpty())
                remaining.remove(grpId);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(RebalanceDownloadFuture.class, this);
        }
    }
}
