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
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.pagemem.store.PageStore;
import org.apache.ignite.internal.processors.affinity.AffinityAssignment;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheDataStoreEx;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStore;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.file.FileTransferManager;
import org.apache.ignite.internal.processors.cache.persistence.file.meta.PartitionFileMetaInfo;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.communication.tcp.channel.IgniteSocketChannel;

import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.MOVING;
import static org.apache.ignite.internal.processors.cache.persistence.preload.GridCachePreloadSharedManager.staleFuture;

/**
 *
 */
public class PartitionDownloadManager {
    /** */
    private GridCacheSharedContext<?, ?> cctx;

    /** */
    private final IgniteLogger log;

    /** */
    private FilePageStoreManager filePageStore;

    /**
     * @param ktx Kernal context to process.
     */
    public PartitionDownloadManager(GridKernalContext ktx) {
        assert CU.isPersistenceEnabled(ktx.config());

        log = ktx.log(getClass());
    }

    /**
     * @param cctx Cache shared context.
     */
    void start0(GridCacheSharedContext<?, ?> cctx) {
        assert cctx.pageStore() instanceof FilePageStoreManager : cctx.pageStore();

        this.cctx = cctx;

        filePageStore = (FilePageStoreManager)cctx.pageStore();
    }

    /**
     * @param cancel <tt>true</tt> to cancel all pending tasks.
     */
    void stop0(boolean cancel) {
        // No-op.
    }

    /**
     * @param nodeId The remote node id.
     * @param channel A blocking socket channel to handle rebalance partitions.
     * @param rebFut The future of assignments handling.
     */
    void onChannelCreated0(
        UUID nodeId,
        IgniteSocketChannel channel,
        Map<Integer, Set<Integer>> nodeAssigns,
        AffinityTopologyVersion topVer,
        GridFutureAdapter<Boolean> rebFut
    ) {
        FileTransferManager<PartitionFileMetaInfo> source = null;

        int totalParts = nodeAssigns.values().stream()
            .mapToInt(Set::size)
            .sum();

        Integer grpId = null;
        Integer partId = null;

        try {
            source = new FileTransferManager<>(cctx.kernalContext(), channel.channel(), rebFut);

            PartitionFileMetaInfo meta;

            for (int i = 0; i < totalParts && !staleFuture(rebFut); i++) {
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
                        assert part.dataStoreMode() == CacheDataStoreEx.StorageMode.LOG_ONLY :
                            "The partition must be set to LOG_ONLY mode [partId=" + part.id() +
                                ", grp=" + part.group().cacheOrGroupName() + ']';

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

                            U.log(log, "The partition file has been downloaded: " + cfgFile.getName());

                            // Start processing delta file.
                            source.readMetaInto(meta = new PartitionFileMetaInfo());

                            assert meta.getType() == 1 : meta;

                            applyPartitionDeltaPages(source, store, meta.getSize());

                            U.log(log, "The partition file deltas has been applied: " + cfgFile.getName());

                            // TODO Validate CRC partition

                            U.log(log, "The partition file has been downloaded and all deltas has been applied [" +
                                "nodeId=" + cctx.localNodeId() + ", grpId=" + grpId +
                                ", partId=" + partId + ", state=" + part.state().name() +
                                ", cfgFile=" + cfgFile.getName() + ']');

                            cctx.preloadMgr().onPartitionDownloaded(nodeId, grp, part);
                        }
                        finally {
                            part.unlock();
                            part.release();
                        }
                    }
                    else {
                        log.error("Skipping partition (state is not MOVING but it must!) " +
                            "[grpId=" + grpId + ", partId=" + partId + ", nodeId=" + nodeId + ']');
                    }
                }
            }
        }
        catch (IOException | IgniteCheckedException e) {
            U.error(log, "An error during downloading data from the remote node: " + nodeId, e);

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

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(PartitionDownloadManager.class, this);
    }
}
