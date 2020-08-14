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

package org.apache.ignite.internal.processors.cache.persistence.defragmentation;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.StreamSupport;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.metric.IoStatisticsHolderNoOp;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.store.PageStore;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.CacheType;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.IgniteCacheOffheapManager.CacheDataStore;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.persistence.CheckpointState;
import org.apache.ignite.internal.processors.cache.persistence.DataRegion;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheOffheapManager;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.freelist.AbstractFreeList;
import org.apache.ignite.internal.processors.cache.persistence.freelist.SimpleDataRow;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryEx;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree.TreeRowClosure;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusLeafIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusMetaIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PagePartitionCountersIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PagePartitionMetaIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PagePartitionMetaIOV2;
import org.apache.ignite.internal.processors.cache.tree.CacheDataTree;
import org.apache.ignite.internal.processors.cache.tree.PendingEntriesTree;
import org.apache.ignite.internal.processors.cache.tree.PendingRow;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteInClosure;

import static org.apache.ignite.internal.pagemem.PageIdAllocator.FLAG_DATA;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.FLAG_IDX;
import static org.apache.ignite.internal.processors.cache.persistence.defragmentation.CachePartitionDefragmentationManager.PageAccessType.ACCESS_READ;
import static org.apache.ignite.internal.processors.cache.persistence.defragmentation.CachePartitionDefragmentationManager.PageAccessType.ACCESS_WRITE;
import static org.apache.ignite.internal.processors.cache.persistence.defragmentation.DefragmentationFileUtils.batchRenameDefragmentedCacheGroupPartitions;
import static org.apache.ignite.internal.processors.cache.persistence.defragmentation.DefragmentationFileUtils.defragmentedIndexTmpFile;
import static org.apache.ignite.internal.processors.cache.persistence.defragmentation.DefragmentationFileUtils.defragmentedPartFile;
import static org.apache.ignite.internal.processors.cache.persistence.defragmentation.DefragmentationFileUtils.defragmentedPartMappingFile;
import static org.apache.ignite.internal.processors.cache.persistence.defragmentation.DefragmentationFileUtils.defragmentedPartTmpFile;
import static org.apache.ignite.internal.processors.cache.persistence.defragmentation.DefragmentationFileUtils.renameTempIndexFile;
import static org.apache.ignite.internal.processors.cache.persistence.defragmentation.DefragmentationFileUtils.renameTempPartitionFile;
import static org.apache.ignite.internal.processors.cache.persistence.defragmentation.DefragmentationFileUtils.skipAlreadyDefragmentedCacheGroup;
import static org.apache.ignite.internal.processors.cache.persistence.defragmentation.DefragmentationFileUtils.skipAlreadyDefragmentedPartition;
import static org.apache.ignite.internal.processors.cache.persistence.defragmentation.DefragmentationFileUtils.writeDefragmentationCompletionMarker;

/** */
public class CachePartitionDefragmentationManager {
    /** */
    @Deprecated public static final String DEFRAGMENTATION = "DEFRAGMENTATION";

    /** */
    @Deprecated public static final String SKIP_CP_ENTRIES = "SKIP_CP_ENTRIES";

    /** Cache shared context. */
    private final GridCacheSharedContext<?, ?> sharedCtx;

    /** Defragmentation context. */
    private final CacheDefragmentationContext defrgCtx;

    /** Logger. */
    private final IgniteLogger log;

    /**
     * @param sharedCtx Cache shared context.
     * @param defrgCtx Defragmentation context.
     */
    public CachePartitionDefragmentationManager(
        GridCacheSharedContext<?, ?> sharedCtx,
        CacheDefragmentationContext defrgCtx
    ) {
        this.sharedCtx = sharedCtx;
        this.defrgCtx = defrgCtx;

        log = sharedCtx.logger(getClass());
    }

    /** */
    public void executeDefragmentation() {
        System.setProperty(SKIP_CP_ENTRIES, "true");

        try {
            FilePageStoreManager filePageStoreMgr = (FilePageStoreManager)sharedCtx.pageStore();

            DataRegion partRegion = defrgCtx.partitionsDataRegion();

            for (int grpId : defrgCtx.groupIdsForDefragmentation()) {
                File workDir = defrgCtx.workDirForGroupId(grpId);

                if (skipAlreadyDefragmentedCacheGroup(workDir, grpId, log))
                    continue;

                int[] parts = defrgCtx.partitionsForGroupId(grpId);

                if (workDir != null && parts != null) {
                    CacheGroupContext grpCtx = defrgCtx.groupContextByGroupId(grpId);

                    boolean encrypted = grpCtx.config().isEncryptionEnabled();

                    FilePageStoreFactory pageStoreFactory = filePageStoreMgr.getPageStoreFactory(grpId, encrypted);

                    //TODO Index partition file has to be deleted before we begin, otherwise there's a chance of reading corrupted file.
                    PageStore idxPageStore = pageStoreFactory.createPageStore(
                        FLAG_IDX,
                        () -> defragmentedIndexTmpFile(workDir).toPath(),
                        val -> {}
                    );

                    idxPageStore.sync();

                    defrgCtx.addPartPageStore(grpId, PageIdAllocator.INDEX_PARTITION, idxPageStore);

                    GridCompoundFuture<Object, Object> cmpFut = new GridCompoundFuture<>();

                    PageMemoryEx oldPageMem = (PageMemoryEx)grpCtx.dataRegion().pageMemory();

                    for (int partId : parts) {
                        if (skipAlreadyDefragmentedPartition(workDir, grpId, partId, log))
                            continue;

                        AtomicLong partPagesAllocated = new AtomicLong();

                        //TODO I think we should do it inside of checkpoint read lock.
                        PageStore partPageStore = pageStoreFactory.createPageStore(
                            FLAG_DATA,
                            () -> defragmentedPartTmpFile(workDir, partId).toPath(),
                            partPagesAllocated::addAndGet
                        );

                        partPageStore.sync();

                        defrgCtx.addPartPageStore(grpId, partId, partPageStore);

                        AtomicLong mappingPagesAllocated = new AtomicLong();

                        PageStore mappingPageStore = pageStoreFactory.createPageStore(
                            FLAG_DATA,
                            () -> defragmentedPartMappingFile(workDir, partId).toPath(),
                            mappingPagesAllocated::addAndGet
                        );

                        mappingPageStore.sync();

                        defrgCtx.addMappingPageStore(grpId, partId, mappingPageStore);

                        sharedCtx.database().checkpointReadLock(); //TODO We should have many small checkpoints.

                        try {
                            defragmentSinglePartition(grpCtx, partId);
                        }
                        finally {
                            sharedCtx.database().checkpointReadUnlock();
                        }

                        //TODO Move inside of defragmentSinglePartition, get rid of that ^ stupid checkpoint read lock.
                        IgniteInClosure<IgniteInternalFuture<?>> cpLsnr = fut -> {
                            if (fut.error() == null) {
                                GridCacheDatabaseSharedManager dbMgr = (GridCacheDatabaseSharedManager)sharedCtx.database();

                                GridCacheOffheapManager offheap = (GridCacheOffheapManager)grpCtx.offheap();

                                // A cheat so that we won't try to save old metadata into a new partition.
                                dbMgr.removeCheckpointListener(offheap);

                                oldPageMem.invalidate(grpId, partId);
                                ((PageMemoryEx)partRegion.pageMemory()).invalidate(grpId, partId);

                                renameTempPartitionFile(workDir, partId);

                                log.info(S.toString(
                                    "Partition defragmented",
                                    "grpId", grpId, false,
                                    "partId", partId, false,
                                    "oldPages", defrgCtx.pageStore(grpId, partId).pages(), false,
                                    "newPages", partPagesAllocated.get(), false,
                                    "saved", (defrgCtx.pageStore(grpId, partId).pages() - partPagesAllocated.get()) * partRegion.pageMemory().pageSize(), false,
                                    "mappingPages", mappingPagesAllocated.get(), false,
                                    "partFile", defragmentedPartFile(workDir, partId).getName(), false,
                                    "workDir", workDir, false
                                ));
                            }
                        };

                        GridFutureAdapter<?> cpFut = sharedCtx.database()
                            .forceCheckpoint("part-" + partId) //TODO Provide a good reason.
                            .futureFor(CheckpointState.FINISHED);

                        cpFut.listen(cpLsnr);

                        cmpFut.add((IgniteInternalFuture<Object>)cpFut);
                    }

                    // A bit too general for now, but I like it more then saving only the last checkpoint future.
                    cmpFut.markInitialized().get();

                    if (sharedCtx.pageStore().hasIndexStore(grpId)) {
                        //TODO Defragment index file.
                    }

                    oldPageMem.invalidate(grpId, PageIdAllocator.INDEX_PARTITION);

                    renameTempIndexFile(workDir);

                    writeDefragmentationCompletionMarker(workDir, log);

                    batchRenameDefragmentedCacheGroupPartitions(workDir, log);

                    defrgCtx.onCacheGroupDefragmented(grpId);
                }
            }
        }
        catch (IgniteCheckedException e) {
            // No-op for now.
            e.printStackTrace();
        }
        finally {
            System.clearProperty(SKIP_CP_ENTRIES);
        }
    }

    /** */
    private void defragmentSinglePartition(CacheGroupContext grpCtx, int partId) throws IgniteCheckedException {
        DataRegion partRegion = defrgCtx.partitionsDataRegion();
        PageMemoryEx partPageMem = (PageMemoryEx)partRegion.pageMemory();

        DataRegion mappingRegion = defrgCtx.mappingDataRegion();

        PageMemoryEx cachePageMem = (PageMemoryEx)grpCtx.dataRegion().pageMemory();

        int grpId = grpCtx.groupId();

        CacheGroupContext newCtx = new CacheGroupContext(
            sharedCtx,
            grpId,
            grpCtx.receivedFrom(),
            CacheType.USER,
            grpCtx.config(),
            grpCtx.affinityNode(),
            partRegion,
            grpCtx.cacheObjectContext(),
            null,
            null,
            grpCtx.localStartVersion(),
            true,
            false,
            true
        );

        newCtx.start();

        GridCacheOffheapManager.GridCacheDataStore newCacheDataStore = new GridCacheOffheapManager.GridCacheDataStore(newCtx, partId, true, defrgCtx.busyLock(), defrgCtx.log);

        newCacheDataStore.init();

        PageMemory memory = mappingRegion.pageMemory();

        FullPageId linkMapMetaPageId = new FullPageId(memory.allocatePage(grpId, partId, FLAG_DATA), grpId);

        LinkMap m = new LinkMap(grpCtx, memory, linkMapMetaPageId.pageId());

        Iterable<CacheDataStore> stores = grpCtx.offheap().cacheDataStores();

        CacheDataStore oldCacheDataStore = StreamSupport
            .stream(stores.spliterator(), false)
            .filter(s -> grpId == s.tree().groupId())
            .findFirst()
            .orElse(null);

        CacheDataTree tree = oldCacheDataStore.tree();

        CacheDataTree newTree = newCacheDataStore.tree();
        PendingEntriesTree newPendingTree = newCacheDataStore.pendingTree();
        AbstractFreeList<?> freeList = newCacheDataStore.getCacheStoreFreeList();

        List<GridCacheContext> cacheContexts = grpCtx.caches();

        iterate(tree, cachePageMem, (tree0, io, pageAddr, idx) -> {
            CacheDataRow row = tree.getRow(io, pageAddr, idx);
            int cacheId = row.cacheId();

            GridCacheContext ctx;

            //TODO Finding the context via iteration is a bad thing.
            if (cacheId == CU.UNDEFINED_CACHE_ID)
                ctx = cacheContexts.get(0);
            else
                ctx = cacheContexts.stream().filter(c -> c.cacheId() == cacheId).findFirst().orElse(null);

            assert ctx != null;

            //TODO mvcc?
            CacheDataRow newRow = newCacheDataStore.createRow(ctx, row.key(), row.value(), row.version(), row.expireTime(), null);

            long link = row.link();

            newTree.put(newRow);

            long newLink = newRow.link();

            m.put(link, newLink);

            if (row.expireTime() != 0)
                newPendingTree.putx(new PendingRow(cacheId, row.expireTime(), newLink));

            return true;
        });

        freeList.saveMetadata(IoStatisticsHolderNoOp.INSTANCE);

        copyCacheMetadata(
            cachePageMem,
            oldCacheDataStore,
            partPageMem,
            newCacheDataStore,
            grpId,
            partId
        );

        //TODO Invalidate mapping in mapping region?
        //TODO Invalidate PageStore for this partition.
    }

    /** */
    private void copyCacheMetadata(
        PageMemoryEx oldPageMemory,
        CacheDataStore oldCacheDataStore,
        PageMemoryEx newPageMemory,
        CacheDataStore newCacheDataStore,
        int grpId,
        int partId
    ) throws IgniteCheckedException {
        long partMetaPageId = oldPageMemory.partitionMetaPageId(grpId, partId); // Same for all page memories.

        access(ACCESS_READ, oldPageMemory, grpId, partMetaPageId, oldPartMetaPageAddr -> {
            PagePartitionMetaIO oldPartMetaIo = PageIO.getPageIO(oldPartMetaPageAddr);

            // Newer meta versions may contain new data that we don't copy during defragmentation.
            assert Arrays.asList(1, 2).contains(oldPartMetaIo.getVersion()) : oldPartMetaIo.getVersion();

            access(ACCESS_WRITE, newPageMemory, grpId, partMetaPageId, newPartMetaPageAddr -> {
                PagePartitionMetaIOV2 newPartMetaIo = PageIO.getPageIO(newPartMetaPageAddr);

                // Copy partition state.
                byte partState = oldPartMetaIo.getPartitionState(oldPartMetaPageAddr);
                newPartMetaIo.setPartitionState(newPartMetaPageAddr, partState);

                // Copy cache size for single cache group.
                long size = oldPartMetaIo.getSize(oldPartMetaPageAddr);
                newPartMetaIo.setSize(newPartMetaPageAddr, size);

                // Copy update counter value.
                long updateCntr = oldPartMetaIo.getUpdateCounter(oldPartMetaPageAddr);
                newPartMetaIo.setUpdateCounter(newPartMetaPageAddr, updateCntr);

                // Copy global remove Id.
                long rmvId = oldPartMetaIo.getGlobalRemoveId(oldPartMetaPageAddr);
                newPartMetaIo.setGlobalRemoveId(newPartMetaPageAddr, rmvId);

                // Copy cache sizes for shared cache group.
                long oldCountersPageId = oldPartMetaIo.getCountersPageId(oldPartMetaPageAddr);
                if (oldCountersPageId != 0L) {
                    //TODO Extract method or something. This code block is too big.
                    long newCountersPageId = newPageMemory.allocatePage(grpId, partId, FLAG_DATA);

                    newPartMetaIo.setCountersPageId(newPartMetaPageAddr, newCountersPageId);

                    AtomicLong nextNewCountersPageIdRef = new AtomicLong(newCountersPageId);
                    AtomicLong nextOldCountersPageIdRef = new AtomicLong(oldCountersPageId);

                    while (nextNewCountersPageIdRef.get() != 0L) {
                        access(ACCESS_READ, oldPageMemory, grpId, nextOldCountersPageIdRef.get(), oldCountersPageAddr ->
                            access(ACCESS_WRITE, newPageMemory, grpId, nextNewCountersPageIdRef.get(), newCountersPageAddr -> {
                                PagePartitionCountersIO newPartCountersIo = PagePartitionCountersIO.VERSIONS.latest();

                                newPartCountersIo.initNewPage(newCountersPageAddr, nextNewCountersPageIdRef.get(), oldPageMemory.pageSize());

                                PagePartitionCountersIO oldCountersPageIo = PageIO.getPageIO(oldCountersPageAddr);

                                oldCountersPageIo.copyCacheSizes(
                                    oldCountersPageAddr,
                                    newCountersPageAddr
                                );

                                if (oldCountersPageIo.getLastFlag(oldCountersPageAddr)) {
                                    newPartCountersIo.setLastFlag(newCountersPageAddr, true);

                                    nextOldCountersPageIdRef.set(0L);
                                    nextNewCountersPageIdRef.set(0L);
                                }
                                else {
                                    nextOldCountersPageIdRef.set(oldCountersPageIo.getNextCountersPageId(oldCountersPageAddr));

                                    long nextNewCountersPageId = newPageMemory.allocatePage(grpId, partId, FLAG_DATA);

                                    newPartCountersIo.setNextCountersPageId(newCountersPageAddr, nextNewCountersPageId);

                                    nextNewCountersPageIdRef.set(nextNewCountersPageId);
                                }

                                return null;
                            })
                        );
                    }
                }

                // Copy counter gaps.
                long oldGapsLink = oldPartMetaIo.getGapsLink(oldPartMetaPageAddr);
                if (oldGapsLink != 0L) {
                    byte[] gapsBytes = oldCacheDataStore.partStorage().readRow(oldGapsLink);

                    SimpleDataRow gapsDataRow = new SimpleDataRow(partId, gapsBytes);

                    newCacheDataStore.partStorage().insertDataRow(gapsDataRow, IoStatisticsHolderNoOp.INSTANCE);

                    newPartMetaIo.setGapsLink(newPartMetaPageAddr, gapsDataRow.link());
                }

                return null;
            });

            return null;
        });
    }

    // Performance impact of constant closures allocation is not clear. So this method should be avoided in massive
    // operations like tree leaves access.
    /** */
    private static <T> T access(
        PageAccessType access,
        PageMemoryEx pageMemory,
        int grpId,
        long pageId,
        PageAccessor<T> accessor
    ) throws IgniteCheckedException {
        assert access != null;
        long page = pageMemory.acquirePage(grpId, pageId);

        try {
            long pageAddr = access == ACCESS_READ
                ? pageMemory.readLock(grpId, pageId, page)
                : pageMemory.writeLock(grpId, pageId, page);

            try {
                return accessor.access(pageAddr);
            }
            finally {
                if (access == ACCESS_READ)
                    pageMemory.readUnlock(grpId, pageId, page);
                else
                    pageMemory.writeUnlock(grpId, pageId, page, null, true);
            }
        }
        finally {
            pageMemory.releasePage(grpId, pageId, page);
        }
    }

    /** */
    @SuppressWarnings("PackageVisibleInnerClass")
    enum PageAccessType {
        /** Read access. */
        ACCESS_READ,

        /** Write access. */
        ACCESS_WRITE;
    }

    /** */
    @FunctionalInterface
    private interface PageAccessor<T> {
        /** */
        public T access(long pageAddr) throws IgniteCheckedException;
    }

    /** */
    private <L, T extends L> void iterate(
        BPlusTree<L, T> tree,
        PageMemoryEx pageMemory,
        TreeRowClosure<L, T> c
    ) throws IgniteCheckedException {
        int grpId = tree.groupId();

        long leafId = findFirstLeafId(grpId, tree.getMetaPageId(), pageMemory);

        ByteBuffer buf = ByteBuffer.allocateDirect(pageMemory.pageSize());

        long bufAddr = GridUnsafe.bufferAddress(buf);

        while (leafId != 0L) {
            long leafPage = pageMemory.acquirePage(grpId, leafId);

            BPlusIO<L> io;

            try {
                long leafPageAddr = pageMemory.readLock(grpId, leafId, leafPage);

                try {
                    io = PageIO.getBPlusIO(leafPageAddr);

                    assert io instanceof BPlusLeafIO;

                    GridUnsafe.copyMemory(leafPageAddr, bufAddr, pageMemory.pageSize());
                }
                finally {
                    pageMemory.readUnlock(grpId, leafId, leafPage);
                }
            }
            finally {
                pageMemory.releasePage(grpId, leafId, leafPage);
            }

            int cnt = io.getCount(bufAddr);

            for (int idx = 0; idx < cnt; idx++)
                c.apply(tree, io, bufAddr, idx);

            leafId = io.getForward(bufAddr);
        }
    }

    /** */
    private long findFirstLeafId(int grpId, long metaPageId, PageMemoryEx partPageMemory) throws IgniteCheckedException {
        return access(ACCESS_READ, partPageMemory, grpId, metaPageId, metaPageAddr -> {
            BPlusMetaIO metaIO = PageIO.getPageIO(metaPageAddr);

            return metaIO.getFirstPageId(metaPageAddr, 0);
        });
    }
}
