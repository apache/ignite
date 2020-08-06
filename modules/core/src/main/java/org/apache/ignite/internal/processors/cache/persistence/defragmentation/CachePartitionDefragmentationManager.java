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
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.StreamSupport;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.metric.IoStatisticsHolderNoOp;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.store.PageStore;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.CacheType;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.IgniteCacheOffheapManager;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.persistence.CheckpointState;
import org.apache.ignite.internal.processors.cache.persistence.DataRegion;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheOffheapManager;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.freelist.AbstractFreeList;
import org.apache.ignite.internal.processors.cache.tree.CacheDataTree;
import org.apache.ignite.internal.processors.cache.tree.PendingEntriesTree;
import org.apache.ignite.internal.processors.cache.tree.PendingRow;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;

import static org.apache.ignite.internal.pagemem.PageIdAllocator.FLAG_DATA;

/** */
public class CachePartitionDefragmentationManager {
    /** */
    private static final String DEFRAGMENTED_INDEX_TMP_FILE_NAME = "index-dfrg.bin.tmp";

    /** Defragmented partition file template. */
    public static final String DEFRAGMENTED_PARTITION_FILE_TEMPLATE = "part-dfrg-%d.bin";

    /** Defragmented partition temp file template. */
    private static final String DEFRAGMENTED_PARTITION_TMP_FILE_TEMPLATE = DEFRAGMENTED_PARTITION_FILE_TEMPLATE + ".tmp";

    /** */
    public static final String DEFRAGMENTATION_MAPPING_FILE_TEMPLATE = "part-map-%d.bin";

    /** */
    private final GridCacheSharedContext<?, ?> sharedCtx;

    /** */
    private CacheDefragmentationContext defrgCtx;

    /** */
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
        FilePageStoreManager filePageStoreMgr = (FilePageStoreManager)sharedCtx.pageStore();

        try {
            for (int grpId : defrgCtx.groupIdsForDefragmentation()) {
                File workDir = defrgCtx.workDirForGroupId(grpId);
                int[] parts = defrgCtx.partitionsForGroupId(grpId);

                boolean encrypted = defrgCtx.groupContextByGroupId(grpId).config().isEncryptionEnabled();

                if (workDir != null && parts != null) {
                    FilePageStoreFactory pageStoreFactory = filePageStoreMgr.getPageStoreFactory(grpId, encrypted);

                    PageStore idxPageStore = pageStoreFactory.createPageStore(
                        FLAG_DATA,
                        () -> defragmentedIndexTmpFile(workDir).toPath(),
                        val -> {}
                    );

                    idxPageStore.sync();

                    defrgCtx.addPartPageStore(grpId, PageIdAllocator.INDEX_PARTITION, idxPageStore);

                    for (int partIdx : parts) {
                        File defragmentedPartFile = defragmentedPartFile(workDir, partIdx);

                        if (defragmentedPartFile.exists()) {
                            if (log.isInfoEnabled()) {
                                log.info(S.toString(
                                    "Already defragmented partition found, skipping.",
                                    "partIdx", partIdx, false,
                                    "absolutePath", defragmentedPartFile.getAbsolutePath(), false
                                ));
                            }

                            continue;
                        }

                        AtomicLong partPagesAllocated = new AtomicLong();

                        PageStore partPageStore = pageStoreFactory.createPageStore(
                            FLAG_DATA,
                            () -> defragmentedPartTmpFile(workDir, partIdx).toPath(),
                            partPagesAllocated::addAndGet
                        );

                        partPageStore.sync();

                        defrgCtx.addPartPageStore(grpId, partIdx, partPageStore);

                        AtomicLong mappingPagesAllocated = new AtomicLong();

                        PageStore mappingPageStore = pageStoreFactory.createPageStore(
                            FLAG_DATA,
                            () -> partMappingFile(workDir, partIdx).toPath(),
                            mappingPagesAllocated::addAndGet
                        );

                        mappingPageStore.sync();

                        defrgCtx.addMappingPageStore(grpId, partIdx, mappingPageStore);

                        sharedCtx.database().checkpointReadLock(); //TODO We should have many small checkpoints.

                        try {
                            defragmentSinglePartition(defrgCtx.groupContextByGroupId(grpId), partIdx);
                        }
                        finally {
                            sharedCtx.database().checkpointReadUnlock();
                        }

                        sharedCtx.database().forceCheckpoint("part-" + partIdx).futureFor(CheckpointState.FINISHED).get();

                        log.info(S.toString(
                            "Partition defragmented, temp file prepared.",
                            "partIdx", partIdx, false,
                            "oldPages", defrgCtx.pageStore(grpId, partIdx).pages(), false,
                            "newPages", partPagesAllocated.get(), false,
                            "mappingPages", mappingPagesAllocated.get(), false,
                            "tmpFilePath", defragmentedPartTmpFile(workDir, partIdx).getAbsolutePath(), false
                        ));
                    }

                    if (sharedCtx.pageStore().hasIndexStore(grpId)) {
                        // Defragment index file.
                        // Leave marker.
                        // Batch rename.
                    }

                    defrgCtx.onCacheGroupDefragmented(grpId);
                }
            }
        }
        catch (IgniteCheckedException e) {
            // No-op for now.
            e.printStackTrace();
        }
    }

    /** */
    private File defragmentedIndexTmpFile(File workDir) {
        return new File(workDir, DEFRAGMENTED_INDEX_TMP_FILE_NAME);
    }

    /** */
    private File defragmentedPartTmpFile(File workDir, int partIdx) {
        return new File(workDir, String.format(DEFRAGMENTED_PARTITION_TMP_FILE_TEMPLATE, partIdx));
    }

    /** */
    private File defragmentedPartFile(File workDir, int partIdx) {
        return new File(workDir, String.format(DEFRAGMENTED_PARTITION_FILE_TEMPLATE, partIdx));
    }

    /** */
    private File partMappingFile(File workDir, int partIdx) {
        return new File(workDir, String.format(DEFRAGMENTATION_MAPPING_FILE_TEMPLATE, partIdx));
    }

    /** */
    private void defragmentSinglePartition(CacheGroupContext grpCtx, int partIdx) throws IgniteCheckedException {
        DataRegion partRegion = defrgCtx.partitionsDataRegion();
        DataRegion mappingRegion = defrgCtx.mappingDataRegion();

        CacheGroupContext newContext = new CacheGroupContext(
            sharedCtx,
            grpCtx.groupId(),
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

        newContext.start();

        GridCacheOffheapManager.GridCacheDataStore cacheDataStore = new GridCacheOffheapManager.GridCacheDataStore(newContext, partIdx, true, defrgCtx.busyLock(), defrgCtx.log);

        cacheDataStore.init();

        PageMemory memory = mappingRegion.pageMemory();

        FullPageId linkMapMetaPageId = new FullPageId(memory.allocatePage(grpCtx.groupId(), partIdx, FLAG_DATA), grpCtx.groupId());

        LinkMap m = new LinkMap(grpCtx, memory, linkMapMetaPageId.pageId());

        Iterable<IgniteCacheOffheapManager.CacheDataStore> stores = grpCtx.offheap().cacheDataStores();

        IgniteCacheOffheapManager.CacheDataStore store = StreamSupport
            .stream(stores.spliterator(), false)
            .filter(s -> grpCtx.groupId() == s.tree().groupId())
            .findFirst()
            .orElse(null);

        CacheDataTree tree = store.tree();

        CacheDataTree newTree = cacheDataStore.tree();
        PendingEntriesTree newPendingTree = cacheDataStore.pendingTree();
        AbstractFreeList freeList = cacheDataStore.getCacheStoreFreeList();

        CacheDataRow lower = tree.findFirst();
        CacheDataRow upper = tree.findLast();

        List<GridCacheContext> cacheContexts = grpCtx.caches();

        tree.iterate(lower, upper, (tree0, io, pageAddr, idx) -> {
            CacheDataRow row = tree.getRow(io, pageAddr, idx);
            int cacheId = row.cacheId();

            GridCacheContext context;

            if (cacheId == CU.UNDEFINED_CACHE_ID)
                context = cacheContexts.get(0);
            else
                context = cacheContexts.stream().filter(c -> c.cacheId() == cacheId).findFirst().orElse(null);

            assert context != null;

            //TODO mvcc?
            CacheDataRow newRow = cacheDataStore.createRow(context, row.key(), row.value(), row.version(), row.expireTime(), null);

            long link = row.link();

            newTree.put(newRow);

            long newLink = newRow.link();

            m.put(link, newLink);

            if (row.expireTime() != 0)
                newPendingTree.putx(new PendingRow(cacheId, row.expireTime(), newLink));

            return true;
        });

        freeList.saveMetadata(IoStatisticsHolderNoOp.INSTANCE);

        // Trigger checkpoint.
        // Invalidate partition in partitions region.
        // Invalidate mapping in mapping region?
        // Invalidate PageStore for this partition.
        // Rename partition file.
    }
}
