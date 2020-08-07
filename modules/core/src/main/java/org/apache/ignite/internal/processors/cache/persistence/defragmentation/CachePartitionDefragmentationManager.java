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
import java.io.IOException;
import java.nio.file.Files;
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
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointProgress;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.freelist.AbstractFreeList;
import org.apache.ignite.internal.processors.cache.tree.CacheDataTree;
import org.apache.ignite.internal.processors.cache.tree.PendingEntriesTree;
import org.apache.ignite.internal.processors.cache.tree.PendingRow;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;

import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.FLAG_DATA;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.FILE_SUFFIX;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.INDEX_FILE_NAME;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.PART_FILE_PREFIX;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.PART_FILE_TEMPLATE;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.TMP_SUFFIX;

/** */
public class CachePartitionDefragmentationManager {
    /** */
    public static final String DEFRAGMENTATION = "DEFRAGMENTATION";

    /** */
    public static final String SKIP_CP_ENTRIES = "SKIP_CP_ENTRIES";

    /** */
    private static final String DFRG_INDEX_FILE_NAME = "index-dfrg.bin";

    /** */
    private static final String DFRG_INDEX_TMP_FILE_NAME = DFRG_INDEX_FILE_NAME + TMP_SUFFIX;

    /** */
    private static final String DFRG_PARTITION_FILE_PREFIX = PART_FILE_PREFIX + "dfrg-";

    /** Defragmented partition file template. */
    public static final String DFRG_PARTITION_FILE_TEMPLATE = DFRG_PARTITION_FILE_PREFIX + "%d" + FILE_SUFFIX;

    /** Defragmented partition temp file template. */
    private static final String DFRG_PARTITION_TMP_FILE_TEMPLATE = DFRG_PARTITION_FILE_TEMPLATE + TMP_SUFFIX;

    /** */
    public static final String DFRG_LINK_MAPPING_FILE_TEMPLATE = "part-map-%d.bin";

    /** */
    public static final String DFRG_COMPLETION_MARKER_FILE_NAME = "dfrg-completion-marker";

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
    public static void batchRenameDefragmentedCacheGroupPartitions(File workDir, IgniteLogger log) {
        File completionMarkerFile = defragmentationCompletionMarkerFile(workDir);

        if (!completionMarkerFile.exists())
            return;

        try {
            for (File mappingFile : workDir.listFiles((dir, name) -> name.startsWith("part-map-")))
                Files.delete(mappingFile.toPath());

            for (File partFile : workDir.listFiles((dir, name) -> name.startsWith(DFRG_PARTITION_FILE_PREFIX))) {
                int partId = extractPartId(partFile.getName());

                File oldPartFile = new File(workDir, String.format(PART_FILE_TEMPLATE, partId));

                Files.move(partFile.toPath(), oldPartFile.toPath(), ATOMIC_MOVE, REPLACE_EXISTING);
            }

            File idxFile = new File(workDir, DFRG_INDEX_FILE_NAME);

            if (idxFile.exists()) {
                File oldIdxFile = new File(workDir, INDEX_FILE_NAME);

                Files.move(idxFile.toPath(), oldIdxFile.toPath(), ATOMIC_MOVE, REPLACE_EXISTING);
            }
        }
        catch (IOException e) {
            //TODO Handle.
            e.printStackTrace();
        }
    }

    /** */
    private static int extractPartId(String partFileName) {
        assert partFileName.endsWith(FILE_SUFFIX) : partFileName;

        String partIdStr = partFileName.substring(
            DFRG_PARTITION_FILE_PREFIX.length(),
            partFileName.length() - FILE_SUFFIX.length()
        );

        return Integer.parseInt(partIdStr);
    }

    /** */
    public void executeDefragmentation() {
        FilePageStoreManager filePageStoreMgr = (FilePageStoreManager)sharedCtx.pageStore();

        System.setProperty(SKIP_CP_ENTRIES, "true");
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

                    for (int partId : parts) {
                        if (skipAlreadyDefragmentedPartition(workDir, partId))
                            continue;

                        AtomicLong partPagesAllocated = new AtomicLong();

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
                            defragmentSinglePartition(defrgCtx.groupContextByGroupId(grpId), partId);
                        }
                        finally {
                            sharedCtx.database().checkpointReadUnlock();
                        }

                        CheckpointProgress cp = sharedCtx.database().forceCheckpoint("part-" + partId);

                        GridFutureAdapter<?> cpFut = cp.futureFor(CheckpointState.FINISHED);

                        cpFut.listen(fut -> {
                            if (fut.error() == null) {
                                File defragmentedPartTmpFile = defragmentedPartTmpFile(workDir, partId);
                                File defragmentedPartFile = defragmentedPartFile(workDir, partId);

                                try {
                                    Files.move(defragmentedPartTmpFile.toPath(), defragmentedPartFile.toPath());
                                }
                                catch (IOException ignore) {
                                    //TODO Handle.
                                }
                            }
                        });

                        log.info(S.toString(
                            "Partition defragmented",
                            "partId", partId, false,
                            "oldPages", defrgCtx.pageStore(grpId, partId).pages(), false,
                            "newPages", partPagesAllocated.get(), false,
                            "mappingPages", mappingPagesAllocated.get(), false,
                            "filePath", defragmentedPartFile(workDir, partId).getAbsolutePath(), false
                        ));

                        cpFut.get();
                    }

                    if (sharedCtx.pageStore().hasIndexStore(grpId)) {
                        // Defragment index file.
                    }

                    writeDefragmentationCompletionMarker(workDir);

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
    private boolean skipAlreadyDefragmentedPartition(File workDir, int partId) {
        File defragmentedPartFile = defragmentedPartFile(workDir, partId);
        File defragmentedPartMappingFile = defragmentedPartMappingFile(workDir, partId);

        if (defragmentedPartFile.exists() && defragmentedPartMappingFile.exists()) {
            if (log.isInfoEnabled()) {
                log.info(S.toString(
                    "Already defragmented partition found, skipping.",
                    "partId", partId, false,
                    "fileName", defragmentedPartFile.getName(), false,
                    "workDir", workDir.getAbsolutePath(), false
                ));
            }

            return true;
        }

        File defragmentedPartTmpFile = defragmentedPartTmpFile(workDir, partId);

        try {
            Files.deleteIfExists(defragmentedPartTmpFile.toPath());

            Files.deleteIfExists(defragmentedPartFile.toPath());

            Files.deleteIfExists(defragmentedPartMappingFile.toPath());
        }
        catch (IOException e) {
            //TODO Handle.
            e.printStackTrace();
        }

        return false;
    }

    /** */
    private static File defragmentedIndexTmpFile(File workDir) {
        return new File(workDir, DFRG_INDEX_TMP_FILE_NAME);
    }

    /** */
    private static File defragmentedPartTmpFile(File workDir, int partId) {
        return new File(workDir, String.format(DFRG_PARTITION_TMP_FILE_TEMPLATE, partId));
    }

    /** */
    private static File defragmentedPartFile(File workDir, int partId) {
        return new File(workDir, String.format(DFRG_PARTITION_FILE_TEMPLATE, partId));
    }

    /** */
    private static File defragmentedPartMappingFile(File workDir, int partId) {
        return new File(workDir, String.format(DFRG_LINK_MAPPING_FILE_TEMPLATE, partId));
    }

    /** */
    private static File defragmentationCompletionMarkerFile(File workDir) {
        return new File(workDir, DFRG_COMPLETION_MARKER_FILE_NAME);
    }

    /** */
    private void defragmentSinglePartition(CacheGroupContext grpCtx, int partId) throws IgniteCheckedException {
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

        GridCacheOffheapManager.GridCacheDataStore cacheDataStore = new GridCacheOffheapManager.GridCacheDataStore(newContext, partId, true, defrgCtx.busyLock(), defrgCtx.log);

        cacheDataStore.init();

        PageMemory memory = mappingRegion.pageMemory();

        FullPageId linkMapMetaPageId = new FullPageId(memory.allocatePage(grpCtx.groupId(), partId, FLAG_DATA), grpCtx.groupId());

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

    /** */
    private void writeDefragmentationCompletionMarker(File workDir) {
        try {
            FileIOFactory ioFactory = sharedCtx.gridConfig().getDataStorageConfiguration().getFileIOFactory();

            File completionMarker = defragmentationCompletionMarkerFile(workDir);

            try (FileIO io = ioFactory.create(completionMarker, CREATE_NEW, WRITE)) {
                io.force(true);
            }
        }
        catch (IOException e) {
            //TODO Handle.
            e.printStackTrace();
        }
    }
}
