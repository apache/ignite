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
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongConsumer;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.DataPageEvictionMode;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.metric.IoStatisticsHolderNoOp;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.store.PageStore;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.CacheType;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.IgniteCacheOffheapManager;
import org.apache.ignite.internal.processors.cache.IgniteCacheOffheapManager.CacheDataStore;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.persistence.CheckpointState;
import org.apache.ignite.internal.processors.cache.persistence.DataRegion;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheOffheapManager;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheOffheapManager.GridCacheDataStore;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointManager;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointTimeoutLock;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.LightweightCheckpointManager;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.freelist.AbstractFreeList;
import org.apache.ignite.internal.processors.cache.persistence.freelist.SimpleDataRow;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryEx;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PagePartitionMetaIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PagePartitionMetaIOV3;
import org.apache.ignite.internal.processors.cache.tree.AbstractDataLeafIO;
import org.apache.ignite.internal.processors.cache.tree.CacheDataTree;
import org.apache.ignite.internal.processors.cache.tree.DataRow;
import org.apache.ignite.internal.processors.cache.tree.PendingEntriesTree;
import org.apache.ignite.internal.processors.cache.tree.PendingRow;
import org.apache.ignite.internal.processors.query.GridQueryIndexing;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.util.GridAtomicLong;
import org.apache.ignite.internal.util.collection.IntHashMap;
import org.apache.ignite.internal.util.collection.IntMap;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgniteOutClosure;
import org.apache.ignite.maintenance.MaintenanceRegistry;

import static java.util.Comparator.comparing;
import static java.util.stream.StreamSupport.stream;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.FLAG_DATA;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.FLAG_IDX;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.INDEX_PARTITION;
import static org.apache.ignite.internal.processors.cache.persistence.CheckpointState.FINISHED;
import static org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager.DEFRAGMENTATION_MAPPING_REGION_NAME;
import static org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager.DEFRAGMENTATION_PART_REGION_NAME;
import static org.apache.ignite.internal.processors.cache.persistence.defragmentation.DefragmentationFileUtils.batchRenameDefragmentedCacheGroupPartitions;
import static org.apache.ignite.internal.processors.cache.persistence.defragmentation.DefragmentationFileUtils.defragmentedIndexFile;
import static org.apache.ignite.internal.processors.cache.persistence.defragmentation.DefragmentationFileUtils.defragmentedIndexTmpFile;
import static org.apache.ignite.internal.processors.cache.persistence.defragmentation.DefragmentationFileUtils.defragmentedPartFile;
import static org.apache.ignite.internal.processors.cache.persistence.defragmentation.DefragmentationFileUtils.defragmentedPartMappingFile;
import static org.apache.ignite.internal.processors.cache.persistence.defragmentation.DefragmentationFileUtils.defragmentedPartTmpFile;
import static org.apache.ignite.internal.processors.cache.persistence.defragmentation.DefragmentationFileUtils.renameTempIndexFile;
import static org.apache.ignite.internal.processors.cache.persistence.defragmentation.DefragmentationFileUtils.renameTempPartitionFile;
import static org.apache.ignite.internal.processors.cache.persistence.defragmentation.DefragmentationFileUtils.skipAlreadyDefragmentedCacheGroup;
import static org.apache.ignite.internal.processors.cache.persistence.defragmentation.DefragmentationFileUtils.skipAlreadyDefragmentedPartition;
import static org.apache.ignite.internal.processors.cache.persistence.defragmentation.DefragmentationFileUtils.writeDefragmentationCompletionMarker;

/**
 * Defragmentation manager is the core class that contains main defragmentation procedure.
 */
public class CachePartitionDefragmentationManager {
    /** */
    public static final String DEFRAGMENTATION_MNTC_TASK_NAME = "defragmentationMaintenanceTask";

    /** */
    private final Set<String> cachesForDefragmentation;

    /** */
    private final Set<CacheGroupContext> cacheGrpCtxsForDefragmentation = new TreeSet<>(comparing(CacheGroupContext::cacheOrGroupName));

    /** Cache shared context. */
    private final GridCacheSharedContext<?, ?> sharedCtx;

    /** Maintenance registry. */
    private final MaintenanceRegistry mntcReg;

    /** Logger. */
    private final IgniteLogger log;

    /** Database shared manager. */
    private final GridCacheDatabaseSharedManager dbMgr;

    /** File page store manager. */
    private final FilePageStoreManager filePageStoreMgr;

    /**
     * Checkpoint for specific defragmentation regions which would store the data to new partitions
     * during the defragmentation.
     */
    private final LightweightCheckpointManager defragmentationCheckpoint;

    /** Default checkpoint for current node. */
    private final CheckpointManager nodeCheckpoint;

    /** Page size. */
    private final int pageSize;

    /** */
    private final DataRegion partDataRegion;

    /** */
    private final DataRegion mappingDataRegion;

    /** */
    private final AtomicBoolean cancel = new AtomicBoolean();

    /** */
    private final Status status = new Status();

    /** */
    private final GridFutureAdapter<?> completionFut = new GridFutureAdapter<>();

    /**
     * @param cacheNames Names of caches to be defragmented. Empty means "all".
     * @param sharedCtx Cache shared context.
     * @param dbMgr Database manager.
     * @param filePageStoreMgr File page store manager.
     * @param nodeCheckpoint Default checkpoint for this node.
     * @param defragmentationCheckpoint Specific checkpoint for defragmentation.
     * @param pageSize Page size.
     */
    public CachePartitionDefragmentationManager(
        List<String> cacheNames,
        GridCacheSharedContext<?, ?> sharedCtx,
        GridCacheDatabaseSharedManager dbMgr,
        FilePageStoreManager filePageStoreMgr,
        CheckpointManager nodeCheckpoint,
        LightweightCheckpointManager defragmentationCheckpoint,
        int pageSize
    ) throws IgniteCheckedException {
        cachesForDefragmentation = new HashSet<>(cacheNames);

        this.dbMgr = dbMgr;
        this.filePageStoreMgr = filePageStoreMgr;
        this.pageSize = pageSize;
        this.sharedCtx = sharedCtx;

        this.mntcReg = sharedCtx.kernalContext().maintenanceRegistry();
        this.log = sharedCtx.logger(getClass());
        this.defragmentationCheckpoint = defragmentationCheckpoint;
        this.nodeCheckpoint = nodeCheckpoint;

        partDataRegion = dbMgr.dataRegion(DEFRAGMENTATION_PART_REGION_NAME);
        mappingDataRegion = dbMgr.dataRegion(DEFRAGMENTATION_MAPPING_REGION_NAME);
    }

    /** */
    public void beforeDefragmentation() throws IgniteCheckedException {
        // Checkpointer must be enabled so all pages on disk are in their latest valid state.
        dbMgr.resumeWalLogging();

        dbMgr.onStateRestored(null);

        nodeCheckpoint.forceCheckpoint("beforeDefragmentation", null).futureFor(FINISHED).get();

        dbMgr.preserveWalTailPointer();

        sharedCtx.wal().onDeActivate(sharedCtx.kernalContext());

        for (CacheGroupContext oldGrpCtx : sharedCtx.cache().cacheGroups()) {
            if (!oldGrpCtx.userCache() || cacheGrpCtxsForDefragmentation.contains(oldGrpCtx))
                continue;

            if (!cachesForDefragmentation.isEmpty()) {
                if (oldGrpCtx.caches().stream().noneMatch(cctx -> cachesForDefragmentation.contains(cctx.name())))
                    continue;
            }

            cacheGrpCtxsForDefragmentation.add(oldGrpCtx);
        }
    }

    /** */
    public void executeDefragmentation() throws IgniteCheckedException {
        Map<Integer, List<CacheDataStore>> oldStores = new HashMap<>();

        for (CacheGroupContext oldGrpCtx : cacheGrpCtxsForDefragmentation) {
            int grpId = oldGrpCtx.groupId();

            final IgniteCacheOffheapManager offheap = oldGrpCtx.offheap();

            List<CacheDataStore> oldCacheDataStores = stream(offheap.cacheDataStores().spliterator(), false)
                .filter(store -> {
                    try {
                        return filePageStoreMgr.exists(grpId, store.partId());
                    }
                    catch (IgniteCheckedException e) {
                        throw new IgniteException(e);
                    }
                })
                .collect(Collectors.toList());

            oldStores.put(grpId, oldCacheDataStores);
        }

        int partitionCount = oldStores.values().stream().mapToInt(List::size).sum();

        status.onStart(cacheGrpCtxsForDefragmentation, partitionCount);

        try {
            // Now the actual process starts.
            TreeIterator treeIter = new TreeIterator(pageSize);

            IgniteInternalFuture<?> idxDfrgFut = null;
            DataPageEvictionMode prevPageEvictionMode = null;

            for (CacheGroupContext oldGrpCtx : cacheGrpCtxsForDefragmentation) {
                int grpId = oldGrpCtx.groupId();

                File workDir = filePageStoreMgr.cacheWorkDir(oldGrpCtx.sharedGroup(), oldGrpCtx.cacheOrGroupName());

                List<CacheDataStore> oldCacheDataStores = oldStores.get(grpId);

                if (skipAlreadyDefragmentedCacheGroup(workDir, grpId, log)) {
                    status.onCacheGroupSkipped(oldGrpCtx, oldCacheDataStores.size());

                    continue;
                }

                try {
                    GridCacheOffheapManager offheap = (GridCacheOffheapManager)oldGrpCtx.offheap();

                    status.onCacheGroupStart(oldGrpCtx, oldCacheDataStores.size());

                    if (workDir == null || oldCacheDataStores.isEmpty()) {
                        status.onCacheGroupFinish(oldGrpCtx);

                        continue;
                    }

                    // We can't start defragmentation of new group on the region that has wrong eviction mode.
                    // So waiting of the previous cache group defragmentation is inevitable.
                    DataPageEvictionMode curPageEvictionMode = oldGrpCtx.dataRegion().config().getPageEvictionMode();

                    if (prevPageEvictionMode == null || prevPageEvictionMode != curPageEvictionMode) {
                        prevPageEvictionMode = curPageEvictionMode;

                        partDataRegion.config().setPageEvictionMode(curPageEvictionMode);

                        if (idxDfrgFut != null)
                            idxDfrgFut.get();
                    }

                    IntMap<CacheDataStore> cacheDataStores = new IntHashMap<>();

                    for (CacheDataStore store : offheap.cacheDataStores()) {
                        // Tree can be null for not yet initialized partitions.
                        // This would mean that these partitions are empty.
                        assert store.tree() == null || store.tree().groupId() == grpId;

                        if (store.tree() != null)
                            cacheDataStores.put(store.partId(), store);
                    }

                    dbMgr.checkpointedDataRegions().remove(oldGrpCtx.dataRegion());

                    // Another cheat. Ttl cleanup manager knows too much shit.
                    oldGrpCtx.caches().stream()
                        .filter(cacheCtx -> cacheCtx.groupId() == grpId)
                        .forEach(cacheCtx -> cacheCtx.ttl().unregister());

                    // Technically wal is already disabled, but "PageHandler.isWalDeltaRecordNeeded" doesn't care
                    // and WAL records will be allocated anyway just to be ignored later if we don't disable WAL for
                    // cache group explicitly.
                    oldGrpCtx.localWalEnabled(false, false);

                    boolean encrypted = oldGrpCtx.config().isEncryptionEnabled();

                    FilePageStoreFactory pageStoreFactory = filePageStoreMgr.getPageStoreFactory(grpId, encrypted);

                    AtomicLong idxAllocationTracker = new GridAtomicLong();

                    createIndexPageStore(grpId, workDir, pageStoreFactory, partDataRegion, idxAllocationTracker::addAndGet);

                    checkCancellation();

                    GridCompoundFuture<Object, Object> cmpFut = new GridCompoundFuture<>();

                    PageMemoryEx oldPageMem = (PageMemoryEx)oldGrpCtx.dataRegion().pageMemory();

                    CacheGroupContext newGrpCtx = new CacheGroupContext(
                        sharedCtx,
                        grpId,
                        oldGrpCtx.receivedFrom(),
                        CacheType.USER,
                        oldGrpCtx.config(),
                        oldGrpCtx.affinityNode(),
                        partDataRegion,
                        oldGrpCtx.cacheObjectContext(),
                        null,
                        null,
                        oldGrpCtx.localStartVersion(),
                        true,
                        false,
                        true
                    );

                    defragmentationCheckpoint.checkpointTimeoutLock().checkpointReadLock();

                    try {
                        // This will initialize partition meta in index partition - meta tree and reuse list.
                        newGrpCtx.start();
                    }
                    finally {
                        defragmentationCheckpoint.checkpointTimeoutLock().checkpointReadUnlock();
                    }

                    IntMap<LinkMap> linkMapByPart = new IntHashMap<>();

                    for (CacheDataStore oldCacheDataStore : oldCacheDataStores) {
                        checkCancellation();

                        int partId = oldCacheDataStore.partId();

                        PartitionContext partCtx = new PartitionContext(
                            workDir,
                            grpId,
                            partId,
                            partDataRegion,
                            mappingDataRegion,
                            oldGrpCtx,
                            newGrpCtx,
                            cacheDataStores.get(partId),
                            pageStoreFactory
                        );

                        if (skipAlreadyDefragmentedPartition(workDir, grpId, partId, log)) {
                            partCtx.createPageStore(
                                () -> defragmentedPartMappingFile(workDir, partId).toPath(),
                                partCtx.mappingPagesAllocated,
                                partCtx.mappingPageMemory
                            );

                            linkMapByPart.put(partId, partCtx.createLinkMapTree(false));

                            continue;
                        }

                        partCtx.createPageStore(
                            () -> defragmentedPartMappingFile(workDir, partId).toPath(),
                            partCtx.mappingPagesAllocated,
                            partCtx.mappingPageMemory
                        );

                        linkMapByPart.put(partId, partCtx.createLinkMapTree(true));

                        checkCancellation();

                        partCtx.createPageStore(
                            () -> defragmentedPartTmpFile(workDir, partId).toPath(),
                            partCtx.partPagesAllocated,
                            partCtx.partPageMemory
                        );

                        partCtx.createNewCacheDataStore(offheap);

                        copyPartitionData(partCtx, treeIter);

                        DefragmentationPageReadWriteManager pageMgr = (DefragmentationPageReadWriteManager)partCtx.partPageMemory.pageManager();

                        PageStore oldPageStore = filePageStoreMgr.getStore(grpId, partId);

                        status.onPartitionDefragmented(
                            oldGrpCtx,
                            oldPageStore.size(),
                            pageSize + partCtx.partPagesAllocated.get() * pageSize // + file header.
                        );

                        //TODO Move inside of defragmentSinglePartition.
                        IgniteInClosure<IgniteInternalFuture<?>> cpLsnr = fut -> {
                            if (fut.error() != null)
                                return;

                            if (log.isDebugEnabled()) {
                                log.debug(S.toString(
                                    "Partition defragmented",
                                    "grpId", grpId, false,
                                    "partId", partId, false,
                                    "oldPages", oldPageStore.pages(), false,
                                    "newPages", partCtx.partPagesAllocated.get() + 1, false,
                                    "mappingPages", partCtx.mappingPagesAllocated.get() + 1, false,
                                    "pageSize", pageSize, false,
                                    "partFile", defragmentedPartFile(workDir, partId).getName(), false,
                                    "workDir", workDir, false
                                ));
                            }

                            oldPageMem.invalidate(grpId, partId);

                            partCtx.partPageMemory.invalidate(grpId, partId);

                            pageMgr.pageStoreMap().removePageStore(grpId, partId); // Yes, it'll be invalid in a second.

                            renameTempPartitionFile(workDir, partId);
                        };

                        GridFutureAdapter<?> cpFut = defragmentationCheckpoint
                            .forceCheckpoint("partition defragmented", null)
                            .futureFor(CheckpointState.FINISHED);

                        cpFut.listen(cpLsnr);

                        cmpFut.add((IgniteInternalFuture<Object>)cpFut);
                    }

                    // A bit too general for now, but I like it more then saving only the last checkpoint future.
                    cmpFut.markInitialized().get();

                    idxDfrgFut = new GridFinishedFuture<>();

                    if (filePageStoreMgr.hasIndexStore(grpId)) {
                        defragmentIndexPartition(oldGrpCtx, newGrpCtx, linkMapByPart);

                        idxDfrgFut = defragmentationCheckpoint
                            .forceCheckpoint("index defragmented", null)
                            .futureFor(CheckpointState.FINISHED);
                    }

                    PageStore oldIdxPageStore = filePageStoreMgr.getStore(grpId, INDEX_PARTITION);

                    idxDfrgFut = idxDfrgFut.chain(fut -> {
                        if (log.isDebugEnabled()) {
                            log.debug(S.toString(
                                "Index partition defragmented",
                                "grpId", grpId, false,
                                "oldPages", oldIdxPageStore.pages(), false,
                                "newPages", idxAllocationTracker.get() + 1, false,
                                "pageSize", pageSize, false,
                                "partFile", defragmentedIndexFile(workDir).getName(), false,
                                "workDir", workDir, false
                            ));
                        }

                        oldPageMem.invalidate(grpId, PageIdAllocator.INDEX_PARTITION);

                        PageMemoryEx partPageMem = (PageMemoryEx)partDataRegion.pageMemory();

                        partPageMem.invalidate(grpId, PageIdAllocator.INDEX_PARTITION);

                        DefragmentationPageReadWriteManager pageMgr = (DefragmentationPageReadWriteManager)partPageMem.pageManager();

                        pageMgr.pageStoreMap().removePageStore(grpId, PageIdAllocator.INDEX_PARTITION);

                        PageMemoryEx mappingPageMem = (PageMemoryEx)mappingDataRegion.pageMemory();

                        pageMgr = (DefragmentationPageReadWriteManager)mappingPageMem.pageManager();

                        pageMgr.pageStoreMap().clear(grpId);

                        renameTempIndexFile(workDir);

                        writeDefragmentationCompletionMarker(filePageStoreMgr.getPageStoreFileIoFactory(), workDir, log);

                        batchRenameDefragmentedCacheGroupPartitions(workDir, log);

                        return null;
                    });

                    status.onIndexDefragmented(
                        oldGrpCtx,
                        oldIdxPageStore.size(),
                        pageSize + idxAllocationTracker.get() * pageSize // + file header.
                    );
                }
                catch (DefragmentationCancelledException e) {
                    DefragmentationFileUtils.deleteLeftovers(workDir);

                    throw e;
                }

                status.onCacheGroupFinish(oldGrpCtx);
            }

            if (idxDfrgFut != null)
                idxDfrgFut.get();

            mntcReg.unregisterMaintenanceTask(DEFRAGMENTATION_MNTC_TASK_NAME);

            status.onFinish();

            completionFut.onDone();
        }
        catch (DefragmentationCancelledException e) {
            mntcReg.unregisterMaintenanceTask(DEFRAGMENTATION_MNTC_TASK_NAME);

            log.info("Defragmentation process has been cancelled.");

            status.onFinish();

            completionFut.onDone();
        }
        catch (Throwable t) {
            log.error("Defragmentation process failed.", t);

            status.onFinish();

            completionFut.onDone(t);

            throw t;
        }
        finally {
            defragmentationCheckpoint.stop(true);
        }
    }

    /** */
    public IgniteInternalFuture<?> completionFuture() {
        return completionFut.chain(future -> null);
    }

    /** */
    public void createIndexPageStore(
        int grpId,
        File workDir,
        FilePageStoreFactory pageStoreFactory,
        DataRegion partRegion,
        LongConsumer allocatedTracker
    ) throws IgniteCheckedException {
        // Index partition file has to be deleted before we begin, otherwise there's a chance of reading corrupted file.
        // There is a time period when index is already defragmented but marker file is not created yet. If node is
        // failed in that time window then index will be deframented once again. That's fine, situation is rare but code
        // to fix that would add unnecessary complications.
        U.delete(defragmentedIndexTmpFile(workDir));

        PageStore idxPageStore;

        defragmentationCheckpoint.checkpointTimeoutLock().checkpointReadLock();
        try {
            idxPageStore = pageStoreFactory.createPageStore(
                FLAG_IDX,
                () -> defragmentedIndexTmpFile(workDir).toPath(),
                allocatedTracker
            );
        }
        finally {
            defragmentationCheckpoint.checkpointTimeoutLock().checkpointReadUnlock();
        }

        idxPageStore.sync();

        PageMemoryEx partPageMem = (PageMemoryEx)partRegion.pageMemory();

        DefragmentationPageReadWriteManager partMgr = (DefragmentationPageReadWriteManager)partPageMem.pageManager();

        partMgr.pageStoreMap().addPageStore(grpId, PageIdAllocator.INDEX_PARTITION, idxPageStore);
    }

    /**
     * Cancel the process of defragmentation.
     *
     * @return {@code true} if process was cancelled by this method.
     */
    public boolean cancel() {
        if (completionFut.isDone())
            return false;

        if (cancel.compareAndSet(false, true)) {
            try {
                completionFut.get();
            }
            catch (Throwable ignore) {
            }

            return true;
        }

        return false;
    }

    /** */
    private void checkCancellation() throws DefragmentationCancelledException {
        if (cancel.get())
            throw new DefragmentationCancelledException();
    }

    /** */
    public Status status() {
        return status;
    }

    /**
     * Defragmentate partition.
     *
     * @param partCtx
     * @param treeIter
     * @throws IgniteCheckedException If failed.
     */
    private void copyPartitionData(
        PartitionContext partCtx,
        TreeIterator treeIter
    ) throws IgniteCheckedException {
        CacheDataTree tree = partCtx.oldCacheDataStore.tree();

        CacheDataTree newTree = partCtx.newCacheDataStore.tree();

        newTree.enableSequentialWriteMode();

        PendingEntriesTree newPendingTree = partCtx.newCacheDataStore.pendingTree();
        AbstractFreeList<CacheDataRow> freeList = partCtx.newCacheDataStore.getCacheStoreFreeList();

        long cpLockThreshold = 150L;

        defragmentationCheckpoint.checkpointTimeoutLock().checkpointReadLock();

        try {
            AtomicLong lastCpLockTs = new AtomicLong(System.currentTimeMillis());
            AtomicInteger entriesProcessed = new AtomicInteger();

            treeIter.iterate(tree, partCtx.cachePageMemory, (tree0, io, pageAddr, idx) -> {
                checkCancellation();

                if (System.currentTimeMillis() - lastCpLockTs.get() >= cpLockThreshold) {
                    defragmentationCheckpoint.checkpointTimeoutLock().checkpointReadUnlock();

                    defragmentationCheckpoint.checkpointTimeoutLock().checkpointReadLock();

                    lastCpLockTs.set(System.currentTimeMillis());
                }

                AbstractDataLeafIO leafIo = (AbstractDataLeafIO)io;
                CacheDataRow row = tree.getRow(io, pageAddr, idx);

                int cacheId = row.cacheId();

                // Reuse row that we just read.
                row.link(0);

                // "insertDataRow" will corrupt page memory if we don't do this.
                if (row instanceof DataRow && !partCtx.oldGrpCtx.storeCacheIdInDataPage())
                    ((DataRow)row).cacheId(CU.UNDEFINED_CACHE_ID);

                freeList.insertDataRow(row, IoStatisticsHolderNoOp.INSTANCE);

                // Put it back.
                if (row instanceof DataRow)
                    ((DataRow)row).cacheId(cacheId);

                newTree.putx(row);

                long newLink = row.link();

                partCtx.linkMap.put(leafIo.getLink(pageAddr, idx), newLink);

                if (row.expireTime() != 0)
                    newPendingTree.putx(new PendingRow(cacheId, row.expireTime(), newLink));

                entriesProcessed.incrementAndGet();

                return true;
            });

            checkCancellation();

            defragmentationCheckpoint.checkpointTimeoutLock().checkpointReadUnlock();

            defragmentationCheckpoint.checkpointTimeoutLock().checkpointReadLock();

            freeList.saveMetadata(IoStatisticsHolderNoOp.INSTANCE);

            copyCacheMetadata(partCtx);
        }
        finally {
            defragmentationCheckpoint.checkpointTimeoutLock().checkpointReadUnlock();
        }
    }

    /** */
    private void copyCacheMetadata(
        PartitionContext partCtx
    ) throws IgniteCheckedException {
        // Same for all page memories. Why does it need to be in PageMemory?
        long partMetaPageId = partCtx.cachePageMemory.partitionMetaPageId(partCtx.grpId, partCtx.partId);

        long oldPartMetaPage = partCtx.cachePageMemory.acquirePage(partCtx.grpId, partMetaPageId);

        try {
            long oldPartMetaPageAddr = partCtx.cachePageMemory.readLock(partCtx.grpId, partMetaPageId, oldPartMetaPage);

            try {
                PagePartitionMetaIO oldPartMetaIo = PageIO.getPageIO(oldPartMetaPageAddr);

                // Newer meta versions may contain new data that we don't copy during defragmentation.
                assert Arrays.asList(1, 2, 3).contains(oldPartMetaIo.getVersion())
                    : "IO version " + oldPartMetaIo.getVersion() + " is not supported by current defragmentation algorithm." +
                    " Please implement copying of all data added in new version.";

                long newPartMetaPage = partCtx.partPageMemory.acquirePage(partCtx.grpId, partMetaPageId);

                try {
                    long newPartMetaPageAddr = partCtx.partPageMemory.writeLock(partCtx.grpId, partMetaPageId, newPartMetaPage);

                    try {
                        PagePartitionMetaIOV3 newPartMetaIo = PageIO.getPageIO(newPartMetaPageAddr);

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
                            Map<Integer, Long> sizes = GridCacheOffheapManager.readSharedGroupCacheSizes(
                                partCtx.cachePageMemory,
                                partCtx.grpId,
                                oldCountersPageId
                            );

                            long newCountersPageId = GridCacheOffheapManager.writeSharedGroupCacheSizes(
                                partCtx.partPageMemory,
                                partCtx.grpId,
                                0L,
                                partCtx.partId,
                                sizes
                            );

                            newPartMetaIo.setCountersPageId(newPartMetaPageAddr, newCountersPageId);
                        }

                        // Copy counter gaps.
                        long oldGapsLink = oldPartMetaIo.getGapsLink(oldPartMetaPageAddr);
                        if (oldGapsLink != 0L) {
                            byte[] gapsBytes = partCtx.oldCacheDataStore.partStorage().readRow(oldGapsLink);

                            SimpleDataRow gapsDataRow = new SimpleDataRow(partCtx.partId, gapsBytes);

                            partCtx.newCacheDataStore.partStorage().insertDataRow(gapsDataRow, IoStatisticsHolderNoOp.INSTANCE);

                            newPartMetaIo.setGapsLink(newPartMetaPageAddr, gapsDataRow.link());
                        }

                        // Encryption stuff.
                        newPartMetaIo.setEncryptedPageCount(newPartMetaPageAddr, 0);
                        newPartMetaIo.setEncryptedPageIndex(newPartMetaPageAddr, 0);
                    }
                    finally {
                        partCtx.partPageMemory.writeUnlock(partCtx.grpId, partMetaPageId, newPartMetaPage, null, true);
                    }
                }
                finally {
                    partCtx.partPageMemory.releasePage(partCtx.grpId, partMetaPageId, newPartMetaPage);
                }
            }
            finally {
                partCtx.cachePageMemory.readUnlock(partCtx.grpId, partMetaPageId, oldPartMetaPage);
            }
        }
        finally {
            partCtx.cachePageMemory.releasePage(partCtx.grpId, partMetaPageId, oldPartMetaPage);
        }
    }

    /**
     * Defragmentate indexing partition.
     *
     * @param grpCtx
     * @param mappingByPartition
     *
     * @throws IgniteCheckedException If failed.
     */
    private void defragmentIndexPartition(
        CacheGroupContext grpCtx,
        CacheGroupContext newCtx,
        IntMap<LinkMap> mappingByPartition
    ) throws IgniteCheckedException {
        GridQueryProcessor query = grpCtx.caches().get(0).kernalContext().query();

        if (!query.moduleEnabled())
            return;

        final GridQueryIndexing idx = query.getIndexing();

        CheckpointTimeoutLock cpLock = defragmentationCheckpoint.checkpointTimeoutLock();

        Runnable cancellationChecker = this::checkCancellation;

        idx.defragment(
            grpCtx,
            newCtx,
            (PageMemoryEx)partDataRegion.pageMemory(),
            mappingByPartition,
            cpLock,
            cancellationChecker
        );
    }

    /** */
    @SuppressWarnings("PublicField")
    private class PartitionContext {
        /** */
        public final File workDir;

        /** */
        public final int grpId;

        /** */
        public final int partId;

        /** */
        public final DataRegion cacheDataRegion;

        /** */
        public final PageMemoryEx cachePageMemory;

        /** */
        public final PageMemoryEx partPageMemory;

        /** */
        public final PageMemoryEx mappingPageMemory;

        /** */
        public final CacheGroupContext oldGrpCtx;

        /** */
        public final CacheGroupContext newGrpCtx;

        /** */
        public final CacheDataStore oldCacheDataStore;

        /** */
        private GridCacheDataStore newCacheDataStore;

        /** */
        public final FilePageStoreFactory pageStoreFactory;

        /** */
        public final AtomicLong partPagesAllocated = new AtomicLong();

        /** */
        public final AtomicLong mappingPagesAllocated = new AtomicLong();

        /** */
        private LinkMap linkMap;

        /** */
        public PartitionContext(
            File workDir,
            int grpId,
            int partId,
            DataRegion partDataRegion,
            DataRegion mappingDataRegion,
            CacheGroupContext oldGrpCtx,
            CacheGroupContext newGrpCtx,
            CacheDataStore oldCacheDataStore,
            FilePageStoreFactory pageStoreFactory
        ) {
            this.workDir = workDir;
            this.grpId = grpId;
            this.partId = partId;
            cacheDataRegion = oldGrpCtx.dataRegion();

            cachePageMemory = (PageMemoryEx)cacheDataRegion.pageMemory();
            partPageMemory = (PageMemoryEx)partDataRegion.pageMemory();
            mappingPageMemory = (PageMemoryEx)mappingDataRegion.pageMemory();

            this.oldGrpCtx = oldGrpCtx;
            this.newGrpCtx = newGrpCtx;
            this.oldCacheDataStore = oldCacheDataStore;
            this.pageStoreFactory = pageStoreFactory;
        }

        /** */
        public PageStore createPageStore(IgniteOutClosure<Path> pathProvider, AtomicLong pagesAllocated, PageMemoryEx pageMemory) throws IgniteCheckedException {
            PageStore partPageStore;

            defragmentationCheckpoint.checkpointTimeoutLock().checkpointReadLock();
            try {
                partPageStore = pageStoreFactory.createPageStore(
                    FLAG_DATA,
                    pathProvider,
                    pagesAllocated::addAndGet
                );
            }
            finally {
                defragmentationCheckpoint.checkpointTimeoutLock().checkpointReadUnlock();
            }

            partPageStore.sync();

            DefragmentationPageReadWriteManager pageMgr = (DefragmentationPageReadWriteManager)pageMemory.pageManager();

            pageMgr.pageStoreMap().addPageStore(grpId, partId, partPageStore);

            return partPageStore;
        }

        /** */
        public LinkMap createLinkMapTree(boolean initNew) throws IgniteCheckedException {
            defragmentationCheckpoint.checkpointTimeoutLock().checkpointReadLock();

            try {
                long mappingMetaPageId = initNew
                    ? mappingPageMemory.allocatePage(grpId, partId, FLAG_DATA)
                    : PageIdUtils.pageId(partId, FLAG_DATA, LinkMap.META_PAGE_IDX);

                assert PageIdUtils.pageIndex(mappingMetaPageId) == LinkMap.META_PAGE_IDX
                    : PageIdUtils.toDetailString(mappingMetaPageId);

                linkMap = new LinkMap(newGrpCtx, mappingPageMemory, mappingMetaPageId, initNew);
            }
            finally {
                defragmentationCheckpoint.checkpointTimeoutLock().checkpointReadUnlock();
            }

            return linkMap;
        }

        /** */
        public void createNewCacheDataStore(GridCacheOffheapManager offheap) {
            GridCacheDataStore newCacheDataStore = offheap.createGridCacheDataStore(
                newGrpCtx,
                partId,
                true,
                log
            );

            defragmentationCheckpoint.checkpointTimeoutLock().checkpointReadLock();

            try {
                newCacheDataStore.init();
            }
            finally {
                defragmentationCheckpoint.checkpointTimeoutLock().checkpointReadUnlock();
            }

            this.newCacheDataStore = newCacheDataStore;
        }
    }

    /** */
    private static class DefragmentationCancelledException extends RuntimeException {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;
    }

    /** Defragmentation status. */
    class Status {
        /** Defragmentation start timestamp. */
        private long startTs;

        /** Defragmentation finish timestamp. */
        private long finishTs;

        /** Total count of partitions. */
        private int totalPartitionCount;

        /** Partitions, that are already defragmented. */
        private int defragmentedPartitionCount;

        /** Cache groups scheduled for defragmentation. */
        private final Set<String> scheduledGroups;

        /** Progress for cache group. */
        private final Map<CacheGroupContext, DefragmentationCacheGroupProgress> progressGroups;

        /** Finished cache groups. */
        private final Map<CacheGroupContext, DefragmentationCacheGroupProgress> finishedGroups;

        /** Skipped cache groups. */
        private final Set<String> skippedGroups;

        /** Constructor. */
        public Status() {
            scheduledGroups = new TreeSet<>();
            progressGroups = new TreeMap<>(comparing(CacheGroupContext::cacheOrGroupName));
            finishedGroups = new TreeMap<>(comparing(CacheGroupContext::cacheOrGroupName));
            skippedGroups = new TreeSet<>();
        }

        /** Copy constructor. */
        public Status(
            long startTs,
            long finishTs,
            Set<String> scheduledGroups,
            Map<CacheGroupContext, DefragmentationCacheGroupProgress> progressGroups,
            Map<CacheGroupContext, DefragmentationCacheGroupProgress> finishedGroups,
            Set<String> skippedGroups
        ) {
            this.startTs = startTs;
            this.finishTs = finishTs;
            this.scheduledGroups = scheduledGroups;
            this.progressGroups = progressGroups;
            this.finishedGroups = finishedGroups;
            this.skippedGroups = skippedGroups;
        }

        /**
         * Mark the start of the defragmentation.
         * @param scheduledGroups Groups scheduled for defragmentation.
         * @param partitions Total partition count.
         */
        public synchronized void onStart(Set<CacheGroupContext> scheduledGroups, int partitions) {
            startTs = System.currentTimeMillis();
            totalPartitionCount = partitions;

            for (CacheGroupContext grp : scheduledGroups)
                this.scheduledGroups.add(grp.cacheOrGroupName());

            log.info("Defragmentation started.");
        }

        /**
         * Mark the start of the cache group defragmentation.
         * @param grpCtx Cache group context.
         * @param parts Partition count.
         */
        private synchronized void onCacheGroupStart(CacheGroupContext grpCtx, int parts) {
            scheduledGroups.remove(grpCtx.cacheOrGroupName());

            progressGroups.put(grpCtx, new DefragmentationCacheGroupProgress(parts));
        }

        /**
         * Mark the end of the partition defragmentation.
         * @param grpCtx Cache group context.
         * @param oldSize Old size.
         * @param newSize New size;
         */
        private synchronized void onPartitionDefragmented(CacheGroupContext grpCtx, long oldSize, long newSize) {
            progressGroups.get(grpCtx).onPartitionDefragmented(oldSize, newSize);

            defragmentedPartitionCount++;
        }

        /**
         * Mark the end of the index partition defragmentation.
         * @param grpCtx Cache group context.
         * @param oldSize Old size.
         * @param newSize New size;
         */
        private synchronized void onIndexDefragmented(CacheGroupContext grpCtx, long oldSize, long newSize) {
            progressGroups.get(grpCtx).onIndexDefragmented(oldSize, newSize);
        }

        /**
         * Mark the end of the cache group defragmentation.
         * @param grpCtx Cache group context.
         */
        private synchronized void onCacheGroupFinish(CacheGroupContext grpCtx) {
            DefragmentationCacheGroupProgress progress = progressGroups.remove(grpCtx);

            progress.onFinish();

            finishedGroups.put(grpCtx, progress);
        }

        /**
         * Mark that cache group defragmentation was skipped.
         * @param grpCtx Cache group context.
         */
        private synchronized void onCacheGroupSkipped(CacheGroupContext grpCtx, int partitions) {
            scheduledGroups.remove(grpCtx.cacheOrGroupName());

            skippedGroups.add(grpCtx.cacheOrGroupName());

            defragmentedPartitionCount += partitions;
        }

        /** Mark the end of the defragmentation. */
        private synchronized void onFinish() {
            finishTs = System.currentTimeMillis();

            progressGroups.clear();

            scheduledGroups.clear();

            log.info("Defragmentation process completed. Time: " + (finishTs - startTs) * 1e-3 + "s.");
        }

        /** Copy object.  */
        private synchronized Status copy() {
            return new Status(
                startTs,
                finishTs,
                new HashSet<>(scheduledGroups),
                new HashMap<>(progressGroups),
                new HashMap<>(finishedGroups),
                new HashSet<>(skippedGroups)
            );
        }

        /** */
        public long getStartTs() {
            return startTs;
        }

        /** */
        public long getFinishTs() {
            return finishTs;
        }

        /** */
        public Set<String> getScheduledGroups() {
            return scheduledGroups;
        }

        /** */
        public Map<CacheGroupContext, DefragmentationCacheGroupProgress> getProgressGroups() {
            return progressGroups;
        }

        /** */
        public Map<CacheGroupContext, DefragmentationCacheGroupProgress> getFinishedGroups() {
            return finishedGroups;
        }

        /** */
        public Set<String> getSkippedGroups() {
            return skippedGroups;
        }

        /** */
        public int getTotalPartitionCount() {
            return totalPartitionCount;
        }

        /** */
        public int getDefragmentedPartitionCount() {
            return defragmentedPartitionCount;
        }
    }

    /** Cache group defragmentation progress. */
    static class DefragmentationCacheGroupProgress {
        /** Partition count. */
        private final int partsTotal;

        /** Defragmented partitions. */
        private int partsCompleted;

        /** Old cache group size. */
        private long oldSize;

        /** New cache group size. */
        private long newSize;

        /** Start timestamp. */
        private final long startTs;

        /** Finish timestamp. */
        private long finishTs;

        /** Constructor. */
        public DefragmentationCacheGroupProgress(int parts) {
            partsTotal = parts;

            startTs = System.currentTimeMillis();
        }

        /**
         * @param oldSize Old partition size.
         * @param newSize New partition size.
         */
        public void onPartitionDefragmented(long oldSize, long newSize) {
            ++partsCompleted;

            this.oldSize += oldSize;
            this.newSize += newSize;
        }

        /**
         * @param oldSize Old partition size.
         * @param newSize New partition size.
         */
        public void onIndexDefragmented(long oldSize, long newSize) {
            this.oldSize += oldSize;
            this.newSize += newSize;
        }

        /** */
        public long getOldSize() {
            return oldSize;
        }

        /** */
        public long getNewSize() {
            return newSize;
        }

        /** */
        public long getStartTs() {
            return startTs;
        }

        /** */
        public long getFinishTs() {
            return finishTs;
        }

        /** */
        public int getPartsTotal() {
            return partsTotal;
        }

        /** */
        public int getPartsCompleted() {
            return partsCompleted;
        }

        /** */
        public void onFinish() {
            finishTs = System.currentTimeMillis();
        }
    }
}
