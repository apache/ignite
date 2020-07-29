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

package org.apache.ignite.internal.processors.cache.persistence;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import javax.cache.processor.EntryProcessor;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.failure.FailureType;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.PageSupport;
import org.apache.ignite.internal.pagemem.store.IgnitePageStoreManager;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.DataEntry;
import org.apache.ignite.internal.pagemem.wal.record.DataRecord;
import org.apache.ignite.internal.pagemem.wal.record.PageSnapshot;
import org.apache.ignite.internal.pagemem.wal.record.RollbackRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.MetaPageInitRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.MetaPageUpdatePartitionDataRecordV2;
import org.apache.ignite.internal.pagemem.wal.record.delta.PartitionDestroyRecord;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheDiagnosticManager;
import org.apache.ignite.internal.processors.cache.CacheEntryPredicate;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.GridCacheMvccEntryInfo;
import org.apache.ignite.internal.processors.cache.GridCacheTtlManager;
import org.apache.ignite.internal.processors.cache.IgniteCacheOffheapManagerImpl;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.PartitionUpdateCounter;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.CachePartitionPartialCountersMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.IgniteHistoricalIterator;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.IgniteHistoricalIteratorException;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.mvcc.MvccSnapshot;
import org.apache.ignite.internal.processors.cache.mvcc.MvccVersion;
import org.apache.ignite.internal.processors.cache.persistence.freelist.AbstractFreeList;
import org.apache.ignite.internal.processors.cache.persistence.freelist.CacheFreeList;
import org.apache.ignite.internal.processors.cache.persistence.freelist.SimpleDataRow;
import org.apache.ignite.internal.processors.cache.persistence.migration.UpgradePendingTreeToPerPartitionTask;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryEx;
import org.apache.ignite.internal.processors.cache.persistence.partstate.GroupPartitionId;
import org.apache.ignite.internal.processors.cache.persistence.partstate.PagesAllocationRange;
import org.apache.ignite.internal.processors.cache.persistence.partstate.PartitionAllocationMap;
import org.apache.ignite.internal.processors.cache.persistence.partstorage.PartitionMetaStorage;
import org.apache.ignite.internal.processors.cache.persistence.partstorage.PartitionMetaStorageImpl;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageMetaIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PagePartitionCountersIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PagePartitionMetaIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PagePartitionMetaIOV2;
import org.apache.ignite.internal.processors.cache.persistence.tree.reuse.ReuseList;
import org.apache.ignite.internal.processors.cache.persistence.tree.reuse.ReuseListImpl;
import org.apache.ignite.internal.processors.cache.persistence.tree.util.PageHandler;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWALPointer;
import org.apache.ignite.internal.processors.cache.tree.CacheDataRowStore;
import org.apache.ignite.internal.processors.cache.tree.CacheDataTree;
import org.apache.ignite.internal.processors.cache.tree.PendingEntriesTree;
import org.apache.ignite.internal.processors.cache.tree.PendingRow;
import org.apache.ignite.internal.processors.cache.tree.mvcc.data.MvccUpdateResult;
import org.apache.ignite.internal.processors.cache.tree.mvcc.search.MvccLinkAwareSearchRow;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.query.GridQueryRowCacheCleaner;
import org.apache.ignite.internal.util.GridLongList;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.internal.util.lang.IgniteInClosure2X;
import org.apache.ignite.internal.util.lang.IgnitePredicateX;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.EVICTED;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.MOVING;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.OWNING;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.RENTING;

/**
 * Used when persistence enabled.
 */
public class GridCacheOffheapManager extends IgniteCacheOffheapManagerImpl implements DbCheckpointListener {
    /**
     * Throttling timeout in millis which avoid excessive PendingTree access on unwind
     * if there is nothing to clean yet.
     */
    private final long unwindThrottlingTimeout = Long.getLong(
        IgniteSystemProperties.IGNITE_UNWIND_THROTTLING_TIMEOUT, 500L);

    /** */
    private IndexStorage indexStorage;

    /** */
    private ReuseListImpl reuseList;

    /** Page list cache limit for data region of this cache group. */
    private AtomicLong pageListCacheLimit;

    /** Flag indicates that all group partitions have restored their state from page memory / disk. */
    private volatile boolean partitionStatesRestored;

    /** {@inheritDoc} */
    @Override protected void initPendingTree(GridCacheContext cctx) throws IgniteCheckedException {
        // No-op. Per-partition PendingTree should be used.
    }

    /** {@inheritDoc} */
    @Override protected void initDataStructures() throws IgniteCheckedException {
        assert ctx.database().checkpointLockIsHeldByThread();

        Metas metas = getOrAllocateCacheMetas();

        CacheDiagnosticManager diagnosticMgr = ctx.diagnostic();

        String reuseListName = grp.cacheOrGroupName() + "##ReuseList";
        String indexStorageTreeName = grp.cacheOrGroupName() + "##IndexStorageTree";

        RootPage reuseListRoot = metas.reuseListRoot;

        pageListCacheLimit = ((GridCacheDatabaseSharedManager)ctx.database()).pageListCacheLimitHolder(grp.dataRegion());

        reuseList = new ReuseListImpl(
            grp.groupId(),
            grp.cacheOrGroupName(),
            grp.dataRegion().pageMemory(),
            ctx.wal(),
            reuseListRoot.pageId().pageId(),
            reuseListRoot.isAllocated(),
            diagnosticMgr.pageLockTracker().createPageLockTracker(reuseListName),
            ctx.kernalContext(),
            pageListCacheLimit
        );

        RootPage metastoreRoot = metas.treeRoot;

        indexStorage = new IndexStorageImpl(
            grp.dataRegion().pageMemory(),
            ctx.wal(),
            globalRemoveId(),
            grp.groupId(),
            grp.sharedGroup(),
            PageIdAllocator.INDEX_PARTITION,
            PageIdAllocator.FLAG_IDX,
            reuseList,
            metastoreRoot.pageId().pageId(),
            metastoreRoot.isAllocated(),
            ctx.kernalContext().failure(),
            diagnosticMgr.pageLockTracker().createPageLockTracker(indexStorageTreeName)
        );

        ((GridCacheDatabaseSharedManager)ctx.database()).addCheckpointListener(this);
    }

    /**
     * Get internal IndexStorage.
     * See {@link UpgradePendingTreeToPerPartitionTask} for details.
     */
    public IndexStorage getIndexStorage() {
        return indexStorage;
    }

    /** {@inheritDoc} */
    @Override protected CacheDataStore createCacheDataStore0(int p) throws IgniteCheckedException {
        if (ctx.database() instanceof GridCacheDatabaseSharedManager)
            ((GridCacheDatabaseSharedManager) ctx.database()).cancelOrWaitPartitionDestroy(grp.groupId(), p);

        boolean exists = ctx.pageStore() != null && ctx.pageStore().exists(grp.groupId(), p);

        return new GridCacheDataStore(p, exists);
    }

    /** {@inheritDoc} */
    @Override public void onCheckpointBegin(Context ctx) throws IgniteCheckedException {
        /* No-op. */
    }

    /** {@inheritDoc} */
    @Override public void onMarkCheckpointBegin(Context ctx) throws IgniteCheckedException {
        assert grp.dataRegion().pageMemory() instanceof PageMemoryEx;

        syncMetadata(ctx);
    }

    /** {@inheritDoc} */
    @Override public void beforeCheckpointBegin(Context ctx) throws IgniteCheckedException {
        if (!ctx.nextSnapshot())
            syncMetadata(ctx);
    }

    /**
     * Syncs and saves meta-information of all data structures to page memory.
     *
     * @throws IgniteCheckedException If failed.
     */
    private void syncMetadata(Context ctx) throws IgniteCheckedException {
        Executor execSvc = ctx.executor();

        boolean needSnapshot = ctx.nextSnapshot() && ctx.needToSnapshot(grp.cacheOrGroupName());

        if (needSnapshot) {
            if (execSvc == null)
                addPartitions(ctx);
            else {
                execSvc.execute(() -> {
                    try {
                        addPartitions(ctx);
                    }
                    catch (IgniteCheckedException e) {
                        throw new IgniteException(e);
                    }
                });
            }
        }

        syncMetadata(ctx, ctx.executor(), needSnapshot);
    }

    /**
     * Syncs and saves meta-information of all data structures to page memory.
     *
     * @param execSvc Executor service to run save process
     * @throws IgniteCheckedException If failed.
     */
    private void syncMetadata(Context ctx, Executor execSvc, boolean needSnapshot) throws IgniteCheckedException {
        if (execSvc == null) {
            reuseList.saveMetadata(grp.statisticsHolderData());

            for (CacheDataStore store : partDataStores.values())
                saveStoreMetadata(store, ctx, false, needSnapshot);
        }
        else {
            execSvc.execute(() -> {
                try {
                    reuseList.saveMetadata(grp.statisticsHolderData());
                }
                catch (IgniteCheckedException e) {
                    throw new IgniteException(e);
                }
            });

            for (CacheDataStore store : partDataStores.values())
                execSvc.execute(() -> {
                    try {
                        saveStoreMetadata(store, ctx, false, needSnapshot);
                    }
                    catch (IgniteCheckedException e) {
                        throw new IgniteException(e);
                    }
                });
        }
    }

    /**
     * @param store Store to save metadata.
     * @throws IgniteCheckedException If failed.
     */
    private void saveStoreMetadata(
        CacheDataStore store,
        Context ctx,
        boolean beforeDestroy,
        boolean needSnapshot
    ) throws IgniteCheckedException {
        RowStore rowStore0 = store.rowStore();

        if (rowStore0 != null) {
            ((CacheFreeList)rowStore0.freeList()).saveMetadata(grp.statisticsHolderData());

            PartitionMetaStorage<SimpleDataRow> partStore = store.partStorage();

            long updCntr = store.updateCounter();
            long size = store.fullSize();
            long rmvId = globalRemoveId().get();

            byte[] updCntrsBytes = store.partUpdateCounter().getBytes();

            PageMemoryEx pageMem = (PageMemoryEx)grp.dataRegion().pageMemory();
            IgniteWriteAheadLogManager wal = this.ctx.wal();

            if (size > 0 || updCntr > 0 || !store.partUpdateCounter().sequential()) {
                GridDhtPartitionState state = null;

                // localPartition will not acquire writeLock here because create=false.
                GridDhtLocalPartition part = null;

                if (!grp.isLocal()) {
                    if (beforeDestroy)
                        state = GridDhtPartitionState.EVICTED;
                    else {
                        part = getPartition(store);

                        if (part != null && part.state() != GridDhtPartitionState.EVICTED)
                            state = part.state();
                    }

                    // Do not save meta for evicted partitions on next checkpoints.
                    if (state == null)
                        return;
                }

                int grpId = grp.groupId();
                long partMetaId = pageMem.partitionMetaPageId(grpId, store.partId());

                long partMetaPage = pageMem.acquirePage(grpId, partMetaId);
                try {
                    long partMetaPageAddr = pageMem.writeLock(grpId, partMetaId, partMetaPage);

                    if (partMetaPageAddr == 0L) {
                        U.warn(log, "Failed to acquire write lock for meta page [metaPage=" + partMetaPage +
                            ", beforeDestroy=" + beforeDestroy + ", size=" + size +
                            ", updCntr=" + updCntr + ", state=" + state + ']');

                        return;
                    }

                    boolean changed = false;

                    try {
                        PagePartitionMetaIOV2 io = PageIO.getPageIO(partMetaPageAddr);

                        long link = io.getGapsLink(partMetaPageAddr);

                        if (updCntrsBytes == null && link != 0) {
                            partStore.removeDataRowByLink(link, grp.statisticsHolderData());

                            io.setGapsLink(partMetaPageAddr, (link = 0));

                            changed = true;
                        }
                        else if (updCntrsBytes != null && link == 0) {
                            SimpleDataRow row = new SimpleDataRow(store.partId(), updCntrsBytes);

                            partStore.insertDataRow(row, grp.statisticsHolderData());

                            io.setGapsLink(partMetaPageAddr, (link = row.link()));

                            changed = true;
                        }
                        else if (updCntrsBytes != null && link != 0) {
                            byte[] prev = partStore.readRow(link);

                            assert prev != null : "Read null gaps using link=" + link;

                            if (!Arrays.equals(prev, updCntrsBytes)) {
                                partStore.removeDataRowByLink(link, grp.statisticsHolderData());

                                SimpleDataRow row = new SimpleDataRow(store.partId(), updCntrsBytes);

                                partStore.insertDataRow(row, grp.statisticsHolderData());

                                io.setGapsLink(partMetaPageAddr, (link = row.link()));

                                changed = true;
                            }
                        }

                        if (changed)
                            partStore.saveMetadata(grp.statisticsHolderData());

                        changed |= io.setUpdateCounter(partMetaPageAddr, updCntr);
                        changed |= io.setGlobalRemoveId(partMetaPageAddr, rmvId);
                        changed |= io.setSize(partMetaPageAddr, size);

                        if (state != null)
                            changed |= io.setPartitionState(partMetaPageAddr, (byte)state.ordinal());
                        else
                            assert grp.isLocal() : grp.cacheOrGroupName();

                        long cntrsPageId;

                        if (grp.sharedGroup()) {
                            long initCntrPageId = io.getCountersPageId(partMetaPageAddr);

                            Map<Integer, Long> newSizes = store.cacheSizes();
                            Map<Integer, Long> prevSizes = readSharedGroupCacheSizes(pageMem, grpId, initCntrPageId);

                            if (prevSizes != null && prevSizes.equals(newSizes))
                                cntrsPageId = initCntrPageId; // Preventing modification of sizes pages for store
                            else {
                                cntrsPageId = writeSharedGroupCacheSizes(pageMem, grpId, initCntrPageId,
                                    store.partId(), newSizes);

                                if (initCntrPageId == 0 && cntrsPageId != 0) {
                                    io.setCountersPageId(partMetaPageAddr, cntrsPageId);

                                    changed = true;
                                }
                            }
                        }
                        else
                            cntrsPageId = 0L;

                        int pageCnt;

                        if (needSnapshot) {
                            pageCnt = this.ctx.pageStore().pages(grpId, store.partId());

                            io.setCandidatePageCount(partMetaPageAddr, size == 0 ? 0 : pageCnt);

                            if (state == OWNING) {
                                assert part != null;

                                if (!addPartition(
                                    part,
                                    ctx.partitionStatMap(),
                                    partMetaPageAddr,
                                    io,
                                    grpId,
                                    store.partId(),
                                    this.ctx.pageStore().pages(grpId, store.partId()),
                                    store.fullSize()
                                ))
                                    U.warn(log, "Partition was concurrently evicted grpId=" + grpId +
                                        ", partitionId=" + part.id());
                            }
                            else if (state == MOVING || state == RENTING) {
                                if (ctx.partitionStatMap().forceSkipIndexPartition(grpId)) {
                                    if (log.isInfoEnabled())
                                        log.info("Will not include SQL indexes to snapshot because there is " +
                                            "a partition not in " + OWNING + " state [grp=" + grp.cacheOrGroupName() +
                                            ", partId=" + store.partId() + ", state=" + state + ']');
                                }
                            }

                            changed = true;
                        }
                        else
                            pageCnt = io.getCandidatePageCount(partMetaPageAddr);

                        if (changed && PageHandler.isWalDeltaRecordNeeded(pageMem, grpId, partMetaId, partMetaPage, wal, null))
                            wal.log(new MetaPageUpdatePartitionDataRecordV2(
                                grpId,
                                partMetaId,
                                updCntr,
                                rmvId,
                                (int)size, // TODO: Partition size may be long
                                cntrsPageId,
                                state == null ? -1 : (byte)state.ordinal(),
                                pageCnt,
                                link
                            ));
                    }
                    finally {
                        pageMem.writeUnlock(grpId, partMetaId, partMetaPage, null, changed);
                    }
                }
                finally {
                    pageMem.releasePage(grpId, partMetaId, partMetaPage);
                }
            }
            else if (needSnapshot)
                tryAddEmptyPartitionToSnapshot(store, ctx);
        }
        else if (needSnapshot)
            tryAddEmptyPartitionToSnapshot(store, ctx);
    }

    /** {@inheritDoc} */
    @Override public long restorePartitionStates(Map<GroupPartitionId, Integer> partitionRecoveryStates) throws IgniteCheckedException {
        if (grp.isLocal() || !grp.affinityNode() || !grp.dataRegion().config().isPersistenceEnabled())
            return 0;

        if (partitionStatesRestored)
            return 0;

        long processed = 0;

        PageMemoryEx pageMem = (PageMemoryEx)grp.dataRegion().pageMemory();

        for (int p = 0; p < grp.affinity().partitions(); p++) {
            Integer recoverState = partitionRecoveryStates.get(new GroupPartitionId(grp.groupId(), p));

            long startTime = U.currentTimeMillis();

            if (ctx.pageStore().exists(grp.groupId(), p)) {
                ctx.pageStore().ensure(grp.groupId(), p);

                if (ctx.pageStore().pages(grp.groupId(), p) <= 1) {
                    if (log.isDebugEnabled())
                        log.debug("Skipping partition on recovery (pages less than 1) " +
                            "[grp=" + grp.cacheOrGroupName() + ", p=" + p + ']');

                    continue;
                }

                if (log.isDebugEnabled())
                    log.debug("Creating partition on recovery (exists in page store) " +
                        "[grp=" + grp.cacheOrGroupName() + ", p=" + p + ']');

                processed++;

                GridDhtLocalPartition part = grp.topology().forceCreatePartition(p);

                // Triggers initialization of existing(having datafile) partition before acquiring cp read lock.
                part.dataStore().init();

                ctx.database().checkpointReadLock();

                try {
                    long partMetaId = pageMem.partitionMetaPageId(grp.groupId(), p);
                    long partMetaPage = pageMem.acquirePage(grp.groupId(), partMetaId);

                    try {
                        long pageAddr = pageMem.writeLock(grp.groupId(), partMetaId, partMetaPage);

                        boolean changed = false;

                        try {
                            PagePartitionMetaIO io = PagePartitionMetaIO.VERSIONS.forPage(pageAddr);

                            if (recoverState != null) {
                                changed = io.setPartitionState(pageAddr, (byte)recoverState.intValue());

                                updateState(part, recoverState);

                                if (log.isDebugEnabled())
                                    log.debug("Restored partition state (from WAL) " +
                                        "[grp=" + grp.cacheOrGroupName() + ", p=" + p + ", state=" + part.state() +
                                        ", updCntr=" + part.initialUpdateCounter() +
                                        ", size=" + part.fullSize() + ']');
                            }
                            else {
                                int stateId = io.getPartitionState(pageAddr);

                                updateState(part, stateId);

                                if (log.isDebugEnabled())
                                    log.debug("Restored partition state (from page memory) " +
                                        "[grp=" + grp.cacheOrGroupName() + ", p=" + p + ", state=" + part.state() +
                                        ", updCntr=" + part.initialUpdateCounter() + ", stateId=" + stateId +
                                        ", size=" + part.fullSize() + ']');
                            }
                        }
                        finally {
                            pageMem.writeUnlock(grp.groupId(), partMetaId, partMetaPage, null, changed);
                        }
                    }
                    finally {
                        pageMem.releasePage(grp.groupId(), partMetaId, partMetaPage);
                    }
                }
                finally {
                    ctx.database().checkpointReadUnlock();
                }
            }
            else if (recoverState != null) { // Pre-create partition if having valid state.
                GridDhtLocalPartition part = grp.topology().forceCreatePartition(p);

                updateState(part, recoverState);

                processed++;

                if (log.isDebugEnabled())
                    log.debug("Restored partition state (from WAL) " +
                        "[grp=" + grp.cacheOrGroupName() + ", p=" + p + ", state=" + part.state() +
                        ", updCntr=" + part.initialUpdateCounter() +
                        ", size=" + part.fullSize() + ']');
            }
            else {
                if (log.isDebugEnabled())
                    log.debug("Skipping partition on recovery (no page store OR wal state) " +
                        "[grp=" + grp.cacheOrGroupName() + ", p=" + p + ']');
            }

            if (log.isDebugEnabled())
                log.debug("Finished restoring partition state " +
                    "[grp=" + grp.cacheOrGroupName() + ", p=" + p +
                    ", time=" + (U.currentTimeMillis() - startTime) + " ms]");
        }

        partitionStatesRestored = true;

        return processed;
    }

    /**
     * @param part Partition to restore state for.
     * @param stateId State enum ordinal.
     */
    private void updateState(GridDhtLocalPartition part, int stateId) {
        if (stateId != -1) {
            GridDhtPartitionState state = GridDhtPartitionState.fromOrdinal(stateId);

            assert state != null;

            part.restoreState(state == EVICTED ? RENTING : state);
        }
    }

    /**
     * Check that we need to snapshot this partition and add it to map.
     *
     * @param store Store.
     * @param ctx Snapshot context.
     */
    private void tryAddEmptyPartitionToSnapshot(CacheDataStore store, Context ctx) {
        if (getPartition(store).state() == OWNING) {
            ctx.partitionStatMap().put(
                new GroupPartitionId(grp.groupId(), store.partId()),
                new PagesAllocationRange(0, 0));
        }
    }

    /**
     * @param store Store.
     *
     * @return corresponding to store local partition
     */
    private GridDhtLocalPartition getPartition(CacheDataStore store) {
        return grp.topology().localPartition(store.partId(),
            AffinityTopologyVersion.NONE, false, true);
    }

    /**
     * Loads cache sizes for all caches in shared group.
     *
     * @param pageMem page memory to perform operations on pages.
     * @param grpId Cache group ID.
     * @param cntrsPageId Counters page ID, if zero is provided that means no counters page exist.
     * @return Cache sizes if store belongs to group containing multiple caches and sizes are available in memory. May
     * return null if counter page does not exist.
     * @throws IgniteCheckedException If page memory operation failed.
     */
    @Nullable private static Map<Integer, Long> readSharedGroupCacheSizes(PageSupport pageMem, int grpId,
        long cntrsPageId) throws IgniteCheckedException {

        if (cntrsPageId == 0L)
            return null;

        Map<Integer, Long> cacheSizes = new HashMap<>();

        long nextId = cntrsPageId;

        while (true) {
            final long curId = nextId;
            final long curPage = pageMem.acquirePage(grpId, curId);

            try {
                final long curAddr = pageMem.readLock(grpId, curId, curPage);

                assert curAddr != 0;

                try {
                    PagePartitionCountersIO cntrsIO = PageIO.getPageIO(curAddr);

                    if (cntrsIO.readCacheSizes(curAddr, cacheSizes))
                        break;

                    nextId = cntrsIO.getNextCountersPageId(curAddr);

                    assert nextId != 0;
                }
                finally {
                    pageMem.readUnlock(grpId, curId, curPage);
                }
            }
            finally {
                pageMem.releasePage(grpId, curId, curPage);
            }
        }
        return cacheSizes;
    }

    /**
     * Saves cache sizes for all caches in shared group. Unconditionally marks pages as dirty.
     *
     * @param pageMem page memory to perform operations on pages.
     * @param grpId Cache group ID.
     * @param cntrsPageId Counters page ID, if zero is provided that means no counters page exist.
     * @param partId Partition ID.
     * @param sizes Cache sizes of all caches in group. Not null.
     * @return new counter page Id. Same as {@code cntrsPageId} or new value if cache size pages were initialized.
     * @throws IgniteCheckedException if page memory operation failed.
     */
    private static long writeSharedGroupCacheSizes(PageMemory pageMem, int grpId,
        long cntrsPageId, int partId, Map<Integer, Long> sizes) throws IgniteCheckedException {
        byte[] data = PagePartitionCountersIO.VERSIONS.latest().serializeCacheSizes(sizes);

        int items = data.length / PagePartitionCountersIO.ITEM_SIZE;
        boolean init = cntrsPageId == 0;

        if (init && !sizes.isEmpty())
            cntrsPageId = pageMem.allocatePage(grpId, partId, PageIdAllocator.FLAG_DATA);

        long nextId = cntrsPageId;
        int written = 0;

        while (written != items) {
            final long curId = nextId;
            final long curPage = pageMem.acquirePage(grpId, curId);

            try {
                final long curAddr = pageMem.writeLock(grpId, curId, curPage);

                assert curAddr != 0;

                try {
                    PagePartitionCountersIO partCntrIo;

                    if (init) {
                        partCntrIo = PagePartitionCountersIO.VERSIONS.latest();

                        partCntrIo.initNewPage(curAddr, curId, pageMem.realPageSize(grpId));
                    }
                    else
                        partCntrIo = PageIO.getPageIO(curAddr);

                    written += partCntrIo.writeCacheSizes(pageMem.realPageSize(grpId), curAddr, data, written);

                    nextId = partCntrIo.getNextCountersPageId(curAddr);

                    if (written != items && (init = nextId == 0)) {
                        //allocate new counters page
                        nextId = pageMem.allocatePage(grpId, partId, PageIdAllocator.FLAG_DATA);
                        partCntrIo.setNextCountersPageId(curAddr, nextId);
                    }
                }
                finally {
                    // Write full page
                    pageMem.writeUnlock(grpId, curId, curPage, Boolean.TRUE, true);
                }
            }
            finally {
                pageMem.releasePage(grpId, curId, curPage);
            }
        }

        return cntrsPageId;
    }

    /**
     * @param ctx Context.
     */
    private void addPartitions(Context ctx) throws IgniteCheckedException {
        int grpId = grp.groupId();
        PageMemoryEx pageMem = (PageMemoryEx)grp.dataRegion().pageMemory();

        long metaPageId = pageMem.metaPageId(grpId);
        long metaPage = pageMem.acquirePage(grpId, metaPageId);

        try {
            long metaPageAddr = pageMem.writeLock(grpId, metaPageId, metaPage);

            try {
                PageMetaIO metaIo = PageMetaIO.getPageIO(metaPageAddr);

                addPartition(
                    null,
                    ctx.partitionStatMap(),
                    metaPageAddr,
                    metaIo,
                    grpId,
                    PageIdAllocator.INDEX_PARTITION,
                    this.ctx.pageStore().pages(grpId, PageIdAllocator.INDEX_PARTITION),
                    -1);
            }
            finally {
                pageMem.writeUnlock(grpId, metaPageId, metaPage, null, true);
            }
        }
        finally {
            pageMem.releasePage(grpId, metaPageId, metaPage);
        }
    }

    /**
     * @param part Local partition.
     * @param map Map to add values to.
     * @param metaPageAddr Meta page address
     * @param io Page Meta IO
     * @param grpId Cache Group ID.
     * @param currAllocatedPageCnt total number of pages allocated for partition <code>[partition, grpId]</code>
     */
    private static boolean addPartition(
        GridDhtLocalPartition part,
        final PartitionAllocationMap map,
        final long metaPageAddr,
        final PageMetaIO io,
        final int grpId,
        final int partId,
        final int currAllocatedPageCnt,
        final long partSize
    ) {
        if (part != null) {
            boolean reserved = part.reserve();

            if (!reserved)
                return false;
        }
        else
            assert partId == PageIdAllocator.INDEX_PARTITION : partId;

        assert PageIO.getPageId(metaPageAddr) != 0;

        int lastAllocatedPageCnt = io.getLastAllocatedPageCount(metaPageAddr);

        int curPageCnt = partSize == 0 ? 0 : currAllocatedPageCnt;

        map.put(
            new GroupPartitionId(grpId, partId),
            new PagesAllocationRange(lastAllocatedPageCnt, curPageCnt));

        return true;
    }

    /** {@inheritDoc} */
    @Override protected void destroyCacheDataStore0(CacheDataStore store) throws IgniteCheckedException {
        assert ctx.database() instanceof GridCacheDatabaseSharedManager
            : "Destroying cache data store when persistence is not enabled: " + ctx.database();

        int partId = store.partId();

        ctx.database().checkpointReadLock();

        try {
            saveStoreMetadata(store, null, true, false);
        }
        finally {
            ctx.database().checkpointReadUnlock();
        }

        store.markDestroyed();

        ((GridCacheDatabaseSharedManager)ctx.database()).schedulePartitionDestroy(grp.groupId(), partId);
    }

    /**
     * Invalidates page memory for given partition. Destroys partition store.
     * <b>NOTE:</b> This method can be invoked only within checkpoint lock or checkpointer thread.
     *
     * @param grpId Group ID.
     * @param partId Partition ID.
     *
     * @throws IgniteCheckedException If destroy has failed.
     */
    public void destroyPartitionStore(int grpId, int partId) throws IgniteCheckedException {
        PageMemoryEx pageMemory = (PageMemoryEx)grp.dataRegion().pageMemory();

        int tag = pageMemory.invalidate(grp.groupId(), partId);

        if (grp.walEnabled())
            ctx.wal().log(new PartitionDestroyRecord(grp.groupId(), partId));

        ctx.pageStore().onPartitionDestroyed(grpId, partId, tag);
    }

    /** {@inheritDoc} */
    @Override public void onPartitionCounterUpdated(int part, long cntr) {
        CacheDataStore store = partDataStores.get(part);

        assert store != null;

        long oldCnt = store.updateCounter();

        if (oldCnt < cntr)
            store.updateCounter(cntr);
    }

    /** {@inheritDoc} */
    @Override public void onPartitionInitialCounterUpdated(int part, long start, long delta) {
        CacheDataStore store = partDataStores.get(part);

        assert store != null;

        store.updateInitialCounter(start, delta);
    }

    /** {@inheritDoc} */
    @Override public long lastUpdatedPartitionCounter(int part) {
        return partDataStores.get(part).updateCounter();
    }

    /** {@inheritDoc} */
    @Override public RootPage rootPageForIndex(int cacheId, String idxName, int segment) throws IgniteCheckedException {
        return indexStorage.allocateCacheIndex(cacheId, idxName, segment);
    }

    /** {@inheritDoc} */
    @Override public void dropRootPageForIndex(int cacheId, String idxName, int segment) throws IgniteCheckedException {
        indexStorage.dropCacheIndex(cacheId, idxName, segment);
    }

    /** {@inheritDoc} */
    @Override public ReuseList reuseListForIndex(String idxName) {
        return reuseList;
    }

    /** {@inheritDoc} */
    @Override public void stop() {
        if (grp.affinityNode())
            ((GridCacheDatabaseSharedManager)ctx.database()).removeCheckpointListener(this);
    }

    /**
     * @return Meta root pages info.
     * @throws IgniteCheckedException If failed.
     */
    private Metas getOrAllocateCacheMetas() throws IgniteCheckedException {
        PageMemoryEx pageMem = (PageMemoryEx)grp.dataRegion().pageMemory();
        IgniteWriteAheadLogManager wal = ctx.wal();

        int grpId = grp.groupId();
        long metaId = pageMem.metaPageId(grpId);
        long metaPage = pageMem.acquirePage(grpId, metaId);

        try {
            final long pageAddr = pageMem.writeLock(grpId, metaId, metaPage);

            boolean allocated = false;

            try {
                long metastoreRoot, reuseListRoot;

                if (PageIO.getType(pageAddr) != PageIO.T_META) {
                    PageMetaIO pageIO = PageMetaIO.VERSIONS.latest();

                    pageIO.initNewPage(pageAddr, metaId, pageMem.realPageSize(grpId));

                    metastoreRoot = pageMem.allocatePage(grpId, PageIdAllocator.INDEX_PARTITION, PageMemory.FLAG_IDX);
                    reuseListRoot = pageMem.allocatePage(grpId, PageIdAllocator.INDEX_PARTITION, PageMemory.FLAG_IDX);

                    pageIO.setTreeRoot(pageAddr, metastoreRoot);
                    pageIO.setReuseListRoot(pageAddr, reuseListRoot);

                    if (PageHandler.isWalDeltaRecordNeeded(pageMem, grpId, metaId, metaPage, wal, null)) {
                        assert pageIO.getType() == PageIO.T_META;

                        wal.log(new MetaPageInitRecord(
                            grpId,
                            metaId,
                            pageIO.getType(),
                            pageIO.getVersion(),
                            metastoreRoot,
                            reuseListRoot
                        ));
                    }

                    allocated = true;
                }
                else {
                    PageMetaIO pageIO = PageIO.getPageIO(pageAddr);

                    metastoreRoot = pageIO.getTreeRoot(pageAddr);
                    reuseListRoot = pageIO.getReuseListRoot(pageAddr);

                    assert reuseListRoot != 0L;
                }

                return new Metas(
                    new RootPage(new FullPageId(metastoreRoot, grpId), allocated),
                    new RootPage(new FullPageId(reuseListRoot, grpId), allocated),
                    null,
                    null);
            }
            finally {
                pageMem.writeUnlock(grpId, metaId, metaPage, null, allocated);
            }
        }
        finally {
            pageMem.releasePage(grpId, metaId, metaPage);
        }
    }

    /** {@inheritDoc} */
    @Override @Nullable protected IgniteHistoricalIterator historicalIterator(
        CachePartitionPartialCountersMap partCntrs,
        Set<Integer> missing
    ) throws IgniteCheckedException {
        if (partCntrs == null || partCntrs.isEmpty())
            return null;

        if (grp.mvccEnabled()) // TODO IGNITE-7384
            return super.historicalIterator(partCntrs, missing);

        Map<Integer, Long> partsCounters = new HashMap<>();

        for (int i = 0; i < partCntrs.size(); i++) {
            int p = partCntrs.partitionAt(i);
            long initCntr = partCntrs.initialUpdateCounterAt(i);

            partsCounters.put(p, initCntr);
        }

        GridCacheDatabaseSharedManager database = (GridCacheDatabaseSharedManager)grp.shared().database();

        FileWALPointer minPtr = (FileWALPointer)database.checkpointHistory().searchEarliestWalPointer(grp.groupId(), partsCounters);

        try {
            WALIterator it = grp.shared().wal().replay(minPtr);

            WALHistoricalIterator histIt = new WALHistoricalIterator(log, grp, partCntrs, it);

            // Add historical partitions which are unabled to reserve to missing set.
            missing.addAll(histIt.missingParts);

            return histIt;
        }
        catch (Exception ex) {
            if (!X.hasCause(ex, IgniteHistoricalIteratorException.class))
                throw new IgniteHistoricalIteratorException(ex);

            throw ex;
        }
    }

    /** {@inheritDoc} */
    @Override public boolean expire(
        GridCacheContext cctx,
        IgniteInClosure2X<GridCacheEntryEx, GridCacheVersion> c,
        int amount
    ) throws IgniteCheckedException {
        assert !cctx.isNear() : cctx.name();

        // Prevent manager being stopped in the middle of pds operation.
        if (!busyLock.enterBusy())
            return false;

        try {
            int cleared = 0;

            for (CacheDataStore store : cacheDataStores()) {
                cleared += ((GridCacheDataStore)store).purgeExpired(cctx, c, amount - cleared);

                if (amount != -1 && cleared >= amount)
                    return true;
            }
        }
        finally {
            busyLock.leaveBusy();
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public long expiredSize() throws IgniteCheckedException {
        long size = 0;

        for (CacheDataStore store : cacheDataStores())
            size += ((GridCacheDataStore)store).expiredSize();

        return size;
    }

    /** {@inheritDoc} */
    @Override public void preloadPartition(int part) throws IgniteCheckedException {
        if (grp.isLocal()) {
            dataStore(part).preload();

            return;
        }

        GridDhtLocalPartition locPart = grp.topology().localPartition(part, AffinityTopologyVersion.NONE, false, false);

        assert locPart != null && locPart.reservations() > 0;

        locPart.dataStore().preload();
    }

    /**
     * Calculates free space of all partition data stores - number of bytes available for use in allocated pages.
     *
     * @return free space size in bytes.
     */
    long freeSpace() {
        long freeSpace = 0;

        for (CacheDataStore store : partDataStores.values()) {
            assert store instanceof GridCacheDataStore;

            AbstractFreeList freeList = ((GridCacheDataStore)store).freeList;

            if (freeList == null)
                continue;

            freeSpace += freeList.freeSpace();
        }

        return freeSpace;
    }

    /**
     * Calculates empty data pages of all partition data stores.
     *
     * @return empty data pages count.
     */
    long emptyDataPages() {
        long emptyDataPages = 0;

        for (CacheDataStore store : partDataStores.values()) {
            assert store instanceof GridCacheDataStore;

            AbstractFreeList freeList = ((GridCacheDataStore)store).freeList;

            if (freeList == null)
                continue;

            emptyDataPages += freeList.emptyDataPages();
        }

        return emptyDataPages;
    }

    /**
     * @param cacheId Which was stopped, but its data still presented.
     * @throws IgniteCheckedException If failed.
     */
    public void findAndCleanupLostIndexesForStoppedCache(int cacheId) throws IgniteCheckedException {
        for (String name : indexStorage.getIndexNames()) {
            if (indexStorage.nameIsAssosiatedWithCache(name, cacheId)) {
                ctx.database().checkpointReadLock();

                try {
                    RootPage page = indexStorage.allocateIndex(name);

                    ctx.kernalContext().query().getIndexing().destroyOrphanIndex(
                        page,
                        name,
                        grp.groupId(),
                        grp.dataRegion().pageMemory(), globalRemoveId(),
                        reuseListForIndex(name),
                        grp.mvccEnabled()
                    );

                    indexStorage.dropIndex(name);
                }
                finally {
                    ctx.database().checkpointReadUnlock();
                }
            }
        }
    }

    /**
     *
     */
    private static class WALHistoricalIterator implements IgniteHistoricalIterator {
        /** */
        private static final long serialVersionUID = 0L;

        /** Logger. */
        private IgniteLogger log;

        /** Cache context. */
        private final CacheGroupContext grp;

        /** Partition counters map. */
        private final CachePartitionPartialCountersMap partMap;

        /** Partitions marked as missing (unable to reserve or partition is not in OWNING state). */
        private final Set<Integer> missingParts = new HashSet<>();

        /** Partitions marked as done. */
        private final Set<Integer> doneParts = new HashSet<>();

        /** Cache IDs. This collection is stored as field to avoid re-calculation on each iteration. */
        private final Set<Integer> cacheIds;

        /** WAL iterator. */
        private WALIterator walIt;

        /** */
        private Iterator<DataEntry> entryIt;

        /** */
        private DataEntry next;

        /**
         * Rebalanced counters in the range from initialUpdateCntr to updateCntr.
         * Invariant: initUpdCntr[idx] + rebalancedCntrs[idx] = updateCntr[idx]
         */
        private long[] rebalancedCntrs;

        /** A partition what will be finished on next iteration. */
        private int donePart = -1;

        /**
         * @param log Logger.
         * @param grp Cache context.
         * @param walIt WAL iterator.
         */
        private WALHistoricalIterator(IgniteLogger log, CacheGroupContext grp, CachePartitionPartialCountersMap partMap,
            WALIterator walIt) {
            this.log = log;
            this.grp = grp;
            this.partMap = partMap;
            this.walIt = walIt;

            cacheIds = grp.cacheIds();

            rebalancedCntrs = new long[partMap.size()];

            for (int i = 0; i < rebalancedCntrs.length; i++)
                rebalancedCntrs[i] = partMap.initialUpdateCounterAt(i);

            reservePartitions();

            advance();
        }

        /** {@inheritDoc} */
        @Override public boolean contains(int partId) {
            return partMap.contains(partId);
        }

        /** {@inheritDoc} */
        @Override public boolean isDone(int partId) {
            return doneParts.contains(partId);
        }

        /** {@inheritDoc} */
        @Override public void close() throws IgniteCheckedException {
            walIt.close();
            releasePartitions();
        }

        /** {@inheritDoc} */
        @Override public boolean isClosed() {
            return walIt.isClosed();
        }

        /** {@inheritDoc} */
        @Override public boolean hasNextX() {
            return hasNext();
        }

        /** {@inheritDoc} */
        @Override public CacheDataRow nextX() throws IgniteCheckedException {
            return next();
        }

        /** {@inheritDoc} */
        @Override public void removeX() throws IgniteCheckedException {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override public Iterator<CacheDataRow> iterator() {
            return this;
        }

        /** {@inheritDoc} */
        @Override public boolean hasNext() {
            return next != null;
        }

        /** {@inheritDoc} */
        @Override public CacheDataRow next() {
            if (next == null)
                throw new NoSuchElementException();

            CacheDataRow val = new DataEntryRow(next);

            if (donePart != -1) {
                int pIdx = partMap.partitionIndex(donePart);

                if (log.isDebugEnabled()) {
                    log.debug("Partition done [grpId=" + grp.groupId() +
                        ", partId=" + donePart +
                        ", from=" + partMap.initialUpdateCounterAt(pIdx) +
                        ", to=" + partMap.updateCounterAt(pIdx) + ']');
                }

                doneParts.add(donePart);

                donePart = -1;
            }

            advance();

            return val;
        }

        /** {@inheritDoc} */
        @Override public void remove() {
            throw new UnsupportedOperationException();
        }

        /**
         * Reserve historical partitions.
         * If partition is unable to reserve, id of that partition is placed to {@link #missingParts} set.
         */
        private void reservePartitions() {
            for (int i = 0; i < partMap.size(); i++) {
                int p = partMap.partitionAt(i);
                GridDhtLocalPartition part = grp.topology().localPartition(p);

                if (part == null || !part.reserve()) {
                    missingParts.add(p);
                    continue;
                }

                if (part.state() != OWNING) {
                    part.release();
                    missingParts.add(p);
                }
            }
        }

        /**
         * Release historical partitions.
         */
        private void releasePartitions() {
            for (int i = 0; i < partMap.size(); i++) {
                int p = partMap.partitionAt(i);

                if (missingParts.contains(p))
                    continue;

                GridDhtLocalPartition part = grp.topology().localPartition(p);

                assert part != null && part.state() == OWNING && part.reservations() > 0
                    : "Partition should in OWNING state and has at least 1 reservation";

                part.release();
            }
        }

        /**
         *
         */
        private void advance() {
            try {
                next = null;

                outer:
                while (doneParts.size() != partMap.size()) {
                    if (entryIt != null) {
                        while (entryIt.hasNext()) {
                            DataEntry entry = entryIt.next();

                            if (cacheIds.contains(entry.cacheId())) {
                                int idx = partMap.partitionIndex(entry.partitionId());

                                if (idx < 0 || missingParts.contains(idx))
                                    continue;

                                long from = partMap.initialUpdateCounterAt(idx);
                                long to = partMap.updateCounterAt(idx);

                                if (entry.partitionCounter() > from && entry.partitionCounter() <= to) {
                                    // Partition will be marked as done for current entry on next iteration.
                                    if (++rebalancedCntrs[idx] == to)
                                        donePart = entry.partitionId();

                                    next = entry;

                                    return;
                                }
                            }
                        }
                    }

                    entryIt = null;

                    // Search for next DataEntry while applying rollback counters.
                    while (walIt.hasNext()) {
                        IgniteBiTuple<WALPointer, WALRecord> rec = walIt.next();

                        if (rec.get2() instanceof DataRecord) {
                            DataRecord data = (DataRecord)rec.get2();

                            entryIt = data.writeEntries().iterator();

                            // Move on to the next valid data entry.
                            continue outer;
                        }
                        else if (rec.get2() instanceof RollbackRecord) {
                            RollbackRecord rbRec = (RollbackRecord)rec.get2();

                            if (grp.groupId() == rbRec.groupId()) {
                                int idx = partMap.partitionIndex(rbRec.partitionId());

                                if (idx < 0 || missingParts.contains(idx))
                                    continue;

                                long from = partMap.initialUpdateCounterAt(idx);
                                long to = partMap.updateCounterAt(idx);

                                rebalancedCntrs[idx] += rbRec.overlap(from, to);

                                if (rebalancedCntrs[idx] == partMap.updateCounterAt(idx)) {
                                    if (log.isDebugEnabled()) {
                                        log.debug("Partition done [grpId=" + grp.groupId() +
                                            ", partId=" + donePart +
                                            ", from=" + from +
                                            ", to=" + to + ']');
                                    }

                                    doneParts.add(rbRec.partitionId()); // Add to done set immediately.
                                }
                            }
                        }
                    }

                    if (entryIt == null && doneParts.size() != partMap.size()) {
                        for (int i = 0; i < partMap.size(); i++) {
                            int p = partMap.partitionAt(i);

                            if (!doneParts.contains(p)) {
                                log.warning("Some partition entries were missed during historical rebalance [grp=" + grp + ", part=" + p + ", missed=" +
                                        (partMap.updateCounterAt(i) - rebalancedCntrs[i]) + ']');

                                    doneParts.add(p);
                                }
                            }

                        return;
                    }
                }
            }
            catch (Exception ex) {
                throw new IgniteHistoricalIteratorException(ex);
            }
        }
    }

    /**
     * Data entry row.
     */
    private static class DataEntryRow implements CacheDataRow {
        /** */
        private final DataEntry entry;

        /**
         * @param entry Data entry.
         */
        private DataEntryRow(DataEntry entry) {
            this.entry = entry;
        }

        /** {@inheritDoc} */
        @Override public KeyCacheObject key() {
            return entry.key();
        }

        /** {@inheritDoc} */
        @Override public void key(KeyCacheObject key) {
            throw new IllegalStateException();
        }

        /** {@inheritDoc} */
        @Override public CacheObject value() {
            return entry.value();
        }

        /** {@inheritDoc} */
        @Override public GridCacheVersion version() {
            return entry.writeVersion();
        }

        /** {@inheritDoc} */
        @Override public long expireTime() {
            return entry.expireTime();
        }

        /** {@inheritDoc} */
        @Override public int partition() {
            return entry.partitionId();
        }

        /** {@inheritDoc} */
        @Override public int size() throws IgniteCheckedException {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override public int headerSize() {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override public long link() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public void link(long link) {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override public int hash() {
            return entry.key().hashCode();
        }

        /** {@inheritDoc} */
        @Override public int cacheId() {
            return entry.cacheId();
        }

        /** {@inheritDoc} */
        @Override public long mvccCoordinatorVersion() {
            return 0; // TODO IGNITE-7384
        }

        /** {@inheritDoc} */
        @Override public long mvccCounter() {
            return 0;  // TODO IGNITE-7384
        }

        /** {@inheritDoc} */
        @Override public int mvccOperationCounter() {
            return 0;  // TODO IGNITE-7384
        }

        /** {@inheritDoc} */
        @Override public long newMvccCoordinatorVersion() {
            return 0; // TODO IGNITE-7384
        }

        /** {@inheritDoc} */
        @Override public long newMvccCounter() {
            return 0; // TODO IGNITE-7384
        }

        /** {@inheritDoc} */
        @Override public int newMvccOperationCounter() {
            return 0;  // TODO IGNITE-7384
        }

        /** {@inheritDoc} */
        @Override public byte mvccTxState() {
            return 0;  // TODO IGNITE-7384
        }

        /** {@inheritDoc} */
        @Override public byte newMvccTxState() {
            return 0; // TODO IGNITE-7384
        }
    }

    /**
     *
     */
    private static class Metas {
        /** */
        @GridToStringInclude
        private final RootPage reuseListRoot;

        /** */
        @GridToStringInclude
        private final RootPage treeRoot;

        /** */
        @GridToStringInclude
        private final RootPage pendingTreeRoot;

        /** */
        @GridToStringInclude
        private final RootPage partMetastoreReuseListRoot;

        /**
         * @param treeRoot Metadata storage root.
         * @param reuseListRoot Reuse list root.
         */
        Metas(RootPage treeRoot, RootPage reuseListRoot, RootPage pendingTreeRoot, RootPage partMetastoreReuseListRoot) {
            this.treeRoot = treeRoot;
            this.reuseListRoot = reuseListRoot;
            this.pendingTreeRoot = pendingTreeRoot;
            this.partMetastoreReuseListRoot = partMetastoreReuseListRoot;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(Metas.class, this);
        }
    }

    /**
     *
     */
    public class GridCacheDataStore implements CacheDataStore {
        /** */
        private final int partId;

        /** */
        private volatile AbstractFreeList<CacheDataRow> freeList;

        /** */
        private PendingEntriesTree pendingTree;

        /** */
        private volatile CacheDataStoreImpl delegate;

        /**
         * Cache id which should be throttled.
         */
        private volatile int lastThrottledCacheId;

        /**
         * Timestamp when next clean try will be allowed for the current partition
         * in accordance with the value of {@code lastThrottledCacheId}.
         * Used for fine-grained throttling on per-partition basis.
         */
        private volatile long nextStoreCleanTimeNanos;

        /** */
        private PartitionMetaStorage<SimpleDataRow> partStorage;

        /** */
        private final boolean exists;

        /** */
        private final AtomicBoolean init = new AtomicBoolean();

        /** */
        private final CountDownLatch latch = new CountDownLatch(1);

        /**
         * @param partId Partition.
         * @param exists {@code True} if store exists.
         */
        private GridCacheDataStore(int partId, boolean exists) {
            this.partId = partId;
            this.exists = exists;
        }

        /**
         * @return Name of free pages list.
         */
        private String freeListName() {
            return grp.cacheOrGroupName() + "-" + partId;
        }

        /**
         * @return Name of partition meta store.
         */
        private String partitionMetaStoreName() {
            return grp.cacheOrGroupName() + "-partstore-" + partId;
        }

        /**
         * @return Name of data tree.
         */
        private String dataTreeName() {
            return grp.cacheOrGroupName() + "-" + treeName(partId);
        }

        /**
         * @return Name of pending entires tree.
         */
        private String pendingEntriesTreeName() {
            return grp.cacheOrGroupName() + "-" + "PendingEntries-" + partId;
        }

        /**
         * @param checkExists If {@code true} data store won't be initialized if it doesn't exists
         * (has non empty data file). This is an optimization for lazy store initialization on writes.
         *
         * @return Store delegate.
         * @throws IgniteCheckedException If failed.
         */
        private CacheDataStore init0(boolean checkExists) throws IgniteCheckedException {
            CacheDataStoreImpl delegate0 = delegate;

            if (delegate0 != null)
                return delegate0;

            if (checkExists) {
                if (!exists)
                    return null;
            }

            if (init.compareAndSet(false, true)) {
                IgniteCacheDatabaseSharedManager dbMgr = ctx.database();

                dbMgr.checkpointReadLock();

                try {
                    Metas metas = getOrAllocatePartitionMetas();

                    if (PageIdUtils.partId(metas.reuseListRoot.pageId().pageId()) != partId ||
                        PageIdUtils.partId(metas.treeRoot.pageId().pageId()) != partId ||
                        PageIdUtils.partId(metas.pendingTreeRoot.pageId().pageId()) != partId ||
                        PageIdUtils.partId(metas.partMetastoreReuseListRoot.pageId().pageId()) != partId
                        ) {
                        throw new IgniteCheckedException("Invalid meta root allocated [" +
                            "cacheOrGroupName=" + grp.cacheOrGroupName() +
                            ", partId=" + partId +
                            ", metas=" + metas + ']');
                    }

                    String freeListName = freeListName();

                    RootPage reuseRoot = metas.reuseListRoot;

                    freeList = new CacheFreeList(
                        grp.groupId(),
                        freeListName,
                        grp.dataRegion().memoryMetrics(),
                        grp.dataRegion(),
                        ctx.wal(),
                        reuseRoot.pageId().pageId(),
                        reuseRoot.isAllocated(),
                        ctx.diagnostic().pageLockTracker().createPageLockTracker(freeListName),
                        ctx.kernalContext(),
                        pageListCacheLimit
                    ) {
                        /** {@inheritDoc} */
                        @Override protected long allocatePageNoReuse() throws IgniteCheckedException {
                            assert grp.shared().database().checkpointLockIsHeldByThread();

                            return pageMem.allocatePage(grpId, partId, PageIdAllocator.FLAG_DATA);
                        }
                    };

                    String partitionMetaStoreName = partitionMetaStoreName();

                    RootPage partMetastoreReuseListRoot = metas.partMetastoreReuseListRoot;

                    partStorage = new PartitionMetaStorageImpl<SimpleDataRow>(
                        grp.groupId(),
                        partitionMetaStoreName,
                        grp.dataRegion().memoryMetrics(),
                        grp.dataRegion(),
                        freeList,
                        ctx.wal(),
                        partMetastoreReuseListRoot.pageId().pageId(),
                        partMetastoreReuseListRoot.isAllocated(),
                        ctx.diagnostic().pageLockTracker().createPageLockTracker(partitionMetaStoreName),
                        ctx.kernalContext(),
                        pageListCacheLimit
                    ) {
                        /** {@inheritDoc} */
                        @Override protected long allocatePageNoReuse() throws IgniteCheckedException {
                            assert grp.shared().database().checkpointLockIsHeldByThread();

                            return pageMem.allocatePage(grpId, partId, PageIdAllocator.FLAG_DATA);
                        }
                    };

                    String dataTreeName = dataTreeName();

                    CacheDataRowStore rowStore = new CacheDataRowStore(grp, freeList, partId);

                    RootPage treeRoot = metas.treeRoot;

                    CacheDataTree dataTree = new CacheDataTree(
                        grp,
                        dataTreeName,
                        freeList,
                        rowStore,
                        treeRoot.pageId().pageId(),
                        treeRoot.isAllocated(),
                        ctx.diagnostic().pageLockTracker().createPageLockTracker(dataTreeName)
                    ) {
                        /** {@inheritDoc} */
                        @Override protected long allocatePageNoReuse() throws IgniteCheckedException {
                            assert grp.shared().database().checkpointLockIsHeldByThread();

                            return pageMem.allocatePage(grpId, partId, PageIdAllocator.FLAG_DATA);
                        }
                    };

                    String pendingEntriesTreeName = pendingEntriesTreeName();

                    RootPage pendingTreeRoot = metas.pendingTreeRoot;

                    final PendingEntriesTree pendingTree0 = new PendingEntriesTree(
                        grp,
                        pendingEntriesTreeName,
                        grp.dataRegion().pageMemory(),
                        pendingTreeRoot.pageId().pageId(),
                        freeList,
                        pendingTreeRoot.isAllocated(),
                        ctx.diagnostic().pageLockTracker().createPageLockTracker(pendingEntriesTreeName)
                    ) {
                        /** {@inheritDoc} */
                        @Override protected long allocatePageNoReuse() throws IgniteCheckedException {
                            assert grp.shared().database().checkpointLockIsHeldByThread();

                            return pageMem.allocatePage(grpId, partId, PageIdAllocator.FLAG_DATA);
                        }
                    };

                    PageMemoryEx pageMem = (PageMemoryEx)grp.dataRegion().pageMemory();

                    delegate0 = new CacheDataStoreImpl(partId, rowStore, dataTree) {
                        /** {@inheritDoc} */
                        @Override public PendingEntriesTree pendingTree() {
                            return pendingTree0;
                        }

                        /** {@inheritDoc} */
                        @Override public void preload() throws IgniteCheckedException {
                            IgnitePageStoreManager pageStoreMgr = ctx.pageStore();

                            if (pageStoreMgr == null)
                                return;

                            final int pages = pageStoreMgr.pages(grp.groupId(), partId);

                            long pageId = pageMem.partitionMetaPageId(grp.groupId(), partId);

                            // For each page sequentially pin/unpin.
                            for (int pageNo = 0; pageNo < pages; pageId++, pageNo++) {
                                long pagePointer = -1;

                                try {
                                    pagePointer = pageMem.acquirePage(grp.groupId(), pageId);
                                }
                                finally {
                                    if (pagePointer != -1)
                                        pageMem.releasePage(grp.groupId(), pageId, pagePointer);
                                }
                            }
                        }
                    };

                    pendingTree = pendingTree0;

                    if (!pendingTree0.isEmpty())
                        grp.caches().forEach(cctx -> cctx.ttl().hasPendingEntries(true));

                    int grpId = grp.groupId();
                    long partMetaId = pageMem.partitionMetaPageId(grpId, partId);
                    long partMetaPage = pageMem.acquirePage(grpId, partMetaId);

                    try {
                        long pageAddr = pageMem.readLock(grpId, partMetaId, partMetaPage);

                        try {
                            if (PageIO.getType(pageAddr) != 0) {
                                PagePartitionMetaIOV2 io = (PagePartitionMetaIOV2)PagePartitionMetaIO.VERSIONS.latest();

                                Map<Integer, Long> cacheSizes = null;

                                if (grp.sharedGroup())
                                    cacheSizes = readSharedGroupCacheSizes(pageMem, grpId, io.getCountersPageId(pageAddr));

                                long link = io.getGapsLink(pageAddr);

                                byte[] data = link == 0 ? null : partStorage.readRow(link);

                                delegate0.restoreState(io.getSize(pageAddr), io.getUpdateCounter(pageAddr), cacheSizes, data);

                                globalRemoveId().setIfGreater(io.getGlobalRemoveId(pageAddr));
                            }
                        }
                        finally {
                            pageMem.readUnlock(grpId, partMetaId, partMetaPage);
                        }
                    }
                    finally {
                        pageMem.releasePage(grpId, partMetaId, partMetaPage);
                    }

                    delegate = delegate0;
                }
                catch (Throwable ex) {
                    U.error(log, "Unhandled exception during page store initialization. All further operations will " +
                        "be failed and local node will be stopped.", ex);

                    ctx.kernalContext().failure().process(new FailureContext(FailureType.CRITICAL_ERROR, ex));

                    throw ex;
                }
                finally {
                    latch.countDown();

                    dbMgr.checkpointReadUnlock();
                }
            }
            else {
                U.await(latch);

                delegate0 = delegate;

                if (delegate0 == null)
                    throw new IgniteCheckedException("Cache store initialization failed.");
            }

            return delegate0;
        }

        /**
         * @return Partition metas.
         */
        private Metas getOrAllocatePartitionMetas() throws IgniteCheckedException {
            PageMemoryEx pageMem = (PageMemoryEx)grp.dataRegion().pageMemory();
            IgniteWriteAheadLogManager wal = ctx.wal();

            int grpId = grp.groupId();
            long partMetaId = pageMem.partitionMetaPageId(grpId, partId);

            AtomicBoolean metaPageAllocated = new AtomicBoolean(false);

            long partMetaPage = pageMem.acquirePage(grpId, partMetaId, metaPageAllocated);

            if (metaPageAllocated.get())
                grp.metrics().incrementInitializedLocalPartitions();

            try {
                boolean allocated = false;
                boolean pendingTreeAllocated = false;
                boolean partMetastoreReuseListAllocated = false;

                long pageAddr = pageMem.writeLock(grpId, partMetaId, partMetaPage);
                try {
                    long treeRoot, reuseListRoot, pendingTreeRoot, partMetaStoreReuseListRoot;

                    // Initialize new page.
                    if (PageIO.getType(pageAddr) != PageIO.T_PART_META) {
                        PagePartitionMetaIOV2 io = (PagePartitionMetaIOV2)PagePartitionMetaIO.VERSIONS.latest();

                        io.initNewPage(pageAddr, partMetaId, pageMem.realPageSize(grpId));

                        treeRoot = pageMem.allocatePage(grpId, partId, PageMemory.FLAG_DATA);
                        reuseListRoot = pageMem.allocatePage(grpId, partId, PageMemory.FLAG_DATA);
                        pendingTreeRoot = pageMem.allocatePage(grpId, partId, PageMemory.FLAG_DATA);
                        partMetaStoreReuseListRoot = pageMem.allocatePage(grpId, partId, PageMemory.FLAG_DATA);

                        assert PageIdUtils.flag(treeRoot) == PageMemory.FLAG_DATA;
                        assert PageIdUtils.flag(reuseListRoot) == PageMemory.FLAG_DATA;
                        assert PageIdUtils.flag(pendingTreeRoot) == PageMemory.FLAG_DATA;
                        assert PageIdUtils.flag(partMetaStoreReuseListRoot) == PageMemory.FLAG_DATA;

                        io.setTreeRoot(pageAddr, treeRoot);
                        io.setReuseListRoot(pageAddr, reuseListRoot);
                        io.setPendingTreeRoot(pageAddr, pendingTreeRoot);
                        io.setPartitionMetaStoreReuseListRoot(pageAddr, partMetaStoreReuseListRoot);

                        if (PageHandler.isWalDeltaRecordNeeded(pageMem, grpId, partMetaId, partMetaPage, wal, null)) {
                            wal.log(new PageSnapshot(new FullPageId(partMetaId, grpId), pageAddr,
                                pageMem.pageSize(), pageMem.realPageSize(grpId)));
                        }

                        allocated = true;
                    }
                    else {
                        PagePartitionMetaIO io = PageIO.getPageIO(pageAddr);

                        treeRoot = io.getTreeRoot(pageAddr);
                        reuseListRoot = io.getReuseListRoot(pageAddr);

                        int pageVer = PagePartitionMetaIO.getVersion(pageAddr);

                        if (pageVer < 2) {
                            assert pageVer == 1;

                            if (log.isDebugEnabled())
                                log.info("Upgrade partition meta page version: [part=" + partId +
                                    ", grpId=" + grpId + ", oldVer=" + pageVer +
                                    ", newVer=" + io.getVersion()
                                );

                            io = PagePartitionMetaIO.VERSIONS.latest();

                            ((PagePartitionMetaIOV2)io).upgradePage(pageAddr);

                            pendingTreeRoot = pageMem.allocatePage(grpId, partId, PageMemory.FLAG_DATA);
                            partMetaStoreReuseListRoot = pageMem.allocatePage(grpId, partId, PageMemory.FLAG_DATA);

                            io.setPendingTreeRoot(pageAddr, pendingTreeRoot);
                            io.setPartitionMetaStoreReuseListRoot(pageAddr, partMetaStoreReuseListRoot);

                            if (PageHandler.isWalDeltaRecordNeeded(pageMem, grpId, partMetaId, partMetaPage, wal,
                                null)) {
                                wal.log(new PageSnapshot(new FullPageId(partMetaId, grpId), pageAddr,
                                    pageMem.pageSize(), pageMem.realPageSize(grpId)));
                            }

                            pendingTreeAllocated = partMetastoreReuseListAllocated = true;
                        }
                        else {
                            pendingTreeRoot = io.getPendingTreeRoot(pageAddr);
                            partMetaStoreReuseListRoot = io.getPartitionMetaStoreReuseListRoot(pageAddr);

                            if (partMetaStoreReuseListRoot == 0) {
                                partMetaStoreReuseListRoot = pageMem.allocatePage(grpId, partId, PageMemory.FLAG_DATA);

                                if (PageHandler.isWalDeltaRecordNeeded(pageMem, grpId, partMetaId, partMetaPage, wal,
                                    null)) {
                                    wal.log(new PageSnapshot(new FullPageId(partMetaId, grpId), pageAddr,
                                        pageMem.pageSize(), pageMem.realPageSize(grpId)));
                                }

                                partMetastoreReuseListAllocated = true;
                            }
                        }

                        if (PageIdUtils.flag(treeRoot) != PageMemory.FLAG_DATA)
                            throw new StorageException("Wrong tree root page id flag: treeRoot="
                                + U.hexLong(treeRoot) + ", part=" + partId + ", grpId=" + grpId);

                        if (PageIdUtils.flag(reuseListRoot) != PageMemory.FLAG_DATA)
                            throw new StorageException("Wrong reuse list root page id flag: reuseListRoot="
                                + U.hexLong(reuseListRoot) + ", part=" + partId + ", grpId=" + grpId);

                        if (PageIdUtils.flag(pendingTreeRoot) != PageMemory.FLAG_DATA)
                            throw new StorageException("Wrong pending tree root page id flag: reuseListRoot="
                                + U.hexLong(reuseListRoot) + ", part=" + partId + ", grpId=" + grpId);

                        if (PageIdUtils.flag(partMetaStoreReuseListRoot) != PageMemory.FLAG_DATA)
                            throw new StorageException("Wrong partition meta store list root page id flag: partMetaStoreReuseListRoot="
                                + U.hexLong(partMetaStoreReuseListRoot) + ", part=" + partId + ", grpId=" + grpId);
                    }

                    return new Metas(
                        new RootPage(new FullPageId(treeRoot, grpId), allocated),
                        new RootPage(new FullPageId(reuseListRoot, grpId), allocated),
                        new RootPage(new FullPageId(pendingTreeRoot, grpId), allocated || pendingTreeAllocated),
                        new RootPage(new FullPageId(partMetaStoreReuseListRoot, grpId), allocated || partMetastoreReuseListAllocated));
                }
                finally {
                    pageMem.writeUnlock(grpId, partMetaId, partMetaPage, null,
                        allocated || pendingTreeAllocated || partMetastoreReuseListAllocated);
                }
            }
            finally {
                pageMem.releasePage(grpId, partMetaId, partMetaPage);
            }
        }

        /** {@inheritDoc} */
        @Override public boolean init() {
            try {
                return init0(true) != null;
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
        }

        /** {@inheritDoc} */
        @Override public int partId() {
            return partId;
        }

        /** {@inheritDoc} */
        @Override public RowStore rowStore() {
            CacheDataStore delegate0 = delegate;

            return delegate0 == null ? null : delegate0.rowStore();
        }

        /** {@inheritDoc} */
        @Override public long fullSize() {
            try {
                CacheDataStore delegate0 = init0(true);

                return delegate0 == null ? 0 : delegate0.fullSize();
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
        }

        /** {@inheritDoc} */
        @Override public boolean isEmpty() {
            try {
                CacheDataStore delegate0 = init0(true);

                return delegate0 == null || delegate0.isEmpty();
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
        }

        /** {@inheritDoc} */
        @Override public long cacheSize(int cacheId) {
            try {
                CacheDataStore delegate0 = init0(true);

                return delegate0 == null ? 0 : delegate0.cacheSize(cacheId);
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
        }

        /** {@inheritDoc} */
        @Override public Map<Integer, Long> cacheSizes() {
            try {
                CacheDataStore delegate0 = init0(true);

                return delegate0 == null ? null : delegate0.cacheSizes();
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
        }

        /** {@inheritDoc} */
        @Override public void updateSize(int cacheId, long delta) {
            try {
                CacheDataStore delegate0 = init0(false);

                if (delegate0 != null)
                    delegate0.updateSize(cacheId, delta);
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
        }

        /** {@inheritDoc} */
        @Override public long updateCounter() {
            try {
                CacheDataStore delegate0 = init0(true);

                return delegate0 == null ? 0 : delegate0.updateCounter();
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
        }

        /** {@inheritDoc} */
        @Override public long reservedCounter() {
            try {
                CacheDataStore delegate0 = init0(true);

                return delegate0 == null ? 0 : delegate0.reservedCounter();
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
        }

        /** {@inheritDoc} */
        @Override public PartitionUpdateCounter partUpdateCounter() {
            try {
                CacheDataStore delegate0 = init0(true);

                return delegate0 == null ? null : delegate0.partUpdateCounter();
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
        }

        /** {@inheritDoc} */
        @Override public long getAndIncrementUpdateCounter(long delta) {
            try {
                CacheDataStore delegate0 = init0(false);

                return delegate0 == null ? 0 : delegate0.getAndIncrementUpdateCounter(delta);
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
        }

        /** {@inheritDoc} */
        @Override public long reserve(long delta) {
            try {
                CacheDataStore delegate0 = init0(false);

                if (delegate0 == null)
                    throw new IllegalStateException("Should be never called.");

                return delegate0.reserve(delta);
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
        }

        /** {@inheritDoc} */
        @Override public void updateCounter(long val) {
            try {
                CacheDataStore delegate0 = init0(false);

                if (delegate0 != null)
                    delegate0.updateCounter(val);
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
        }

        /** {@inheritDoc} */
        @Override public boolean updateCounter(long start, long delta) {
            try {
                CacheDataStore delegate0 = init0(false);

                return delegate0 != null && delegate0.updateCounter(start, delta);
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
        }

        /** {@inheritDoc} */
        @Override public GridLongList finalizeUpdateCounters() {
            try {
                CacheDataStore delegate0 = init0(true);

                return delegate0 != null ? delegate0.finalizeUpdateCounters() : null;
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
        }

        /** {@inheritDoc} */
        @Override public long nextUpdateCounter() {
            try {
                CacheDataStore delegate0 = init0(false);

                if (delegate0 == null)
                    throw new IllegalStateException("Should be never called.");

                return delegate0.nextUpdateCounter();
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
        }

        /** {@inheritDoc} */
        @Override public long initialUpdateCounter() {
            try {
                CacheDataStore delegate0 = init0(true);

                return delegate0 == null ? 0 : delegate0.initialUpdateCounter();
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
        }

        /** {@inheritDoc} */
        @Override public void updateInitialCounter(long start, long delta) {
            try {
                CacheDataStore delegate0 = init0(false);

                // Partition may not exists before recovery starts in case of recovering counters from RollbackRecord.
                delegate0.updateInitialCounter(start, delta);
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
        }

        /** {@inheritDoc} */
        @Override public void setRowCacheCleaner(GridQueryRowCacheCleaner rowCacheCleaner) {
            try {
                CacheDataStore delegate0 = init0(true);

                if (delegate0 != null)
                    delegate0.setRowCacheCleaner(rowCacheCleaner);
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
        }

        /** {@inheritDoc} */
        @Override public void update(
            GridCacheContext cctx,
            KeyCacheObject key,
            CacheObject val,
            GridCacheVersion ver,
            long expireTime,
            @Nullable CacheDataRow oldRow
        ) throws IgniteCheckedException {
            assert ctx.database().checkpointLockIsHeldByThread();

            CacheDataStore delegate = init0(false);

            delegate.update(cctx, key, val, ver, expireTime, oldRow);
        }

        /** {@inheritDoc} */
        @Override public boolean mvccInitialValue(
            GridCacheContext cctx,
            KeyCacheObject key,
            @Nullable CacheObject val,
            GridCacheVersion ver,
            long expireTime,
            MvccVersion mvccVer,
            MvccVersion newMvccVer)
            throws IgniteCheckedException
        {
            CacheDataStore delegate = init0(false);

            return delegate.mvccInitialValue(cctx, key, val, ver, expireTime, mvccVer, newMvccVer);
        }

        /** {@inheritDoc} */
        @Override public boolean mvccApplyHistoryIfAbsent(
            GridCacheContext cctx,
            KeyCacheObject key,
            List<GridCacheMvccEntryInfo> hist)
            throws IgniteCheckedException {
            CacheDataStore delegate = init0(false);

            return delegate.mvccApplyHistoryIfAbsent(cctx, key, hist);
        }

        /** {@inheritDoc} */
        @Override public boolean mvccUpdateRowWithPreloadInfo(
            GridCacheContext cctx,
            KeyCacheObject key,
            @Nullable CacheObject val,
            GridCacheVersion ver,
            long expireTime,
            MvccVersion mvccVer,
            MvccVersion newMvccVer,
            byte mvccTxState,
            byte newMvccTxState) throws IgniteCheckedException {

            CacheDataStore delegate = init0(false);

            return delegate.mvccUpdateRowWithPreloadInfo(cctx,
                key,
                val,
                ver,
                expireTime,
                mvccVer,
                newMvccVer,
                mvccTxState,
                newMvccTxState);
        }

        /** {@inheritDoc} */
        @Override public MvccUpdateResult mvccUpdate(
            GridCacheContext cctx,
            KeyCacheObject key,
            CacheObject val,
            GridCacheVersion ver,
            long expireTime,
            MvccSnapshot mvccVer,
            CacheEntryPredicate filter,
            EntryProcessor entryProc,
            Object[] invokeArgs,
            boolean primary,
            boolean needHistory,
            boolean noCreate,
            boolean needOldVal,
            boolean retVal,
            boolean keepBinary) throws IgniteCheckedException {
            CacheDataStore delegate = init0(false);

            return delegate.mvccUpdate(cctx, key, val, ver, expireTime, mvccVer, filter, entryProc, invokeArgs, primary,
                needHistory, noCreate, needOldVal, retVal, keepBinary);
        }

        /** {@inheritDoc} */
        @Override public MvccUpdateResult mvccRemove(
            GridCacheContext cctx,
            KeyCacheObject key,
            MvccSnapshot mvccVer,
            CacheEntryPredicate filter,
            boolean primary,
            boolean needHistory,
            boolean needOldVal,
            boolean retVal) throws IgniteCheckedException {
            CacheDataStore delegate = init0(false);

            return delegate.mvccRemove(cctx, key, mvccVer,filter, primary, needHistory, needOldVal, retVal);
        }

        /** {@inheritDoc} */
        @Override public MvccUpdateResult mvccLock(
            GridCacheContext cctx,
            KeyCacheObject key,
            MvccSnapshot mvccSnapshot) throws IgniteCheckedException {
            CacheDataStore delegate = init0(false);

            return delegate.mvccLock(cctx, key, mvccSnapshot);
        }

        /** {@inheritDoc} */
        @Override public void mvccRemoveAll(GridCacheContext cctx, KeyCacheObject key) throws IgniteCheckedException {
            CacheDataStore delegate = init0(false);

            delegate.mvccRemoveAll(cctx, key);
        }

        /** {@inheritDoc} */
        @Override public void mvccApplyUpdate(GridCacheContext cctx, KeyCacheObject key, CacheObject val, GridCacheVersion ver,
            long expireTime, MvccVersion mvccVer) throws IgniteCheckedException {
            CacheDataStore delegate = init0(false);

            delegate.mvccApplyUpdate(cctx, key, val, ver, expireTime, mvccVer);
        }

        /** {@inheritDoc} */
        @Override public CacheDataRow createRow(
            GridCacheContext cctx,
            KeyCacheObject key,
            CacheObject val,
            GridCacheVersion ver,
            long expireTime,
            @Nullable CacheDataRow oldRow) throws IgniteCheckedException {
            assert ctx.database().checkpointLockIsHeldByThread();

            CacheDataStore delegate = init0(false);

            return delegate.createRow(cctx, key, val, ver, expireTime, oldRow);
        }

        /** {@inheritDoc} */
        @Override public void insertRows(Collection<DataRowCacheAware> rows,
            IgnitePredicateX<CacheDataRow> initPred) throws IgniteCheckedException {
            CacheDataStore delegate = init0(false);

            delegate.insertRows(rows, initPred);
        }

        /** {@inheritDoc} */
        @Override public int cleanup(GridCacheContext cctx,
            @Nullable List<MvccLinkAwareSearchRow> cleanupRows) throws IgniteCheckedException {
            CacheDataStore delegate = init0(false);

            return delegate.cleanup(cctx, cleanupRows);
        }

        /** {@inheritDoc} */
        @Override public void updateTxState(GridCacheContext cctx, CacheSearchRow row) throws IgniteCheckedException {
            CacheDataStore delegate = init0(false);

            delegate.updateTxState(cctx, row);
        }

        /** {@inheritDoc} */
        @Override public void invoke(GridCacheContext cctx, KeyCacheObject key, OffheapInvokeClosure c)
            throws IgniteCheckedException {
            assert ctx.database().checkpointLockIsHeldByThread();

            CacheDataStore delegate = init0(false);

            delegate.invoke(cctx, key, c);
        }

        /** {@inheritDoc} */
        @Override public void remove(GridCacheContext cctx, KeyCacheObject key, int partId)
            throws IgniteCheckedException {
            assert ctx.database().checkpointLockIsHeldByThread();

            CacheDataStore delegate = init0(false);

            delegate.remove(cctx, key, partId);
        }

        /** {@inheritDoc} */
        @Override public CacheDataRow find(GridCacheContext cctx, KeyCacheObject key) throws IgniteCheckedException {
            CacheDataStore delegate = init0(true);

            if (delegate != null)
                return delegate.find(cctx, key);

            return null;
        }

        /** {@inheritDoc} */
        @Override public CacheDataRow mvccFind(GridCacheContext cctx, KeyCacheObject key, MvccSnapshot snapshot)
            throws IgniteCheckedException {
            CacheDataStore delegate = init0(true);

            if (delegate != null)
                return delegate.mvccFind(cctx, key, snapshot);

            return null;
        }

        /** {@inheritDoc} */
        @Override public List<IgniteBiTuple<Object, MvccVersion>> mvccFindAllVersions(GridCacheContext cctx, KeyCacheObject key)
            throws IgniteCheckedException {
            CacheDataStore delegate = init0(true);

            if (delegate != null)
                return delegate.mvccFindAllVersions(cctx, key);

            return Collections.emptyList();
        }

        /** {@inheritDoc} */
        @Override public GridCursor<CacheDataRow> mvccAllVersionsCursor(GridCacheContext cctx,
            KeyCacheObject key, Object x) throws IgniteCheckedException {
            CacheDataStore delegate = init0(true);

            if (delegate != null)
                return delegate.mvccAllVersionsCursor(cctx, key, x);

            return EMPTY_CURSOR;
        }


        /** {@inheritDoc} */
        @Override public GridCursor<? extends CacheDataRow> cursor() throws IgniteCheckedException {
            CacheDataStore delegate = init0(true);

            if (delegate != null)
                return delegate.cursor();

            return EMPTY_CURSOR;
        }

        /** {@inheritDoc} */
        @Override public GridCursor<? extends CacheDataRow> cursor(Object x) throws IgniteCheckedException {
            CacheDataStore delegate = init0(true);

            if (delegate != null)
                return delegate.cursor(x);

            return EMPTY_CURSOR;
        }

        /** {@inheritDoc} */
        @Override public GridCursor<? extends CacheDataRow> cursor(MvccSnapshot mvccSnapshot)
            throws IgniteCheckedException {
            CacheDataStore delegate = init0(true);

            if (delegate != null)
                return delegate.cursor(mvccSnapshot);

            return EMPTY_CURSOR;
        }

        /** {@inheritDoc} */
        @Override public GridCursor<? extends CacheDataRow> cursor(
            int cacheId,
            KeyCacheObject lower,
            KeyCacheObject upper) throws IgniteCheckedException {
            CacheDataStore delegate = init0(true);

            if (delegate != null)
                return delegate.cursor(cacheId, lower, upper);

            return EMPTY_CURSOR;
        }

        /** {@inheritDoc} */
        @Override public GridCursor<? extends CacheDataRow> cursor(int cacheId,
            KeyCacheObject lower,
            KeyCacheObject upper,
            Object x)
            throws IgniteCheckedException {
            CacheDataStore delegate = init0(true);

            if (delegate != null)
                return delegate.cursor(cacheId, lower, upper, x);

            return EMPTY_CURSOR;
        }

        /** {@inheritDoc} */
        @Override public GridCursor<? extends CacheDataRow> cursor(int cacheId,
            KeyCacheObject lower,
            KeyCacheObject upper,
            Object x,
            MvccSnapshot mvccSnapshot)
            throws IgniteCheckedException {
            CacheDataStore delegate = init0(true);

            if (delegate != null)
                return delegate.cursor(cacheId, lower, upper, x, mvccSnapshot);

            return EMPTY_CURSOR;
        }

        /** {@inheritDoc} */
        @Override public void destroy() throws IgniteCheckedException {
            // No need to destroy delegate.
        }

        /** {@inheritDoc} */
        @Override public void markDestroyed() throws IgniteCheckedException {
            CacheDataStore delegate = init0(true);

            if (delegate != null)
                delegate.markDestroyed();
        }

        /** {@inheritDoc} */
        @Override public GridCursor<? extends CacheDataRow> cursor(int cacheId) throws IgniteCheckedException {
            CacheDataStore delegate = init0(true);

            if (delegate != null)
                return delegate.cursor(cacheId);

            return EMPTY_CURSOR;
        }

        /** {@inheritDoc} */
        @Override public GridCursor<? extends CacheDataRow> cursor(int cacheId,
            MvccSnapshot mvccSnapshot) throws IgniteCheckedException {
            CacheDataStore delegate = init0(true);

            if (delegate != null)
                return delegate.cursor(cacheId, mvccSnapshot);

            return EMPTY_CURSOR;
        }

        /** {@inheritDoc} */
        @Override public void clear(int cacheId) throws IgniteCheckedException {
            assert ctx.database().checkpointLockIsHeldByThread();

            CacheDataStore delegate0 = init0(true);

            if (delegate0 == null)
                return;

            // Clear persistent pendingTree
            if (pendingTree != null) {
                PendingRow row = new PendingRow(cacheId);

                GridCursor<PendingRow> cursor = pendingTree.find(row, row, PendingEntriesTree.WITHOUT_KEY);

                while (cursor.next()) {
                    PendingRow row0 = cursor.get();

                    assert row0.link != 0 : row;

                    boolean res = pendingTree.removex(row0);

                    assert res;
                }
            }

            delegate0.clear(cacheId);
        }

        /**
         * Gets the number of entries pending expire.
         *
         * @return Number of pending entries.
         * @throws IgniteCheckedException If failed to get number of pending entries.
         */
        public long expiredSize() throws IgniteCheckedException {
            CacheDataStore delegate0 = init0(true);

            return delegate0 == null ? 0 : pendingTree.size();
        }

        /**
         * Try to remove expired entries from data store.
         *
         * @param cctx Cache context.
         * @param c Expiry closure that should be applied to expired entry. See {@link GridCacheTtlManager} for details.
         * @param amount Limit of processed entries by single call, {@code -1} for no limit.
         * @return cleared entries count.
         * @throws IgniteCheckedException If failed.
         */
        public int purgeExpired(
            GridCacheContext cctx,
            IgniteInClosure2X<GridCacheEntryEx, GridCacheVersion> c,
            int amount
        ) throws IgniteCheckedException {
            CacheDataStore delegate0 = init0(true);

            long nowNanos = System.nanoTime();

            if (delegate0 == null || (cctx.cacheId() == lastThrottledCacheId && nextStoreCleanTimeNanos - nowNanos > 0))
                return 0;

            assert pendingTree != null : "Partition data store was not initialized.";

            int cleared = purgeExpiredInternal(cctx, c, amount);

            // Throttle if there is nothing to clean anymore.
            if (cleared < amount) {
                lastThrottledCacheId = cctx.cacheId();

                nextStoreCleanTimeNanos = nowNanos + U.millisToNanos(unwindThrottlingTimeout);
            }

            return cleared;
        }

        /**
         * Removes expired entries from data store.
         *
         * @param cctx Cache context.
         * @param c Expiry closure that should be applied to expired entry. See {@link GridCacheTtlManager} for details.
         * @param amount Limit of processed entries by single call, {@code -1} for no limit.
         * @return cleared entries count.
         * @throws IgniteCheckedException If failed.
         */
        private int purgeExpiredInternal(
            GridCacheContext cctx,
            IgniteInClosure2X<GridCacheEntryEx, GridCacheVersion> c,
            int amount
        ) throws IgniteCheckedException {
            GridDhtLocalPartition part = null;

            if (!grp.isLocal()) {
                part = cctx.topology().localPartition(partId, AffinityTopologyVersion.NONE, false, false);

                // Skip non-owned partitions.
                if (part == null || part.state() != OWNING)
                    return 0;
            }

            cctx.shared().database().checkpointReadLock();

            try {
                if (part != null && !part.reserve())
                    return 0;

                try {
                    if (part != null && part.state() != OWNING)
                        return 0;

                    long now = U.currentTimeMillis();

                    GridCursor<PendingRow> cur;

                    if (grp.sharedGroup())
                        cur = pendingTree.find(new PendingRow(cctx.cacheId()), new PendingRow(cctx.cacheId(), now, 0));
                    else
                        cur = pendingTree.find(null, new PendingRow(CU.UNDEFINED_CACHE_ID, now, 0));

                    if (!cur.next())
                        return 0;

                    GridCacheVersion obsoleteVer = null;

                    int cleared = 0;

                    do {
                        PendingRow row = cur.get();

                        if (amount != -1 && cleared > amount)
                            return cleared;

                        assert row.key != null && row.link != 0 && row.expireTime != 0 : row;

                        row.key.partition(partId);

                        if (pendingTree.removex(row)) {
                            if (obsoleteVer == null)
                                obsoleteVer = cctx.cache().nextVersion();

                            GridCacheEntryEx e1 = cctx.cache().entryEx(row.key);

                            if (e1 != null)
                                c.apply(e1, obsoleteVer);
                        }

                        cleared++;
                    }
                    while (cur.next());

                    return cleared;
                }
                finally {
                    if (part != null)
                        part.release();
                }
            }
            finally {
                cctx.shared().database().checkpointReadUnlock();
            }
        }

        /** {@inheritDoc} */
        @Override public PendingEntriesTree pendingTree() {
            try {
                CacheDataStore delegate0 = init0(true);

                return delegate0 == null ? null : pendingTree;
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
        }

        /** {@inheritDoc} */
        @Override public void preload() throws IgniteCheckedException {
            CacheDataStore delegate0 = init0(true);

            if (delegate0 != null)
                delegate0.preload();
        }

        /** {@inheritDoc} */
        @Override public void resetUpdateCounter() {
            try {
                CacheDataStore delegate0 = init0(true);

                if (delegate0 == null)
                    return;

                delegate0.resetUpdateCounter();
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
        }

        /** {@inheritDoc} */
        @Override public void resetInitialUpdateCounter() {
            try {
                CacheDataStore delegate0 = init0(true);

                if (delegate0 == null)
                    return;

                delegate0.resetInitialUpdateCounter();
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
        }

        @Override public PartitionMetaStorage partStorage() {
            return partStorage;
        }
    }

    /**
     *
     */
    public static final GridCursor<CacheDataRow> EMPTY_CURSOR = new GridCursor<CacheDataRow>() {
        /** {@inheritDoc} */
        @Override public boolean next() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public CacheDataRow get() {
            return null;
        }
    };
}
