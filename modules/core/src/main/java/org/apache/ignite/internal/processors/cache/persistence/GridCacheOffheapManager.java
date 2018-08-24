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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.failure.FailureType;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.PageSupport;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.DataEntry;
import org.apache.ignite.internal.pagemem.wal.record.DataRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.MetaPageInitRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.MetaPageUpdateNextSnapshotId;
import org.apache.ignite.internal.pagemem.wal.record.delta.MetaPageUpdatePartitionDataRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.PartitionDestroyRecord;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.IgniteCacheOffheapManagerImpl;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.CachePartitionPartialCountersMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.IgniteHistoricalIterator;
import org.apache.ignite.internal.processors.cache.persistence.freelist.CacheFreeListImpl;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryEx;
import org.apache.ignite.internal.processors.cache.persistence.partstate.GroupPartitionId;
import org.apache.ignite.internal.processors.cache.persistence.partstate.PagesAllocationRange;
import org.apache.ignite.internal.processors.cache.persistence.partstate.PartitionAllocationMap;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageMetaIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PagePartitionCountersIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PagePartitionMetaIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.reuse.ReuseList;
import org.apache.ignite.internal.processors.cache.persistence.tree.reuse.ReuseListImpl;
import org.apache.ignite.internal.processors.cache.persistence.tree.util.PageHandler;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWALPointer;
import org.apache.ignite.internal.processors.cache.tree.CacheDataRowStore;
import org.apache.ignite.internal.processors.cache.tree.CacheDataTree;
import org.apache.ignite.internal.processors.cache.tree.PendingEntriesTree;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.query.GridQueryRowCacheCleaner;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionState.MOVING;
import static org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionState.OWNING;
import static org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionState.RENTING;

/**
 * Used when persistence enabled.
 */
public class GridCacheOffheapManager extends IgniteCacheOffheapManagerImpl implements DbCheckpointListener {
    /** */
    private IndexStorage indexStorage;

    /** */
    private ReuseListImpl reuseList;

    /** {@inheritDoc} */
    @Override protected void initDataStructures() throws IgniteCheckedException {
        Metas metas = getOrAllocateCacheMetas();

        RootPage reuseListRoot = metas.reuseListRoot;

        reuseList = new ReuseListImpl(grp.groupId(),
            grp.cacheOrGroupName(),
            grp.dataRegion().pageMemory(),
            ctx.wal(),
            reuseListRoot.pageId().pageId(),
            reuseListRoot.isAllocated());

        RootPage metastoreRoot = metas.treeRoot;

        indexStorage = new IndexStorageImpl(grp.dataRegion().pageMemory(),
            ctx.wal(),
            globalRemoveId(),
            grp.groupId(),
            PageIdAllocator.INDEX_PARTITION,
            PageIdAllocator.FLAG_IDX,
            reuseList,
            metastoreRoot.pageId().pageId(),
            metastoreRoot.isAllocated(),
            ctx.kernalContext().failure());

        ((GridCacheDatabaseSharedManager)ctx.database()).addCheckpointListener(this);
    }

    /** {@inheritDoc} */
    @Override public void onCacheStarted(GridCacheContext cctx) throws IgniteCheckedException {
        if (cctx.affinityNode() && cctx.ttl().eagerTtlEnabled() && pendingEntries == null) {
            ctx.database().checkpointReadLock();

            try {
                final String name = "PendingEntries";

                RootPage pendingRootPage = indexStorage.getOrAllocateForTree(name);

                pendingEntries = new PendingEntriesTree(
                    grp,
                    name,
                    grp.dataRegion().pageMemory(),
                    pendingRootPage.pageId().pageId(),
                    reuseList,
                    pendingRootPage.isAllocated()
                );
            }
            finally {
                ctx.database().checkpointReadUnlock();
            }
        }
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
        assert grp.dataRegion().pageMemory() instanceof PageMemoryEx;

        reuseList.saveMetadata();

        boolean metaWasUpdated = false;

        for (CacheDataStore store : partDataStores.values())
            metaWasUpdated |= saveStoreMetadata(store, ctx, !metaWasUpdated, false);
    }

    /**
     * @param store Store to save metadata.
     * @throws IgniteCheckedException If failed.
     */
    private boolean saveStoreMetadata(
        CacheDataStore store,
        Context ctx,
        boolean saveMeta,
        boolean beforeDestroy
    ) throws IgniteCheckedException {
        RowStore rowStore0 = store.rowStore();

        boolean needSnapshot = ctx != null && ctx.nextSnapshot() && ctx.needToSnapshot(grp.cacheOrGroupName());

        boolean wasSaveToMeta = false;

        if (rowStore0 != null) {
            CacheFreeListImpl freeList = (CacheFreeListImpl)rowStore0.freeList();

            freeList.saveMetadata();

            long updCntr = store.updateCounter();
            long size = store.fullSize();
            long rmvId = globalRemoveId().get();

            PageMemoryEx pageMem = (PageMemoryEx)grp.dataRegion().pageMemory();
            IgniteWriteAheadLogManager wal = this.ctx.wal();

            if (size > 0 || updCntr > 0) {
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
                        return false;
                }

                int grpId = grp.groupId();
                long partMetaId = pageMem.partitionMetaPageId(grpId, store.partId());
                long partMetaPage = pageMem.acquirePage(grpId, partMetaId);

                try {
                    long partMetaPageAddr = pageMem.writeLock(grpId, partMetaId, partMetaPage);

                    if (partMetaPageAddr == 0L) {
                        U.warn(log, "Failed to acquire write lock for meta page [metaPage=" + partMetaPage +
                            ", saveMeta=" + saveMeta + ", beforeDestroy=" + beforeDestroy + ", size=" + size +
                            ", updCntr=" + updCntr + ", state=" + state + ']');

                        return false;
                    }

                    boolean changed = false;

                    try {
                        PagePartitionMetaIO io = PageIO.getPageIO(partMetaPageAddr);

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

                            io.setCandidatePageCount(partMetaPageAddr, size == 0 ? 0: pageCnt);

                            if (saveMeta) {
                                saveMeta(ctx);

                                wasSaveToMeta = true;
                            }

                            if (state == OWNING) {
                                assert part != null;

                                if(!addPartition(
                                    part,
                                    ctx.partitionStatMap(),
                                    partMetaPageAddr,
                                    io,
                                    grpId,
                                    store.partId(),
                                    this.ctx.pageStore().pages(grpId, store.partId()),
                                    store.fullSize()
                                ))
                                    U.warn(log,"Partition was concurrently evicted grpId=" +  grpId +
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

                        if (PageHandler.isWalDeltaRecordNeeded(pageMem, grpId, partMetaId, partMetaPage, wal, null))
                            wal.log(new MetaPageUpdatePartitionDataRecord(
                                grpId,
                                partMetaId,
                                updCntr,
                                rmvId,
                                (int)size, // TODO: Partition size may be long
                                cntrsPageId,
                                state == null ? -1 : (byte)state.ordinal(),
                                pageCnt
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
                tryAddEmptyPartitionToSnapshot(store, ctx);;
        }
        else if (needSnapshot)
            tryAddEmptyPartitionToSnapshot(store, ctx);

        return wasSaveToMeta;
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

        while (true){
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

                int pageSize = pageMem.pageSize();

                assert curAddr != 0;

                try {
                    PagePartitionCountersIO partCntrIo;

                    if (init) {
                        partCntrIo = PagePartitionCountersIO.VERSIONS.latest();

                        partCntrIo.initNewPage(curAddr, curId, pageSize);
                    }
                    else
                        partCntrIo = PageIO.getPageIO(curAddr);

                    written += partCntrIo.writeCacheSizes(pageSize, curAddr, data, written);

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
    private void saveMeta(Context ctx) throws IgniteCheckedException {
        int grpId = grp.groupId();
        PageMemoryEx pageMem = (PageMemoryEx)grp.dataRegion().pageMemory();
        IgniteWriteAheadLogManager wal = this.ctx.wal();

        long metaPageId = pageMem.metaPageId(grpId);
        long metaPage = pageMem.acquirePage(grpId, metaPageId);

        try {
            long metaPageAddr = pageMem.writeLock(grpId, metaPageId, metaPage);

            try {
                PageMetaIO metaIo = PageMetaIO.getPageIO(metaPageAddr);

                long nextSnapshotTag = metaIo.getNextSnapshotTag(metaPageAddr);

                metaIo.setNextSnapshotTag(metaPageAddr, nextSnapshotTag + 1);

                if (log != null && log.isDebugEnabled())
                    log.debug("Save next snapshot before checkpoint start for grId = " + grpId
                        + ", nextSnapshotTag = " + nextSnapshotTag);

                if (PageHandler.isWalDeltaRecordNeeded(pageMem, grpId, metaPageId,
                    metaPage, wal, null))
                    wal.log(new MetaPageUpdateNextSnapshotId(grpId, metaPageId,
                        nextSnapshotTag + 1));

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

            if(!reserved)
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
            saveStoreMetadata(store, null, false, true);
        }
        finally {
            ctx.database().checkpointReadUnlock();
        }

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

    /**
     * Destroys given {@code store} and creates new with the same update counters as in given.
     *
     * @param store Store to destroy.
     * @return New cache data store.
     * @throws IgniteCheckedException If failed.
     */
    public CacheDataStore recreateCacheDataStore(CacheDataStore store) throws IgniteCheckedException {
        long updCounter = store.updateCounter();
        long initUpdCounter = store.initialUpdateCounter();

        int p = store.partId();

        PageMemoryEx pageMemory = (PageMemoryEx)grp.dataRegion().pageMemory();

        int tag = pageMemory.invalidate(grp.groupId(), p);

        ctx.pageStore().onPartitionDestroyed(grp.groupId(), p, tag);

        CacheDataStore store0;

        partStoreLock.lock(p);

        try {
            store0 = createCacheDataStore0(p);
            store0.updateCounter(updCounter);
            store0.updateInitialCounter(initUpdCounter);

            partDataStores.put(p, store0);
        }
        finally {
            partStoreLock.unlock(p);
        }

        return store0;
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
    @Override public void onPartitionInitialCounterUpdated(int part, long cntr) {
        CacheDataStore store = partDataStores.get(part);

        assert store != null;

        long oldCnt = store.initialUpdateCounter();

        if (oldCnt < cntr)
            store.updateInitialCounter(cntr);
    }

    /** {@inheritDoc} */
    @Override public long lastUpdatedPartitionCounter(int part) {
        return partDataStores.get(part).updateCounter();
    }

    /** {@inheritDoc} */
    @Override public RootPage rootPageForIndex(int cacheId, String idxName) throws IgniteCheckedException {
        if (grp.sharedGroup())
            idxName = Integer.toString(cacheId) + "_" + idxName;

        return indexStorage.getOrAllocateForTree(idxName);
    }

    /** {@inheritDoc} */
    @Override public void dropRootPageForIndex(int cacheId, String idxName) throws IgniteCheckedException {
        if (grp.sharedGroup())
            idxName = Integer.toString(cacheId) + "_" + idxName;

        indexStorage.dropRootPage(idxName);
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

                    pageIO.initNewPage(pageAddr, metaId, pageMem.pageSize());

                    metastoreRoot = pageMem.allocatePage(grpId, PageIdAllocator.INDEX_PARTITION, PageMemory.FLAG_IDX);
                    reuseListRoot = pageMem.allocatePage(grpId, PageIdAllocator.INDEX_PARTITION, PageMemory.FLAG_IDX);

                    pageIO.setTreeRoot(pageAddr, metastoreRoot);
                    pageIO.setReuseListRoot(pageAddr, reuseListRoot);

                    if (PageHandler.isWalDeltaRecordNeeded(pageMem, grpId, metaId, metaPage, wal, null))
                        wal.log(new MetaPageInitRecord(
                            grpId,
                            metaId,
                            pageIO.getType(),
                            pageIO.getVersion(),
                            metastoreRoot,
                            reuseListRoot
                        ));

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
                    new RootPage(new FullPageId(reuseListRoot, grpId), allocated));
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
    @Override @Nullable protected WALHistoricalIterator historicalIterator(
        CachePartitionPartialCountersMap partCntrs, Set<Integer> missing) throws IgniteCheckedException {
        if (partCntrs == null || partCntrs.isEmpty())
            return null;

        GridCacheDatabaseSharedManager database = (GridCacheDatabaseSharedManager)grp.shared().database();

        FileWALPointer minPtr = null;

        for (int i = 0; i < partCntrs.size(); i++) {
            int p = partCntrs.partitionAt(i);
            long initCntr = partCntrs.initialUpdateCounterAt(i);

            FileWALPointer startPtr = (FileWALPointer)database.checkpointHistory().searchPartitionCounter(
                grp.groupId(), p, initCntr);

            if (startPtr == null)
                throw new IgniteCheckedException("Could not find start pointer for partition [part=" + p + ", partCntrSince=" + initCntr + "]");

            if (minPtr == null || startPtr.compareTo(minPtr) < 0)
                minPtr = startPtr;
        }

        WALIterator it = grp.shared().wal().replay(minPtr);

        WALHistoricalIterator iterator = new WALHistoricalIterator(grp, partCntrs, it);

        // Add historical partitions which are unabled to reserve to missing set.
        missing.addAll(iterator.missingParts);

        return iterator;
    }

    /**
     * Calculates free space of all partition data stores - number of bytes available for use in allocated pages.
     *
     * @return Tuple (numenator, denominator).
     */
    long freeSpace() {
        long freeSpace = 0;

        for (CacheDataStore store : partDataStores.values()) {
            assert store instanceof GridCacheDataStore;

            CacheFreeListImpl freeList = ((GridCacheDataStore)store).freeList;

            if (freeList == null)
                continue;

            freeSpace += freeList.freeSpace();
        }

        return freeSpace;
    }

    /**
     *
     */
    private static class WALHistoricalIterator implements IgniteHistoricalIterator {
        /** */
        private static final long serialVersionUID = 0L;

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

        /** Flag indicates that partition belongs to current {@link #next} is finished and no longer needs to rebalance. */
        private boolean reachedPartitionEnd;

        /**
         * @param grp Cache context.
         * @param walIt WAL iterator.
         */
        private WALHistoricalIterator(CacheGroupContext grp, CachePartitionPartialCountersMap partMap, WALIterator walIt) {
            this.grp = grp;
            this.partMap = partMap;
            this.walIt = walIt;

            cacheIds = grp.cacheIds();

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

            if (reachedPartitionEnd) {
                doneParts.add(next.partitionId());

                reachedPartitionEnd = false;
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
            next = null;

            while (true) {
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
                                if (entry.partitionCounter() == to)
                                    reachedPartitionEnd = true;

                                next = entry;

                                return;
                            }
                        }
                    }
                }

                entryIt = null;

                while (walIt.hasNext()) {
                    IgniteBiTuple<WALPointer, WALRecord> rec = walIt.next();

                    if (rec.get2() instanceof DataRecord) {
                        DataRecord data = (DataRecord)rec.get2();

                        entryIt = data.writeEntries().iterator();
                        // Move on to the next valid data entry.

                        break;
                    }
                }

                if (entryIt == null)
                    return;
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

        /**
         * @param treeRoot Metadata storage root.
         * @param reuseListRoot Reuse list root.
         */
        Metas(RootPage treeRoot, RootPage reuseListRoot) {
            this.treeRoot = treeRoot;
            this.reuseListRoot = reuseListRoot;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(Metas.class, this);
        }
    }

    /**
     *
     */
    private class GridCacheDataStore implements CacheDataStore {
        /** */
        private final int partId;

        /** */
        private String name;

        /** */
        private volatile CacheFreeListImpl freeList;

        /** */
        private volatile CacheDataStore delegate;

        /** */
        private final boolean exists;

        /** */
        private final AtomicBoolean init = new AtomicBoolean();

        /** */
        private final CountDownLatch latch = new CountDownLatch(1);

        /**
         * @param partId Partition.
         * @param exists {@code True} if store for this index exists.
         */
        private GridCacheDataStore(int partId, boolean exists) {
            this.partId = partId;
            this.exists = exists;

            name = treeName(partId);
        }

        /**
         * @return Store delegate.
         * @throws IgniteCheckedException If failed.
         */
        private CacheDataStore init0(boolean checkExists) throws IgniteCheckedException {
            CacheDataStore delegate0 = delegate;

            if (delegate0 != null)
                return delegate0;

            if (checkExists) {
                if (!exists)
                    return null;
            }

            IgniteCacheDatabaseSharedManager dbMgr = ctx.database();

                dbMgr.checkpointReadLock();if (init.compareAndSet(false, true)) {
                try {
                    Metas metas = getOrAllocatePartitionMetas();

                    RootPage reuseRoot = metas.reuseListRoot;

                    freeList = new CacheFreeListImpl(
                        grp.groupId(),
                        grp.cacheOrGroupName() + "-" + partId,
                        grp.dataRegion().memoryMetrics(),
                        grp.dataRegion(),
                        null,
                        ctx.wal(),
                        reuseRoot.pageId().pageId(),
                        reuseRoot.isAllocated()) {
                        @Override protected long allocatePageNoReuse() throws IgniteCheckedException {
                            assert grp.shared().database().checkpointLockIsHeldByThread();

                            return pageMem.allocatePage(grpId, partId, PageIdAllocator.FLAG_DATA);
                        }
                    };

                    CacheDataRowStore rowStore = new CacheDataRowStore(grp, freeList, partId);

                    RootPage treeRoot = metas.treeRoot;

                    CacheDataTree dataTree = new CacheDataTree(
                        grp,
                        name,
                        freeList,
                        rowStore,
                        treeRoot.pageId().pageId(),
                        treeRoot.isAllocated()) {
                        @Override protected long allocatePageNoReuse() throws IgniteCheckedException {
                            assert grp.shared().database().checkpointLockIsHeldByThread();

                            return pageMem.allocatePage(grpId, partId, PageIdAllocator.FLAG_DATA);
                        }
                    };

                    PageMemoryEx pageMem = (PageMemoryEx)grp.dataRegion().pageMemory();

                    delegate0 = new CacheDataStoreImpl(partId, name, rowStore, dataTree);

                    int grpId = grp.groupId();
                    long partMetaId = pageMem.partitionMetaPageId(grpId, partId);
                    long partMetaPage = pageMem.acquirePage(grpId, partMetaId);

                    try {
                        long pageAddr = pageMem.readLock(grpId, partMetaId, partMetaPage);

                        try {
                            if (PageIO.getType(pageAddr) != 0) {
                                PagePartitionMetaIO io = PagePartitionMetaIO.VERSIONS.latest();

                                Map<Integer, Long> cacheSizes = null;

                                if (grp.sharedGroup())
                                    cacheSizes = readSharedGroupCacheSizes(pageMem, grpId, io.getCountersPageId(pageAddr));

                                delegate0.init(io.getSize(pageAddr), io.getUpdateCounter(pageAddr), cacheSizes);

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
                dbMgr.checkpointReadUnlock();

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
            long partMetaPage = pageMem.acquirePage(grpId, partMetaId);
            try {
                boolean allocated = false;
                long pageAddr = pageMem.writeLock(grpId, partMetaId, partMetaPage);

                try {
                    long treeRoot, reuseListRoot;

                    // Initialize new page.
                    if (PageIO.getType(pageAddr) != PageIO.T_PART_META) {
                        PagePartitionMetaIO io = PagePartitionMetaIO.VERSIONS.latest();

                        io.initNewPage(pageAddr, partMetaId, pageMem.pageSize());

                        treeRoot = pageMem.allocatePage(grpId, partId, PageMemory.FLAG_DATA);
                        reuseListRoot = pageMem.allocatePage(grpId, partId, PageMemory.FLAG_DATA);

                        assert PageIdUtils.flag(treeRoot) == PageMemory.FLAG_DATA;
                        assert PageIdUtils.flag(reuseListRoot) == PageMemory.FLAG_DATA;

                        io.setTreeRoot(pageAddr, treeRoot);
                        io.setReuseListRoot(pageAddr, reuseListRoot);

                        if (PageHandler.isWalDeltaRecordNeeded(pageMem, grpId, partMetaId, partMetaPage, wal, null))
                            wal.log(new MetaPageInitRecord(
                                grpId,
                                partMetaId,
                                io.getType(),
                                io.getVersion(),
                                treeRoot,
                                reuseListRoot
                            ));

                        allocated = true;
                    }
                    else {
                        PagePartitionMetaIO io = PageIO.getPageIO(pageAddr);

                        treeRoot = io.getTreeRoot(pageAddr);
                        reuseListRoot = io.getReuseListRoot(pageAddr);

                        if (PageIdUtils.flag(treeRoot) != PageMemory.FLAG_DATA)
                            throw new StorageException("Wrong tree root page id flag: treeRoot="
                                + U.hexLong(treeRoot) + ", part=" + partId + ", grpId=" + grpId);

                        if (PageIdUtils.flag(reuseListRoot) != PageMemory.FLAG_DATA)
                            throw new StorageException("Wrong reuse list root page id flag: reuseListRoot="
                                + U.hexLong(reuseListRoot) + ", part=" + partId + ", grpId=" + grpId);
                    }

                    return new Metas(
                        new RootPage(new FullPageId(treeRoot, grpId), allocated),
                        new RootPage(new FullPageId(reuseListRoot, grpId), allocated));
                }
                finally {
                    pageMem.writeUnlock(grpId, partMetaId, partMetaPage, null, allocated);
                }
            }
            finally {
                pageMem.releasePage(grpId, partMetaId, partMetaPage);
            }
        }

        /** {@inheritDoc} */
        @Override public int partId() {
            return partId;
        }

        /** {@inheritDoc} */
        @Override public String name() {
            return name;
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
        @Override public void init(long size, long updCntr, @Nullable Map<Integer, Long> cacheSizes) {
            throw new IllegalStateException("Should be never called.");
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
        @Override public long nextUpdateCounter() {
            try {
                CacheDataStore delegate0 = init0(false);

                return delegate0 == null ? 0 : delegate0.nextUpdateCounter();
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
        @Override public void updateInitialCounter(long cntr) {
            try {
                CacheDataStore delegate0 = init0(true);

                if (delegate0 != null)
                    delegate0.updateInitialCounter(cntr);
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
            CacheDataStore delegate = init0(false);

            delegate.update(cctx, key, val, ver, expireTime, oldRow);
        }

        /** {@inheritDoc} */
        @Override public CacheDataRow createRow(
            GridCacheContext cctx,
            KeyCacheObject key,
            CacheObject val,
            GridCacheVersion ver,
            long expireTime,
            @Nullable CacheDataRow oldRow) throws IgniteCheckedException {
            CacheDataStore delegate = init0(false);

            return delegate.createRow(cctx, key, val, ver, expireTime, oldRow);
        }

        /** {@inheritDoc} */
        @Override public void invoke(GridCacheContext cctx, KeyCacheObject key, OffheapInvokeClosure c)
            throws IgniteCheckedException {
            CacheDataStore delegate = init0(false);

            delegate.invoke(cctx, key, c);
        }

        /** {@inheritDoc} */
        @Override public void remove(GridCacheContext cctx, KeyCacheObject key, int partId)
            throws IgniteCheckedException {
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
        @Override public GridCursor<? extends CacheDataRow> cursor() throws IgniteCheckedException {
            CacheDataStore delegate = init0(true);

            if (delegate != null)
                return delegate.cursor();

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
        @Override public void destroy() throws IgniteCheckedException {
            // No need to destroy delegate.
        }

        /** {@inheritDoc} */
        @Override public GridCursor<? extends CacheDataRow> cursor(int cacheId) throws IgniteCheckedException {
            CacheDataStore delegate = init0(true);

            if (delegate != null)
                return delegate.cursor(cacheId);

            return EMPTY_CURSOR;
        }

        /** {@inheritDoc} */
        @Override public void clear(int cacheId) throws IgniteCheckedException {
            CacheDataStore delegate = init0(true);

            if (delegate != null)
                delegate.clear(cacheId);
        }
    }

    /**
     *
     */
    private static final GridCursor<CacheDataRow> EMPTY_CURSOR = new GridCursor<CacheDataRow>() {
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
