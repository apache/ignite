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

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.PageMemory;
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
import org.apache.ignite.internal.processors.cache.IgniteRebalanceIterator;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionMap;
import org.apache.ignite.internal.processors.cache.mvcc.MvccVersion;
import org.apache.ignite.internal.processors.cache.mvcc.MvccCounter;
import org.apache.ignite.internal.processors.cache.persistence.freelist.FreeListImpl;
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
import org.apache.ignite.internal.processors.cache.tree.CacheDataRowStore;
import org.apache.ignite.internal.processors.cache.tree.CacheDataTree;
import org.apache.ignite.internal.processors.cache.tree.PendingEntriesTree;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.GridLongList;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.Nullable;

/**
 * Used when persistence enabled.
 */
public class GridCacheOffheapManager extends IgniteCacheOffheapManagerImpl implements DbCheckpointListener {
    /** */
    private MetaStore metaStore;

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

        metaStore = new MetadataStorage(grp.dataRegion().pageMemory(),
            ctx.wal(),
            globalRemoveId(),
            grp.groupId(),
            PageIdAllocator.INDEX_PARTITION,
            PageIdAllocator.FLAG_IDX,
            reuseList,
            metastoreRoot.pageId().pageId(),
            metastoreRoot.isAllocated());

        ((GridCacheDatabaseSharedManager)ctx.database()).addCheckpointListener(this);
    }

    /** {@inheritDoc} */
    @Override public void onCacheStarted(GridCacheContext cctx) throws IgniteCheckedException {
        if (cctx.affinityNode() && cctx.ttl().eagerTtlEnabled() && pendingEntries == null) {
            ctx.database().checkpointReadLock();

            try {
                final String name = "PendingEntries";

                RootPage pendingRootPage = metaStore.getOrAllocateForTree(name);

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
    @Override protected CacheDataStore createCacheDataStore0(final int p)
        throws IgniteCheckedException {
        boolean exists = ctx.pageStore() != null && ctx.pageStore().exists(grp.groupId(), p);

        return new GridCacheDataStore(p, exists);
    }

    /** {@inheritDoc} */
    @Override public void onCheckpointBegin(Context ctx) throws IgniteCheckedException {
        assert grp.dataRegion().pageMemory() instanceof PageMemoryEx;

        reuseList.saveMetadata();

        boolean metaWasUpdated = false;

        for (CacheDataStore store : partDataStores.values()) {
            RowStore rowStore = store.rowStore();

            if (rowStore == null)
                continue;

            metaWasUpdated |= saveStoreMetadata(store, ctx, !metaWasUpdated, false);
        }
    }

    /**
     * @param store Store to save metadata.
     * @throws IgniteCheckedException If failed.
     */
    private boolean saveStoreMetadata(CacheDataStore store, Context ctx, boolean saveMeta,
        boolean beforeDestroy) throws IgniteCheckedException {
        RowStore rowStore0 = store.rowStore();

        boolean needSnapshot = ctx != null && ctx.nextSnapshot() && ctx.needToSnapshot(grp.cacheOrGroupName());

        boolean wasSaveToMeta = false;

        if (rowStore0 != null) {
            FreeListImpl freeList = (FreeListImpl)rowStore0.freeList();

            freeList.saveMetadata();

            long updCntr = store.updateCounter();
            int size = store.fullSize();
            long rmvId = globalRemoveId().get();

            PageMemoryEx pageMem = (PageMemoryEx)grp.dataRegion().pageMemory();
            IgniteWriteAheadLogManager wal = this.ctx.wal();

            if (size > 0 || updCntr > 0) {
                GridDhtPartitionState state = null;

                if (!grp.isLocal()) {
                    if (beforeDestroy)
                        state = GridDhtPartitionState.EVICTED;
                    else {
                        // localPartition will not acquire writeLock here because create=false.
                        GridDhtLocalPartition part = grp.topology().localPartition(store.partId(),
                            AffinityTopologyVersion.NONE, false, true);

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
                            cntrsPageId = io.getCountersPageId(partMetaPageAddr);

                            byte[] data = serializeCacheSizes(store.cacheSizes());

                            int items = data.length / 12;
                            int written = 0;
                            int pageSize = pageMem.pageSize();

                            boolean init = cntrsPageId == 0;

                            if (init && items > 0) {
                                cntrsPageId = pageMem.allocatePage(grpId, store.partId(), PageIdAllocator.FLAG_DATA);

                                io.setCountersPageId(partMetaPageAddr, cntrsPageId);

                                changed = true;
                            }

                            long nextId = cntrsPageId;

                            while (written != items) {
                                final long curId = nextId;
                                final long curPage = pageMem.acquirePage(grpId, curId);

                                try {
                                    final long curAddr = pageMem.writeLock(grpId, curId, curPage);

                                    assert curAddr != 0;

                                    try {
                                        PagePartitionCountersIO partMetaIo;

                                        if (init) {
                                            partMetaIo = PagePartitionCountersIO.VERSIONS.latest();

                                            partMetaIo.initNewPage(curAddr, curId, pageSize);
                                        }
                                        else
                                            partMetaIo = PageIO.getPageIO(curAddr);

                                        written += partMetaIo.writeCacheSizes(pageSize, curAddr, data, written);

                                        nextId = partMetaIo.getNextCountersPageId(curAddr);

                                        if(written != items && (init = nextId == 0)) {
                                            //allocate new counters page
                                            nextId = pageMem.allocatePage(grpId, store.partId(), PageIdAllocator.FLAG_DATA);
                                            partMetaIo.setNextCountersPageId(curAddr, nextId);
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
                        }
                        else
                            cntrsPageId = 0L;

                        int pageCnt;

                        if (needSnapshot) {
                            pageCnt = this.ctx.pageStore().pages(grpId, store.partId());
                            io.setCandidatePageCount(partMetaPageAddr, pageCnt);

                            if (saveMeta) {
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

                                        if (state == GridDhtPartitionState.OWNING)
                                            addPartition(ctx.partitionStatMap(), metaPageAddr, metaIo, grpId, PageIdAllocator.INDEX_PARTITION,
                                                    this.ctx.kernalContext().cache().context().pageStore().pages(grpId, PageIdAllocator.INDEX_PARTITION));
                                    }
                                    finally {
                                        pageMem.writeUnlock(grpId, metaPageId, metaPage, null, true);
                                    }
                                }
                                finally {
                                    pageMem.releasePage(grpId, metaPageId, metaPage);
                                }

                                wasSaveToMeta = true;
                            }

                            GridDhtPartitionMap partMap = grp.topology().localPartitionMap();

                            if (partMap.containsKey(store.partId()) &&
                                partMap.get(store.partId()) == GridDhtPartitionState.OWNING)
                                addPartition(ctx.partitionStatMap(), partMetaPageAddr, io, grpId, store.partId(),
                                    this.ctx.pageStore().pages(grpId, store.partId()));

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
                                size,
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
        }

        return wasSaveToMeta;
    }

    /**
     * @param cacheSizes Cache sizes.
     * @return Serialized cache sizes
     */
    private byte[] serializeCacheSizes(Map<Integer, Long> cacheSizes) {
        // Item size = 4 bytes (cache ID) + 8 bytes (cache size) = 12 bytes
        byte[] data = new byte[cacheSizes.size() * 12];
        long off = GridUnsafe.BYTE_ARR_OFF;

        for (Map.Entry<Integer, Long> entry : cacheSizes.entrySet()) {
            GridUnsafe.putInt(data, off, entry.getKey()); off += 4;
            GridUnsafe.putLong(data, off, entry.getValue()); off += 8;
        }

        return data;
    }

    /**
     * @param map Map to add values to.
     * @param metaPageAddr Meta page address
     * @param io Page Meta IO
     * @param cacheId Cache ID.
     * @param partId Partition ID. Or {@link PageIdAllocator#INDEX_PARTITION} for index partition
     * @param currAllocatedPageCnt total number of pages allocated for partition <code>[partition, cacheId]</code>
     */
    private static void addPartition(
        final PartitionAllocationMap map,
        final long metaPageAddr,
        final PageMetaIO io,
        final int cacheId,
        final int partId,
        final int currAllocatedPageCnt
    ) {
        if (currAllocatedPageCnt <= 1)
            return;

        assert PageIO.getPageId(metaPageAddr) != 0;

        int lastAllocatedPageCnt = io.getLastAllocatedPageCount(metaPageAddr);
        map.put(
            new GroupPartitionId(cacheId, partId),
            new PagesAllocationRange(lastAllocatedPageCnt, currAllocatedPageCnt));
    }

    /** {@inheritDoc} */
    @Override protected void destroyCacheDataStore0(CacheDataStore store) throws IgniteCheckedException {
        ctx.database().checkpointReadLock();

        try {
            int p = store.partId();

            saveStoreMetadata(store, null, false, true);

            PageMemoryEx pageMemory = (PageMemoryEx)grp.dataRegion().pageMemory();

            int tag = pageMemory.invalidate(grp.groupId(), p);

            ctx.wal().log(new PartitionDestroyRecord(grp.groupId(), p));

            ctx.pageStore().onPartitionDestroyed(grp.groupId(), p, tag);
        }
        finally {
            ctx.database().checkpointReadUnlock();
        }
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

        return metaStore.getOrAllocateForTree(idxName);
    }

    /** {@inheritDoc} */
    @Override public void dropRootPageForIndex(int cacheId, String idxName) throws IgniteCheckedException {
        if (grp.sharedGroup())
            idxName = Integer.toString(cacheId) + "_" + idxName;

        metaStore.dropRootPage(idxName);
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
    @Override public IgniteRebalanceIterator rebalanceIterator(int part, AffinityTopologyVersion topVer,
        Long partCntrSince) throws IgniteCheckedException {
        if (partCntrSince == null)
            return super.rebalanceIterator(part, topVer, partCntrSince);

        GridCacheDatabaseSharedManager database = (GridCacheDatabaseSharedManager)ctx.database();

        try {
            WALPointer startPtr = database.searchPartitionCounter(grp.groupId(), part, partCntrSince);

            if (startPtr == null) {
                assert false : "partCntr=" + partCntrSince + ", reservations=" + S.toString(Map.class, database.reservedForPreloading());

                return super.rebalanceIterator(part, topVer, partCntrSince);
            }

            WALIterator it = ctx.wal().replay(startPtr);

            return new RebalanceIteratorAdapter(grp, it, part);
        }
        catch (IgniteCheckedException e) {
            U.warn(log, "Failed to create WAL-based rebalance iterator (a full partition will transferred to a " +
                "remote node) [part=" + part + ", partCntrSince=" + partCntrSince + ", err=" + e + ']');

            return super.rebalanceIterator(part, topVer, partCntrSince);
        }
    }

    /**
     * Calculates fill factor of all partition data stores.
     *
     * @return Tuple (numenator, denominator).
     */
    T2<Long, Long> fillFactor() {
        long loadSize = 0;
        long totalSize = 0;

        for (CacheDataStore store : partDataStores.values()) {
            assert store instanceof GridCacheDataStore;

            FreeListImpl freeList = ((GridCacheDataStore)store).freeList;

            if (freeList == null)
                continue;

            T2<Long, Long> fillFactor = freeList.fillFactor();

            loadSize += fillFactor.get1();
            totalSize += fillFactor.get2();
        }

        return new T2<>(loadSize, totalSize);
    }

    /**
     *
     */
    private static class RebalanceIteratorAdapter implements IgniteRebalanceIterator {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /** Cache group caches. */
        private final Set<Integer> cacheGrpCaches;

        /** WAL iterator. */
        private final WALIterator walIt;

        /** Partition to scan. */
        private final int part;

        /** */
        private Iterator<DataEntry> entryIt;

        /** */
        private CacheDataRow next;

        /**
         * @param grp Cache group.
         * @param walIt WAL iterator.
         * @param part Partition ID.
         */
        private RebalanceIteratorAdapter(CacheGroupContext grp, WALIterator walIt, int part) {
            this.cacheGrpCaches = grp.cacheIds();
            this.walIt = walIt;
            this.part = part;

            advance();
        }

        /** {@inheritDoc} */
        @Override public boolean historical() {
            return true;
        }

        /** {@inheritDoc} */
        @Override public void close() throws IgniteCheckedException {
            walIt.close();
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

            CacheDataRow val = next;

            advance();

            return val;
        }

        /** {@inheritDoc} */
        @Override public void remove() {
            throw new UnsupportedOperationException();
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

                        if (entry.partitionId() == part && cacheGrpCaches.contains(entry.cacheId())) {

                            next = new DataEntryRow(entry);

                            return;
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
        @Override public void mvccVersion(long crdVer, long mvccCntr) {
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
        @Override public long mvccCounter() {
            return 0; // TODO IGNITE-3478.
        }

        /** {@inheritDoc} */
        @Override public long mvccCoordinatorVersion() {
            return 0; // TODO IGNITE-3478.
        }

        /** {@inheritDoc} */
        @Override public boolean removed() {
            return false;  // TODO IGNITE-3478.
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
        private volatile FreeListImpl freeList;

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

            dbMgr.checkpointReadLock();

            if (init.compareAndSet(false, true)) {
                try {
                    Metas metas = getOrAllocatePartitionMetas();

                    RootPage reuseRoot = metas.reuseListRoot;

                    freeList = new FreeListImpl(
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

                                if (grp.sharedGroup()) {
                                    long cntrsPageId = io.getCountersPageId(pageAddr);

                                    if (cntrsPageId != 0L) {
                                        cacheSizes = new HashMap<>();

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
                                    }
                                }

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

                        assert PageIdUtils.flag(treeRoot) == PageMemory.FLAG_DATA :
                            U.hexLong(treeRoot) + ", part=" + partId + ", grpId=" + grpId;
                        assert PageIdUtils.flag(reuseListRoot) == PageMemory.FLAG_DATA :
                            U.hexLong(reuseListRoot) + ", part=" + partId + ", grpId=" + grpId;
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
        @Override public int fullSize() {
            try {
                CacheDataStore delegate0 = init0(true);

                return delegate0 == null ? 0 : delegate0.fullSize();
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
        }

        /** {@inheritDoc} */
        @Override public int cacheSize(int cacheId) {
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
        @Override public boolean mvccInitialValue(
            GridCacheContext cctx,
            KeyCacheObject key,
            @Nullable CacheObject val,
            GridCacheVersion ver,
            long expireTime,
            MvccVersion mvccVer)
            throws IgniteCheckedException
        {
            CacheDataStore delegate = init0(false);

            return delegate.mvccInitialValue(cctx, key, val, ver, expireTime, mvccVer);
        }

        /** {@inheritDoc} */
        @Override public GridLongList mvccUpdate(
            GridCacheContext cctx,
            boolean primary,
            KeyCacheObject key,
            CacheObject val,
            GridCacheVersion ver,
            long expireTime,
            MvccVersion mvccVer) throws IgniteCheckedException {
            CacheDataStore delegate = init0(false);

            return delegate.mvccUpdate(cctx, primary, key, val, ver, expireTime, mvccVer);
        }

        /** {@inheritDoc} */
        @Override public GridLongList mvccRemove(
            GridCacheContext cctx,
            boolean primary,
            KeyCacheObject key,
            MvccVersion mvccVer) throws IgniteCheckedException {
            CacheDataStore delegate = init0(false);

            return delegate.mvccRemove(cctx, primary, key, mvccVer);
        }

        /** {@inheritDoc} */
        @Override public void mvccRemoveAll(GridCacheContext cctx, KeyCacheObject key) throws IgniteCheckedException {
            CacheDataStore delegate = init0(false);

            delegate.mvccRemoveAll(cctx, key);
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
        @Override public CacheDataRow mvccFind(GridCacheContext cctx, KeyCacheObject key, MvccVersion mvccVer)
            throws IgniteCheckedException {
            CacheDataStore delegate = init0(true);

            if (delegate != null)
                return delegate.mvccFind(cctx, key, mvccVer);

            return null;
        }

        /** {@inheritDoc} */
        @Override public List<T2<Object, MvccCounter>> mvccFindAllVersions(GridCacheContext cctx, KeyCacheObject key)
            throws IgniteCheckedException {
            CacheDataStore delegate = init0(true);

            if (delegate != null)
                return delegate.mvccFindAllVersions(cctx, key);

            return Collections.emptyList();
        }

        /** {@inheritDoc} */
        @Override public GridCursor<? extends CacheDataRow> cursor() throws IgniteCheckedException {
            CacheDataStore delegate = init0(true);

            if (delegate != null)
                return delegate.cursor();

            return EMPTY_CURSOR;
        }

        /** {@inheritDoc} */
        @Override public GridCursor<? extends CacheDataRow> cursor(MvccVersion ver)
            throws IgniteCheckedException {
            CacheDataStore delegate = init0(true);

            if (delegate != null)
                return delegate.cursor(ver);

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
            MvccVersion mvccVer)
            throws IgniteCheckedException {
            CacheDataStore delegate = init0(true);

            if (delegate != null)
                return delegate.cursor(cacheId, lower, upper, x, mvccVer);

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
        @Override public GridCursor<? extends CacheDataRow> cursor(int cacheId,
            MvccVersion mvccVer) throws IgniteCheckedException {
            CacheDataStore delegate = init0(true);

            if (delegate != null)
                return delegate.cursor(cacheId, mvccVer);

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
