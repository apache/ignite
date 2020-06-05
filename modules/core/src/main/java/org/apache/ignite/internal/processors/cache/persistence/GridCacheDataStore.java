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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.failure.FailureType;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.store.IgnitePageStoreManager;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.pagemem.wal.record.PageSnapshot;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheEntryPredicate;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.GridCacheMvccEntryInfo;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.GridCacheTtlManager;
import org.apache.ignite.internal.processors.cache.IgniteCacheOffheapManager;
import org.apache.ignite.internal.processors.cache.IgniteCacheOffheapManagerImpl;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.PartitionUpdateCounter;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.mvcc.MvccSnapshot;
import org.apache.ignite.internal.processors.cache.mvcc.MvccVersion;
import org.apache.ignite.internal.processors.cache.persistence.freelist.AbstractFreeList;
import org.apache.ignite.internal.processors.cache.persistence.freelist.CacheFreeList;
import org.apache.ignite.internal.processors.cache.persistence.freelist.SimpleDataRow;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryEx;
import org.apache.ignite.internal.processors.cache.persistence.partstorage.PartitionMetaStorage;
import org.apache.ignite.internal.processors.cache.persistence.partstorage.PartitionMetaStorageImpl;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PagePartitionMetaIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PagePartitionMetaIOV2;
import org.apache.ignite.internal.processors.cache.persistence.tree.util.PageHandler;
import org.apache.ignite.internal.processors.cache.tree.CacheDataRowStore;
import org.apache.ignite.internal.processors.cache.tree.CacheDataTree;
import org.apache.ignite.internal.processors.cache.tree.PendingEntriesTree;
import org.apache.ignite.internal.processors.cache.tree.PendingRow;
import org.apache.ignite.internal.processors.cache.tree.mvcc.data.MvccUpdateResult;
import org.apache.ignite.internal.processors.cache.tree.mvcc.search.MvccLinkAwareSearchRow;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.query.GridQueryRowCacheCleaner;
import org.apache.ignite.internal.util.GridLongList;
import org.apache.ignite.internal.util.GridSpinBusyLock;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.internal.util.lang.IgniteInClosure2X;
import org.apache.ignite.internal.util.lang.IgnitePredicateX;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.Nullable;

import javax.cache.processor.EntryProcessor;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.OWNING;

/**
 *
 */
public class GridCacheDataStore implements IgniteCacheOffheapManager.CacheDataStore {
    /** */
    private final int partId;

    /** */
    private final CacheGroupContext grpCtx;

    /** */
    private volatile AbstractFreeList<CacheDataRow> freeList;

    /** */
    private PendingEntriesTree pendingTree;

    /** */
    private volatile IgniteCacheOffheapManagerImpl.CacheDataStoreImpl delegate;

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
    private final GridSpinBusyLock busyLock;

    /** */
    private final IgniteLogger log;

    /** */
    private final AtomicBoolean init = new AtomicBoolean();

    /** */
    private final CountDownLatch latch = new CountDownLatch(1);

    /**
     * @param partId Partition.
     * @param exists {@code True} if store exists.
     */
    public GridCacheDataStore(CacheGroupContext grpCtx, int partId, boolean exists,
                       GridSpinBusyLock busyLock,
                       IgniteLogger log) {
        this.grpCtx = grpCtx;
        this.partId = partId;
        this.exists = exists;
        this.busyLock = busyLock;
        this.log = log;
    }

    /** */
    public AbstractFreeList getCacheStoreFreeList() {
        return freeList;
    }

    /**
     * @return Name of free pages list.
     */
    private String freeListName() {
        return grpCtx.cacheOrGroupName() + "-" + partId;
    }

    /**
     * @return Name of partition meta store.
     */
    private String partitionMetaStoreName() {
        return grpCtx.cacheOrGroupName() + "-partstore-" + partId;
    }

    /**
     * @return Name of data tree.
     */
    private String dataTreeName() {
        return grpCtx.cacheOrGroupName() + "-" + BPlusTree.treeName("p-" + partId, "CacheData");
    }

    /**
     * @return Name of pending entires tree.
     */
    private String pendingEntriesTreeName() {
        return grpCtx.cacheOrGroupName() + "-" + "PendingEntries-" + partId;
    }

    /**
     * @param checkExists If {@code true} data store won't be initialized if it doesn't exists
     * (has non empty data file). This is an optimization for lazy store initialization on writes.
     *
     * @return Store delegate.
     * @throws IgniteCheckedException If failed.
     */
    private IgniteCacheOffheapManager.CacheDataStore init0(boolean checkExists) throws IgniteCheckedException {
        IgniteCacheOffheapManagerImpl.CacheDataStoreImpl delegate0 = delegate;

        if (delegate0 != null)
            return delegate0;

        if (checkExists) {
            if (!exists)
                return null;
        }

        final GridCacheSharedContext grpSharedCtx = grpCtx.shared();

        AtomicLong pageListCacheLimit = ((GridCacheDatabaseSharedManager) grpSharedCtx.database()).pageListCacheLimitHolder(grpCtx.dataRegion());

        if (init.compareAndSet(false, true)) {
            IgniteCacheDatabaseSharedManager dbMgr = grpSharedCtx.database();

            dbMgr.checkpointReadLock();

            try {
                Metas metas = getOrAllocatePartitionMetas();

                if (PageIdUtils.partId(metas.reuseListRoot.pageId().pageId()) != partId ||
                    PageIdUtils.partId(metas.treeRoot.pageId().pageId()) != partId ||
                    PageIdUtils.partId(metas.pendingTreeRoot.pageId().pageId()) != partId ||
                    PageIdUtils.partId(metas.partMetastoreReuseListRoot.pageId().pageId()) != partId
                    ) {
                    throw new IgniteCheckedException("Invalid meta root allocated [" +
                        "cacheOrGroupName=" + grpCtx.cacheOrGroupName() +
                        ", partId=" + partId +
                        ", metas=" + metas + ']');
                }

                String freeListName = freeListName();

                RootPage reuseRoot = metas.reuseListRoot;

                freeList = new CacheFreeList(
                    grpCtx.groupId(),
                    freeListName,
                    grpCtx.dataRegion().memoryMetrics(),
                    grpCtx.dataRegion(),
                    grpSharedCtx.wal(),
                    reuseRoot.pageId().pageId(),
                    reuseRoot.isAllocated(),
                    grpSharedCtx.diagnostic().pageLockTracker().createPageLockTracker(freeListName),
                    grpSharedCtx.kernalContext(),
                    pageListCacheLimit
                ) {
                    /** {@inheritDoc} */
                    @Override protected long allocatePageNoReuse() throws IgniteCheckedException {
                        assert grpSharedCtx.database().checkpointLockIsHeldByThread();

                        return pageMem.allocatePage(grpId, partId, PageIdAllocator.FLAG_DATA);
                    }
                };

                String partitionMetaStoreName = partitionMetaStoreName();

                RootPage partMetastoreReuseListRoot = metas.partMetastoreReuseListRoot;

                partStorage = new PartitionMetaStorageImpl<SimpleDataRow>(
                    grpCtx.groupId(),
                    partitionMetaStoreName,
                    grpCtx.dataRegion().memoryMetrics(),
                    grpCtx.dataRegion(),
                    freeList,
                    grpSharedCtx.wal(),
                    partMetastoreReuseListRoot.pageId().pageId(),
                    partMetastoreReuseListRoot.isAllocated(),
                    grpSharedCtx.diagnostic().pageLockTracker().createPageLockTracker(partitionMetaStoreName),
                    grpSharedCtx.kernalContext(),
                    pageListCacheLimit
                ) {
                    /** {@inheritDoc} */
                    @Override protected long allocatePageNoReuse() throws IgniteCheckedException {
                        assert grpSharedCtx.database().checkpointLockIsHeldByThread();

                        return pageMem.allocatePage(grpId, partId, PageIdAllocator.FLAG_DATA);
                    }
                };

                String dataTreeName = dataTreeName();

                CacheDataRowStore rowStore = new CacheDataRowStore(grpCtx, freeList, partId);

                RootPage treeRoot = metas.treeRoot;

                CacheDataTree dataTree = new CacheDataTree(
                    grpCtx,
                    dataTreeName,
                    freeList,
                    rowStore,
                    treeRoot.pageId().pageId(),
                    treeRoot.isAllocated(),
                    grpSharedCtx.diagnostic().pageLockTracker().createPageLockTracker(dataTreeName)
                ) {
                    /** {@inheritDoc} */
                    @Override protected long allocatePageNoReuse() throws IgniteCheckedException {
                        assert grpSharedCtx.database().checkpointLockIsHeldByThread();

                        return pageMem.allocatePage(grpId, partId, PageIdAllocator.FLAG_DATA);
                    }
                };

                String pendingEntriesTreeName = pendingEntriesTreeName();

                RootPage pendingTreeRoot = metas.pendingTreeRoot;

                final PendingEntriesTree pendingTree0 = new PendingEntriesTree(
                    grpCtx,
                    pendingEntriesTreeName,
                    grpCtx.dataRegion().pageMemory(),
                    pendingTreeRoot.pageId().pageId(),
                    freeList,
                    pendingTreeRoot.isAllocated(),
                    grpSharedCtx.diagnostic().pageLockTracker().createPageLockTracker(pendingEntriesTreeName)
                ) {
                    /** {@inheritDoc} */
                    @Override protected long allocatePageNoReuse() throws IgniteCheckedException {
                        assert grpSharedCtx.database().checkpointLockIsHeldByThread();

                        return pageMem.allocatePage(grpId, partId, PageIdAllocator.FLAG_DATA);
                    }
                };

                PageMemoryEx pageMem = (PageMemoryEx) grpCtx.dataRegion().pageMemory();

                int grpId = grpCtx.groupId();

                delegate0 =
                    new IgniteCacheOffheapManagerImpl.CacheDataStoreImpl(partId,
                        rowStore,
                        dataTree,
                        pendingTree0,
                        grpCtx,
                        busyLock,
                        log
                        ) {
                    /** {@inheritDoc} */
                    @Override public PendingEntriesTree pendingTree() {
                        return pendingTree0;
                    }

                    /** {@inheritDoc} */
                    @Override public void preload() throws IgniteCheckedException {
                        IgnitePageStoreManager pageStoreMgr = grpSharedCtx.pageStore();

                        if (pageStoreMgr == null)
                            return;

                        final int pages = pageStoreMgr.pages(grpId, partId);

                        long pageId = pageMem.partitionMetaPageId(grpId, partId);

                        // For each page sequentially pin/unpin.
                        for (int pageNo = 0; pageNo < pages; pageId++, pageNo++) {
                            long pagePointer = -1;

                            try {
                                pagePointer = pageMem.acquirePage(grpId, pageId);
                            }
                            finally {
                                if (pagePointer != -1)
                                    pageMem.releasePage(grpId, pageId, pagePointer);
                            }
                        }
                    }
                };

                pendingTree = pendingTree0;

                if (!pendingTree0.isEmpty())
                    grpCtx.caches().forEach(cctx -> cctx.ttl().hasPendingEntries(true));

                long partMetaId = pageMem.partitionMetaPageId(grpId, partId);
                long partMetaPage = pageMem.acquirePage(grpId, partMetaId);

                try {
                    long pageAddr = pageMem.readLock(grpId, partMetaId, partMetaPage);

                    try {
                        if (PageIO.getType(pageAddr) != 0) {
                            PagePartitionMetaIOV2 io = (PagePartitionMetaIOV2) PagePartitionMetaIO.VERSIONS.latest();

                            Map<Integer, Long> cacheSizes = null;

                            if (grpCtx.sharedGroup())
                                cacheSizes = GridCacheOffheapManager.readSharedGroupCacheSizes(pageMem, grpId, io.getCountersPageId(pageAddr));

                            long link = io.getGapsLink(pageAddr);

                            byte[] data = link == 0 ? null : partStorage.readRow(link);

                            delegate0.restoreState(io.getSize(pageAddr), io.getUpdateCounter(pageAddr), cacheSizes, data);

                            grpCtx.offheap().globalRemoveId().setIfGreater(io.getGlobalRemoveId(pageAddr));
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

                grpSharedCtx.kernalContext().failure().process(new FailureContext(FailureType.CRITICAL_ERROR, ex));

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
        PageMemoryEx pageMem = (PageMemoryEx)grpCtx.dataRegion().pageMemory();
        IgniteWriteAheadLogManager wal = grpCtx.shared().wal();

        int grpId = grpCtx.groupId();
        long partMetaId = pageMem.partitionMetaPageId(grpId, partId);

        AtomicBoolean metaPageAllocated = new AtomicBoolean(false);

        long partMetaPage = pageMem.acquirePage(grpId, partMetaId, metaPageAllocated);

        if (metaPageAllocated.get())
            grpCtx.metrics().incrementInitializedLocalPartitions();

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
                            log.debug("Upgrade partition meta page version: [part=" + partId +
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
        IgniteCacheOffheapManager.CacheDataStore delegate0 = delegate;

        return delegate0 == null ? null : delegate0.rowStore();
    }

    /** {@inheritDoc} */
    @Override public long fullSize() {
        try {
            IgniteCacheOffheapManager.CacheDataStore delegate0 = init0(true);

            return delegate0 == null ? 0 : delegate0.fullSize();
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean isEmpty() {
        try {
            IgniteCacheOffheapManager.CacheDataStore delegate0 = init0(true);

            return delegate0 == null || delegate0.isEmpty();
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public long cacheSize(int cacheId) {
        try {
            IgniteCacheOffheapManager.CacheDataStore delegate0 = init0(true);

            return delegate0 == null ? 0 : delegate0.cacheSize(cacheId);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public Map<Integer, Long> cacheSizes() {
        try {
            IgniteCacheOffheapManager.CacheDataStore delegate0 = init0(true);

            return delegate0 == null ? null : delegate0.cacheSizes();
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void updateSize(int cacheId, long delta) {
        try {
            IgniteCacheOffheapManager.CacheDataStore delegate0 = init0(false);

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
            IgniteCacheOffheapManager.CacheDataStore delegate0 = init0(true);

            return delegate0 == null ? 0 : delegate0.updateCounter();
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public long reservedCounter() {
        try {
            IgniteCacheOffheapManager.CacheDataStore delegate0 = init0(true);

            return delegate0 == null ? 0 : delegate0.reservedCounter();
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public PartitionUpdateCounter partUpdateCounter() {
        try {
            IgniteCacheOffheapManager.CacheDataStore delegate0 = init0(true);

            return delegate0 == null ? null : delegate0.partUpdateCounter();
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public long getAndIncrementUpdateCounter(long delta) {
        try {
            IgniteCacheOffheapManager.CacheDataStore delegate0 = init0(false);

            return delegate0 == null ? 0 : delegate0.getAndIncrementUpdateCounter(delta);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public long reserve(long delta) {
        try {
            IgniteCacheOffheapManager.CacheDataStore delegate0 = init0(false);

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
            IgniteCacheOffheapManager.CacheDataStore delegate0 = init0(false);

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
            IgniteCacheOffheapManager.CacheDataStore delegate0 = init0(false);

            return delegate0 != null && delegate0.updateCounter(start, delta);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public GridLongList finalizeUpdateCounters() {
        try {
            IgniteCacheOffheapManager.CacheDataStore delegate0 = init0(true);

            return delegate0 != null ? delegate0.finalizeUpdateCounters() : null;
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public long nextUpdateCounter() {
        try {
            IgniteCacheOffheapManager.CacheDataStore delegate0 = init0(false);

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
            IgniteCacheOffheapManager.CacheDataStore delegate0 = init0(true);

            return delegate0 == null ? 0 : delegate0.initialUpdateCounter();
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void updateInitialCounter(long start, long delta) {
        try {
            IgniteCacheOffheapManager.CacheDataStore delegate0 = init0(false);

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
            IgniteCacheOffheapManager.CacheDataStore delegate0 = init0(true);

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
        assert grpCtx.shared().database().checkpointLockIsHeldByThread();

        IgniteCacheOffheapManager.CacheDataStore delegate = init0(false);

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
        IgniteCacheOffheapManager.CacheDataStore delegate = init0(false);

        return delegate.mvccInitialValue(cctx, key, val, ver, expireTime, mvccVer, newMvccVer);
    }

    /** {@inheritDoc} */
    @Override public boolean mvccApplyHistoryIfAbsent(
        GridCacheContext cctx,
        KeyCacheObject key,
        List<GridCacheMvccEntryInfo> hist)
        throws IgniteCheckedException {
        IgniteCacheOffheapManager.CacheDataStore delegate = init0(false);

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

        IgniteCacheOffheapManager.CacheDataStore delegate = init0(false);

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
        IgniteCacheOffheapManager.CacheDataStore delegate = init0(false);

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
        IgniteCacheOffheapManager.CacheDataStore delegate = init0(false);

        return delegate.mvccRemove(cctx, key, mvccVer,filter, primary, needHistory, needOldVal, retVal);
    }

    /** {@inheritDoc} */
    @Override public MvccUpdateResult mvccLock(
        GridCacheContext cctx,
        KeyCacheObject key,
        MvccSnapshot mvccSnapshot) throws IgniteCheckedException {
        IgniteCacheOffheapManager.CacheDataStore delegate = init0(false);

        return delegate.mvccLock(cctx, key, mvccSnapshot);
    }

    /** {@inheritDoc} */
    @Override public void mvccRemoveAll(GridCacheContext cctx, KeyCacheObject key) throws IgniteCheckedException {
        IgniteCacheOffheapManager.CacheDataStore delegate = init0(false);

        delegate.mvccRemoveAll(cctx, key);
    }

    /** {@inheritDoc} */
    @Override public void mvccApplyUpdate(GridCacheContext cctx, KeyCacheObject key, CacheObject val, GridCacheVersion ver,
        long expireTime, MvccVersion mvccVer) throws IgniteCheckedException {
        IgniteCacheOffheapManager.CacheDataStore delegate = init0(false);

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
        assert grpCtx.shared().database().checkpointLockIsHeldByThread();

        IgniteCacheOffheapManager.CacheDataStore delegate = init0(false);

        return delegate.createRow(cctx, key, val, ver, expireTime, oldRow);
    }

    /** {@inheritDoc} */
    @Override public void insertRows(Collection<DataRowCacheAware> rows,
                                     IgnitePredicateX<CacheDataRow> initPred) throws IgniteCheckedException {
        IgniteCacheOffheapManager.CacheDataStore delegate = init0(false);

        delegate.insertRows(rows, initPred);
    }

    /** {@inheritDoc} */
    @Override public int cleanup(GridCacheContext cctx,
        @Nullable List<MvccLinkAwareSearchRow> cleanupRows) throws IgniteCheckedException {
        IgniteCacheOffheapManager.CacheDataStore delegate = init0(false);

        return delegate.cleanup(cctx, cleanupRows);
    }

    /** {@inheritDoc} */
    @Override public void updateTxState(GridCacheContext cctx, CacheSearchRow row) throws IgniteCheckedException {
        IgniteCacheOffheapManager.CacheDataStore delegate = init0(false);

        delegate.updateTxState(cctx, row);
    }

    /** {@inheritDoc} */
    @Override public void invoke(GridCacheContext cctx, KeyCacheObject key, IgniteCacheOffheapManager.OffheapInvokeClosure c)
        throws IgniteCheckedException {
        assert grpCtx.shared().database().checkpointLockIsHeldByThread();

        IgniteCacheOffheapManager.CacheDataStore delegate = init0(false);

        delegate.invoke(cctx, key, c);
    }

    /** {@inheritDoc} */
    @Override public void remove(GridCacheContext cctx, KeyCacheObject key, int partId)
        throws IgniteCheckedException {
        assert grpCtx.shared().database().checkpointLockIsHeldByThread();

        IgniteCacheOffheapManager.CacheDataStore delegate = init0(false);

        delegate.remove(cctx, key, partId);
    }

    /** {@inheritDoc} */
    @Override public CacheDataRow find(GridCacheContext cctx, KeyCacheObject key) throws IgniteCheckedException {
        IgniteCacheOffheapManager.CacheDataStore delegate = init0(true);

        if (delegate != null)
            return delegate.find(cctx, key);

        return null;
    }

    /** {@inheritDoc} */
    @Override public CacheDataRow mvccFind(GridCacheContext cctx, KeyCacheObject key, MvccSnapshot snapshot)
        throws IgniteCheckedException {
        IgniteCacheOffheapManager.CacheDataStore delegate = init0(true);

        if (delegate != null)
            return delegate.mvccFind(cctx, key, snapshot);

        return null;
    }

    /** {@inheritDoc} */
    @Override public List<IgniteBiTuple<Object, MvccVersion>> mvccFindAllVersions(GridCacheContext cctx, KeyCacheObject key)
        throws IgniteCheckedException {
        IgniteCacheOffheapManager.CacheDataStore delegate = init0(true);

        if (delegate != null)
            return delegate.mvccFindAllVersions(cctx, key);

        return Collections.emptyList();
    }

    /** {@inheritDoc} */
    @Override public GridCursor<CacheDataRow> mvccAllVersionsCursor(GridCacheContext cctx,
                                                                    KeyCacheObject key, Object x) throws IgniteCheckedException {
        IgniteCacheOffheapManager.CacheDataStore delegate = init0(true);

        if (delegate != null)
            return delegate.mvccAllVersionsCursor(cctx, key, x);

        return GridCacheOffheapManager.EMPTY_CURSOR;
    }


    /** {@inheritDoc} */
    @Override public GridCursor<? extends CacheDataRow> cursor() throws IgniteCheckedException {
        IgniteCacheOffheapManager.CacheDataStore delegate = init0(true);

        if (delegate != null)
            return delegate.cursor();

        return GridCacheOffheapManager.EMPTY_CURSOR;
    }

    /** {@inheritDoc} */
    @Override public GridCursor<? extends CacheDataRow> cursor(Object x) throws IgniteCheckedException {
        IgniteCacheOffheapManager.CacheDataStore delegate = init0(true);

        if (delegate != null)
            return delegate.cursor(x);

        return GridCacheOffheapManager.EMPTY_CURSOR;
    }

    /** {@inheritDoc} */
    @Override public GridCursor<? extends CacheDataRow> cursor(MvccSnapshot mvccSnapshot)
        throws IgniteCheckedException {
        IgniteCacheOffheapManager.CacheDataStore delegate = init0(true);

        if (delegate != null)
            return delegate.cursor(mvccSnapshot);

        return GridCacheOffheapManager.EMPTY_CURSOR;
    }

    /** {@inheritDoc} */
    @Override public GridCursor<? extends CacheDataRow> cursor(
        int cacheId,
        KeyCacheObject lower,
        KeyCacheObject upper) throws IgniteCheckedException {
        IgniteCacheOffheapManager.CacheDataStore delegate = init0(true);

        if (delegate != null)
            return delegate.cursor(cacheId, lower, upper);

        return GridCacheOffheapManager.EMPTY_CURSOR;
    }

    /** {@inheritDoc} */
    @Override public GridCursor<? extends CacheDataRow> cursor(int cacheId,
        KeyCacheObject lower,
        KeyCacheObject upper,
        Object x)
        throws IgniteCheckedException {
        IgniteCacheOffheapManager.CacheDataStore delegate = init0(true);

        if (delegate != null)
            return delegate.cursor(cacheId, lower, upper, x);

        return GridCacheOffheapManager.EMPTY_CURSOR;
    }

    /** {@inheritDoc} */
    @Override public GridCursor<? extends CacheDataRow> cursor(int cacheId,
        KeyCacheObject lower,
        KeyCacheObject upper,
        Object x,
        MvccSnapshot mvccSnapshot)
        throws IgniteCheckedException {
        IgniteCacheOffheapManager.CacheDataStore delegate = init0(true);

        if (delegate != null)
            return delegate.cursor(cacheId, lower, upper, x, mvccSnapshot);

        return GridCacheOffheapManager.EMPTY_CURSOR;
    }

    /** {@inheritDoc} */
    @Override public void destroy() throws IgniteCheckedException {
        // No need to destroy delegate.
    }

    /** {@inheritDoc} */
    @Override public void markDestroyed() throws IgniteCheckedException {
        IgniteCacheOffheapManager.CacheDataStore delegate = init0(true);

        if (delegate != null)
            delegate.markDestroyed();
    }

    /** {@inheritDoc} */
    @Override public GridCursor<? extends CacheDataRow> cursor(int cacheId) throws IgniteCheckedException {
        IgniteCacheOffheapManager.CacheDataStore delegate = init0(true);

        if (delegate != null)
            return delegate.cursor(cacheId);

        return GridCacheOffheapManager.EMPTY_CURSOR;
    }

    /** {@inheritDoc} */
    @Override public GridCursor<? extends CacheDataRow> cursor(int cacheId,
        MvccSnapshot mvccSnapshot) throws IgniteCheckedException {
        IgniteCacheOffheapManager.CacheDataStore delegate = init0(true);

        if (delegate != null)
            return delegate.cursor(cacheId, mvccSnapshot);

        return GridCacheOffheapManager.EMPTY_CURSOR;
    }

    /** {@inheritDoc} */
    @Override public void clear(int cacheId) throws IgniteCheckedException {
        assert grpCtx.shared().database().checkpointLockIsHeldByThread();

        IgniteCacheOffheapManager.CacheDataStore delegate0 = init0(true);

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
        IgniteCacheOffheapManager.CacheDataStore delegate0 = init0(true);

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
        long throttlingTimeout,
        int amount
    ) throws IgniteCheckedException {
        IgniteCacheOffheapManager.CacheDataStore delegate0 = init0(true);

        long nowNanos = System.nanoTime();

        if (delegate0 == null || (cctx.cacheId() == lastThrottledCacheId && nextStoreCleanTimeNanos - nowNanos > 0))
            return 0;

        assert pendingTree != null : "Partition data store was not initialized.";

        int cleared = purgeExpiredInternal(cctx, c, amount);

        // Throttle if there is nothing to clean anymore.
        if (cleared < amount) {
            lastThrottledCacheId = cctx.cacheId();

            nextStoreCleanTimeNanos = nowNanos + U.millisToNanos(throttlingTimeout);
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

        if (!grpCtx.isLocal()) {
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

                if (grpCtx.sharedGroup())
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
                            obsoleteVer = grpCtx.shared().versions().next();

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
            IgniteCacheOffheapManager.CacheDataStore delegate0 = init0(true);

            return delegate0 == null ? null : pendingTree;
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void preload() throws IgniteCheckedException {
        IgniteCacheOffheapManager.CacheDataStore delegate0 = init0(true);

        if (delegate0 != null)
            delegate0.preload();
    }

    /** {@inheritDoc} */
    @Override public void resetUpdateCounter() {
        try {
            IgniteCacheOffheapManager.CacheDataStore delegate0 = init0(true);

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
            IgniteCacheOffheapManager.CacheDataStore delegate0 = init0(true);

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
