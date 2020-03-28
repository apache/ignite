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

package org.apache.ignite.internal.processors.cache.persistence.migration;

import java.util.Set;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.IgniteCacheOffheapManager;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRowAdapter;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheOffheapManager;
import org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.IndexStorage;
import org.apache.ignite.internal.processors.cache.persistence.RootPage;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.tree.PendingEntriesTree;
import org.apache.ignite.internal.processors.cache.tree.PendingRow;
import org.apache.ignite.internal.util.IgniteTree;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.tree.PendingEntriesTree.WITHOUT_KEY;

/**
 * Ignite native persistence migration task upgrades existed PendingTrees to per-partition basis. It's ignore possible
 * assertions errors when a pointer to an entry exists in tree but the entry itself was removed due to some reason (e.g.
 * when partition was evicted after restart).
 *
 * Task goes through persistent cache groups and copy entries to certain partitions.
 */
public class UpgradePendingTreeToPerPartitionTask implements IgniteCallable<Boolean> {
    /** */
    private static final String PENDING_ENTRIES_TREE_NAME = "PendingEntries";

    /** */
    private static final long serialVersionUID = 0L;

    /** */
    public static final int BATCH_SIZE = 500;

    /** */
    @IgniteInstanceResource
    private IgniteEx node;

    /** */
    @LoggerResource
    private IgniteLogger log;

    /** {@inheritDoc} */
    @Override public Boolean call() throws IgniteException {
        GridCacheSharedContext<Object, Object> sharedCtx = node.context().cache().context();

        for (CacheGroupContext grp : sharedCtx.cache().cacheGroups()) {
            if (!grp.persistenceEnabled() || !grp.affinityNode()) {
                if (!grp.persistenceEnabled())
                    log.info("Skip pending tree upgrade for non-persistent cache group: [grpId=" + grp.groupId() +
                        ", grpName=" + grp.name() + ']');
                else
                    log.info("Skip pending tree upgrade on non-affinity node for cache group: [grpId=" + grp.groupId() +
                        ", grpName=" + grp.name() + ']');

                continue;
            }

            try {
                processCacheGroup(grp);
            }
            catch (Exception ex) {
                if (Thread.interrupted() || X.hasCause(ex, InterruptedException.class))
                    log.info("Upgrade pending tree has been cancelled.");
                else
                    log.warning("Failed to upgrade pending tree for cache group:  [grpId=" + grp.groupId() +
                        ", grpName=" + grp.name() + ']', ex);

                return false;
            }

            if (Thread.interrupted()) {
                log.info("Upgrade pending tree has been cancelled.");

                return false;
            }
        }

        log.info("All pending trees upgraded successfully.");

        return true;
    }

    /**
     * Converts CacheGroup pending tree to per-partition basis.
     *
     * @param grp Cache group.
     * @throws IgniteCheckedException If error occurs.
     */
    private void processCacheGroup(CacheGroupContext grp) throws IgniteCheckedException {
        assert grp.offheap() instanceof GridCacheOffheapManager;

        PendingEntriesTree oldPendingTree;

        final IgniteCacheDatabaseSharedManager db = grp.shared().database();

        db.checkpointReadLock();
        try {
            IndexStorage indexStorage = ((GridCacheOffheapManager)grp.offheap()).getIndexStorage();

            //TODO: IGNITE-5874: replace with some check-method to avoid unnecessary page allocation.
            RootPage pendingRootPage = indexStorage.allocateIndex(PENDING_ENTRIES_TREE_NAME);

            if (pendingRootPage.isAllocated()) {
                log.info("No pending tree found for cache group: [grpId=" + grp.groupId() +
                    ", grpName=" + grp.name() + ']');

                // Nothing to do here as just allocated tree is obviously empty.
                indexStorage.dropIndex(PENDING_ENTRIES_TREE_NAME);

                return;
            }

            oldPendingTree = new PendingEntriesTree(
                grp,
                PENDING_ENTRIES_TREE_NAME,
                grp.dataRegion().pageMemory(),
                pendingRootPage.pageId().pageId(),
                ((GridCacheOffheapManager)grp.offheap()).reuseListForIndex(null),
                false,
                null
            );
        }
        finally {
            db.checkpointReadUnlock();
        }

        processPendingTree(grp, oldPendingTree);

        if (Thread.currentThread().isInterrupted())
            return;

        db.checkpointReadLock();
        try {
            oldPendingTree.destroy();
        }
        finally {
            db.checkpointReadUnlock();
        }
    }

    /**
     * Move pending rows for CacheGroup entries to per-partition PendingTree.
     * Invalid pending rows will be ignored.
     *
     * @param grp Cache group.
     * @param oldPendingEntries Old-style PendingTree.
     * @throws IgniteCheckedException If error occurs.
     */
    private void processPendingTree(CacheGroupContext grp, PendingEntriesTree oldPendingEntries)
        throws IgniteCheckedException {
        final PageMemory pageMemory = grp.dataRegion().pageMemory();

        final IgniteCacheDatabaseSharedManager db = grp.shared().database();

        final Set<Integer> cacheIds = grp.cacheIds();

        PendingRow row = null;

        int processedEntriesCnt = 0;
        int skippedEntries = 0;

        // Re-acquire checkpoint lock for every next batch.
        while (!Thread.currentThread().isInterrupted()) {
            int cnt = 0;

            db.checkpointReadLock();
            try {
                GridCursor<PendingRow> cursor = oldPendingEntries.find(row, null, WITHOUT_KEY);

                while (cnt++ < BATCH_SIZE && cursor.next()) {
                    row = cursor.get();

                    assert row.link != 0 && row.expireTime != 0 : row;

                    GridCacheEntryEx entry;

                    // Lost cache or lost entry.
                    if (!cacheIds.contains(row.cacheId) || (entry = getEntry(grp, row)) == null) {
                        skippedEntries++;

                        oldPendingEntries.removex(row);

                        continue;
                    }

                    entry.lockEntry();
                    try {
                        if (processRow(pageMemory, grp, row))
                            processedEntriesCnt++;
                        else
                            skippedEntries++;
                    }
                    finally {
                        entry.unlockEntry();
                    }

                    oldPendingEntries.removex(row);
                }

                if (cnt < BATCH_SIZE)
                    break;
            }
            finally {
                db.checkpointReadUnlock();
            }
        }

        log.info("PendingTree upgraded: " +
            "[grpId=" + grp.groupId() +
            ", grpName=" + grp.name() +
            ", processedEntries=" + processedEntriesCnt +
            ", failedEntries=" + skippedEntries +
            ']');
    }

    /**
     * Return CacheEntry instance for lock purpose.
     *
     * @param grp Cache group
     * @param row Pending row.
     * @return CacheEntry if found or null otherwise.
     */
    private GridCacheEntryEx getEntry(CacheGroupContext grp, PendingRow row) {
        try {
            CacheDataRowAdapter rowData = new CacheDataRowAdapter(row.link);

            rowData.initFromLink(grp, CacheDataRowAdapter.RowData.KEY_ONLY);

            GridCacheContext cctx = grp.shared().cacheContext(row.cacheId);

            assert cctx != null;

            return cctx.cache().entryEx(rowData.key());
        }
        catch (Throwable ex) {
            if (Thread.currentThread().isInterrupted() || X.hasCause(ex, InterruptedException.class))
                throw new IgniteException(new InterruptedException());

            log.warning("Failed to move old-version pending entry " +
                "to per-partition PendingTree: key not found (skipping): " +
                "[grpId=" + grp.groupId() +
                ", grpName=" + grp.name() +
                ", pendingRow=" + row + "]");

            return null;
        }

    }

    /**
     * Validates PendingRow and add it to per-partition PendingTree.
     *
     * @param pageMemory Page memory.
     * @param grp Cache group.
     * @param row Pending row.
     * @return {@code True} if pending row successfully moved, {@code False} otherwise.
     */
    private boolean processRow(PageMemory pageMemory, CacheGroupContext grp, PendingRow row) {
        final long pageId = PageIdUtils.pageId(row.link);

        final int partition = PageIdUtils.partId(pageId);

        assert partition >= 0;

        try {
            final long page = pageMemory.acquirePage(grp.groupId(), pageId);
            long pageAddr = pageMemory.readLock(grp.groupId(), pageId, page);
            try {
                assert PageIO.getType(pageAddr) != 0;
                assert PageIO.getVersion(pageAddr) != 0;

                IgniteCacheOffheapManager.CacheDataStore store =
                    ((GridCacheOffheapManager)grp.offheap()).dataStore(partition);

                if (store == null) {
                    log.warning("Failed to move old-version pending entry " +
                        "to per-partition PendingTree: Node has no partition anymore (skipping): " +
                        "[grpId=" + grp.groupId() +
                        ", grpName=" + grp.name() +
                        ", partId=" + partition +
                        ", pendingRow=" + row + "]");

                    return false;
                }

                assert store instanceof GridCacheOffheapManager.GridCacheDataStore;
                assert store.pendingTree() != null;

                store.pendingTree().invoke(row, WITHOUT_KEY, new PutIfAbsentClosure(row));
            }
            finally {
                pageMemory.readUnlock(grp.groupId(), pageId, page);
            }
        }
        catch (AssertionError | Exception ex) {
            if (Thread.currentThread().isInterrupted() || X.hasCause(ex, InterruptedException.class)) {
                Thread.currentThread().interrupt();

                throw new IgniteException(ex);
            }

            String msg = "Unexpected error occurs while moving old-version pending entry " +
                "to per-partition PendingTree. Seems page doesn't longer exists (skipping): " +
                "[grpId=" + grp.groupId() +
                ", grpName=" + grp.name() +
                ", partId=" + partition +
                ", pendingRow=" + row + ']';

            if (log.isDebugEnabled())
                log.warning(msg, ex);
            else
                log.warning(msg);

            return false;
        }

        return true;
    }

    /** */
    private static class PutIfAbsentClosure implements IgniteTree.InvokeClosure<PendingRow> {
        /** */
        private final PendingRow pendingRow;

        /** */
        private IgniteTree.OperationType op;

        /** */
        PutIfAbsentClosure(PendingRow pendingRow) {
            this.pendingRow = pendingRow;
        }

        /** {@inheritDoc} */
        @Override public void call(@Nullable PendingRow oldRow) throws IgniteCheckedException {
            op = (oldRow == null) ? IgniteTree.OperationType.PUT : IgniteTree.OperationType.NOOP;
        }

        /** {@inheritDoc} */
        @Override public PendingRow newRow() {
            return pendingRow;
        }

        /** {@inheritDoc} */
        @Override public IgniteTree.OperationType operationType() {
            return op;
        }
    }
}
