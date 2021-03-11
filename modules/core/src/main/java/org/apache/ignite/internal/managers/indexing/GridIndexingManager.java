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

package org.apache.ignite.internal.managers.indexing;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.SkipDaemon;
import org.apache.ignite.internal.cache.query.index.Index;
import org.apache.ignite.internal.cache.query.index.IndexDefinition;
import org.apache.ignite.internal.cache.query.index.IndexFactory;
import org.apache.ignite.internal.cache.query.index.sorted.IndexRow;
import org.apache.ignite.internal.cache.query.index.sorted.defragmentation.IndexingDefragmentation;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndex;
import org.apache.ignite.internal.cache.query.index.sorted.inline.JavaObjectKeySerializer;
import org.apache.ignite.internal.cache.query.index.sorted.inline.io.AbstractInlineInnerIO;
import org.apache.ignite.internal.cache.query.index.sorted.inline.io.AbstractInlineLeafIO;
import org.apache.ignite.internal.managers.GridManagerAdapter;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.persistence.RootPage;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointTimeoutLock;
import org.apache.ignite.internal.processors.cache.persistence.defragmentation.LinkMap;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryEx;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusMetaIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.reuse.ReuseList;
import org.apache.ignite.internal.processors.cache.persistence.tree.util.PageLockListener;
import org.apache.ignite.internal.processors.query.schema.SchemaIndexCacheVisitor;
import org.apache.ignite.internal.util.GridAtomicLong;
import org.apache.ignite.internal.util.GridEmptyCloseableIterator;
import org.apache.ignite.internal.util.GridSpinBusyLock;
import org.apache.ignite.internal.util.collection.IntMap;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.IgniteSpiCloseableIterator;
import org.apache.ignite.spi.indexing.IndexingQueryFilter;
import org.apache.ignite.spi.indexing.IndexingSpi;
import org.apache.ignite.thread.IgniteThreadPoolExecutor;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.failure.FailureType.CRITICAL_ERROR;

/**
 * Manages cache indexing.
 */
@SkipDaemon
public class GridIndexingManager extends GridManagerAdapter<IndexingSpi> {
    /** */
    private final GridSpinBusyLock busyLock = new GridSpinBusyLock();

    /** For tests to emulate long rebuild process. */
    public static Class<? extends IndexesRebuildTask> idxRebuildCls;

    /** Indexes rebuild job. */
    private final IndexesRebuildTask idxRebuild;

    /** Serializer for representing JO as byte array in inline. */
    public static JavaObjectKeySerializer serializer;

    /**
     * @param ctx  Kernal context.
     */
    public GridIndexingManager(GridKernalContext ctx) throws IgniteCheckedException {
        super(ctx, ctx.config().getIndexingSpi());

        if (idxRebuildCls != null) {
            idxRebuild = U.newInstance(idxRebuildCls);

            idxRebuildCls = null;
        }
        else
            idxRebuild = new IndexesRebuildTask();

        serializer = new JavaObjectKeySerializer(ctx.config());
    }

    /**
     * @throws IgniteCheckedException Thrown in case of any errors.
     */
    @Override public void start() throws IgniteCheckedException {
        startSpi();

        if (log.isDebugEnabled())
            log.debug(startInfo());
    }

    /** {@inheritDoc} */
    @Override protected void onKernalStop0(boolean cancel) {
        if (ctx.config().isDaemon())
            return;

        busyLock.block();
    }

    /**
     * @throws IgniteCheckedException Thrown in case of any errors.
     */
    @Override public void stop(boolean cancel) throws IgniteCheckedException {
        if (ctx.config().isDaemon())
            return;

        stopSpi();

        if (log.isDebugEnabled())
            log.debug(stopInfo());
    }

    /**
     * Writes cache row to index.
     *
     * @throws IgniteCheckedException In case of error.
     */
    public void store(GridCacheContext cctx, CacheDataRow newRow, @Nullable CacheDataRow prevRow,
        boolean prevRowAvailable)
        throws IgniteCheckedException {
        assert enabled();

        if (!busyLock.enterBusy())
            throw new IllegalStateException("Failed to write to index (grid is stopping).");

        try {
            getSpi().store(cctx, newRow, prevRow, prevRowAvailable);
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Writes cache row to specified indexes.
     *
     * @throws IgniteCheckedException In case of error.
     */
    public void store(Collection<? extends Index> idxs, CacheDataRow newRow, @Nullable CacheDataRow prevRow,
        boolean prevRowAvailable)
        throws IgniteCheckedException {
        assert enabled();

        if (!busyLock.enterBusy())
            throw new IllegalStateException("Failed to write to index (grid is stopping).");

        try {
            getSpi().store(idxs, newRow, prevRow, prevRowAvailable);
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Creates a new index.
     *
     * @param cctx Cache context.
     * @param factory Index factory.
     * @param definition Description of an index to create.
     * @param cacheVisitor Enable to cancel dynamic index populating.
     */
    public Index createIndexDynamically(GridCacheContext cctx, IndexFactory factory, IndexDefinition definition,
        SchemaIndexCacheVisitor cacheVisitor) {
        if (!busyLock.enterBusy())
            throw new IllegalStateException("Failed to write to index (grid is stopping).");

        try {
            Index idx = getSpi().createIndex(cctx, factory, definition);

            // Populate index with cache rows.
            cacheVisitor.visit(row -> {
                try {
                    if (idx.canHandle(row))
                        idx.onUpdate(null, row, false);
                }
                catch (Throwable t) {
                    cctx.kernalContext().failure().process(new FailureContext(CRITICAL_ERROR, t));

                    throw t;

                }
            });

            return idx;
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Start rebuild of indexes for specified cache.
     */
    public IgniteInternalFuture<?> rebuildIndexesForCache(GridCacheContext cctx) {
        return idxRebuild.rebuild(cctx);
    }

    /**
     * Mark or unmark index for rebuild.
     */
    public void markIndexesRebuildForCache(GridCacheContext cctx, boolean val) {
        getSpi().markRebuildIndexesForCache(cctx, val);
    }

    /**
     * Creates a new index.
     *
     * @param factory Index factory.
     * @param definition Description of an index to create.
     */
    public Index createIndex(GridCacheContext cctx, IndexFactory factory, IndexDefinition definition) {
        if (!busyLock.enterBusy())
            throw new IllegalStateException("Failed to write to index (grid is stopping).");

        try {
            return getSpi().createIndex(cctx, factory, definition);

        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Deletes specified index.
     *
     * @param def Index definition.
     * @param softDelete Soft delete flag.
     */
    public void removeIndex(GridCacheContext cctx, IndexDefinition def, boolean softDelete) {
        if (!busyLock.enterBusy())
            throw new IllegalStateException("Failed to write to index (grid is stopping).");

        try {
            getSpi().removeIndex(cctx, def, softDelete);

        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Remove cache row from index.
     *
     * @param cacheName Cache name.
     * @throws IgniteCheckedException Thrown in case of any errors.
     */
    public void remove(String cacheName, @Nullable CacheDataRow prevRow) throws IgniteCheckedException {
        assert enabled();

        if (!busyLock.enterBusy())
            throw new IllegalStateException("Failed to remove from index (grid is stopping).");

        try {
            getSpi().remove(cacheName, prevRow);
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * @param cacheName Cache name.
     * @param key Key.
     * @throws IgniteCheckedException Thrown in case of any errors.
     */
    public void remove(String cacheName, Object key) throws IgniteCheckedException {
        assert key != null;
        assert enabled();

        if (!busyLock.enterBusy())
            throw new IllegalStateException("Failed to remove from index (grid is stopping).");

        try {
            getSpi().remove(cacheName, key);
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * @param cacheName Cache name.
     * @param params Parameters collection.
     * @param filters Filters.
     * @return Query result.
     * @throws IgniteCheckedException If failed.
     */
    public IgniteSpiCloseableIterator<?> query(String cacheName, Collection<Object> params, IndexingQueryFilter filters)
        throws IgniteCheckedException {
        if (!enabled())
            throw new IgniteCheckedException("Indexing SPI is not configured.");

        if (!busyLock.enterBusy())
            throw new IllegalStateException("Failed to execute query (grid is stopping).");

        try {
            final Iterator<?> res = getSpi().query(cacheName, params, filters);

            if (res == null)
                return new GridEmptyCloseableIterator<>();

            return new IgniteSpiCloseableIterator<Object>() {
                @Override public void close() throws IgniteCheckedException {
                    if (res instanceof AutoCloseable) {
                        try {
                            ((AutoCloseable)res).close();
                        }
                        catch (Exception e) {
                            throw new IgniteCheckedException(e);
                        }
                    }
                }

                @Override public boolean hasNext() {
                    return res.hasNext();
                }

                @Override public Object next() {
                    return res.next();
                }

                @Override public void remove() {
                    throw new UnsupportedOperationException();
                }
            };
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Destroy founded index which belongs to stopped cache.
     *
     * @param page Root page.
     * @param indexName Index name.
     * @param grpId Group id which contains garbage.
     * @param pageMemory Page memory to work with.
     * @param removeId Global remove id.
     * @param reuseList Reuse list where free pages should be stored.
     * @throws IgniteCheckedException If failed.
     */
    public void destroyOrphanIndex(
        GridKernalContext ctx,
        RootPage page,
        String indexName,
        int grpId,
        PageMemory pageMemory,
        final GridAtomicLong removeId,
        final ReuseList reuseList) throws IgniteCheckedException {

        assert ctx.cache().context().database().checkpointLockIsHeldByThread();

        long metaPageId = page.pageId().pageId();

        int inlineSize = inlineSize(page, grpId, pageMemory);

        String grpName = ctx.cache().cacheGroup(grpId).cacheOrGroupName();

        PageLockListener lockLsnr = ctx.cache().context().diagnostic()
            .pageLockTracker().createPageLockTracker(grpName + "IndexTree##" + indexName);

        BPlusTree<IndexRow, IndexRow> tree = new BPlusTree<IndexRow, IndexRow>(
            indexName,
            grpId,
            grpName,
            pageMemory,
            ctx.cache().context().wal(),
            removeId,
            metaPageId,
            reuseList,
            AbstractInlineInnerIO.versions(inlineSize),
            AbstractInlineLeafIO.versions(inlineSize),
            PageIdAllocator.FLAG_IDX,
            ctx.failure(),
            lockLsnr
        ) {
            @Override protected int compare(BPlusIO io, long pageAddr, int idx, IndexRow row) {
                throw new AssertionError();
            }

            @Override public IndexRow getRow(BPlusIO io, long pageAddr, int idx, Object x) {
                throw new AssertionError();
            }
        };

        tree.destroy();
    }

    /**
     * @param page Root page.
     * @param grpId Cache group id.
     * @param pageMemory Page memory.
     * @return Inline size.
     * @throws IgniteCheckedException If something went wrong.
     */
    private int inlineSize(RootPage page, int grpId, PageMemory pageMemory) throws IgniteCheckedException {
        long metaPageId = page.pageId().pageId();

        final long metaPage = pageMemory.acquirePage(grpId, metaPageId);

        try {
            long pageAddr = pageMemory.readLock(grpId, metaPageId, metaPage); // Meta can't be removed.

            assert pageAddr != 0 : "Failed to read lock meta page [metaPageId=" +
                U.hexLong(metaPageId) + ']';

            try {
                BPlusMetaIO io = BPlusMetaIO.VERSIONS.forPage(pageAddr);

                return io.getInlineSize(pageAddr);
            }
            finally {
                pageMemory.readUnlock(grpId, metaPageId, metaPage);
            }
        }
        finally {
            pageMemory.releasePage(grpId, metaPageId, metaPage);
        }
    }

    /**
     * Collect indexes for rebuild.
     *
     * @param createdOnly Get only created indexes (not restored from dick).
     */
    public List<InlineIndex> treeIndexes(GridCacheContext cctx, boolean createdOnly) {
        Collection<Index> idxs = getSpi().indexes(cctx);

        List<InlineIndex> treeIdxs = new ArrayList<>();

        for (Index idx: idxs) {
            if (idx instanceof InlineIndex) {
                InlineIndex idx0 = (InlineIndex)idx;

                if (!createdOnly || idx0.created())
                    treeIdxs.add(idx0);
            }
        }

        return treeIdxs;
    }

    /**
     * Defragment index partition.
     *
     * @param grpCtx Old group context.
     * @param newCtx New group context.
     * @param partPageMem Partition page memory.
     * @param mappingByPart Mapping page memory.
     * @param cpLock Defragmentation checkpoint read lock.
     * @param cancellationChecker Cancellation checker.
     * @param defragmentationThreadPool Thread pool for defragmentation.
     *
     * @throws IgniteCheckedException If failed.
     */
    public void defragment(
        CacheGroupContext grpCtx,
        CacheGroupContext newCtx,
        PageMemoryEx partPageMem,
        IntMap<LinkMap> mappingByPart,
        CheckpointTimeoutLock cpLock,
        Runnable cancellationChecker,
        IgniteThreadPoolExecutor defragmentationThreadPool
    ) throws IgniteCheckedException {
        new IndexingDefragmentation(this)
            .defragment(grpCtx, newCtx, partPageMem, mappingByPart, cpLock, cancellationChecker,
                defragmentationThreadPool, log);
    }

    /**
     * Returns IndexDefinition used for creating index specified id.
     *
     * @param idxId UUID of index.
     * @return IndexDefinition used for creating index with id {@code idxId}.
     */
    public IndexDefinition indexDefition(UUID idxId) {
        return getSpi().indexDefinition(idxId);
    }

    /**
     * @return Logger.
     */
    public IgniteLogger logger() {
        return log;
    }

    /** For tests purposes. */
    public IndexesRebuildTask idxRebuild() {
        return idxRebuild;
    }
}
