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

package org.apache.ignite.internal.cache.query.index;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cache.query.index.sorted.IndexRow;
import org.apache.ignite.internal.cache.query.index.sorted.IndexRowCache;
import org.apache.ignite.internal.cache.query.index.sorted.IndexRowCacheRegistry;
import org.apache.ignite.internal.cache.query.index.sorted.defragmentation.IndexingDefragmentation;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndex;
import org.apache.ignite.internal.cache.query.index.sorted.inline.JavaObjectKeySerializer;
import org.apache.ignite.internal.cache.query.index.sorted.inline.io.AbstractInlineInnerIO;
import org.apache.ignite.internal.cache.query.index.sorted.inline.io.AbstractInlineLeafIO;
import org.apache.ignite.internal.cache.query.index.sorted.inline.io.InnerIO;
import org.apache.ignite.internal.cache.query.index.sorted.inline.io.LeafIO;
import org.apache.ignite.internal.cache.query.index.sorted.inline.io.MvccInnerIO;
import org.apache.ignite.internal.cache.query.index.sorted.inline.io.MvccLeafIO;
import org.apache.ignite.internal.managers.indexing.IndexesRebuildTask;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheContextInfo;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.persistence.RootPage;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointTimeoutLock;
import org.apache.ignite.internal.processors.cache.persistence.defragmentation.LinkMap;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryEx;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusMetaIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.reuse.ReuseList;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.schema.IndexRebuildCancelToken;
import org.apache.ignite.internal.processors.query.schema.SchemaIndexCacheVisitor;
import org.apache.ignite.internal.util.GridAtomicLong;
import org.apache.ignite.internal.util.collection.IntMap;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.thread.IgniteThreadPoolExecutor;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation of IndexingSpi that tracks all cache indexes.
 */
public class IndexProcessor extends GridProcessorAdapter {
    /**
     * Register inline IOs for sorted indexes.
     */
    static {
        registerIO();
    }

    /**
     * Register inline IOs for sorted indexes.
     */
    public static void registerIO() {
        PageIO.registerH2(InnerIO.VERSIONS, LeafIO.VERSIONS, MvccInnerIO.VERSIONS, MvccLeafIO.VERSIONS);

        AbstractInlineInnerIO.register();
        AbstractInlineLeafIO.register();
    }

    /** For tests to emulate long rebuild process. */
    public static Class<? extends IndexesRebuildTask> idxRebuildCls;

    /** Indexes rebuild job. */
    private final IndexesRebuildTask idxRebuild;

    /** Serializer for representing JO as byte array in inline. */
    public static JavaObjectKeySerializer serializer;

    /** Row cache. */
    private final IndexRowCacheRegistry idxRowCacheRegistry = new IndexRowCacheRegistry();

    /**
     * Registry of all indexes. High key is a cache name, lower key is an unique index name.
     */
    private final Map<String, Map<String, Index>> cacheToIdx = new ConcurrentHashMap<>();

    /**
     * Registry of all index definitions. Key is {@link Index#id()}, value is IndexDefinition used for creating index.
     */
    private final Map<UUID, IndexDefinition> idxDefs = new ConcurrentHashMap<>();

    /** Exclusive lock for DDL operations. */
    private final ReentrantReadWriteLock ddlLock = new ReentrantReadWriteLock();

    /**
     * @param ctx Kernal context.
     */
    public IndexProcessor(GridKernalContext ctx) throws IgniteCheckedException {
        super(ctx);

        if (idxRebuildCls != null) {
            idxRebuild = U.newInstance(idxRebuildCls);

            idxRebuildCls = null;
        }
        else
            idxRebuild = new IndexesRebuildTask();

        serializer = new JavaObjectKeySerializer(ctx.config());
    }

    /**
     * Updates index with new row. Note that key is unique for cache, so if cache contains multiple indexes
     * the key should be removed from indexes other than one being updated.
     *
     * @param cctx Cache context.
     * @param newRow cache row to store in index.
     * @param prevRow optional cache row that will be replaced with new row.
     */
    public void store(GridCacheContext<?, ?> cctx, CacheDataRow newRow, @Nullable CacheDataRow prevRow,
        boolean prevRowAvailable)
        throws IgniteSpiException {
        try {
            updateIndexes(cctx.name(), newRow, prevRow, prevRowAvailable);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteSpiException("Failed to store row in cache", e);
        }
    }

    /**
     * Updates index with new row. Note that key is unique for cache, so if cache contains multiple indexes
     * the key should be removed from indexes other than one being updated.
     *
     * @param idxs List of indexes to update.
     * @param newRow cache row to store in index.
     * @param prevRow optional cache row that will be replaced with new row.
     */
    public void store(Collection<? extends Index> idxs, CacheDataRow newRow, @Nullable CacheDataRow prevRow,
        boolean prevRowAvailable) throws IgniteSpiException {
        IgniteCheckedException err = null;

        ddlLock.readLock().lock();

        try {
            for (Index idx : idxs)
                err = updateIndex(idx, newRow, prevRow, prevRowAvailable, err);

            if (err != null)
                throw err;

        }
        catch (IgniteCheckedException e) {
            throw new IgniteSpiException("Failed to store row in index", e);

        }
        finally {
            ddlLock.readLock().unlock();
        }
    }

    /**
     * Delete specified row from index.
     *
     * @param cacheName Cache name.
     * @param prevRow Cache row to delete from index.
     */
    public void remove(String cacheName, @Nullable CacheDataRow prevRow) throws IgniteSpiException {
        try {
            updateIndexes(cacheName, null, prevRow, true);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteSpiException("Failed to remove row in cache", e);
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

        Index idx = createIndex(cctx, factory, definition);

        // Populate index with cache rows.
        cacheVisitor.visit(row -> {
            if (idx.canHandle(row))
                idx.onUpdate(null, row, false);
        });

        return idx;
    }

    /**
     * Creates a new index.
     *
     * @param cctx Cache context.
     * @param factory Index factory.
     * @param definition Description of an index to create.
     */
    public Index createIndex(GridCacheContext<?, ?> cctx, IndexFactory factory, IndexDefinition definition) {
        ddlLock.writeLock().lock();

        try {
            String cacheName = definition.idxName().cacheName();

            cacheToIdx.putIfAbsent(cacheName, new ConcurrentHashMap<>());

            String uniqIdxName = definition.idxName().fullName();

            // GridQueryProcessor already checked schema operation for index duplication.
            assert cacheToIdx.get(cacheName).get(uniqIdxName) == null : "Duplicated index name " + uniqIdxName;

            Index idx = factory.createIndex(cctx, definition);

            cacheToIdx.get(cacheName).put(uniqIdxName, idx);

            idxDefs.put(idx.id(), definition);

            return idx;

        }
        finally {
            ddlLock.writeLock().unlock();
        }
    }

    /**
     * Removes an index.
     *
     * @param cctx Cache context.
     * @param idxName Index name.
     * @param softDelete whether it's required to delete underlying structures.
     */
    public void removeIndex(GridCacheContext<?, ?> cctx, IndexName idxName, boolean softDelete) {
        ddlLock.writeLock().lock();

        try {
            String cacheName = idxName.cacheName();

            Map<String, Index> idxs = cacheToIdx.get(cacheName);

            assert idxs != null : "Try remove index for non registered cache " + cacheName;

            Index idx = idxs.remove(idxName.fullName());

            if (idx != null) {
                idx.destroy(softDelete);
                idxDefs.remove(idx.id());
            }

        }
        finally {
            ddlLock.writeLock().unlock();
        }
    }

    /** */
    private void updateIndexes(String cacheName, CacheDataRow newRow, CacheDataRow prevRow,
        boolean prevRowAvailable) throws IgniteCheckedException {
        IgniteCheckedException err = null;

        ddlLock.readLock().lock();

        try {
            Map<String, Index> indexes = cacheToIdx.get(cacheName);

            if (F.isEmpty(indexes))
                return;

            for (Index idx: indexes.values())
                err = updateIndex(idx, newRow, prevRow, prevRowAvailable, err);

        }
        finally {
            ddlLock.readLock().unlock();
        }

        if (err != null)
            throw err;
    }

    /**
     * Index row cache.
     *
     * @param grpId Cache group id.
     * @return Index row cache.
     */
    public IndexRowCache rowCacheCleaner(int grpId) {
        return idxRowCacheRegistry.forGroup(grpId);
    }

    /**
     * Index row cache registry.
     *
     * @return Index row cache registry.
     */
    public IndexRowCacheRegistry idxRowCacheRegistry() {
        return idxRowCacheRegistry;
    }

    /**
     * Mark/unmark for rebuild indexes for a specific cache.
     */
    public void markRebuildIndexesForCache(GridCacheContext<?, ?> cctx, boolean val) {
        ddlLock.readLock().lock();

        try {
            if (!cacheToIdx.containsKey(cctx.name()))
                return;

            Collection<Index> idxs = cacheToIdx.get(cctx.name()).values();

            for (Index idx: idxs) {
                if (idx instanceof AbstractIndex)
                    ((AbstractIndex)idx).markIndexRebuild(val);
            }

        }
        finally {
            ddlLock.readLock().unlock();
        }
    }

    /**
     * Start rebuild of indexes for specified cache.
     *
     * @param cctx Cache context.
     * @param force Force rebuild indexes.
     * @return A future of rebuilding cache indexes.
     */
    @Nullable public IgniteInternalFuture<?> rebuildIndexesForCache(
        GridCacheContext<?, ?> cctx,
        boolean force,
        IndexRebuildCancelToken cancelTok
    ) {
        return idxRebuild.rebuild(cctx, force, cancelTok);
    }

    /** */
    public IndexesRebuildTask idxRebuild() {
        return idxRebuild;
    }

    /**
     * Returns collection of indexes for specified cache.
     *
     * @param cctx Cache context.
     * @return Collection of indexes for specified cache.
     */
    public Collection<Index> indexes(GridCacheContext<?, ?> cctx) {
        ddlLock.readLock().lock();

        try {
            Map<String, Index> idxs = cacheToIdx.get(cctx.name());

            if (idxs == null)
                return Collections.emptyList();

            return idxs.values();

        }
        finally {
            ddlLock.readLock().unlock();
        }
    }

    /**
     * Returns index for specified name.
     *
     * @param idxName Index name.
     * @return Index for specified index name or {@code null} if not found.
     */
    public @Nullable Index index(IndexName idxName) {
        ddlLock.readLock().lock();

        try {
            Map<String, Index> idxs = cacheToIdx.get(idxName.cacheName());

            if (idxs == null)
                return null;

            return idxs.get(idxName.fullName());
        }
        finally {
            ddlLock.readLock().unlock();
        }
    }

    /**
     * Returns IndexDefinition used for creating index specified id.
     *
     * @param idxId UUID of index.
     * @return IndexDefinition used for creating index with id {@code idxId}.
     */
    public IndexDefinition indexDefinition(UUID idxId) {
        return idxDefs.get(idxId);
    }

    /**
     * Unregisters cache.
     *
     * @param cacheInfo Cache context info.
     */
    public void unregisterCache(GridCacheContextInfo cacheInfo) {
        idxRowCacheRegistry.onCacheUnregistered(cacheInfo);

        idxRebuild.stopRebuild(cacheInfo, log);
    }

    /**
     * Add row to index.
     * @param idx Index to add row to.
     * @param row Row to add to index.
     * @param prevRow Previous row state, if any.
     * @param prevRowAvailable Whether previous row is available.
     * @param prevErr Error on index add.
     */
    private IgniteCheckedException updateIndex(
        Index idx, CacheDataRow row, CacheDataRow prevRow, boolean prevRowAvailable, IgniteCheckedException prevErr
    ) throws IgniteCheckedException {
        try {
            if (row != null && !idx.canHandle(row))
                return prevErr;

            if (prevRow != null && !idx.canHandle(prevRow))
                return prevErr;

            idx.onUpdate(prevRow, row, prevRowAvailable);

            return prevErr;
        }
        catch (Throwable t) {
            IgniteSQLException ex = X.cause(t, IgniteSQLException.class);

            if (ex != null && ex.statusCode() == IgniteQueryErrorCode.FIELD_TYPE_MISMATCH) {
                if (prevErr != null) {
                    prevErr.addSuppressed(t);

                    return prevErr;
                }
                else
                    return new IgniteCheckedException("Error on add row to index.", t);
            }
            else
                throw t;
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
     * @param mvccEnabled Whether mvcc is enabled.
     * @throws IgniteCheckedException If failed.
     */
    public void destroyOrphanIndex(
        GridKernalContext ctx,
        RootPage page,
        String indexName,
        int grpId,
        PageMemory pageMemory,
        GridAtomicLong removeId,
        ReuseList reuseList,
        boolean mvccEnabled) throws IgniteCheckedException {

        assert ctx.cache().context().database().checkpointLockIsHeldByThread();

        long metaPageId = page.pageId().pageId();

        int inlineSize = inlineSize(page, grpId, pageMemory);

        String grpName = ctx.cache().cacheGroup(grpId).cacheOrGroupName();

        BPlusTree<IndexRow, IndexRow> tree = new BPlusTree<IndexRow, IndexRow>(
            grpName + "IndexTree##" + indexName,
            grpId,
            grpName,
            pageMemory,
            ctx.cache().context().wal(),
            removeId,
            metaPageId,
            reuseList,
            AbstractInlineInnerIO.versions(inlineSize, mvccEnabled),
            AbstractInlineLeafIO.versions(inlineSize, mvccEnabled),
            PageIdAllocator.FLAG_IDX,
            ctx.failure(),
            ctx.cache().context().diagnostic().pageLockTracker()
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
     * Collect indexes for rebuild.
     *
     * @param cctx Cache context.
     * @param createdOnly Get only created indexes (not restored from dick).
     */
    public List<InlineIndex> treeIndexes(GridCacheContext cctx, boolean createdOnly) {
        Collection<Index> idxs = indexes(cctx);

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
     * @return Logger.
     */
    public IgniteLogger logger() {
        return log;
    }
}
