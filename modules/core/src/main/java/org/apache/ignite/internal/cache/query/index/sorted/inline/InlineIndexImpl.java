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

package org.apache.ignite.internal.cache.query.index.sorted.inline;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.query.index.AbstractIndex;
import org.apache.ignite.cache.query.index.Index;
import org.apache.ignite.cache.query.index.SingleCursor;
import org.apache.ignite.cache.query.index.sorted.IndexKey;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.internal.cache.query.index.sorted.IndexValueCursor;
import org.apache.ignite.internal.cache.query.index.sorted.InlineIndexRowHandler;
import org.apache.ignite.internal.cache.query.index.sorted.SortedIndexDefinition;
import org.apache.ignite.internal.cache.query.index.sorted.inline.io.IndexRow;
import org.apache.ignite.internal.cache.query.index.sorted.inline.io.IndexRowImpl;
import org.apache.ignite.internal.cache.query.index.sorted.inline.io.ThreadLocalSchemaHolder;
import org.apache.ignite.internal.metric.IoStatisticsHolderIndex;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.pendingtask.DurableBackgroundTask;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.indexing.IndexingQueryCacheFilter;
import org.apache.ignite.spi.indexing.IndexingQueryFilter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.failure.FailureType.CRITICAL_ERROR;

/**
 * Sorted index implementation.
 */
public class InlineIndexImpl extends AbstractIndex implements InlineIndex {
    /** Unique ID. */
    private final UUID id = UUID.randomUUID();

    /** Segments. */
    private final InlineIndexTree[] segments;

    /** Index function. */
    private final SortedIndexDefinition def;

    /** Name of underlying tree name. */
    private final String treeName;

    /** Cache context. */
    private final GridCacheContext<?, ?> cctx;

    /** */
    private final IoStatisticsHolderIndex stats;

    /** Row handler. */
    private final InlineIndexRowHandler rowHnd;

    /** Constructor. */
    public InlineIndexImpl(GridCacheContext<?, ?> cctx, SortedIndexDefinition def, InlineIndexTree[] segments,
        IoStatisticsHolderIndex stats) {
        this.cctx = cctx;
        this.segments = segments.clone();
        this.def = def;
        treeName = def.getTreeName();
        this.stats = stats;
        rowHnd = segments[0].getRowHandler();
    }

    /** {@inheritDoc} */
    @Override public GridCursor<IndexRow> find(IndexKey lower, IndexKey upper, int segment) throws IgniteCheckedException {
        return find(lower, upper, segment, null);
    }

    /** {@inheritDoc} */
    @Override public GridCursor<IndexRow> find(IndexKey lower, IndexKey upper, int segment, IndexingQueryFilter filter) throws IgniteCheckedException {
        InlineTreeFilterClosure closure = getFilterClosure(filter);

        IndexRow rlower = (IndexRow) lower;
        IndexRow rupper = (IndexRow) upper;

        // If it is known that only one row will be returned an optimization is employed
        if (isSingleRowLookup(rlower, rupper)) {
            try {
                ThreadLocalSchemaHolder.setSchema(rowHnd);

                IndexRowImpl row = segments[segment].findOne(rlower, closure, null);

                if (row == null || isExpired(row))
                    return IndexValueCursor.EMPTY;

                return new SingleCursor<>(row);

            } finally {
                ThreadLocalSchemaHolder.cleanSchema();
            }
        }

        try {
            ThreadLocalSchemaHolder.setSchema(rowHnd);

            return segments[segment].find(rlower, rupper, closure, null);

        } finally {
            ThreadLocalSchemaHolder.cleanSchema();
        }
    }

    /** {@inheritDoc} */
    @Override public long count(int segment) throws IgniteCheckedException {
        return segments[segment].size();
    }

    /** {@inheritDoc} */
    @Override public long count(int segment, IndexingQueryFilter filter) throws IgniteCheckedException {
        return segments[segment].size(getFilterClosure(filter));
    }

    /**
     * Returns number of elements in the tree by scanning pages of the bottom (leaf) level.
     *
     * @return Number of elements in the tree.
     * @throws IgniteCheckedException If failed.
     */
    @Override public long totalCount() throws IgniteCheckedException {
        long ret = 0;

        for (int i = 0; i < segmentsCount(); i++)
            ret += segments[i].size();

        return ret;
    }

    /** */
    private boolean isSingleRowLookup(IndexKey lower, IndexKey upper) throws IgniteCheckedException {
        return def.isPrimary() && lower != null && isFullSchemaSearch(lower) && checkRowsTheSame(lower, upper);
    }

    /**
     * If {@code true} then length of keys for search must be equal to length of schema, so use full
     * schema to search. If {@code false} then it's possible to use only part of schema for search.
     */
    private boolean isFullSchemaSearch(IndexKey key) {
        int schemaLength = def.getIndexKeyDefinitions().size();

        for (int i = 0; i < schemaLength; i++) {
            // Java null means that column is not specified in a search row, for SQL NULL a special constant is used
            if (key.getKey(i) == null)
                return false;
        }

        return true;
    }

    /**
     * Checks both rows are the same.
     * <p/>
     * Primarly used to verify if the single row lookup optimization can be applied.
     *
     * @param r1 The first row.
     * @param r2 Another row.
     * @return {@code true} in case both rows are efficiently the same, {@code false} otherwise.
     */
    private boolean checkRowsTheSame(IndexKey r1, IndexKey r2) throws IgniteCheckedException {
        if (r1 == r2)
            return true;

        if (!(r1 != null && r2 != null))
            return false;

        int keysLen = def.getIndexKeyDefinitions().size();

        for (int i = 0; i < keysLen; i++) {
            Object v1 = r1.getKey(i);
            Object v2 = r2.getKey(i);

            if (v1 == null && v2 == null)
                continue;

            if (!(v1 != null && v2 != null))
                return false;

            if (def.getRowComparator().compareKey((IndexRow) r1, (IndexRow) r2, i) != 0)
                return false;
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public GridCursor<IndexRow> findFirst(int segment, IndexingQueryFilter filter) throws IgniteCheckedException {
        try {
            ThreadLocalSchemaHolder.setSchema(rowHnd);

            InlineTreeFilterClosure closure = getFilterClosure(filter);

            IndexRow found = segments[segment].findFirst(closure);

            if (found == null || isExpired(found))
                return IndexValueCursor.EMPTY;

            return new SingleCursor<>(found);

        } finally {
            ThreadLocalSchemaHolder.cleanSchema();
        }
    }

    /** {@inheritDoc} */
    @Override public GridCursor<IndexRow> findLast(int segment, IndexingQueryFilter filter) throws IgniteCheckedException {
        try {
            ThreadLocalSchemaHolder.setSchema(rowHnd);

            InlineTreeFilterClosure closure = getFilterClosure(filter);

            IndexRow found = segments[segment].findLast(closure);

            if (found == null || isExpired(found))
                return IndexValueCursor.EMPTY;

            return new SingleCursor<>(found);

        } finally {
            ThreadLocalSchemaHolder.cleanSchema();
        }
    }

    /** {@inheritDoc} */
    @Override public UUID id() {
        return id;
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return def.getIdxName().idxName();
    }

    /** {@inheritDoc} */
    @Override public void onUpdate(@Nullable CacheDataRow oldRow, @Nullable CacheDataRow newRow,
        boolean prevRowAvailable) throws IgniteCheckedException {
        try {
            if (destroyed.get())
                return;

            ThreadLocalSchemaHolder.setSchema(rowHnd);

            boolean replaced = false;

            // Create or Update.
            if (newRow != null) {
                int segment = segmentForRow(newRow);

                IndexRowImpl row0 = new IndexRowImpl(rowHnd, newRow);

                if (prevRowAvailable && !rebuildInProgress())
                    replaced = segments[segment].putx(row0);
                else {
                    IndexRow prevRow0 = segments[segment].put(row0);

                    replaced = prevRow0 != null;
                }
            }

            // Delete.
            if (!replaced && oldRow != null) {
                int segment = segmentForRow(oldRow);

                segments[segment].remove(new IndexRowImpl(rowHnd, oldRow));
            }

        } finally {
            ThreadLocalSchemaHolder.cleanSchema();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean putx(CacheDataRow row) throws IgniteCheckedException {
        IndexRowImpl r = new IndexRowImpl(rowHnd, row);

        return putx(r);
    }

    /**
     * Put index row to index.
     *
     * @param row Index row.
     * @return {@code True} if replaced existing row.
     */
    public boolean putx(IndexRowImpl row) throws IgniteCheckedException {
        int segment = segmentForRow(row.getCacheDataRow());

        // Validate all keys.
        for (int i = 0; i < def.getIndexKeyDefinitions().size(); ++i)
            row.getKey(i);

        try {
            ThreadLocalSchemaHolder.setSchema(rowHnd);

            return segments[segment].putx(row);
        }
        catch (Throwable t) {
            cctx.kernalContext().failure().process(new FailureContext(CRITICAL_ERROR, t));

            throw t;
        }
        finally {
            ThreadLocalSchemaHolder.cleanSchema();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean removex(CacheDataRow row) throws IgniteCheckedException {
        int segment = segmentForRow(row);

        return segments[segment].removex(new IndexRowImpl(rowHnd, row));
    }

    /** {@inheritDoc} */
    @Override public <T extends Index> T unwrap(Class<T> clazz) {
        if (clazz == null)
            return null;

        if (clazz.isAssignableFrom(getClass()))
            return clazz.cast(this);

        throw new IllegalArgumentException(
            String.format("Cannot unwrap [%s] to [%s]", getClass().getName(), clazz.getName())
        );
    }

    /** {@inheritDoc} */
    @Override public int inlineSize() {
        return segments[0].getInlineSize();
    }

    /** {@inheritDoc} */
    @Override public int segmentsCount() {
        return segments.length;
    }

    /**
     * @param row cache row.
     * @return Segment ID for given key
     */
    public int segmentForRow(CacheDataRow row) {
        return segmentsCount() == 1 ? 0 : (rowHnd.partition(row) % segmentsCount());
    }

    /** */
    public InlineTreeFilterClosure getFilterClosure(IndexingQueryFilter filter) {
        if (filter != null) {
            IndexingQueryCacheFilter f = filter.forCache(cctx.cache().name());
            if (f != null)
                return new InlineTreeFilterClosure(f);
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean isCreated() {
        assert segments != null;

        for (int i = 0; i < segments.length; i++) {
            try {
                InlineIndexTree segment = segments[i];

                if (segment.isCreated())
                    return true;
            }
            catch (Exception e) {
                throw new IgniteException("Failed to check index tree root page existence [cacheName=" +
                    cctx.name() + ", tblName=" + def.getIdxName().tableName() + ", idxName=" + def.getIdxName().idxName() +
                    ", segment=" + i + ']');
            }
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public InlineIndexTree getSegment(int segment) {
        return segments[segment];
    }

    /**
     * Determines if provided row can be treated as expired at the current moment.
     *
     * @param row row to check.
     * @throws NullPointerException if provided row is {@code null}.
     */
    private static boolean isExpired(@NotNull IndexRow row) {
        return row.getCacheDataRow().expireTime() > 0 && row.getCacheDataRow().expireTime() <= U.currentTimeMillis();
    }

    /** If {code true} then this index is already marked as destroyed. */
    private final AtomicBoolean destroyed = new AtomicBoolean();

    /** {@inheritDoc} */
    @Override public void destroy(boolean softDelete) {
        // Already destroyed.
        if (!destroyed.compareAndSet(false, true))
            return;

        try {
            if (cctx.affinityNode() && !softDelete) {
                List<Long> rootPages = new ArrayList<>(segments.length);
                List<InlineIndexTree> trees = new ArrayList<>(segments.length);

                cctx.shared().database().checkpointReadLock();

                try {
                    for (int i = 0; i < segments.length; i++) {
                        InlineIndexTree tree = segments[i];

                        // Just mark it as destroyed. Actual destroy later in background task.
                        tree.markDestroyed();

                        rootPages.add(tree.getMetaPageId());
                        trees.add(tree);

                        dropMetaPage(i);
                    }
                }
                finally {
                    cctx.shared().database().checkpointReadUnlock();
                }

                cctx.kernalContext().metric().remove(stats.metricRegistryName());

                // Actual destroy index task.
                DurableBackgroundTask task = new DurableBackgroundCleanupIndexTreeTask(
                    rootPages,
                    trees,
                    cctx.group().name(),
                    cctx.cache().name(),
                    def.getIdxName()
                );

                cctx.kernalContext().durableBackgroundTasksProcessor().startDurableBackgroundTask(task, cctx.config());
            }
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /**
     * @param segIdx Segment index.
     * @throws IgniteCheckedException If failed.
     */
    private void dropMetaPage(int segIdx) throws IgniteCheckedException {
        cctx.offheap().dropRootPageForIndex(cctx.cacheId(), treeName, segIdx);
    }

    /** {@inheritDoc} */
    @Override public boolean canHandle(CacheDataRow row) throws IgniteCheckedException {
        return cctx.kernalContext().query().belongsToTable(
            cctx, def.getIdxName().cacheName(), def.getIdxName().tableName(), row.key(), row.value());
    }
}
