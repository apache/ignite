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
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyDefinition;
import org.apache.ignite.internal.cache.query.index.sorted.IndexValueCursor;
import org.apache.ignite.internal.cache.query.index.sorted.SortedIndexDefinition;
import org.apache.ignite.internal.cache.query.index.sorted.inline.io.IndexRow;
import org.apache.ignite.internal.cache.query.index.sorted.inline.io.IndexRowImpl;
import org.apache.ignite.internal.cache.query.index.sorted.inline.io.IndexSearchRow;
import org.apache.ignite.internal.cache.query.index.sorted.inline.io.IndexSearchRowImpl;
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
    private final GridCacheContext cctx;

    /** */
    private final IoStatisticsHolderIndex stats;

    /** Constructor. */
    public InlineIndexImpl(GridCacheContext cctx, SortedIndexDefinition def, InlineIndexTree[] segments,
        IoStatisticsHolderIndex stats) {
        this.cctx = cctx;
        this.segments = segments.clone();
        this.def = def;
        treeName = def.getTreeName();
        this.stats = stats;
    }

    /** {@inheritDoc} */
    @Override public GridCursor<IndexSearchRow> find(IndexKey lower, IndexKey upper, int segment) throws IgniteCheckedException {
        return find(lower, upper, segment, null);
    }

    /** {@inheritDoc} */
    @Override public GridCursor<IndexSearchRow> find(IndexKey lower, IndexKey upper, int segment, IndexingQueryFilter filter) throws IgniteCheckedException {
        validateConditions(lower, upper);

        InlineTreeFilterClosure closure = getFilterClosure(filter);

        IndexSearchRowImpl rlower = lower == null ? null : new IndexSearchRowImpl(lower.keys(), def.getSchema());
        IndexSearchRowImpl rupper = upper == null ? null : new IndexSearchRowImpl(upper.keys(), def.getSchema());

        // If it is known that only one row will be returned an optimization is employed
        if (isSingleRowLookup(rlower, rupper)) {
            try {
                ThreadLocalSchemaHolder.setSchema(def.getSchema());

                IndexRowImpl row = segments[segment].findOne(rlower, closure, null);

                if (row == null || isExpired(row))
                    return IndexValueCursor.EMPTY;

                return new SingleCursor<>(row);

            } finally {
                ThreadLocalSchemaHolder.cleanSchema();
            }
        }

        try {
            ThreadLocalSchemaHolder.setSchema(def.getSchema());

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
    private boolean isSingleRowLookup(IndexSearchRowImpl lower, IndexSearchRowImpl upper) throws IgniteCheckedException {
        return lower != null && lower.isFullSchemaSearch() && checkRowsTheSame(lower, upper);
    }

    /**
     * Checks both rows are the same. <p/>
     * Primarly used to verify both search rows are the same and we can apply
     * the single row lookup optimization.
     *
     * @param r1 The first row.
     * @param r2 Another row.
     * @return {@code true} in case both rows are efficiently the same, {@code false} otherwise.
     */
    private boolean checkRowsTheSame(IndexSearchRowImpl r1, IndexSearchRowImpl r2) throws IgniteCheckedException {
        if (r1 == r2)
            return true;

        if (!(r1 != null && r2 != null))
            return false;

        IndexKeyDefinition[] keys = def.getSchema().getKeyDefinitions();

        for (int i = 0, len = keys.length; i < len; i++) {
            Object v1 = r1.keys()[i];
            Object v2 = r2.keys()[i];

            if (v1 == null && v2 == null)
                continue;

            if (!(v1 != null && v2 != null))
                return false;

            if (def.getRowComparator().compareKey(r1, r2, i) != 0)
                return false;
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public GridCursor<IndexSearchRow> findFirstOrLast(
        boolean firstOrLast, int segment, IndexingQueryFilter filter) throws IgniteCheckedException {

        try {
            ThreadLocalSchemaHolder.setSchema(def.getSchema());

            InlineTreeFilterClosure closure = getFilterClosure(filter);

            IndexSearchRow found = firstOrLast ? segments[segment].findFirst(closure) : segments[segment].findLast(closure);

            if (found == null || isExpired(found))
                return IndexValueCursor.EMPTY;

            return new SingleCursor(found);

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

            ThreadLocalSchemaHolder.setSchema(def.getSchema());

            boolean replaced = false;

            // Create or Update.
            if (newRow != null && belongsToIndex(newRow)) {
                int segment = segmentForRow(newRow);

                IndexRowImpl row0 = new IndexRowImpl(def.getSchema(), newRow);

                if (prevRowAvailable && !rebuildInProgress())
                    replaced = segments[segment].putx(row0);
                else {
                    IndexSearchRow prevRow0 = segments[segment].put(row0);

                    replaced = prevRow0 != null;
                }
            }

            // Delete.
            if (!replaced && oldRow != null && belongsToIndex(oldRow)) {
                int segment = segmentForRow(oldRow);

                segments[segment].remove(new IndexRowImpl(def.getSchema(), oldRow));
            }

        } finally {
            ThreadLocalSchemaHolder.cleanSchema();
        }
    }

    /** {@inheritDoc} */
    @Override public CacheDataRow put(CacheDataRow row) throws IgniteCheckedException {
        if (!belongsToIndex(row))
            return null;

        int segment = segmentForRow(row);

        try {
            ThreadLocalSchemaHolder.setSchema(def.getSchema());

            IndexRow oldRow = segments[segment].put(new IndexRowImpl(def.getSchema(), row));

            if (oldRow != null)
                return oldRow.getCacheDataRow();

            return null;

        } finally {
            ThreadLocalSchemaHolder.cleanSchema();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean putx(CacheDataRow row) throws IgniteCheckedException {
        if (!belongsToIndex(row))
            return false;

        int segment = segmentForRow(row);

        try {
            ThreadLocalSchemaHolder.setSchema(def.getSchema());

            IndexRowImpl r = new IndexRowImpl(def.getSchema(), row);

            // Validate all keys.
            for (int i = 0; i < def.getSchema().getKeyDefinitions().length; ++i)
                r.getKey(i);

            return segments[segment].putx(r);
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
        if (!belongsToIndex(row))
            return false;

        int segment = segmentForRow(row);

        return segments[segment].removex(new IndexRowImpl(def.getSchema(), row));
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

    /** */
    private void validateConditions(IndexKey lower, IndexKey upper) {
        if (lower != null && upper != null && lower.keys().length != upper.keys().length)
            throw new IgniteException("Number of key conditions differs from left and right.");

        if (lower != null && lower.keys().length > def.getSchema().getKeyDefinitions().length)
            throw new IgniteException("Number of conditions differs from index functions.");

        if (upper != null && upper.keys().length > def.getSchema().getKeyDefinitions().length)
            throw new IgniteException("Number of conditions differs from index functions.");
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
        return segmentsCount() == 1 ? 0 : (def.getSchema().partition(row) % segmentsCount());
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

        GridKernalContext ctx = cctx.kernalContext();

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

    /**
     * @return whether cache row belongs to this index.
     */
    private boolean belongsToIndex(CacheDataRow row) throws IgniteCheckedException {
        return cctx.kernalContext().query().belongsToTable(
            cctx, def.getIdxName().cacheName(), def.getIdxName().tableName(), row.key(), row.value());
    }
}
