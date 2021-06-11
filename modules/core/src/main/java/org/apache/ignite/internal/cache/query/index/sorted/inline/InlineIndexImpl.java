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
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.internal.cache.query.index.AbstractIndex;
import org.apache.ignite.internal.cache.query.index.Index;
import org.apache.ignite.internal.cache.query.index.SingleCursor;
import org.apache.ignite.internal.cache.query.index.sorted.DurableBackgroundCleanupIndexTreeTask;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyTypeSettings;
import org.apache.ignite.internal.cache.query.index.sorted.IndexRow;
import org.apache.ignite.internal.cache.query.index.sorted.IndexRowImpl;
import org.apache.ignite.internal.cache.query.index.sorted.IndexValueCursor;
import org.apache.ignite.internal.cache.query.index.sorted.InlineIndexRowHandler;
import org.apache.ignite.internal.cache.query.index.sorted.SortedIndexDefinition;
import org.apache.ignite.internal.cache.query.index.sorted.ThreadLocalRowHandlerHolder;
import org.apache.ignite.internal.metric.IoStatisticsHolderIndex;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.mvcc.MvccSnapshot;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.pendingtask.DurableBackgroundTask;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.indexing.IndexingQueryCacheFilter;
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
        treeName = def.treeName();
        this.stats = stats;
        rowHnd = segments[0].rowHandler();
    }

    /** {@inheritDoc} */
    @Override public GridCursor<IndexRow> find(IndexRow lower, IndexRow upper, int segment) throws IgniteCheckedException {
        return find(lower, upper, segment, null);
    }

    /** {@inheritDoc} */
    @Override public GridCursor<IndexRow> find(
        IndexRow lower,
        IndexRow upper,
        int segment,
        IndexQueryContext qryCtx
    ) throws IgniteCheckedException {
        InlineTreeFilterClosure closure = filterClosure(qryCtx);

        // If it is known that only one row will be returned an optimization is employed
        if (isSingleRowLookup(lower, upper)) {
            IndexRowImpl row = segments[segment].findOne(lower, closure, null);

            if (row == null || isExpired(row))
                return IndexValueCursor.EMPTY;

            return new SingleCursor<>(row);
        }

        return segments[segment].find(lower, upper, closure, null);
    }

    /** {@inheritDoc} */
    @Override public long count(int segment) throws IgniteCheckedException {
        return segments[segment].size();
    }

    /** {@inheritDoc} */
    @Override public long count(int segment, IndexQueryContext qryCtx) throws IgniteCheckedException {
        return segments[segment].size(filterClosure(qryCtx));
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
    private boolean isSingleRowLookup(IndexRow lower, IndexRow upper) throws IgniteCheckedException {
        return !cctx.mvccEnabled() && def.primary() && lower != null && isFullSchemaSearch(lower) && checkRowsTheSame(lower, upper);
    }

    /**
     * If {@code true} then length of keys for search must be equal to length of schema, so use full
     * schema to search. If {@code false} then it's possible to use only part of schema for search.
     */
    private boolean isFullSchemaSearch(IndexRow key) {
        int schemaLength = def.indexKeyDefinitions().size();

        for (int i = 0; i < schemaLength; i++) {
            // Java null means that column is not specified in a search row, for SQL NULL a special constant is used
            if (key.key(i) == null)
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
    private boolean checkRowsTheSame(IndexRow r1, IndexRow r2) throws IgniteCheckedException {
        if (r1 == r2)
            return true;

        if (!(r1 != null && r2 != null))
            return false;

        int keysLen = def.indexKeyDefinitions().size();

        for (int i = 0; i < keysLen; i++) {
            Object v1 = r1.key(i);
            Object v2 = r2.key(i);

            if (v1 == null && v2 == null)
                continue;

            if (!(v1 != null && v2 != null))
                return false;

            if (def.rowComparator().compareKey((IndexRow) r1, (IndexRow) r2, i) != 0)
                return false;
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public GridCursor<IndexRow> findFirst(int segment, IndexQueryContext qryCtx) throws IgniteCheckedException {
        InlineTreeFilterClosure closure = filterClosure(qryCtx);

        IndexRow found = segments[segment].findFirst(closure);

        if (found == null || isExpired(found))
            return IndexValueCursor.EMPTY;

        return new SingleCursor<>(found);
    }

    /** {@inheritDoc} */
    @Override public GridCursor<IndexRow> findLast(int segment, IndexQueryContext qryCtx) throws IgniteCheckedException {
        InlineTreeFilterClosure closure = filterClosure(qryCtx);

        IndexRow found = segments[segment].findLast(closure);

        if (found == null || isExpired(found))
            return IndexValueCursor.EMPTY;

        return new SingleCursor<>(found);
    }

    /** {@inheritDoc} */
    @Override public UUID id() {
        return id;
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return def.idxName().idxName();
    }

    /** {@inheritDoc} */
    @Override public void onUpdate(@Nullable CacheDataRow oldRow, @Nullable CacheDataRow newRow,
        boolean prevRowAvailable) throws IgniteCheckedException {
        try {
            if (destroyed.get())
                return;

            ThreadLocalRowHandlerHolder.rowHandler(rowHnd);

            boolean replaced = false;

            // Create or Update.
            if (newRow != null) {
                int segment = segmentForRow(newRow);

                IndexRowImpl row0 = new IndexRowImpl(rowHnd, newRow);

                row0.prepareCache();

                // Validate all keys before an actual put. User may specify wrong data types for an insert query.
                for (int i = 0; i < def.indexKeyDefinitions().size(); ++i)
                    row0.key(i);

                replaced = putx(row0, segment, prevRowAvailable && !rebuildInProgress());
            }

            // Delete.
            if (!replaced && oldRow != null)
                remove(oldRow);

        } finally {
            ThreadLocalRowHandlerHolder.clearRowHandler();
        }
    }

    /** */
    private boolean putx(IndexRowImpl idxRow, int segment, boolean flag) throws IgniteCheckedException {
        try {
            boolean replaced;

            if (flag)
                replaced = segments[segment].putx(idxRow);
            else {
                IndexRow prevRow0 = segments[segment].put(idxRow);

                replaced = prevRow0 != null;
            }

            return replaced;

        } catch (Throwable t) {
            cctx.kernalContext().failure().process(new FailureContext(CRITICAL_ERROR, t));

            throw t;
        }
    }

    /** */
    private void remove(CacheDataRow row) throws IgniteCheckedException {
        try {
            int segment = segmentForRow(row);

            IndexRowImpl idxRow = new IndexRowImpl(rowHnd, row);

            idxRow.prepareCache();

            segments[segment].removex(idxRow);

        } catch (Throwable t) {
            cctx.kernalContext().failure().process(new FailureContext(CRITICAL_ERROR, t));

            throw t;
        }
    }

    /**
     * Put index row to index. This method is for internal use only.
     *
     * @param row Index row.
     */
    public void putIndexRow(IndexRowImpl row) throws IgniteCheckedException {
        int segment = segmentForRow(row.cacheDataRow());

        try {
            ThreadLocalRowHandlerHolder.rowHandler(rowHnd);

            segments[segment].putx(row);
        }
        finally {
            ThreadLocalRowHandlerHolder.clearRowHandler();
        }
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
        return segments[0].inlineSize();
    }

    /** */
    public IndexKeyTypeSettings keyTypeSettings() {
        return rowHnd.indexKeyTypeSettings();
    }

    /** {@inheritDoc} */
    @Override public int segmentsCount() {
        return segments.length;
    }

    /**
     * @param row Сache row.
     * @return Segment ID for given key.
     */
    public int segmentForRow(CacheDataRow row) {
        return calculateSegment(segmentsCount(), segmentsCount() == 1 ? 0 : rowHnd.partition(row));
    }

    /**
     * @param segmentsCnt Сount of segments in cache.
     * @param part Partition.
     * @return Segment ID for given segment count and partition.
     */
    public static int calculateSegment(int segmentsCnt, int part) {
        return segmentsCnt == 1 ? 0 : (part % segmentsCnt);
    }

    /** */
    private InlineTreeFilterClosure filterClosure(IndexQueryContext qryCtx) {
        if (qryCtx == null)
            return null;

        IndexingQueryCacheFilter cacheFilter = qryCtx.filter() == null ? null
            : qryCtx.filter().forCache(cctx.cache().name());

        MvccSnapshot v = qryCtx.mvccSnapshot();

        assert !cctx.mvccEnabled() || v != null;

        if (cacheFilter == null && v == null)
            return null;

        return new InlineTreeFilterClosure(
            cacheFilter, v, cctx, cctx.kernalContext().config().getGridLogger());
    }

    /** {@inheritDoc} */
    @Override public boolean created() {
        assert segments != null;

        for (int i = 0; i < segments.length; i++) {
            try {
                InlineIndexTree segment = segments[i];

                if (segment.created())
                    return true;
            }
            catch (Exception e) {
                throw new IgniteException("Failed to check index tree root page existence [cacheName=" +
                    cctx.name() + ", tblName=" + def.idxName().tableName() + ", idxName=" + def.idxName().idxName() +
                    ", segment=" + i + ']');
            }
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public InlineIndexTree segment(int segment) {
        return segments[segment];
    }

    /**
     * Determines if provided row can be treated as expired at the current moment.
     *
     * @param row row to check.
     * @throws NullPointerException if provided row is {@code null}.
     */
    private static boolean isExpired(IndexRow row) {
        return row.cacheDataRow().expireTime() > 0 && row.cacheDataRow().expireTime() <= U.currentTimeMillis();
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
                    cctx.group().name() == null ? cctx.cache().name() : cctx.group().name(),
                    cctx.cache().name(),
                    def.idxName(),
                    treeName
                );

                cctx.kernalContext().durableBackgroundTask().executeAsync(task, cctx.config());
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
            cctx, def.idxName().cacheName(), def.idxName().tableName(), row.key(), row.value());
    }
}
