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

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.internal.cache.query.index.AbstractIndex;
import org.apache.ignite.internal.cache.query.index.SingleCursor;
import org.apache.ignite.internal.cache.query.index.SortOrder;
import org.apache.ignite.internal.cache.query.index.sorted.DurableBackgroundCleanupIndexTreeTaskV2;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyDefinition;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyTypeSettings;
import org.apache.ignite.internal.cache.query.index.sorted.IndexRow;
import org.apache.ignite.internal.cache.query.index.sorted.IndexRowComparator;
import org.apache.ignite.internal.cache.query.index.sorted.IndexRowImpl;
import org.apache.ignite.internal.cache.query.index.sorted.IndexValueCursor;
import org.apache.ignite.internal.cache.query.index.sorted.InlineIndexRowHandler;
import org.apache.ignite.internal.cache.query.index.sorted.SortedIndexDefinition;
import org.apache.ignite.internal.cache.query.index.sorted.ThreadLocalRowHandlerHolder;
import org.apache.ignite.internal.metric.IoStatisticsHolderIndex;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.mvcc.MvccSnapshot;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.indexing.IndexingQueryCacheFilter;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cluster.ClusterState.INACTIVE;
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

    /** */
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

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
    @Override public GridCursor<IndexRow> find(
        IndexRow lower,
        IndexRow upper,
        boolean lowIncl,
        boolean upIncl,
        int segment,
        IndexQueryContext qryCtx
    ) throws IgniteCheckedException {
        InlineTreeFilterClosure closure = filterClosure(qryCtx);

        lock.readLock().lock();

        try {
            // If it is known that only one row will be returned an optimization is employed
            if (isSingleRowLookup(lower, upper)) {
                IndexRowImpl row = segments[segment].findOne(lower, closure, null);

                if (row == null || isExpired(row))
                    return IndexValueCursor.EMPTY;

                return new SingleCursor<>(row);
            }

            return segments[segment].find(lower, upper, lowIncl, upIncl, closure, null);
        }
        finally {
            lock.readLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public GridCursor<IndexRow> takeFirstOrLast(IndexQueryContext qryCtx, boolean first)
        throws IgniteCheckedException {
        lock.readLock().lock();

        try {
            int segmentsCnt = segmentsCount();

            final GridCursor<IndexRow>[] segmentCursors = new GridCursor[segmentsCnt];

            for (int i = 0; i < segmentsCnt; i++)
                segmentCursors[i] = first ? findFirst(i, qryCtx) : findLast(i, qryCtx);

            return new SingleValueSegmentedIndexCursor(segmentCursors, def);
        }
        finally {
            lock.readLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public GridCursor<IndexRow> find(
        IndexRow lower,
        IndexRow upper,
        boolean lowIncl,
        boolean upIncl,
        IndexQueryContext qryCtx
    ) throws IgniteCheckedException {
        lock.readLock().lock();

        try {
            int segmentsCnt = segmentsCount();

            if (segmentsCnt == 1)
                return find(lower, upper, lowIncl, upIncl, 0, qryCtx);

            final GridCursor<IndexRow>[] segmentCursors = new GridCursor[segmentsCnt];

            for (int i = 0; i < segmentsCnt; i++)
                segmentCursors[i] = find(lower, upper, lowIncl, upIncl, i, qryCtx);

            return new SegmentedIndexCursor(segmentCursors, def);
        }
        finally {
            lock.readLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public long count(int segment) throws IgniteCheckedException {
        lock.readLock().lock();

        try {
            return segments[segment].size();
        }
        finally {
            lock.readLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public long count(int segment, IndexQueryContext qryCtx) throws IgniteCheckedException {
        lock.readLock().lock();

        try {
            return segments[segment].size(filterClosure(qryCtx));
        }
        finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Returns number of elements in the tree by scanning pages of the bottom (leaf) level.
     *
     * @return Number of elements in the tree.
     * @throws IgniteCheckedException If failed.
     */
    @Override public long totalCount() throws IgniteCheckedException {
        lock.readLock().lock();

        try {
            long ret = 0;

            for (int i = 0; i < segmentsCount(); i++)
                ret += segments[i].size();

            return ret;
        }
        finally {
            lock.readLock().unlock();
        }
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

            if (def.rowComparator().compareRow(r1, r2, i) != 0)
                return false;
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public GridCursor<IndexRow> findFirst(int segment, IndexQueryContext qryCtx) throws IgniteCheckedException {
        InlineTreeFilterClosure closure = filterClosure(qryCtx);

        lock.readLock().lock();

        try {
            IndexRow found = segments[segment].findFirst(closure);

            if (found == null || isExpired(found))
                return IndexValueCursor.EMPTY;

            return new SingleCursor<>(found);
        }
        finally {
            lock.readLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public GridCursor<IndexRow> findLast(int segment, IndexQueryContext qryCtx) throws IgniteCheckedException {
        InlineTreeFilterClosure closure = filterClosure(qryCtx);

        lock.readLock().lock();

        try {
            IndexRow found = segments[segment].findLast(closure);

            if (found == null || isExpired(found))
                return IndexValueCursor.EMPTY;

            return new SingleCursor<>(found);
        }
        finally {
            lock.readLock().unlock();
        }
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

        }
        finally {
            ThreadLocalRowHandlerHolder.clearRowHandler();
        }
    }

    /** */
    private boolean putx(IndexRowImpl idxRow, int segment, boolean flag) throws IgniteCheckedException {
        lock.readLock().lock();

        try {
            boolean replaced;

            if (flag)
                replaced = segments[segment].putx(idxRow);
            else {
                IndexRow prevRow0 = segments[segment].put(idxRow);

                replaced = prevRow0 != null;
            }

            return replaced;

        }
        catch (Throwable t) {
            cctx.kernalContext().failure().process(new FailureContext(CRITICAL_ERROR, t));

            throw t;
        }
        finally {
            lock.readLock().unlock();
        }
    }

    /** */
    private void remove(CacheDataRow row) throws IgniteCheckedException {
        lock.readLock().lock();

        try {
            int segment = segmentForRow(row);

            IndexRowImpl idxRow = new IndexRowImpl(rowHnd, row);

            idxRow.prepareCache();

            segments[segment].removex(idxRow);

        }
        catch (Throwable t) {
            cctx.kernalContext().failure().process(new FailureContext(CRITICAL_ERROR, t));

            throw t;
        }
        finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Put index row to index. This method is for internal use only.
     *
     * @param row Index row.
     */
    public void putIndexRow(IndexRowImpl row) throws IgniteCheckedException {
        int segment = segmentForRow(row.cacheDataRow());

        lock.readLock().lock();

        try {
            ThreadLocalRowHandlerHolder.rowHandler(rowHnd);

            segments[segment].putx(row);
        }
        finally {
            lock.readLock().unlock();
            ThreadLocalRowHandlerHolder.clearRowHandler();
        }
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

        IndexingQueryCacheFilter cacheFilter = qryCtx.cacheFilter() == null ? null
            : qryCtx.cacheFilter().forCache(cctx.cache().name());

        MvccSnapshot v = qryCtx.mvccSnapshot();

        assert !cctx.mvccEnabled() || v != null;

        if (cacheFilter == null && v == null && qryCtx.rowFilter() == null)
            return null;

        return new InlineTreeFilterClosure(
            cacheFilter, qryCtx.rowFilter(), v, cctx, cctx.kernalContext().config().getGridLogger());
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
    @Override public void destroy(boolean softDel) {
        try {
            destroy0(softDel);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /**
     * Destroys the index and if {@code renameImmediately} is {@code true} renames index trees.
     *
     * @param softDel If {@code true} then perform logical deletion.
     * @throws IgniteCheckedException If failed to rename index trees.
     */
    private void destroy0(boolean softDel) throws IgniteCheckedException {
        // Already destroyed.
        if (!destroyed.compareAndSet(false, true))
            return;

        if (cctx.affinityNode() && !softDel) {
            lock.writeLock().lock();

            try {
                for (InlineIndexTree segment : segments) {
                    segment.markDestroyed();

                    segment.close();
                }

                cctx.kernalContext().metric().remove(stats.metricRegistryName());

                if (cctx.group().persistenceEnabled() ||
                    cctx.shared().kernalContext().state().clusterState().state() != INACTIVE) {
                    // Actual destroy index task.
                    DurableBackgroundCleanupIndexTreeTaskV2 task = new DurableBackgroundCleanupIndexTreeTaskV2(
                        cctx.group().name(),
                        cctx.name(),
                        def.idxName().idxName(),
                        treeName,
                        UUID.randomUUID().toString(),
                        segments.length,
                        segments
                    );

                    // In maintenance mode, durable task is not started immediately (only after restart and activation),
                    // but to rebuild the index we need to create a new tree, and old tree should be renamed prior to this.
                    if (cctx.kernalContext().maintenanceRegistry().isMaintenanceMode())
                        task.renameIndexTrees(cctx.group());

                    cctx.kernalContext().durableBackgroundTask().executeAsync(task, cctx.config());
                }
            }
            finally {
                lock.writeLock().unlock();
            }
        }
    }

    /** {@inheritDoc} */
    @Override public boolean canHandle(CacheDataRow row) throws IgniteCheckedException {
        return cctx.kernalContext().query().belongsToTable(
            cctx, def.idxName().cacheName(), def.idxName().tableName(), row.key(), row.value());
    }

    /**
     * @return Index definition.
     */
    public SortedIndexDefinition indexDefinition() {
        return def;
    }

    /** Single cursor over multiple segments. The next value is chosen with the index row comparator. */
    private static class SegmentedIndexCursor implements GridCursor<IndexRow> {
        /** Cursors over segments. */
        private final Queue<GridCursor<IndexRow>> cursors;

        /** Comparator to compare index rows. */
        private final Comparator<GridCursor<IndexRow>> cursorComp;

        /** */
        private IndexRow head;

        /** */
        SegmentedIndexCursor(GridCursor<IndexRow>[] cursors, SortedIndexDefinition idxDef) throws IgniteCheckedException {
            cursorComp = new Comparator<GridCursor<IndexRow>>() {
                private final IndexRowComparator rowComparator = idxDef.rowComparator();

                private final IndexKeyDefinition[] keyDefs =
                    idxDef.indexKeyDefinitions().values().toArray(new IndexKeyDefinition[0]);

                @Override public int compare(GridCursor<IndexRow> o1, GridCursor<IndexRow> o2) {
                    try {
                        int keysLen = o1.get().keys().length;

                        for (int i = 0; i < keysLen; i++) {
                            int cmp = rowComparator.compareRow(o1.get(), o2.get(), i);

                            if (cmp != 0) {
                                boolean desc = keyDefs[i].order().sortOrder() == SortOrder.DESC;

                                return desc ? -cmp : cmp;
                            }
                        }

                        return 0;
                    }
                    catch (IgniteCheckedException e) {
                        throw new IgniteException("Failed to sort remote index rows", e);
                    }
                }
            };

            this.cursors = cursorsQueue(cursors);
        }

        /** {@inheritDoc} */
        @Override public boolean next() throws IgniteCheckedException {
            if (cursors.isEmpty())
                return false;

            GridCursor<IndexRow> c = cursors.poll();

            head = c.get();

            if (c.next())
                cursors.add(c);

            return true;
        }

        /** {@inheritDoc} */
        @Override public IndexRow get() throws IgniteCheckedException {
            return head;
        }

        /** */
        protected Queue<GridCursor<IndexRow>> cursorsQueue(GridCursor<IndexRow>[] cursors)
            throws IgniteCheckedException {
            PriorityQueue<GridCursor<IndexRow>> q = new PriorityQueue<>(cursors.length, cursorComp);

            for (GridCursor<IndexRow> c: cursors) {
                if (c.next())
                    q.add(c);
            }

            return q;
        }
    }

    /** First-only, single-value-only segmented cursor. */
    private static class SingleValueSegmentedIndexCursor extends SegmentedIndexCursor {
        /** Ctor. */
        SingleValueSegmentedIndexCursor(
            GridCursor<IndexRow>[] cursors,
            SortedIndexDefinition idxDef
        ) throws IgniteCheckedException {
            super(cursors, idxDef);
        }

        /** {@inheritDoc} */
        @Override protected Queue<GridCursor<IndexRow>> cursorsQueue(GridCursor<IndexRow>[] cursors)
            throws IgniteCheckedException {
            Queue<GridCursor<IndexRow>> srcQueue = super.cursorsQueue(cursors);

            if (!srcQueue.isEmpty()) {
                GridCursor<IndexRow> cur = srcQueue.poll();

                IndexRow row = cur.get();

                assert !cur.next();

                return new ArrayDeque<>(Collections.singleton(new SingleCursor<>(row)));
            }

            return srcQueue;
        }
    }
}
