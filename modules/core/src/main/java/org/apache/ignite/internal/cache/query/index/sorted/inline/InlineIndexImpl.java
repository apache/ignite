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
import org.apache.ignite.cache.query.index.Index;
import org.apache.ignite.cache.query.index.SingleCursor;
import org.apache.ignite.internal.cache.query.index.sorted.IndexValueCursor;
import org.apache.ignite.internal.cache.query.index.sorted.inline.io.IndexRow;
import org.apache.ignite.internal.cache.query.index.sorted.inline.io.IndexRowImpl;
import org.apache.ignite.internal.cache.query.index.sorted.inline.io.IndexSearchRow;
import org.apache.ignite.internal.cache.query.index.sorted.inline.io.IndexSearchRowImpl;
import org.apache.ignite.internal.cache.query.index.sorted.inline.io.ThreadLocalSchemaHolder;
import org.apache.ignite.cache.query.index.sorted.IndexKey;
import org.apache.ignite.internal.cache.query.index.sorted.SortedIndexDefinition;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.pendingtask.DurableBackgroundTask;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.spi.indexing.IndexingQueryCacheFilter;
import org.apache.ignite.spi.indexing.IndexingQueryFilter;
import org.jetbrains.annotations.Nullable;

/**
 * Sorted index implementation.
 */
public class InlineIndexImpl implements InlineIndex {
    /** Unique ID. */
    private final UUID id = UUID.randomUUID();

    /** Segments. */
    private final InlineIndexTree[] segments;

    /** Index function. */
    private final SortedIndexDefinition def;

    /** Name of underlying tree name. */
    private final String treeName;

    /** Constructor. */
    public InlineIndexImpl(SortedIndexDefinition def, InlineIndexTree[] segments) {
        this.segments = segments.clone();
        this.def = def;
        treeName = def.getTreeName();
    }

    /** {@inheritDoc} */
    @Override public GridCursor<IndexSearchRow> find(IndexKey lower, IndexKey upper, int segment) throws IgniteCheckedException {
        return find(lower, upper, segment, null);
    }

    /** {@inheritDoc} */
    @Override public GridCursor<IndexSearchRow> find(IndexKey lower, IndexKey upper, int segment, IndexingQueryFilter filter) throws IgniteCheckedException {
        validateConditions(lower, upper);

        InlineTreeFilterClosure closure = getFilterClosure(filter);

        // If it is known that only one row will be returned an optimization is employed
        if (isSingleRowLookup(lower, upper)) {
            try {
                ThreadLocalSchemaHolder.setSchema(def.getSchema());

                IndexRow row = segments[segment].findOne(
                    new IndexSearchRowImpl(lower.keys(), def.getSchema(), false),
                    closure,
                    null);

                if (row == null)  // TODO isExpired(row))
                    return IndexValueCursor.EMPTY;

                return new SingleCursor(row);

            } finally {
                ThreadLocalSchemaHolder.cleanSchema();
            }
        }

        try {
            ThreadLocalSchemaHolder.setSchema(def.getSchema());

            IndexSearchRowImpl rlower = lower == null
                ? null
                : new IndexSearchRowImpl(lower.keys(), def.getSchema(), false);

            IndexSearchRowImpl rupper = upper == null
                ? null
                : new IndexSearchRowImpl(upper.keys(), def.getSchema(), false);

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
    private boolean isSingleRowLookup(IndexKey lower, IndexKey upper) {
        return lower != null && lower.equals(upper) && hasAllIndexColumns(lower);
    }

    /** */
    private boolean hasAllIndexColumns(IndexKey key) {
        for (int i = 0; i < def.getSchema().getKeyDefinitions().length; i++) {
            // TODO: Special null?
            // Java null means that column is not specified in a search row, for SQL NULL a special constant is used
            if (key.keys()[i] == null)
                return false;
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public GridCursor<IndexSearchRow> findFirstOrLast(boolean firstOrLast, int segment, IndexingQueryFilter filter)
        throws IgniteCheckedException{

        try {
            ThreadLocalSchemaHolder.setSchema(def.getSchema());

            InlineTreeFilterClosure closure = getFilterClosure(filter);

            IndexRow found = firstOrLast ?
                segments[segment].findFirst(closure)
                : segments[segment].findLast(closure);

            // TODO: expiration
            if (found == null) // || isExpired(found))
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
        return def.getIdxName();
    }

    /** {@inheritDoc} */
    @Override public void onUpdate(@Nullable CacheDataRow oldRow, @Nullable CacheDataRow newRow) throws IgniteCheckedException {
        try {
            ThreadLocalSchemaHolder.setSchema(def.getSchema());

            // Delete or clean before Update.
            if (oldRow != null) {
                int segment = segmentForRow(oldRow);

                segments[segment].remove(new IndexRowImpl(def.getSchema(), oldRow));
            }

            if (newRow == null)
                return;

            // Create.
            try {
                int segment = segmentForRow(newRow);

                segments[segment].put(new IndexRowImpl(def.getSchema(), newRow));

                // TODO: statistic

            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        } finally {
            ThreadLocalSchemaHolder.cleanSchema();
        }

    }

    /** {@inheritDoc} */
    @Override public CacheDataRow put(CacheDataRow row) throws IgniteCheckedException {
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
        int segment = segmentForRow(row);

        try {
            ThreadLocalSchemaHolder.setSchema(def.getSchema());

            return segments[segment].putx(new IndexRowImpl(def.getSchema(), row));

        } finally {
            ThreadLocalSchemaHolder.cleanSchema();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean removex(CacheDataRow row) throws IgniteCheckedException {
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
    private InlineTreeFilterClosure getFilterClosure(IndexingQueryFilter filter) {
        if (filter != null) {
            IndexingQueryCacheFilter f = filter.forCache(def.getContext().cache().name());
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
                // TODO
                throw new IgniteException("Failed to check index tree root page existence");
//                throw new IgniteException("Failed to check index tree root page existence [cacheName=" + cctx.name() +
//                    ", tblName=" + tblName + ", idxName=" + idxName + ", segment=" + i + ']');
            }
        }

        return false;
    }

    /** If {code true} then this index is already marked as destroyed. */
    private final AtomicBoolean destroyed = new AtomicBoolean();

    /** {@inheritDoc} */
    @Override public void destroy(boolean softDelete) {
        // Already destroyed.
        if (!destroyed.compareAndSet(false, true))
            return;

        GridCacheContext cctx = def.getContext();
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

//                ctx.metric().remove(stats.metricRegistryName()); // TODO
//
                // Actual destroy index task.
                DurableBackgroundTask task = new DurableBackgroundCleanupIndexTreeTask(
                    rootPages,
                    trees,
                    cctx.group().name(),
                    cctx.cache().name(),
                    "",
//                    table.getSchema().getName(),  // TODO
                    def.getIdxName()
                );

                cctx.kernalContext().durableBackgroundTasksProcessor().startDurableBackgroundTask(task, cctx.config());
            }
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
        finally {
//            if (msgLsnr != null)  TODO
//                ctx.io().removeMessageListener(msgTopic, msgLsnr);
        }
    }

    /**
     * @param segIdx Segment index.
     * @throws IgniteCheckedException If failed.
     */
    private void dropMetaPage(int segIdx) throws IgniteCheckedException {
        GridCacheContext cctx = def.getContext();
        cctx.offheap().dropRootPageForIndex(cctx.cacheId(), treeName, segIdx);
    }
}
