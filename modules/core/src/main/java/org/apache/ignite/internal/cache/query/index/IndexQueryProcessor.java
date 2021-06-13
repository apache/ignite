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
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.query.IndexConditionBuilder;
import org.apache.ignite.cache.query.IndexQuery;
import org.apache.ignite.internal.cache.query.IndexCondition;
import org.apache.ignite.internal.cache.query.RangeIndexCondition;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyDefinition;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyTypeSettings;
import org.apache.ignite.internal.cache.query.index.sorted.IndexRow;
import org.apache.ignite.internal.cache.query.index.sorted.IndexRowComparator;
import org.apache.ignite.internal.cache.query.index.sorted.IndexSearchRowImpl;
import org.apache.ignite.internal.cache.query.index.sorted.InlineIndexRowHandler;
import org.apache.ignite.internal.cache.query.index.sorted.SortedIndexDefinition;
import org.apache.ignite.internal.cache.query.index.sorted.inline.IndexQueryContext;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndex;
import org.apache.ignite.internal.cache.query.index.sorted.keys.IndexKey;
import org.apache.ignite.internal.cache.query.index.sorted.keys.IndexKeyFactory;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.CacheObjectUtils;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.query.IndexQueryDesc;
import org.apache.ignite.internal.util.GridCloseableIteratorAdapter;
import org.apache.ignite.internal.util.lang.GridCloseableIterator;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;

import static org.apache.ignite.internal.cache.query.index.SortOrder.DESC;

/**
 * Processor of {@link IndexQuery}.
 */
public class IndexQueryProcessor {
    /** */
    private final IndexProcessor idxProc;

    /** */
    public IndexQueryProcessor(IndexProcessor idxProc) {
        this.idxProc = idxProc;
    }

    /** Run query on local node. */
    public <K, V> GridCloseableIterator<IgniteBiTuple<K, V>> queryLocal(
        GridCacheContext<K, V> cctx, IndexQueryDesc idxQryDesc, IndexQueryContext qryCtx, boolean keepBinary)
        throws IgniteCheckedException {

        Index idx = index(cctx, idxQryDesc);

        if (idx == null)
            throw new IgniteCheckedException(
                "No index matches index query. Cache=" + cctx.name() + "; Qry=" + idxQryDesc);

        GridCursor<IndexRow> cursor = query(cctx, idx, idxQryDesc.idxCond(), qryCtx);

        // Map IndexRow to Cache Key-Value pair.
        return new GridCloseableIteratorAdapter<IgniteBiTuple<K, V>>() {
            private IndexRow currVal;

            private final CacheObjectContext coctx = cctx.cacheObjectContext();

            /** {@inheritDoc} */
            @Override protected boolean onHasNext() throws IgniteCheckedException {
                if (currVal != null)
                    return true;

                if (!cursor.next())
                    return false;

                currVal = cursor.get();

                return true;
            }

            /** {@inheritDoc} */
            @Override protected IgniteBiTuple<K, V> onNext() {
                if (currVal == null)
                    if (!hasNext())
                        throw new NoSuchElementException();

                IndexRow row = currVal;

                currVal = null;

                K k = (K) CacheObjectUtils.unwrapBinaryIfNeeded(coctx, row.cacheDataRow().key(), keepBinary, false);
                V v = (V) CacheObjectUtils.unwrapBinaryIfNeeded(coctx, row.cacheDataRow().value(), keepBinary, false);

                return new IgniteBiTuple<>(k, v);
            }
        };
    }

    /** Get index to run query by specified description. */
    private Index index(GridCacheContext cctx, IndexQueryDesc idxQryDesc) throws IgniteCheckedException {
        Class<?> valCls = idxQryDesc.valCls() != null ? loadValClass(cctx, idxQryDesc.valCls()) : null;

        String tableName = cctx.kernalContext().query().tableName(cctx.name(), valCls);

        if (tableName == null)
            return null;

        // Find index by specified name.
        if (idxQryDesc.idxName() != null) {
            String name = "_key_PK".equals(idxQryDesc.idxName()) ? "_key_PK" : idxQryDesc.idxName().toUpperCase();

            String schema = idxQryDesc.schema() == null ? cctx.name() : idxQryDesc.schema();

            IndexName idxName = new IndexName(cctx.name(), schema, tableName, name);

            Index idx = idxProc.index(idxName);

            if (idx == null)
                return null;

            return checkIndex(idxProc.indexDefinition(idx.id()), idxQryDesc.idxCond()) ? idx : null;
        }

        // Try get index by list of fields to query.
        // Check all indexes by cache to find best index match: count of index fields equals to count of index condition fields.
        Collection<Index> idxs = idxProc.indexes(cctx);

        Index idx = null;
        int idxFieldsCnt = 0;

        for (Index i: idxs) {
            IndexDefinition idxDef = idxProc.indexDefinition(i.id());

            if (!tableName.equals(idxDef.idxName().tableName()))
                continue;

            int fldsCnt = idxDef.indexKeyDefinitions().size();

            if (checkIndex(idxDef, idxQryDesc.idxCond())) {
                if (idx == null) {
                    idx = i;
                    idxFieldsCnt = fldsCnt;
                }
                else if (fldsCnt < idxFieldsCnt) {
                    idx = i;
                    idxFieldsCnt = fldsCnt;
                }
                else continue;

                // Best match. Query condition matches full index.
                if (idxQryDesc.idxCond().fields().size() == idxDef.indexKeyDefinitions().size())
                    break;
            }
        }

        return idx;
    }

    /** Checks that specified index matches index query condition. */
    private boolean checkIndex(IndexDefinition idxDef, IndexCondition idxCond) {
        if (idxCond.fields().size() > idxDef.indexKeyDefinitions().size())
            return false;

        for (int i = 0; i < idxCond.fields().size(); i++) {
            if (!idxDef.indexKeyDefinitions().get(i).name().equalsIgnoreCase(idxCond.fields().get(i)))
                return false;
        }

        return true;
    }

    /** */
    private Class<?> loadValClass(GridCacheContext cctx, String valClsName) throws IgniteCheckedException {
        try {
            ClassLoader clsLdr = U.resolveClassLoader(cctx.kernalContext().config());

            return clsLdr.loadClass(valClsName);
        }
        catch (ClassNotFoundException e) {
            throw new IgniteCheckedException("No cache serves class: " + valClsName);
        }
    }

    /** Runs a query and return single cursor or cursor over multiple index segments. */
    private GridCursor<IndexRow> query(GridCacheContext cctx, Index idx, IndexCondition idxCond, IndexQueryContext qryCtx)
        throws IgniteCheckedException {

        int segmentsCnt = cctx.isPartitioned() ? cctx.config().getQueryParallelism() : 1;

        if (segmentsCnt == 1)
            return query(0, idx, idxCond, qryCtx);

        final GridCursor<IndexRow>[] segments = new GridCursor[segmentsCnt];

        // Actually it just traverse BPlusTree to find boundaries. It's too fast to parallelize this.
        for (int i = 0; i < segmentsCnt; i++)
            segments[i] = query(i, idx, idxCond, qryCtx);

        return new SegmentedIndexCursor(segments, ((SortedIndexDefinition) idxProc.indexDefinition(idx.id())).rowComparator());
    }

    /** Coordinate query conditions. */
    private GridCursor<IndexRow> query(int segment, Index idx, IndexCondition idxCond, IndexQueryContext qryCtx)
        throws IgniteCheckedException {

        if (idxCond instanceof RangeIndexCondition)
            return treeIndexRange((InlineIndex) idx, (RangeIndexCondition) idxCond, segment, qryCtx);

        throw new IllegalStateException("Doesn't support index condition: " + idxCond.getClass().getName());
    }

    /**
     * Runs range query over specified segment. There are 2 steps to run query:
     * 1. Traverse index by specified boundaries;
     * 2. Scan over cursor and filter rows that doesn't match user condition.
     *
     * Filtering is required in 2 cases:
     * 1. Exclusion of one of boundaries, as idx.find() includes both of them;
     * 2. To apply conditions on non-first index fields. Tree apply boundaries field by field, if first field match
     * a boundary, then second field isn't checked within traversing.
     */
    private GridCursor<IndexRow> treeIndexRange(InlineIndex idx, RangeIndexCondition cond, int segment,
        IndexQueryContext qryCtx) throws IgniteCheckedException {

        InlineIndexRowHandler hnd = idx.segment(0).rowHandler();
        CacheObjectContext coctx = idx.segment(0).cacheContext().cacheObjectContext();

        IndexKey[] lowerBounds = new IndexKey[hnd.indexKeyDefinitions().size()];
        IndexKey[] upperBounds = new IndexKey[hnd.indexKeyDefinitions().size()];

        boolean lowerAllNulls = true;
        boolean upperAllNulls = true;

        List<RangeIndexCondition.SingleFieldRangeCondition> treeConds = new ArrayList<>();

        for (int i = 0; i < cond.fields().size(); i++) {
            String f = cond.fields().get(i);

            IndexKeyDefinition def = hnd.indexKeyDefinitions().get(i);

            if (!def.name().equalsIgnoreCase(f))
                throw new IgniteCheckedException("Range query doesn't match index '" + idx.name() + "'");

            RangeIndexCondition.SingleFieldRangeCondition c = cond.conditions().get(i);

            // If index is desc, then we need to swap boundaries as user declare condition in straight manner.
            // For example, there is an idx (int Val desc). It means that index stores data in reverse order (1 < 0).
            // But user won't expect for condition gt(1) to get 0 as result, instead user will use lt(1) for getting
            // 0. Then we need to swap user condition.
            if (def.order().sortOrder() == DESC)
                c = c.swap();

            treeConds.add(c);

            IndexKey l = key(c.lower(), def, hnd.indexKeyTypeSettings(), coctx);
            IndexKey u = key(c.upper(), def, hnd.indexKeyTypeSettings(), coctx);

            if (l != null) {
                // This is completely wrong condition that returns incorrect result. Instead access predicate should be used.
                if (lowerAllNulls && i > 0)
                    throw new IgniteCheckedException("Range query doesn't match index '" + idx.name() + "'");

                lowerAllNulls = false;
            }

            if (u != null) {
                // This is completely wrong condition that returns incorrect result. Instead access predicate should be used.
                if (upperAllNulls && i > 0)
                    throw new IgniteCheckedException("Range query doesn't match index '" + idx.name() + "'");

                upperAllNulls = false;
            }

            lowerBounds[i] = l;
            upperBounds[i] = u;
        }

        IndexRow lower = lowerAllNulls ? null : new IndexSearchRowImpl(lowerBounds, hnd);
        IndexRow upper = upperAllNulls ? null : new IndexSearchRowImpl(upperBounds, hnd);

        // Step 1. Traverse index.
        GridCursor<IndexRow> findRes = idx.find(lower, upper, segment, qryCtx);

        // Step 2. Scan and filter.
        return new GridCursor<IndexRow>() {
            /** Whether returns first row. */
            private boolean returnFirst;

            private final IndexRowComparator rowCmp = ((SortedIndexDefinition) idxProc.indexDefinition(idx.id())).rowComparator();

            /** {@inheritDoc} */
            @Override public boolean next() throws IgniteCheckedException {
                if (!findRes.next())
                    return false;

                if (!returnFirst) {
                    while (match(get(), lower, 1)) {
                        if (!findRes.next())
                            return false;
                    }

                    returnFirst = true;
                }

                if (match(get(), upper, -1))
                    return false;

                return true;
            }

            /** {@inheritDoc} */
            @Override public IndexRow get() throws IgniteCheckedException {
                return findRes.get();
            }

            /**
             * Matches index row, boundary and inclusion mask to decide whether this row will be excluded from result.
             *
             * @param row Result row to check.
             * @param boundary Index search boundary.
             * @param boundarySign {@code 1} for lower boundary and {@code -1} for upper boundary.
             * @return {@code true} if specified row has to be excluded from result.
             */
            private boolean match(IndexRow row, IndexRow boundary, int boundarySign) throws IgniteCheckedException {
                // Unbounded search, include all.
                if (boundary == null)
                    return false;

                int condKeysCnt = treeConds.size();

                for (int i = 0; i < condKeysCnt; i++) {
                    RangeIndexCondition.SingleFieldRangeCondition c = treeConds.get(i);

                    // Include all values on this field.
                    if (boundary.key(i) == null)
                        return false;

                    int cmp = rowCmp.compareKey(row, boundary, i);

                    // Swap direction.
                    if (hnd.indexKeyDefinitions().get(i).order().sortOrder() == DESC)
                        cmp = -1 * cmp;

                    // Exclude if field equals boundary field and condition is excluding.
                    if (cmp == 0) {
                        if (boundarySign > 0 && !c.lowerIncl())
                            return true;

                        if (boundarySign < 0 && !c.upperIncl())
                            return true;
                    }

                    // Check sign. Exclude if field is out of boundaries.
                    if (cmp * boundarySign < 0)
                        return true;
                }

                return false;
            }
        };
    }

    /** */
    private IndexKey key(Object val, IndexKeyDefinition def, IndexKeyTypeSettings settings, CacheObjectContext coctx) {
        IndexKey key = null;

        if (val != null) {
            if (val instanceof IndexConditionBuilder.Null)
                val = null;

            key = IndexKeyFactory.wrap(
                val, def.idxType(), coctx, settings);
        }

        return key;
    }

    /** Single cursor over multiple segments. Next value is choose with the index row comparator. */
    private class SegmentedIndexCursor implements GridCursor<IndexRow> {
        /** Cursors over segments. */
        private final GridCursor<IndexRow>[] cursors;

        /** Whether returns first value for user. */
        private boolean returnFirst;

        /** Offset of current segmented cursor to return value. */
        private int cursorOff;

        /** Comparator to compare index rows. */
        private final Comparator<GridCursor<IndexRow>> cursorComp;

        /** */
        SegmentedIndexCursor(GridCursor<IndexRow>[] cursors, IndexRowComparator rowCmp) {
            this.cursors = cursors;

            cursorComp = new Comparator<GridCursor<IndexRow>>() {
                @Override public int compare(GridCursor<IndexRow> o1, GridCursor<IndexRow> o2) {
                    try {
                        if (o1 == o2)
                            return 0;

                        if (o1 == null)
                            return -1;

                        if (o2 == null)
                            return 1;

                        return rowCmp.compareKey(o1.get(), o2.get(), 0);

                    } catch (IgniteCheckedException e) {
                        throw new IgniteException(e);
                    }
                }
            };
        }

        /** {@inheritDoc} */
        @Override public boolean next() throws IgniteCheckedException {
            if (!returnFirst) {
                for (int i = 0; i < cursors.length; i++) {
                    if (!cursors[i].next()) {
                        cursors[i] = null;
                        cursorOff++;
                    }
                }

                if (cursorOff == cursors.length)
                    return false;

                Arrays.sort(cursors, cursorComp);

                returnFirst = true;

            } else {
                if (cursorOff == cursors.length)
                    return false;

                if (!cursors[cursorOff].next())
                    cursors[cursorOff++] = null;

                bubbleUp();
            }

            return cursorOff != cursors.length;
        }

        /** {@inheritDoc} */
        @Override public IndexRow get() throws IgniteCheckedException {
            return cursors[cursorOff].get();
        }

        /** */
        private void bubbleUp() {
            for (int i = cursorOff, last = cursors.length - 1; i < last; i++) {
                if (cursorComp.compare(cursors[i], cursors[i + 1]) <= 0)
                    break;

                U.swap(cursors, i, i + 1);
            }
        }
    }
}
