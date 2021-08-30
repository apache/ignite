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

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.query.IndexQuery;
import org.apache.ignite.cache.query.IndexQueryCriterion;
import org.apache.ignite.internal.cache.query.RangeIndexQueryCriterion;
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
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.util.GridCloseableIteratorAdapter;
import org.apache.ignite.internal.util.lang.GridCloseableIterator;
import org.apache.ignite.internal.util.lang.GridCursor;
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

        List<IndexQueryCriterion> criteria = alineCriteriaWithIndex(idxProc.indexDefinition(idx.id()), idxQryDesc);

        GridCursor<IndexRow> cursor = query(cctx, idx, criteria, qryCtx);

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
    private Index index(GridCacheContext<?, ?> cctx, IndexQueryDesc idxQryDesc) throws IgniteCheckedException {
        String tableName = cctx.kernalContext().query().tableName(cctx.name(), idxQryDesc.valType());

        if (tableName == null)
            throw failIndexQuery("No table found for type: " + idxQryDesc.valType(), null, idxQryDesc);

        Index idx = indexByName(cctx, idxQryDesc, tableName);

        if (idx == null)
            throw failIndexQuery("No index found: " + idxQryDesc.idxName(), null, idxQryDesc);

        return idx;
    }

    /** Get index by name, or return {@code null}. */
    private Index indexByName(GridCacheContext<?, ?> cctx, IndexQueryDesc idxQryDesc, String tableName) {
        String name = idxQryDesc.idxName();

        if (!QueryUtils.PRIMARY_KEY_INDEX.equals(name))
            name = name.toUpperCase();

        String schema = cctx.kernalContext().query().schemaName(cctx);

        IndexName normIdxName = new IndexName(cctx.name(), schema, tableName, name);

        Index idx = idxProc.index(normIdxName);

        if (idx != null)
            return idx;

        IndexName origIdxName = new IndexName(cctx.name(), schema, tableName, idxQryDesc.idxName());

        return idxProc.index(origIdxName);
    }

    /** */
    private IgniteCheckedException failIndexQueryCriteria(IndexDefinition idxDef, IndexQueryDesc idxQryDesc) {
        return failIndexQuery( "Index doesn't match query", idxDef, idxQryDesc);
    }

    /** */
    private IgniteCheckedException failIndexQuery(String msg, IndexDefinition idxDef, IndexQueryDesc desc) {
        String exMsg = "Failed to parse IndexQuery. " + msg + ".";

        if (idxDef != null)
            exMsg += " Index=" + idxDef;

        return new IgniteCheckedException(exMsg + "; Query=" + desc);
    }

    /** Checks that specified index matches index query criteria. */
    private boolean checkIndex(IndexDefinition idxDef, List<IndexQueryCriterion> criteria) {
        return criteria.size() <= idxDef.indexKeyDefinitions().size();
    }

    /** Checks that specified index matches index query criteria. */
    private List<IndexQueryCriterion> alineCriteriaWithIndex(IndexDefinition idxDef, IndexQueryDesc idxQryDesc)
        throws IgniteCheckedException {
        if (idxQryDesc.criteria().size() > idxDef.indexKeyDefinitions().size())
            throw failIndexQueryCriteria(idxDef, idxQryDesc);

        Map<String, IndexQueryCriterion> critFlds = new HashMap<>();
        Map<String, IndexQueryCriterion> normCritFlds = new HashMap<>();

        // We need check both original and normalized field names.
        idxQryDesc.criteria().forEach(c -> {
            critFlds.put(c.field(), c);
            normCritFlds.put(c.field().toUpperCase(), c);
        });

        List<IndexQueryCriterion> aligned = new ArrayList<>();

        // Checks that users criteria matches a prefix subset of index fields.
        for (int i = 0; i < idxQryDesc.criteria().size(); i++) {
            String idxFld = idxDef.indexKeyDefinitions().get(i).name();

            IndexQueryCriterion c = normCritFlds.remove(idxFld);

            if (c == null) {
                // Check this field is escaped.
                c = critFlds.remove(idxFld);

                if (c == null)
                    throw failIndexQueryCriteria(idxDef, idxQryDesc);
            }
            else
                critFlds.remove(c.field());

            aligned.add(c);
        }

        if (!critFlds.isEmpty())
            throw failIndexQueryCriteria(idxDef, idxQryDesc);

        return aligned;
    }

    /**
     * Runs an index query.
     *
     * @return Result cursor over index segments.
     */
    private GridCursor<IndexRow> query(GridCacheContext<?, ?> cctx, Index idx, List<IndexQueryCriterion> criteria, IndexQueryContext qryCtx)
        throws IgniteCheckedException {

        int segmentsCnt = cctx.isPartitioned() ? cctx.config().getQueryParallelism() : 1;

        if (segmentsCnt == 1)
            return query(0, idx, criteria, qryCtx);

        final GridCursor<IndexRow>[] segmentCursors = new GridCursor[segmentsCnt];

        // Actually it just traverses BPlusTree to find boundaries. It's too fast to parallelize this.
        for (int i = 0; i < segmentsCnt; i++)
            segmentCursors[i] = query(i, idx, criteria, qryCtx);

        return new SegmentedIndexCursor(segmentCursors, ((SortedIndexDefinition) idxProc.indexDefinition(idx.id())).rowComparator());
    }

    /**
     * Runs an index query for single {@code segment}.
     *
     * @return Result cursor over segment.
     */
    private GridCursor<IndexRow> query(int segment, Index idx, List<IndexQueryCriterion> criteria, IndexQueryContext qryCtx)
        throws IgniteCheckedException {

        assert !criteria.isEmpty() : "Index query criteria list has not to be empty.";

        if (criteria.get(0) instanceof RangeIndexQueryCriterion)
            return treeIndexRange((InlineIndex) idx, criteria, segment, qryCtx);

        throw new IllegalStateException("Doesn't support index query criteria: " + criteria.getClass().getName());
    }

    /**
     * Runs range query over specified segment. There are 2 steps to run query:
     * 1. Traverse index by specified boundaries;
     * 2. Scan over cursor and filter rows that doesn't match user criteria.
     *
     * Filtering is required in 2 cases:
     * 1. Exclusion of one of boundaries, as idx.find() includes both of them;
     * 2. To apply criteria on non-first index fields. Tree apply boundaries field by field, if first field match
     * a boundary, then second field isn't checked within traversing.
     */
    private GridCursor<IndexRow> treeIndexRange(InlineIndex idx, List<IndexQueryCriterion> criteria, int segment,
        IndexQueryContext qryCtx) throws IgniteCheckedException {

        InlineIndexRowHandler hnd = idx.segment(0).rowHandler();
        CacheObjectContext coctx = idx.segment(0).cacheGroupContext().cacheObjectContext();

        IndexKey[] lowerBounds = new IndexKey[hnd.indexKeyDefinitions().size()];
        IndexKey[] upperBounds = new IndexKey[hnd.indexKeyDefinitions().size()];

        boolean lowerAllNulls = true;
        boolean upperAllNulls = true;

        List<RangeIndexQueryCriterion> treeCriteria = new ArrayList<>();

        for (int i = 0; i < criteria.size(); i++) {
            RangeIndexQueryCriterion c = (RangeIndexQueryCriterion) criteria.get(i);

            IndexKeyDefinition def = hnd.indexKeyDefinitions().get(i);

            if (!def.name().equalsIgnoreCase(c.field()))
                throw new IgniteCheckedException("Range query doesn't match index '" + idx.name() + "'");

            // If index is desc, then we need to swap boundaries as user declare criteria in straight manner.
            // For example, there is an idx (int Val desc). It means that index stores data in reverse order (1 < 0).
            // But user won't expect for criterion gt(1) to get 0 as result, instead user will use lt(1) for getting
            // 0. Then we need to swap user criterion.
            if (def.order().sortOrder() == DESC)
                c = c.swap();

            treeCriteria.add(c);

            IndexKey l = key(c.lower(), c.lowerNull(), def, hnd.indexKeyTypeSettings(), coctx);
            IndexKey u = key(c.upper(), c.upperNull(), def, hnd.indexKeyTypeSettings(), coctx);

            if (l != null)
                lowerAllNulls = false;

            if (u != null)
                upperAllNulls = false;

            lowerBounds[i] = l;
            upperBounds[i] = u;
        }

        IndexRow lower = lowerAllNulls ? null : new IndexSearchRowImpl(lowerBounds, hnd);
        IndexRow upper = upperAllNulls ? null : new IndexSearchRowImpl(upperBounds, hnd);

        // Step 1. Traverse index.
        GridCursor<IndexRow> findRes = idx.find(lower, upper, segment, qryCtx);

        // Step 2. Scan and filter.
        return new GridCursor<IndexRow>() {
            /** */
            private final IndexRowComparator rowCmp = ((SortedIndexDefinition) idxProc.indexDefinition(idx.id())).rowComparator();

            /** {@inheritDoc} */
            @Override public boolean next() throws IgniteCheckedException {
                if (!findRes.next())
                    return false;

                while (exclude(get(), lower, 1) || exclude(get(), upper, -1)) {
                    if (!findRes.next())
                        return false;
                }

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
            private boolean exclude(IndexRow row, IndexRow boundary, int boundarySign) throws IgniteCheckedException {
                // Unbounded search, include all.
                if (boundary == null)
                    return false;

                int criteriaKeysCnt = treeCriteria.size();

                for (int i = 0; i < criteriaKeysCnt; i++) {
                    RangeIndexQueryCriterion c = treeCriteria.get(i);

                    // Include all values on this field.
                    if (boundary.key(i) == null)
                        continue;

                    int cmp = rowCmp.compareKey(row, boundary, i);

                    // Swap direction.
                    if (hnd.indexKeyDefinitions().get(i).order().sortOrder() == DESC)
                        cmp = -cmp;

                    // Exclude if field equals boundary field and criteria is excluding.
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

    /**
     * @param isNull {@code true} if user explicitly set {@code null} with a query argument.
     */
    private IndexKey key(Object val, boolean isNull, IndexKeyDefinition def, IndexKeyTypeSettings settings, CacheObjectContext coctx) {
        IndexKey key = null;

        if (val != null || isNull)
            key = IndexKeyFactory.wrap(val, def.idxType(), coctx, settings);

        return key;
    }

    /** Single cursor over multiple segments. Next value is choose with the index row comparator. */
    private static class SegmentedIndexCursor implements GridCursor<IndexRow> {
        /** Cursors over segments. */
        private final PriorityQueue<GridCursor<IndexRow>> cursors;

        /** Whether returns first value for user. */
        private boolean returnFirst;

        /** Comparator to compare index rows. */
        private final Comparator<GridCursor<IndexRow>> cursorComp;

        /** */
        SegmentedIndexCursor(GridCursor<IndexRow>[] cursors, IndexRowComparator rowCmp) {
            cursorComp = new Comparator<GridCursor<IndexRow>>() {
                @Override public int compare(GridCursor<IndexRow> o1, GridCursor<IndexRow> o2) {
                    try {
                        if (!returnFirst)
                            return -1;

                        return rowCmp.compareKey(o1.get(), o2.get(), 0);
                    }
                    catch (IgniteCheckedException e) {
                        throw new IgniteException(e);
                    }
                }
            };

            this.cursors = new PriorityQueue<>(cursors.length, cursorComp);

            this.cursors.addAll(Arrays.asList(cursors));
        }

        /** {@inheritDoc} */
        @Override public boolean next() throws IgniteCheckedException {
            if (!returnFirst) {
                returnFirst = true;

                GridCursor<IndexRow>[] cc = cursors
                    .toArray((GridCursor[]) Array.newInstance(GridCursor.class, cursors.size()));

                cursors.clear();

                for (GridCursor<IndexRow> c: cc) {
                    if (c.next())
                        cursors.add(c);
                }
            } else {
                GridCursor<IndexRow> c = cursors.poll();

                if (c != null && c.next())
                    cursors.add(c);
            }

            return !cursors.isEmpty();
        }

        /** {@inheritDoc} */
        @Override public IndexRow get() throws IgniteCheckedException {
            return cursors.peek().get();
        }
    }
}
