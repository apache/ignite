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

import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
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
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndexImpl;
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

        GridCursor<IndexRow> cursor = query(cctx, idx, idxQryDesc, qryCtx);

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

        if (idxQryDesc.idxName() != null) {
            Index idx = indexByName(cctx, idxQryDesc, tableName);

            if (idx == null)
                throw failIndexQuery("No index found for name: " + idxQryDesc.idxName(), null, idxQryDesc);

            return idx;
        }

        Index idx = indexByCriteria(cctx, idxQryDesc, tableName);

        if (idx == null)
            throw failIndexQuery("No index found for criteria", null, idxQryDesc);

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

    /**
     * Get index by list of fields to query, or return {@code null}.
     */
    private Index indexByCriteria(GridCacheContext<?, ?> cctx, IndexQueryDesc idxQryDesc, String tableName) {
        Collection<Index> idxs = idxProc.indexes(cctx);

        // Check both fields (original and normalized).
        final Set<String> critFields = idxQryDesc.criteria().stream()
            .map(IndexQueryCriterion::field)
            .flatMap(f -> Stream.of(f, QueryUtils.normalizeObjectName(f, false)))
            .collect(Collectors.toSet());

        for (Index idx: idxs) {
            IndexDefinition idxDef = idxProc.indexDefinition(idx.id());

            if (!tableName.equals(idxDef.idxName().tableName()))
                continue;

            if (checkIndex(idxDef, idxQryDesc.criteria().size(), critFields))
                return idx;
        }

        return null;
    }

    /**
     * Checks that specified index matches index query criteria.
     *
     * Criteria fields have to match to a prefix of the index. Order of fields in criteria doesn't matter.
     */
    private boolean checkIndex(IndexDefinition idxDef, int critLen, final Set<String> criteriaFlds) {
        if (critLen > idxDef.indexKeyDefinitions().size())
            return false;

        int matches = 0;

        for (String idxFldName: idxDef.indexKeyDefinitions().keySet()) {
            if (!criteriaFlds.contains(idxFldName))
                return false;

            if (++matches == critLen)
                return true;
        }

        return false;
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

        return new IgniteCheckedException(exMsg + " Query=" + desc);
    }

    /** Merges multiple criteria for the same field into single criterion. */
    private Map<String, RangeIndexQueryCriterion> mergeIndexQueryCriteria(InlineIndexImpl idx, SortedIndexDefinition idxDef,
        IndexQueryDesc idxQryDesc) throws IgniteCheckedException {
        Map<String, RangeIndexQueryCriterion> mergedCriteria = new HashMap<>();

        Map<String, IndexKeyDefinition> idxFlds = idxDef.indexKeyDefinitions();
        IndexKeyTypeSettings keyTypeSettings = idx.segment(0).rowHandler().indexKeyTypeSettings();
        CacheObjectContext coctx = idx.segment(0).cacheGroupContext().cacheObjectContext();

        IndexRowComparator keyCmp = idxDef.rowComparator();

        for (IndexQueryCriterion c: idxQryDesc.criteria()) {
            RangeIndexQueryCriterion crit = (RangeIndexQueryCriterion) c;

            String fldName = idxFlds.containsKey(crit.field()) ? crit.field()
                : QueryUtils.normalizeObjectName(crit.field(), false);

            IndexKeyDefinition keyDef = idxFlds.get(fldName);

            if (keyDef == null)
                throw failIndexQueryCriteria(idxDef, idxQryDesc);

            IndexKey l = key(crit.lower(), crit.lowerNull(), keyDef, keyTypeSettings, coctx);
            IndexKey u = key(crit.upper(), crit.upperNull(), keyDef, keyTypeSettings, coctx);

            boolean lowIncl = crit.lowerIncl();
            boolean upIncl = crit.upperIncl();

            boolean lowNull = crit.lowerNull();
            boolean upNull = crit.upperNull();

            if (mergedCriteria.containsKey(fldName)) {
                RangeIndexQueryCriterion prev = mergedCriteria.get(fldName);

                int lowCmp = 0;

                // Use previous lower boudary, as it's greater than the current.
                if (l == null || (prev.lower() != null && (lowCmp = keyCmp.compareKey((IndexKey)prev.lower(), l)) >= 0)) {
                    l = (IndexKey)prev.lower();
                    lowIncl = lowCmp != 0 ? prev.lowerIncl() : prev.lowerIncl() ? lowIncl : prev.lowerIncl();
                    lowNull = prev.lowerNull();
                }

                int upCmp = 0;

                // Use previous upper boudary, as it's less than the current.
                if (u == null || (prev.upper() != null && (upCmp = keyCmp.compareKey((IndexKey)prev.upper(), u)) <= 0)) {
                    u = (IndexKey)prev.upper();
                    upIncl = upCmp != 0 ? prev.upperIncl() : prev.upperIncl() ? upIncl : prev.upperIncl();
                    upNull = prev.upperNull();
                }
            }

            RangeIndexQueryCriterion idxKeyCrit = new RangeIndexQueryCriterion(fldName, l, u);
            idxKeyCrit.lowerIncl(lowIncl);
            idxKeyCrit.upperIncl(upIncl);
            idxKeyCrit.lowerNull(lowNull);
            idxKeyCrit.upperNull(upNull);

            mergedCriteria.put(fldName, idxKeyCrit);
        }

        return mergedCriteria;
    }

    /** Checks that specified index matches index query criteria. */
    private IndexRangeQuery alignCriteriaWithIndex(InlineIndexImpl idx, Map<String, RangeIndexQueryCriterion> criteria,
        IndexDefinition idxDef, IndexQueryDesc idxQryDesc) throws IgniteCheckedException {
        // Size of bounds array has to be equal to count of indexed fields.
        IndexKey[] lowerBounds = new IndexKey[idxDef.indexKeyDefinitions().size()];
        IndexKey[] upperBounds = new IndexKey[idxDef.indexKeyDefinitions().size()];

        boolean lowerAllNulls = true;
        boolean upperAllNulls = true;

        IndexRangeQuery qry = new IndexRangeQuery(criteria.size());

        // Checks that users criteria matches a prefix subset of index fields.
        int i = 0;

        for (Map.Entry<String, IndexKeyDefinition> keyDef: idxDef.indexKeyDefinitions().entrySet()) {
            RangeIndexQueryCriterion criterion = criteria.remove(keyDef.getKey());

            if (criterion == null)
                throw failIndexQueryCriteria(idxDef, idxQryDesc);

            if (keyDef.getValue().order().sortOrder() == DESC)
                criterion = criterion.swap();

            qry.criteria[i] = criterion;

            IndexKey l = (IndexKey) criterion.lower();
            IndexKey u = (IndexKey) criterion.upper();

            if (l != null)
                lowerAllNulls = false;

            if (u != null)
                upperAllNulls = false;

            lowerBounds[i] = l;
            upperBounds[i++] = u;

            if (criteria.isEmpty())
                break;
        }

        if (!criteria.isEmpty())
            throw failIndexQueryCriteria(idxDef, idxQryDesc);

        InlineIndexRowHandler hnd = idx.segment(0).rowHandler();

        qry.lower = lowerAllNulls ? null : new IndexSearchRowImpl(lowerBounds, hnd);
        qry.upper = upperAllNulls ? null : new IndexSearchRowImpl(upperBounds, hnd);

        return qry;
    }

    /**
     * Runs an index query.
     *
     * @return Result cursor over index segments.
     */
    private GridCursor<IndexRow> query(GridCacheContext<?, ?> cctx, Index idx, IndexQueryDesc idxQryDesc, IndexQueryContext qryCtx)
        throws IgniteCheckedException {

        IndexQueryCriterion c = idxQryDesc.criteria().get(0);

        if (c instanceof RangeIndexQueryCriterion)
            return querySortedIndex(cctx, (InlineIndexImpl) idx, idxQryDesc, qryCtx);

        throw new IllegalStateException("Doesn't support index query criteria: " + c.getClass().getName());
    }

    /**
     * Runs an index query for single {@code segment}.
     *
     * @return Result cursor over segment.
     */
    private GridCursor<IndexRow> querySortedIndex(GridCacheContext<?, ?> cctx, InlineIndexImpl idx, IndexQueryDesc idxQryDesc,
        IndexQueryContext qryCtx) throws IgniteCheckedException {
        SortedIndexDefinition idxDef = (SortedIndexDefinition) idxProc.indexDefinition(idx.id());

        Map<String, RangeIndexQueryCriterion> merged = mergeIndexQueryCriteria(idx, idxDef, idxQryDesc);

        IndexRangeQuery qry = alignCriteriaWithIndex(idx, merged, idxDef, idxQryDesc);

        int segmentsCnt = cctx.isPartitioned() ? cctx.config().getQueryParallelism() : 1;

        if (segmentsCnt == 1)
            return treeIndexRange(idx, 0, qry, qryCtx);

        final GridCursor<IndexRow>[] segmentCursors = new GridCursor[segmentsCnt];

        // Actually it just traverses BPlusTree to find boundaries. It's too fast to parallelize this.
        for (int i = 0; i < segmentsCnt; i++)
            segmentCursors[i] = treeIndexRange(idx, i, qry, qryCtx);

        return new SegmentedIndexCursor(
            segmentCursors, ((SortedIndexDefinition) idxProc.indexDefinition(idx.id())).rowComparator());
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
    private GridCursor<IndexRow> treeIndexRange(InlineIndexImpl idx, int segment, IndexRangeQuery qry, IndexQueryContext qryCtx)
        throws IgniteCheckedException {

        InlineIndexRowHandler hnd = idx.segment(segment).rowHandler();

        // Step 1. Traverse index.
        GridCursor<IndexRow> findRes = idx.find(qry.lower, qry.upper, segment, qryCtx);

        // Step 2. Scan and filter.
        return new GridCursor<IndexRow>() {
            /** */
            private final IndexRowComparator rowCmp = ((SortedIndexDefinition) idxProc.indexDefinition(idx.id())).rowComparator();

            /** {@inheritDoc} */
            @Override public boolean next() throws IgniteCheckedException {
                if (!findRes.next())
                    return false;

                while (rowIsOutOfRange(get(), qry.lower, qry.upper)) {
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
             * Checks that {@code row} belongs to the range specified with {@code lower} and {@code upper}.
             *
             * @return {@code true} if the row doesn't belong the range, otherwise {@code false}.
             */
            private boolean rowIsOutOfRange(IndexRow row, IndexRow low, IndexRow high) throws IgniteCheckedException {
                if (low == null && high == null)
                    return true;  // Unbounded search, include all.

                int criteriaKeysCnt = qry.criteria.length;

                for (int i = 0; i < criteriaKeysCnt; i++) {
                    RangeIndexQueryCriterion c = qry.criteria[i];

                    boolean descOrder = hnd.indexKeyDefinitions().get(i).order().sortOrder() == DESC;

                    if (low != null && low.key(i) != null) {
                        int cmp = rowCmp.compareRow(row, low, i);

                        if (cmp == 0) {
                            if (!c.lowerIncl())
                                return true;  // Exclude if field equals boundary field and criteria is excluding.
                        }
                        else if ((cmp < 0) ^ descOrder)
                            return true;  // Out of bound. Either below 'low' margin or column with desc order.
                    }

                    if (high != null && high.key(i) != null) {
                        int cmp = rowCmp.compareRow(row, high, i);

                        if (cmp == 0) {
                            if (!c.upperIncl())
                                return true;  // Exclude if field equals boundary field and criteria is excluding.
                        }
                        else if ((cmp > 0) ^ descOrder)
                            return true;  // Out of bound. Either above 'high' margin or column with desc order.
                    }
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

        /** Comparator to compare index rows. */
        private final Comparator<GridCursor<IndexRow>> cursorComp;

        /** */
        private IndexRow head;

        /** */
        SegmentedIndexCursor(GridCursor<IndexRow>[] cursors, IndexRowComparator rowCmp) throws IgniteCheckedException {
            cursorComp = new Comparator<GridCursor<IndexRow>>() {
                @Override public int compare(GridCursor<IndexRow> o1, GridCursor<IndexRow> o2) {
                    try {
                        return rowCmp.compareRow(o1.get(), o2.get(), 0);
                    }
                    catch (IgniteCheckedException e) {
                        throw new IgniteException(e);
                    }
                }
            };

            this.cursors = new PriorityQueue<>(cursors.length, cursorComp);

            for (GridCursor<IndexRow> c: cursors) {
                if (c.next())
                    this.cursors.add(c);
            }
        }

        /** {@inheritDoc} */
        @Override public boolean next() throws IgniteCheckedException {
            if (cursors.isEmpty())
                return false;

            GridCursor<IndexRow> c = cursors.poll();

            head = c.get();

            if (c != null && c.next())
                cursors.add(c);

            return true;
        }

        /** {@inheritDoc} */
        @Override public IndexRow get() throws IgniteCheckedException {
            return head;
        }
    }

    /** */
    private static class IndexRangeQuery {
        /** Ordered list of criteria. Order matches index fields order. */
        private final RangeIndexQueryCriterion[] criteria;

        /** */
        private IndexRangeQuery(int critSize) {
            criteria = new RangeIndexQueryCriterion[critSize];
        }

        /** */
        private IndexRow lower;

        /** */
        private IndexRow upper;
    }
}
