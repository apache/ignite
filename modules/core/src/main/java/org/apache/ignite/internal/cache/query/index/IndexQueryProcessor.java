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
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;
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
import org.apache.ignite.internal.cache.query.index.sorted.SortedSegmentedIndex;
import org.apache.ignite.internal.cache.query.index.sorted.inline.IndexQueryContext;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndexImpl;
import org.apache.ignite.internal.cache.query.index.sorted.keys.IndexKey;
import org.apache.ignite.internal.cache.query.index.sorted.keys.IndexKeyFactory;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.CacheObjectUtils;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.query.IndexQueryDesc;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.util.GridCloseableIteratorAdapter;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.Nullable;

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

    /**
     * Run query on local node.
     *
     * @return Query result that contains data iterator and related metadata.
     */
    public <K, V> IndexQueryResult<K, V> queryLocal(
        GridCacheContext<K, V> cctx,
        IndexQueryDesc idxQryDesc,
        @Nullable IgniteBiPredicate<K, V> filter,
        IndexQueryContext qryCtx,
        boolean keepBinary
    ) throws IgniteCheckedException {
        SortedSegmentedIndex idx = findSortedIndex(cctx, idxQryDesc);

        IndexRangeQuery qry = prepareQuery(idx, idxQryDesc);

        GridCursor<IndexRow> cursor = querySortedIndex(cctx, idx, qryCtx, qry);

        SortedIndexDefinition def = (SortedIndexDefinition)idxProc.indexDefinition(idx.id());

        IndexQueryResultMeta meta = new IndexQueryResultMeta(def, qry.criteria.length);

        // Map IndexRow to Cache Key-Value pair.
        return new IndexQueryResult<>(meta, new GridCloseableIteratorAdapter<IgniteBiTuple<K, V>>() {
            private IgniteBiTuple<K, V> currVal;

            private final CacheObjectContext coctx = cctx.cacheObjectContext();

            /** {@inheritDoc} */
            @Override protected boolean onHasNext() throws IgniteCheckedException {
                if (currVal != null)
                    return true;

                while (currVal == null && cursor.next()) {
                    IndexRow r = cursor.get();

                    K k = unwrap(r.cacheDataRow().key(), true);
                    V v = unwrap(r.cacheDataRow().value(), true);

                    if (filter != null) {
                        K k0 = keepBinary ? k : unwrap(r.cacheDataRow().key(), false);
                        V v0 = keepBinary ? v : unwrap(r.cacheDataRow().value(), false);

                        if (!filter.apply(k0, v0))
                            continue;
                    }

                    currVal = new IgniteBiTuple<>(k, v);
                }

                return currVal != null;
            }

            /** {@inheritDoc} */
            @Override protected IgniteBiTuple<K, V> onNext() {
                if (currVal == null)
                    if (!hasNext())
                        throw new NoSuchElementException();

                IgniteBiTuple<K, V> row = currVal;

                currVal = null;

                return row;
            }

            /** */
            private <T> T unwrap(CacheObject o, boolean keepBinary) {
                return (T)CacheObjectUtils.unwrapBinaryIfNeeded(coctx, o, keepBinary, false);
            }
        });
    }

    /**
     * Finds sorted index to run query by specified description.
     *
     * @return Index to run query by specified description.
     * @throws IgniteCheckedException If index not found.
     */
    private SortedSegmentedIndex findSortedIndex(GridCacheContext<?, ?> cctx, IndexQueryDesc idxQryDesc) throws IgniteCheckedException {
        final String tableName = cctx.kernalContext().query().tableName(cctx.name(), idxQryDesc.valType());

        if (tableName == null)
            throw failIndexQuery("No table found for type: " + idxQryDesc.valType(), null, idxQryDesc);

        // Collect both fields (original and normalized).
        Map<String, String> critFlds;

        if (!F.isEmpty(idxQryDesc.criteria())) {
            critFlds = idxQryDesc.criteria().stream()
                .map(IndexQueryCriterion::field)
                .flatMap(f -> {
                    String norm = QueryUtils.normalizeObjectName(f, false);

                    if (f.equals(norm))
                        return Stream.of(new T2<>(f, f));
                    else
                        return Stream.of(new T2<>(f, norm), new T2<>(norm, f));
                })
                .collect(Collectors.toMap(IgniteBiTuple::get1, IgniteBiTuple::get2, (l, r) -> l));
        }
        else
            critFlds = Collections.emptyMap();

        if (idxQryDesc.idxName() == null && !critFlds.isEmpty())
            return indexByCriteria(cctx, critFlds, tableName, idxQryDesc);

        // If index name isn't specified and criteria aren't set then use the PK index.
        String name = idxQryDesc.idxName() == null ? QueryUtils.PRIMARY_KEY_INDEX : idxQryDesc.idxName();

        IndexName idxName = new IndexName(cctx.name(), cctx.kernalContext().query().schemaName(cctx), tableName, name);

        return indexByName(idxName, idxQryDesc, critFlds);
    }

    /**
     * @return Sorted index found by name.
     * @throws IgniteCheckedException If index not found or specified index doesn't match query criteria.
     */
    private SortedSegmentedIndex indexByName(
        IndexName idxName,
        IndexQueryDesc idxQryDesc,
        final Map<String, String> criteriaFlds
    ) throws IgniteCheckedException {
        SortedSegmentedIndex idx = assertSortedIndex(idxProc.index(idxName), idxQryDesc);

        if (idx == null && !QueryUtils.PRIMARY_KEY_INDEX.equals(idxName.idxName())) {
            String normIdxName = QueryUtils.normalizeObjectName(idxName.idxName(), false);

            idxName = new IndexName(idxName.cacheName(), idxName.schemaName(), idxName.tableName(), normIdxName);

            idx = assertSortedIndex(idxProc.index(idxName), idxQryDesc);
        }

        if (idx == null)
            throw failIndexQuery("No index found for name: " + idxName.idxName(), null, idxQryDesc);

        if (!checkIndex(idx, idxName.tableName(), criteriaFlds))
            throw failIndexQuery("Index doesn't match criteria", null, idxQryDesc);

        return idx;
    }

    /**
     * @return Index found by list of criteria fields.
     * @throws IgniteCheckedException if suitable index not found.
     */
    private SortedSegmentedIndex indexByCriteria(
        GridCacheContext<?, ?> cctx,
        final Map<String, String> criteriaFlds,
        String tableName,
        IndexQueryDesc idxQryDesc
    ) throws IgniteCheckedException {
        Collection<Index> idxs = idxProc.indexes(cctx);

        for (Index idx: idxs) {
            SortedSegmentedIndex sortedIdx = assertSortedIndex(idx, idxQryDesc);

            if (checkIndex(sortedIdx, tableName, criteriaFlds))
                return sortedIdx;
        }

        throw failIndexQuery("No index found for criteria", null, idxQryDesc);
    }

    /** Assert if specified index is not an instance of {@link SortedSegmentedIndex}. */
    private SortedSegmentedIndex assertSortedIndex(Index idx, IndexQueryDesc idxQryDesc) throws IgniteCheckedException {
        if (idx == null)
            return null;

        if (!(idx instanceof SortedSegmentedIndex))
            throw failIndexQuery("IndexQuery is not supported for index: " + idx.name(), null, idxQryDesc);

        return (SortedSegmentedIndex)idx;
    }

    /**
     * Checks that specified sorted index matches index query criteria.
     *
     * Criteria fields have to match to a prefix of the index. Order of fields in criteria doesn't matter.
     */
    private boolean checkIndex(SortedSegmentedIndex idx, String tblName, Map<String, String> criteriaFlds) {
        IndexDefinition idxDef = idxProc.indexDefinition(idx.id());

        if (!tblName.equals(idxDef.idxName().tableName()))
            return false;

        if (F.isEmpty(criteriaFlds))
            return true;

        Map<String, String> flds = new HashMap<>(criteriaFlds);

        for (String idxFldName: idxDef.indexKeyDefinitions().keySet()) {
            String alias = flds.remove(idxFldName);

            // Has not to be null, as criteriaFlds contains both original and normalized field names.
            if (alias == null)
                return false;

            flds.remove(alias);

            if (flds.isEmpty())
                return true;
        }

        return false;
    }

    /** */
    private IgniteCheckedException failIndexQuery(String msg, IndexDefinition idxDef, IndexQueryDesc desc) {
        String exMsg = "Failed to parse IndexQuery. " + msg + ".";

        if (idxDef != null)
            exMsg += " Index=" + idxDef;

        return new IgniteCheckedException(exMsg + " Query=" + desc);
    }

    /** Merges multiple criteria for the same field into single criterion. */
    private Map<String, RangeIndexQueryCriterion> mergeIndexQueryCriteria(
        InlineIndexImpl idx,
        SortedIndexDefinition idxDef,
        IndexQueryDesc idxQryDesc
    ) throws IgniteCheckedException {
        Map<String, RangeIndexQueryCriterion> mergedCriteria = new HashMap<>();

        Map<String, IndexKeyDefinition> idxFlds = idxDef.indexKeyDefinitions();
        IndexKeyTypeSettings keyTypeSettings = idx.segment(0).rowHandler().indexKeyTypeSettings();
        CacheObjectContext coctx = idx.segment(0).cacheGroupContext().cacheObjectContext();

        IndexRowComparator keyCmp = idxDef.rowComparator();

        for (IndexQueryCriterion c: idxQryDesc.criteria()) {
            RangeIndexQueryCriterion crit = (RangeIndexQueryCriterion)c;

            String fldName = idxFlds.containsKey(crit.field()) ? crit.field()
                : QueryUtils.normalizeObjectName(crit.field(), false);

            IndexKeyDefinition keyDef = idxFlds.get(fldName);

            if (keyDef == null)
                throw failIndexQuery("Index doesn't match criteria", idxDef, idxQryDesc);

            IndexKey l = key(crit.lower(), crit.lowerNull(), keyDef, keyTypeSettings, coctx);
            IndexKey u = key(crit.upper(), crit.upperNull(), keyDef, keyTypeSettings, coctx);

            if (l != null && u != null && keyCmp.compareKey(l, u) > 0) {
                throw failIndexQuery("Illegal criterion: lower boundary is greater than the upper boundary: " +
                    rangeDesc(crit, fldName, null, null), idxDef, idxQryDesc);
            }

            boolean lowIncl = crit.lowerIncl();
            boolean upIncl = crit.upperIncl();

            boolean lowNull = crit.lowerNull();
            boolean upNull = crit.upperNull();

            if (mergedCriteria.containsKey(fldName)) {
                RangeIndexQueryCriterion prev = mergedCriteria.get(fldName);

                IndexKey prevLower = (IndexKey)prev.lower();
                IndexKey prevUpper = (IndexKey)prev.upper();

                // Validate merged criteria.
                if (!checkBoundaries(l, prevUpper, crit.lowerIncl(), prev.upperIncl(), keyCmp) ||
                    !checkBoundaries(prevLower, u, prev.lowerIncl(), crit.upperIncl(), keyCmp)) {

                    String prevDesc = rangeDesc(prev, null,
                        prevLower == null ? null : prevLower.key(),
                        prevUpper == null ? null : prevUpper.key());

                    throw failIndexQuery("Failed to merge criterion " + rangeDesc(crit, fldName, null, null) +
                        " with previous criteria range " + prevDesc, idxDef, idxQryDesc);
                }

                int lowCmp = 0;

                // Use previous lower boudary, as it's greater than the current.
                if (l == null || (prevLower != null && (lowCmp = keyCmp.compareKey(prevLower, l)) >= 0)) {
                    l = prevLower;
                    lowIncl = lowCmp != 0 ? prev.lowerIncl() : prev.lowerIncl() ? lowIncl : prev.lowerIncl();
                    lowNull = prev.lowerNull();
                }

                int upCmp = 0;

                // Use previous upper boudary, as it's less than the current.
                if (u == null || (prevUpper != null && (upCmp = keyCmp.compareKey(prevUpper, u)) <= 0)) {
                    u = prevUpper;
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

    /**
     * @return {@code} true if boudaries are intersected, otherwise {@code false}.
     */
    private boolean checkBoundaries(
        IndexKey left,
        IndexKey right,
        boolean leftIncl,
        boolean rightIncl,
        IndexRowComparator keyCmp
    ) throws IgniteCheckedException {
        boolean boundaryCheck = left != null && right != null;

        if (boundaryCheck) {
            int cmp = keyCmp.compareKey(left, right);

            return cmp < 0 || (cmp == 0 && leftIncl && rightIncl);
        }

        return true;
    }

    /** Checks that specified index matches index query criteria. */
    private IndexRangeQuery alignCriteriaWithIndex(
        InlineIndexImpl idx,
        Map<String, RangeIndexQueryCriterion> criteria,
        IndexDefinition idxDef
    ) {
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

            if (keyDef.getValue().order().sortOrder() == DESC)
                criterion = criterion.swap();

            qry.criteria[i] = criterion;

            IndexKey l = (IndexKey)criterion.lower();
            IndexKey u = (IndexKey)criterion.upper();

            if (l != null)
                lowerAllNulls = false;

            if (u != null)
                upperAllNulls = false;

            lowerBounds[i] = l;
            upperBounds[i++] = u;

            if (criteria.isEmpty())
                break;
        }

        InlineIndexRowHandler hnd = idx.segment(0).rowHandler();

        qry.lower = lowerAllNulls ? null : new IndexSearchRowImpl(lowerBounds, hnd);
        qry.upper = upperAllNulls ? null : new IndexSearchRowImpl(upperBounds, hnd);

        return qry;
    }

    /**
     * Prepare index query.
     *
     * @return Prepared query for index range.
     */
    private IndexRangeQuery prepareQuery(SortedSegmentedIndex idx, IndexQueryDesc idxQryDesc) throws IgniteCheckedException {
        SortedIndexDefinition idxDef = (SortedIndexDefinition)idxProc.indexDefinition(idx.id());

        // For PK indexes will serialize _KEY column.
        if (F.isEmpty(idxQryDesc.criteria()))
            return new IndexRangeQuery(1);

        InlineIndexImpl sortedIdx = (InlineIndexImpl)idx;

        Map<String, RangeIndexQueryCriterion> merged = mergeIndexQueryCriteria(sortedIdx, idxDef, idxQryDesc);

        return alignCriteriaWithIndex(sortedIdx, merged, idxDef);
    }

    /**
     * Runs an index query.
     *
     * @return Result cursor.
     */
    private GridCursor<IndexRow> querySortedIndex(
        GridCacheContext<?, ?> cctx,
        SortedSegmentedIndex idx,
        IndexQueryContext qryCtx,
        IndexRangeQuery qry
    ) throws IgniteCheckedException {
        int segmentsCnt = cctx.isPartitioned() ? cctx.config().getQueryParallelism() : 1;

        if (segmentsCnt == 1)
            return treeIndexRange(idx, 0, qry, qryCtx);

        final GridCursor<IndexRow>[] segmentCursors = new GridCursor[segmentsCnt];

        // Actually it just traverses BPlusTree to find boundaries. It's too fast to parallelize this.
        for (int i = 0; i < segmentsCnt; i++)
            segmentCursors[i] = treeIndexRange(idx, i, qry, qryCtx);

        return new SegmentedIndexCursor(segmentCursors, (SortedIndexDefinition)idxProc.indexDefinition(idx.id()));
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
    private GridCursor<IndexRow> treeIndexRange(SortedSegmentedIndex idx, int segment, IndexRangeQuery qry, IndexQueryContext qryCtx)
        throws IgniteCheckedException {

        LinkedHashMap<String, IndexKeyDefinition> idxDef = idxProc.indexDefinition(idx.id()).indexKeyDefinitions();

        boolean lowIncl = inclBoundary(qry, true);
        boolean upIncl = inclBoundary(qry, false);

        // Step 1. Traverse index and find index boundaries.
        GridCursor<IndexRow> findRes = idx.find(qry.lower, qry.upper, lowIncl, upIncl, segment, qryCtx);

        // No need in the additional filter step for queries with 0 or 1 criteria.
        if (qry.criteria.length <= 1)
            return findRes;

        // Step 2. Filter range if the criteria apply to multiple fields.
        return new GridCursor<IndexRow>() {
            /** */
            private final IndexRowComparator rowCmp = ((SortedIndexDefinition)idxProc.indexDefinition(idx.id())).rowComparator();

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
                    return false;  // Unbounded search, include all.

                int criteriaKeysCnt = qry.criteria.length;

                for (int i = 0; i < criteriaKeysCnt; i++) {
                    RangeIndexQueryCriterion c = qry.criteria[i];

                    boolean descOrder = idxDef.get(c.field()).order().sortOrder() == DESC;

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
     * Checks whether index thraversing should include boundary or not. Includes a boundary for unbounded searches, for
     * others it checks user criteria.
     *
     * @param lower {@code true} for lower bound and {@code false} for upper bound.
     * @return {@code true} for inclusive boundary, otherwise {@code false}.
     */
    private boolean inclBoundary(IndexRangeQuery qry, boolean lower) {
        for (RangeIndexQueryCriterion c: qry.criteria) {
            if (c == null || (lower ? c.lower() : c.upper()) == null)
                break;

            if (!(lower ? c.lowerIncl() : c.upperIncl()))
                return false;
        }

        return true;
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

    /** Single cursor over multiple segments. The next value is chosen with the index row comparator. */
    private static class SegmentedIndexCursor implements GridCursor<IndexRow> {
        /** Cursors over segments. */
        private final PriorityQueue<GridCursor<IndexRow>> cursors;

        /** Comparator to compare index rows. */
        private final Comparator<GridCursor<IndexRow>> cursorComp;

        /** */
        private IndexRow head;

        /** */
        SegmentedIndexCursor(GridCursor<IndexRow>[] cursors, SortedIndexDefinition idxDef) throws IgniteCheckedException {
            cursorComp = new Comparator<GridCursor<IndexRow>>() {
                @Override public int compare(GridCursor<IndexRow> o1, GridCursor<IndexRow> o2) {
                    try {
                        int keysLen = o1.get().keys().length;

                        Iterator<IndexKeyDefinition> it = idxDef.indexKeyDefinitions().values().iterator();

                        for (int i = 0; i < keysLen; i++) {
                            int cmp = idxDef.rowComparator().compareRow(o1.get(), o2.get(), i);

                            IndexKeyDefinition def = it.next();

                            if (cmp != 0) {
                                boolean desc = def.order().sortOrder() == SortOrder.DESC;

                                return desc ? -cmp : cmp;
                            }
                        }

                        return 0;

                    } catch (IgniteCheckedException e) {
                        throw new IgniteException("Failed to sort remote index rows", e);
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

    /**
     * @return Modified description for criterion in case of error.
     */
    private static String rangeDesc(RangeIndexQueryCriterion c, String fldName, Object lower, Object upper) {
        String fld = fldName == null ? c.field() : fldName;

        Object l = lower == null ? c.lower() : lower;
        Object u = upper == null ? c.upper() : upper;

        RangeIndexQueryCriterion r = new RangeIndexQueryCriterion(fld, l, u);

        r.lowerIncl(c.lowerIncl());
        r.upperIncl(c.upperIncl());

        return r.toString();
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
