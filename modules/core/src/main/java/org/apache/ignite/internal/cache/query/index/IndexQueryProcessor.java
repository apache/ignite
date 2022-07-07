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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.IgniteCheckedException;
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
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndexTree;
import org.apache.ignite.internal.cache.query.index.sorted.keys.IndexKey;
import org.apache.ignite.internal.cache.query.index.sorted.keys.IndexKeyFactory;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.CacheObjectUtils;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.cache.query.IndexQueryDesc;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.util.GridCloseableIteratorAdapter;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.spi.indexing.IndexingQueryFilter;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.cache.query.index.SortOrder.DESC;
import static org.apache.ignite.internal.cache.query.index.sorted.inline.types.NullableInlineIndexKeyType.CANT_BE_COMPARE;
import static org.apache.ignite.internal.cache.query.index.sorted.inline.types.NullableInlineIndexKeyType.COMPARE_UNSUPPORTED;

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
        IndexingQueryFilter cacheFilter,
        boolean keepBinary
    ) throws IgniteCheckedException {
        SortedSegmentedIndex idx = findSortedIndex(cctx, idxQryDesc);

        IndexRangeQuery qry = prepareQuery(idx, idxQryDesc);

        GridCursor<IndexRow> cursor = querySortedIndex(idx, cacheFilter, qry);

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
        SortedSegmentedIndex idx,
        IndexingQueryFilter cacheFilter,
        IndexRangeQuery qry
    ) throws IgniteCheckedException {
        BPlusTree.TreeRowClosure<IndexRow, IndexRow> treeFilter = null;

        // No need in the additional filter step for queries with 0 or 1 criteria.
        // Also skips filtering if the current search is unbounded (both boundaries equal to null).
        if (qry.criteria.length > 1 && !(qry.lower == null && qry.upper == null)) {
            LinkedHashMap<String, IndexKeyDefinition> idxDef = idxProc.indexDefinition(idx.id()).indexKeyDefinitions();

            treeFilter = new IndexQueryCriteriaClosure(
                qry, idxDef, ((SortedIndexDefinition)idxProc.indexDefinition(idx.id())).rowComparator());
        }

        IndexQueryContext qryCtx = new IndexQueryContext(cacheFilter, treeFilter, null);

        return treeIndexRange(idx, qry, qryCtx);
    }

    /**
     * Runs range query over all segments. There are 2 steps to run query:
     * 1. Traverse index by specified boundaries;
     * 2. Scan over cursor and filter rows that doesn't match user criteria.
     *
     * Filtering is required in 2 cases:
     * 1. Exclusion of one of boundaries, as idx.find() includes both of them;
     * 2. To apply criteria on non-first index fields. Tree apply boundaries field by field, if first field match
     * a boundary, then second field isn't checked within traversing.
     */
    private GridCursor<IndexRow> treeIndexRange(SortedSegmentedIndex idx, IndexRangeQuery qry, IndexQueryContext qryCtx)
        throws IgniteCheckedException {

        boolean lowIncl = inclBoundary(qry, true);
        boolean upIncl = inclBoundary(qry, false);

        return idx.find(qry.lower, qry.upper, lowIncl, upIncl, qryCtx);
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

    /**
     * Checks index rows for matching to specified index criteria.
     */
    private static class IndexQueryCriteriaClosure implements BPlusTree.TreeRowClosure<IndexRow, IndexRow> {
        /** */
        private final IndexRangeQuery qry;

        /** */
        private final IndexRowComparator rowCmp;

        /** */
        private final boolean[] descOrderCache;

        /** */
        IndexQueryCriteriaClosure(
            IndexRangeQuery qry,
            LinkedHashMap<String, IndexKeyDefinition> idxDef,
            IndexRowComparator rowCmp
        ) {
            this.qry = qry;
            this.rowCmp = rowCmp;
            descOrderCache = new boolean[qry.criteria.length];

            for (int i = 0; i < qry.criteria.length; i++) {
                RangeIndexQueryCriterion c = qry.criteria[i];

                descOrderCache[i] = idxDef.get(c.field()).order().sortOrder() == DESC;
            }
        }

        /** {@inheritDoc} */
        @Override public boolean apply(
            BPlusTree<IndexRow, IndexRow> tree,
            BPlusIO<IndexRow> io,
            long pageAddr,
            int idx
        ) throws IgniteCheckedException {
            return !rowIsOutOfRange((InlineIndexTree)tree, io, pageAddr, idx, qry.lower, qry.upper);
        }

        /**
         * Checks that {@code row} belongs to the range specified with {@code low} and {@code high}.
         *
         * @return {@code true} if the row doesn't belong the range, otherwise {@code false}.
         */
        private boolean rowIsOutOfRange(
            InlineIndexTree tree,
            BPlusIO<IndexRow> io,
            long pageAddr,
            int idx,
            IndexRow low,
            IndexRow high
        ) throws IgniteCheckedException {
            int criteriaKeysCnt = qry.criteria.length;

            int off = io.offset(idx);

            int fieldOff = 0;

            InlineIndexRow currRow = new InlineIndexRow(tree, io, pageAddr, idx);

            List<InlineIndexKeyType> keyTypes = tree.rowHandler().inlineIndexKeyTypes();

            for (int keyIdx = 0; keyIdx < criteriaKeysCnt; keyIdx++) {
                RangeIndexQueryCriterion c = qry.criteria[keyIdx];

                InlineIndexKeyType keyType = keyIdx < keyTypes.size() ? keyTypes.get(keyIdx) : null;

                boolean descOrder = descOrderCache[keyIdx];

                int maxSize = tree.inlineSize() - fieldOff;

                if (low != null && low.key(keyIdx) != null) {
                    int cmp = currRow.compare(rowCmp, low, keyIdx, off + fieldOff, maxSize, keyType);

                    if (cmp == 0) {
                        if (!c.lowerIncl())
                            return true;  // Exclude if field equals boundary field and criteria is excluding.
                    }
                    else if ((cmp < 0) ^ descOrder)
                        return true;  // Out of bound. Either below 'low' margin or column with desc order.
                }

                if (high != null && high.key(keyIdx) != null) {
                    int cmp = currRow.compare(rowCmp, high, keyIdx, off + fieldOff, maxSize, keyType);

                    if (cmp == 0) {
                        if (!c.upperIncl())
                            return true;  // Exclude if field equals boundary field and criteria is excluding.
                    }
                    else if ((cmp > 0) ^ descOrder)
                        return true;  // Out of bound. Either above 'high' margin or column with desc order.
                }

                if (keyType != null)
                    fieldOff += keyType.inlineSize(pageAddr, off + fieldOff);
            }

            return false;
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

    /**
     * Wrapper class over index row. It is suitable for comparison. It tries to check inlined keys first, and fetches a
     * cache entry only if the inlined information is not full enough for comparison.
     */
    private static class InlineIndexRow {
        /** */
        private final long pageAddr;

        /** */
        private final int idx;

        /** */
        private final InlineIndexTree tree;

        /** */
        private final BPlusIO<IndexRow> io;

        /** Set it for accessing keys from underlying cache entry. */
        private IndexRow currRow;

        /** */
        private InlineIndexRow(InlineIndexTree tree, BPlusIO<IndexRow> io, long addr, int idx) {
            pageAddr = addr;
            this.idx = idx;
            this.tree = tree;
            this.io = io;
        }

        /** Compare using inline. {@code keyType} is {@code null} for non-inlined keys. */
        private int compare(
            IndexRowComparator rowCmp,
            IndexRow o,
            int keyIdx,
            int off,
            int maxSize,
            @Nullable InlineIndexKeyType keyType
        ) throws IgniteCheckedException {
            if (currRow == null) {
                int cmp = COMPARE_UNSUPPORTED;

                if (keyType != null)
                    cmp = rowCmp.compareKey(pageAddr, off, maxSize, o.key(keyIdx), keyType);

                if (cmp == COMPARE_UNSUPPORTED || cmp == CANT_BE_COMPARE)
                    currRow = tree.getRow(io, pageAddr, idx);
                else
                    return cmp;
            }

            return rowCmp.compareRow(currRow, o, keyIdx);
        }
    }
}
