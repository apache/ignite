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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.query.IndexQuery;
import org.apache.ignite.cache.query.IndexQueryCriterion;
import org.apache.ignite.internal.cache.query.RangeIndexQueryCriterion;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyDefinition;
import org.apache.ignite.internal.cache.query.index.sorted.IndexRow;
import org.apache.ignite.internal.cache.query.index.sorted.IndexRowComparator;
import org.apache.ignite.internal.cache.query.index.sorted.IndexSearchRowImpl;
import org.apache.ignite.internal.cache.query.index.sorted.SortedIndexDefinition;
import org.apache.ignite.internal.cache.query.index.sorted.SortedSegmentedIndex;
import org.apache.ignite.internal.cache.query.index.sorted.inline.IndexQueryContext;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndexImpl;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndexTree;
import org.apache.ignite.internal.cache.query.index.sorted.keys.IndexKey;
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
        InlineIndexImpl idx = (InlineIndexImpl)findSortedIndex(cctx, idxQryDesc);

        IndexMultipleRangeQuery qry = prepareQuery(idx, idxQryDesc);

        GridCursor<IndexRow> cursor = queryMultipleRanges(idx, cacheFilter, qry);

        SortedIndexDefinition def = (SortedIndexDefinition)idxProc.indexDefinition(idx.id());

        IndexQueryResultMeta meta = new IndexQueryResultMeta(def, qry.critSize());

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
            throw new IgniteCheckedException("No table found for type: " + idxQryDesc.valType());

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
            return indexByCriteria(cctx, critFlds, tableName);

        // If index name isn't specified and criteria aren't set then use the PK index.
        String name = idxQryDesc.idxName() == null ? QueryUtils.PRIMARY_KEY_INDEX : idxQryDesc.idxName();

        IndexName idxName = new IndexName(cctx.name(), cctx.kernalContext().query().schemaName(cctx), tableName, name);

        return indexByName(idxName, critFlds);
    }

    /**
     * @return Sorted index found by name.
     * @throws IgniteCheckedException If index not found or specified index doesn't match query criteria.
     */
    private SortedSegmentedIndex indexByName(IndexName idxName, final Map<String, String> criteriaFlds) throws IgniteCheckedException {
        SortedSegmentedIndex idx = assertSortedIndex(idxProc.index(idxName));

        if (idx == null && !QueryUtils.PRIMARY_KEY_INDEX.equals(idxName.idxName())) {
            String normIdxName = QueryUtils.normalizeObjectName(idxName.idxName(), false);

            idxName = new IndexName(idxName.cacheName(), idxName.schemaName(), idxName.tableName(), normIdxName);

            idx = assertSortedIndex(idxProc.index(idxName));
        }

        if (idx == null)
            throw new IgniteCheckedException("No index found for name: " + idxName.idxName());

        if (!checkIndex(idx, idxName.tableName(), criteriaFlds))
            throw new IgniteCheckedException("Index doesn't match criteria. Index " + idxName.idxName());

        return idx;
    }

    /**
     * @return Index found by list of criteria fields.
     * @throws IgniteCheckedException if suitable index not found.
     */
    private SortedSegmentedIndex indexByCriteria(
        GridCacheContext<?, ?> cctx,
        final Map<String, String> criteriaFlds,
        String tableName
    ) throws IgniteCheckedException {
        Collection<Index> idxs = idxProc.indexes(cctx.name());

        for (Index idx: idxs) {
            SortedSegmentedIndex sortedIdx = assertSortedIndex(idx);

            if (checkIndex(sortedIdx, tableName, criteriaFlds))
                return sortedIdx;
        }

        throw new IgniteCheckedException("No index found for criteria.");
    }

    /** Assert if specified index is not an instance of {@link SortedSegmentedIndex}. */
    private SortedSegmentedIndex assertSortedIndex(Index idx) throws IgniteCheckedException {
        if (idx == null)
            return null;

        if (!(idx instanceof SortedSegmentedIndex))
            throw new IgniteCheckedException("IndexQuery is not supported for index: " + idx.name());

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

    /** Merges multiple criteria for the same field into single criterion. */
    private IndexMultipleRangeQuery mergeIndexQueryCriteria(InlineIndexImpl idx, IndexQueryDesc idxQryDesc) throws IgniteCheckedException {
        Map<String, IndexKeyQueryCondition> mergedCriteria = new HashMap<>();

        SortedIndexDefinition idxDef = idx.indexDefinition();

        Map<String, IndexKeyDefinition> idxFlds = idxDef.indexKeyDefinitions();

        // Merge.
        for (IndexQueryCriterion crit: idxQryDesc.criteria()) {
            String fldName = idxFlds.containsKey(crit.field()) ? crit.field()
                : QueryUtils.normalizeObjectName(crit.field(), false);

            IndexKeyDefinition keyDef = idxFlds.get(fldName);

            if (keyDef == null)
                throw new IgniteCheckedException("Index doesn't match criteria. Index " + idxDef + ", criterion field=" + fldName);

            mergedCriteria.putIfAbsent(fldName, new IndexKeyQueryCondition(fldName, idx));

            IndexKeyQueryCondition idxKeyCond = mergedCriteria.get(fldName);

            idxKeyCond.accumulate(crit);
        }

        // Allign with index and check that users criteria matches a prefix subset of index fields.
        int i = 0;

        IndexMultipleRangeQuery multipleQry = new IndexMultipleRangeQuery(idxFlds.size(), mergedCriteria.size());

        for (Map.Entry<String, IndexKeyDefinition> keyDef: idxFlds.entrySet()) {
            IndexKeyQueryCondition keyCond = mergedCriteria.remove(keyDef.getKey());

            if (keyCond == null)
                break;

            multipleQry.addIndexKeyCondition(i++, keyCond);
        }

        return multipleQry;
    }

    /**
     * Prepare index query.
     *
     * @return Prepared query for index range.
     */
    private IndexMultipleRangeQuery prepareQuery(InlineIndexImpl idx, IndexQueryDesc idxQryDesc) throws IgniteCheckedException {
        if (F.isEmpty(idxQryDesc.criteria())) {
            IndexMultipleRangeQuery multQry = new IndexMultipleRangeQuery(
                idx.indexDefinition().indexKeyDefinitions().size(), 1);

            multQry.addIndexKeyCondition(0, new IndexKeyQueryCondition(QueryUtils.KEY_FIELD_NAME, idx));

            return multQry;
        }

        return mergeIndexQueryCriteria(idx, idxQryDesc);
    }

    /**
     * Queries multiple ranges.
     *
     * @return Cursor over IndexRows that match user's criteria.
     */
    private GridCursor<IndexRow> queryMultipleRanges(
        InlineIndexImpl idx,
        IndexingQueryFilter cacheFilter,
        IndexMultipleRangeQuery qry
    ) throws IgniteCheckedException {
        List<IndexSingleRangeQuery> queries = qry.queries;

        if (queries.size() == 1)
            return querySortedIndex(idx, cacheFilter, queries.get(0));

        return new GridCursor<IndexRow>() {
            private GridCursor<IndexRow> currCursor;

            private int qryNum;

            /** {@inheritDoc} */
            @Override public boolean next() throws IgniteCheckedException {
                while (currCursor == null || !currCursor.next()) {
                    if (qryNum == queries.size())
                        return false;

                    IndexSingleRangeQuery q = queries.get(qryNum++);

                    currCursor = querySortedIndex(idx, cacheFilter, q);
                }

                return true;
            }

            /** {@inheritDoc} */
            @Override public IndexRow get() throws IgniteCheckedException {
                return currCursor.get();
            }
        };
    }

    /**
     * Runs single index query.
     *
     * @return Result cursor.
     */
    private GridCursor<IndexRow> querySortedIndex(
        SortedSegmentedIndex idx,
        IndexingQueryFilter cacheFilter,
        IndexSingleRangeQuery qry
    ) throws IgniteCheckedException {
        BPlusTree.TreeRowClosure<IndexRow, IndexRow> treeFilter = null;

        // No need in the additional filter step for queries with 0 or 1 criteria.
        // Also skips filtering if the current search is unbounded (both boundaries equal to null).
        if (qry.keyCond.length > 1 && !(qry.lowerAllNulls && qry.upperAllNulls)) {
            treeFilter = new IndexQueryCriteriaClosure(
                qry, ((SortedIndexDefinition)idxProc.indexDefinition(idx.id())).rowComparator());
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
    private GridCursor<IndexRow> treeIndexRange(SortedSegmentedIndex idx, IndexSingleRangeQuery qry, IndexQueryContext qryCtx)
        throws IgniteCheckedException {

        boolean lowIncl = inclBoundary(qry, true);
        boolean upIncl = inclBoundary(qry, false);

        return idx.find(qry.lower(), qry.upper(), lowIncl, upIncl, qryCtx);
    }

    /**
     * Checks whether index thraversing should include boundary or not. Includes a boundary for unbounded searches, for
     * others it checks user criteria.
     *
     * @param lower {@code true} for lower bound and {@code false} for upper bound.
     * @return {@code true} for inclusive boundary, otherwise {@code false}.
     */
    private boolean inclBoundary(IndexSingleRangeQuery qry, boolean lower) {
        for (IndexKeyQueryCondition cond: qry.keyCond) {
            RangeIndexQueryCriterion c = cond.range();

            if (c == null || (lower ? c.lower() : c.upper()) == null)
                break;

            if (!(lower ? c.lowerIncl() : c.upperIncl()))
                return false;
        }

        return true;
    }

    /**
     * Checks index rows for matching to specified index criteria.
     */
    private static class IndexQueryCriteriaClosure implements BPlusTree.TreeRowClosure<IndexRow, IndexRow> {
        /** */
        private final IndexSingleRangeQuery qry;

        /** */
        private final IndexRowComparator rowCmp;

        /** */
        private final boolean[] descOrderCache;

        /** */
        IndexQueryCriteriaClosure(IndexSingleRangeQuery qry, IndexRowComparator rowCmp) {
            this.qry = qry;
            this.rowCmp = rowCmp;
            descOrderCache = new boolean[qry.keyCond.length];

            for (int i = 0; i < qry.keyCond.length; i++) {
                RangeIndexQueryCriterion c = qry.keyCond[i].range();

                if (c != null)
                    descOrderCache[i] = qry.keyCond[i].desc();
            }
        }

        /** {@inheritDoc} */
        @Override public boolean apply(
            BPlusTree<IndexRow, IndexRow> tree,
            BPlusIO<IndexRow> io,
            long pageAddr,
            int idx
        ) throws IgniteCheckedException {
            return !rowIsOutOfRange((InlineIndexTree)tree, io, pageAddr, idx, qry.lower(), qry.upper());
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
            int criteriaKeysCnt = qry.keyCond.length;

            int off = io.offset(idx);

            int fieldOff = 0;

            InlineIndexRow currRow = new InlineIndexRow(tree, io, pageAddr, idx);

            List<InlineIndexKeyType> keyTypes = tree.rowHandler().inlineIndexKeyTypes();

            for (int keyIdx = 0; keyIdx < criteriaKeysCnt; keyIdx++) {
                RangeIndexQueryCriterion c = qry.keyCond[keyIdx].range();
                Set<IndexKey> inVals = qry.keyCond[keyIdx].inVals();

                InlineIndexKeyType keyType = keyIdx < keyTypes.size() ? keyTypes.get(keyIdx) : null;

                boolean descOrder = descOrderCache[keyIdx];

                int maxSize = tree.inlineSize() - fieldOff;

                if (inVals != null) {
                    IndexRow row = io.getLookupRow(tree, pageAddr, idx);

                    // Range boundaries were already checked for all IN values.
                    return !inVals.contains(row.key(keyIdx));
                }

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
    public static String rangeDesc(RangeIndexQueryCriterion c, String fldName, Object lower, Object upper) {
        String fld = fldName == null ? c.field() : fldName;

        Object l = lower == null ? c.lower() : lower;
        Object u = upper == null ? c.upper() : upper;

        RangeIndexQueryCriterion r = new RangeIndexQueryCriterion(fld, l, u);

        r.lowerIncl(c.lowerIncl());
        r.upperIncl(c.upperIncl());

        return r.toString();
    }

    /**
     * Represents ordered list of independent index range queries.
     */
    private static class IndexMultipleRangeQuery {
        /** Ordered list of index ranges queries. */
        private final List<IndexSingleRangeQuery> queries = new ArrayList<>();

        /** */
        private final int critSize;

        /** */
        private final int idxRowSize;

        /** */
        IndexMultipleRangeQuery(int idxRowSize, int critSize) {
            this.critSize = critSize;
            this.idxRowSize = idxRowSize;
        }

        /** */
        int critSize() {
            return critSize;
        }

        /**
         * Adds condition. In case of multiple queries it adds to every query.
         */
        void addIndexKeyCondition(int i, IndexKeyQueryCondition cond) {
            if (i == 0)
                addFirstIndexKeyCondition(cond);
            else {
                for (IndexSingleRangeQuery qry: queries)
                    qry.addCondition(cond, i);
            }
        }

        /**
         * Add first condition. If it contains IN clause then split query to multiple index ranges joint with OR:
         *
         * IN(A, B) and GT(C) = (EQ(A) and GT(C)) or (EQ(B) and GT(C)).
         *
         * It ignores RANGE criterion if IN is specified. Intersection of them was already checked on prepare query phase.
         */
        private void addFirstIndexKeyCondition(IndexKeyQueryCondition keyCond) {
            if (keyCond.inVals() != null) {
                for (IndexKey k: keyCond.inVals()) {
                    RangeIndexQueryCriterion c = new RangeIndexQueryCriterion(keyCond.fieldName(), k, k);
                    c.lowerIncl(true);
                    c.upperIncl(true);

                    IndexKeyQueryCondition cond = new IndexKeyQueryCondition(keyCond.fieldName(), keyCond.index(), c, null);

                    IndexSingleRangeQuery q = new IndexSingleRangeQuery(idxRowSize, critSize);

                    q.addCondition(cond, 0);

                    queries.add(q);
                }
            }
            else {
                IndexSingleRangeQuery qry = new IndexSingleRangeQuery(idxRowSize, critSize);

                qry.addCondition(keyCond, 0);

                queries.add(qry);
            }
        }
    }

    /** */
    private static class IndexSingleRangeQuery {
        /** Ordered list of criteria. Order matches index fields order. */
        private final IndexKeyQueryCondition[] keyCond;

        /** Array of IndexKeys to query underlying index. */
        private final IndexKey[] lowerBounds;

        /** Array of IndexKeys to query underlying index. */
        private final IndexKey[] upperBounds;

        /** {@code true} if all {@link #lowerBounds} keys are null. */
        private boolean lowerAllNulls = true;

        /** {@code true} if all {@link #upperBounds} keys are null. */
        private boolean upperAllNulls = true;

        /** Lower bound to query underlying index. */
        private @Nullable IndexSearchRowImpl lower;

        /** Upper bound to query underlying index. */
        private @Nullable IndexSearchRowImpl upper;

        /** */
        IndexSingleRangeQuery(int idxRowSize, int critSize) {
            keyCond = new IndexKeyQueryCondition[critSize];

            // Size of bounds array has to be equal to count of indexed fields.
            lowerBounds = new IndexKey[idxRowSize];
            upperBounds = new IndexKey[idxRowSize];
        }

        /** */
        void addCondition(IndexKeyQueryCondition cond, int i) {
            keyCond[i] = cond;

            if (cond.range() != null) {
                IndexKey l = (IndexKey)cond.range().lower();
                IndexKey u = (IndexKey)cond.range().upper();

                if (l != null)
                    lowerAllNulls = false;

                if (u != null)
                    upperAllNulls = false;

                lowerBounds[i] = l;
                upperBounds[i] = u;
            }
            else {
                lowerAllNulls = false;
                upperAllNulls = false;
            }
        }

        /** */
        @Nullable IndexSearchRowImpl lower() {
            if (lower == null && !lowerAllNulls)
                lower = new IndexSearchRowImpl(lowerBounds, null);

            return lower;
        }

        /** */
        @Nullable IndexSearchRowImpl upper() {
            if (upper == null && !upperAllNulls)
                upper = new IndexSearchRowImpl(upperBounds, null);

            return upper;
        }
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
