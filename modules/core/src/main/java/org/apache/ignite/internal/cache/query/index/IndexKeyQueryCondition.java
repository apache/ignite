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

import java.util.Collections;
import java.util.Iterator;
import java.util.SortedSet;
import java.util.TreeSet;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.query.IndexQueryCriterion;
import org.apache.ignite.internal.cache.query.InIndexQueryCriterion;
import org.apache.ignite.internal.cache.query.RangeIndexQueryCriterion;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyDefinition;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyTypeSettings;
import org.apache.ignite.internal.cache.query.index.sorted.IndexRowComparator;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndexImpl;
import org.apache.ignite.internal.cache.query.index.sorted.keys.IndexKey;
import org.apache.ignite.internal.cache.query.index.sorted.keys.IndexKeyFactory;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.cache.query.index.IndexQueryProcessor.rangeDesc;
import static org.apache.ignite.internal.cache.query.index.SortOrder.DESC;

/**
 * IndexQuery condition for single indexed key. It accumulates user defined criteria (with Object as parameters)
 * for the same key to single one and transforms it to apply to {@link IndexKey} parameters.
 */
class IndexKeyQueryCondition {
    /** Index field name. */
    private final String fldName;

    /** Accumulated IndexKey RANGE criterion. */
    private RangeIndexQueryCriterion range;

    /**
     * Accumulated IndexKey IN criterion values. Values are sorted to guarantee order of IndexQuery result.
     */
    private SortedSet<IndexKey> inVals;

    /** Index. */
    private final InlineIndexImpl idx;

    /** IndexKey comparator. */
    private IndexRowComparator keyCmp;

    /**
     * Constructor for preparing index key condition.
     *
     * @param fldName Index field name for index key.
     * @param idx Index.
     */
    IndexKeyQueryCondition(String fldName, InlineIndexImpl idx) {
        this.idx = idx;
        this.fldName = fldName;

        keyCmp = idx.indexDefinition().rowComparator();
    }

    /**
     * Constructor for already prepared index key condition.
     *
     * @param fldName Index field name for index key.
     * @param range Prepared range criterion for IndexKeys.
     * @param inVals Prepared IndexKey values for IN criterion.
     */
    IndexKeyQueryCondition(String fldName, InlineIndexImpl idx, RangeIndexQueryCriterion range, SortedSet<IndexKey> inVals) {
        this.range = range;
        this.idx = idx;
        this.fldName = fldName;
        this.inVals = inVals;
    }

    /**
     * Accumulate User's criterion for index key.
     *
     * @param criterion User's criterion for index key.
     */
    void accumulate(IndexQueryCriterion criterion) throws IgniteCheckedException {
        if (criterion instanceof InIndexQueryCriterion)
            addInCriterion((InIndexQueryCriterion)criterion);
        else if (criterion instanceof RangeIndexQueryCriterion)
            addRangeCriterion((RangeIndexQueryCriterion)criterion);
        else
            throw new IgniteCheckedException("Unexpected IndexQueryCriterion class: " + criterion);
    }

    /** */
    public InlineIndexImpl index() {
        return idx;
    }

    /** */
    public String fieldName() {
        return fldName;
    }

    /**
     * @return Prepared IndexKey range.
     */
    public RangeIndexQueryCriterion range() {
        return range;
    }

    /**
     * @return Prepared IndexKey values for IN operation.
     */
    public SortedSet<IndexKey> inVals() {
        return inVals == null ? null : Collections.unmodifiableSortedSet(inVals);
    }

    /**
     * @param criterion User IN criterion.
     */
    private void addInCriterion(InIndexQueryCriterion criterion) throws IgniteCheckedException {
        if (inVals != null)
            throw new IgniteCheckedException("Multiple IN criteria for same field arent't supported.");

        inVals = new TreeSet<>((idxKeyLeft, idxKeyRight) -> {
            try {
                return compare(idxKeyLeft, idxKeyRight);
            }
            catch (IgniteCheckedException e) {
                // Never reach this.
                throw new IgniteException(e);
            }
        });

        IndexKeyType expCondType = idx.indexDefinition().indexKeyDefinitions().get(fldName).idxType();

        for (Object v: criterion.values()) {
            IndexKey k = key(v, v == null);

            if (k.type() != IndexKeyType.NULL && k.type() != expCondType)
                throw new IgniteCheckedException("Wrong type of value in IN criterion. Expect " + expCondType + ", receieve " + k.type());

            inVals.add(k);
        }

        narrowRangeWithIn();
    }

    /**
     * Sets or merges (if multiple criteria for the same field) RANGE criterion.
     *
     * @param criterion User RANGE criterion.
     */
    private void addRangeCriterion(RangeIndexQueryCriterion criterion) throws IgniteCheckedException {
        boolean desc = desc();

        if (desc)
            criterion = criterion.swap();

        IndexKey l = key(criterion.lower(), criterion.lowerNull());
        IndexKey u = key(criterion.upper(), criterion.upperNull());

        if (l != null && u != null && (desc ? -1 : 1 ) * keyCmp.compareKey(l, u) > 0) {
            throw new IgniteCheckedException("Illegal criterion: lower boundary is greater than the upper boundary: "
                + rangeDesc(criterion, fldName, null, null));
        }

        RangeIndexQueryCriterion crit = new RangeIndexQueryCriterion(fldName, l, u);
        crit.lowerIncl(criterion.lowerIncl());
        crit.upperIncl(criterion.upperIncl());
        crit.lowerNull(criterion.lowerNull());
        crit.upperNull(criterion.upperNull());

        range = range == null ? crit : mergeRanges(crit);

        checkInAndRangeCriteria();
    }

    /**
     * Merges existing {@link #range} criterion with another one for the same indexed field.
     */
    private RangeIndexQueryCriterion mergeRanges(RangeIndexQueryCriterion idxKeyCrit) throws IgniteCheckedException {
        IndexKey prevLower = (IndexKey)range.lower();
        IndexKey prevUpper = (IndexKey)range.upper();

        IndexKey l = (IndexKey)idxKeyCrit.lower();
        IndexKey u = (IndexKey)idxKeyCrit.upper();

        // Validate merged criteria.
        if (!checkBoundaries(l, prevUpper, idxKeyCrit.lowerIncl(), range.upperIncl()) ||
            !checkBoundaries(prevLower, u, range.lowerIncl(), idxKeyCrit.upperIncl())) {

            String prevDesc = rangeDesc(range, range.field(),
                prevLower == null ? null : prevLower.key(),
                prevUpper == null ? null : prevUpper.key());

            throw new IgniteCheckedException("Failed to merge criterion "
                + rangeDesc(idxKeyCrit, idxKeyCrit.field(), null, null)
                + " with previous criteria range " + prevDesc);
        }

        int lowCmp = 0;
        boolean lowIncl = idxKeyCrit.lowerIncl();
        boolean lowNull = idxKeyCrit.lowerNull();

        // Use previous lower boudary, as it's greater than the current.
        if (l == null || (prevLower != null && (lowCmp = compare(prevLower, l)) >= 0)) {
            l = prevLower;
            lowIncl = lowCmp != 0 ? range.lowerIncl() : range.lowerIncl() ? lowIncl : range.lowerIncl();
            lowNull = range.lowerNull();
        }

        int upCmp = 0;
        boolean upIncl = idxKeyCrit.upperIncl();
        boolean upNull = idxKeyCrit.upperNull();

        // Use previous upper boudary, as it's less than the current.
        if (u == null || (prevUpper != null && (upCmp = compare(prevUpper, u)) <= 0)) {
            u = prevUpper;
            upIncl = upCmp != 0 ? range.upperIncl() : range.upperIncl() ? upIncl : range.upperIncl();
            upNull = range.upperNull();
        }

        RangeIndexQueryCriterion crit = new RangeIndexQueryCriterion(idxKeyCrit.field(), l, u);
        crit.lowerIncl(lowIncl);
        crit.upperIncl(upIncl);
        crit.lowerNull(lowNull);
        crit.upperNull(upNull);

        return crit;
    }

    /**
     * @return {@code} true if boudaries are intersected, otherwise {@code false}.
     */
    private boolean checkBoundaries(
        IndexKey left,
        IndexKey right,
        boolean leftIncl,
        boolean rightIncl
    ) throws IgniteCheckedException {
        boolean boundaryCheck = left != null && right != null;

        if (boundaryCheck) {
            int cmp = compare(left, right);

            return cmp < 0 || (cmp == 0 && leftIncl && rightIncl);
        }

        return true;
    }

    /**
     * Checks that specified {@link #range} and {@link #inVals} criterion are intersected, otherwise failes. Unbounded
     * values are removed from {@link #inVals}.
     */
    private void checkInAndRangeCriteria() throws IgniteCheckedException {
        boolean changed = false;

        if (inVals != null && range != null) {
            for (Iterator<IndexKey> it = inVals.iterator(); it.hasNext(); ) {
                IndexKey k = it.next();

                if (range.lower() != null) {
                    int cmp = compare((IndexKey)range.lower(), k);

                    if (cmp > 0 || (cmp == 0 && !range.lowerIncl())) {
                        it.remove();

                        changed = true;
                    }
                }

                if (range.upper() != null) {
                    int cmp = compare((IndexKey)range.upper(), k);

                    if (cmp < 0 || (cmp == 0 && !range.upperIncl())) {
                        it.remove();

                        changed = true;
                    }
                }
            }

            if (inVals != null && inVals.isEmpty()) {
                throw new IgniteCheckedException(
                    "Failed to merge IN and RANGE criteria. No IN values match range criterion " + range);
            }
        }

        if (changed)
            narrowRangeWithIn();
    }

    /**
     * In case IN criterion specified it's possible to narrow existing index ranges to avoid wasteful checking.
     */
    private void narrowRangeWithIn() throws IgniteCheckedException {
        // Define narrow range to avoid misreading wide ranges.
        RangeIndexQueryCriterion r = new RangeIndexQueryCriterion(fldName, inVals.first(), inVals.last());
        r.lowerIncl(true);
        r.upperIncl(true);

        range = range == null ? r : mergeRanges(r);

        // Re-check after merging criteria.
        checkInAndRangeCriteria();
    }

    /**
     * @param val Value to wrap to IndexKey.
     * @param isNull {@code true} if user explicitly set {@code null} with a query argument.
     */
    private IndexKey key(@Nullable Object val, boolean isNull) {
        IndexKeyTypeSettings keyTypeSettings = idx.segment(0).rowHandler().indexKeyTypeSettings();
        CacheObjectContext coctx = idx.segment(0).cacheGroupContext().cacheObjectContext();

        IndexKey key = null;
        IndexKeyType keyType = val == null ? IndexKeyType.NULL : IndexKeyType.forClass(val.getClass());

        if (val != null || isNull)
            key = IndexKeyFactory.wrap(val, keyType, coctx, keyTypeSettings);

        return key;
    }

    /** */
    private int compare(IndexKey left, IndexKey right) throws IgniteCheckedException {
        return (desc() ? -1 : 1) * keyCmp.compareKey(left, right);
    }

    /** */
    boolean desc() {
        IndexKeyDefinition keyDef = idx.indexDefinition().indexKeyDefinitions().get(fldName);

        return keyDef.order().sortOrder() == DESC;
    }
}
