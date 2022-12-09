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

import org.apache.ignite.internal.cache.query.RangeIndexQueryCriterion;
import org.apache.ignite.internal.cache.query.index.sorted.IndexPlainRowImpl;
import org.apache.ignite.internal.cache.query.index.sorted.IndexRow;
import org.apache.ignite.internal.cache.query.index.sorted.IndexRowComparator;
import org.apache.ignite.internal.cache.query.index.sorted.keys.IndexKey;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.jetbrains.annotations.Nullable;

/** */
class IndexSingleRangeQuery {
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
    private @Nullable IndexPlainRowImpl lower;

    /** Upper bound to query underlying index. */
    private @Nullable IndexPlainRowImpl upper;

    /** */
    IndexSingleRangeQuery(int idxRowSize, int critSize) {
        keyCond = new IndexKeyQueryCondition[critSize];

        // Size of bounds array has to be equal to count of indexed fields.
        lowerBounds = new IndexKey[idxRowSize];
        upperBounds = new IndexKey[idxRowSize];
    }

    /**
     * @return Index tree filter for this query.
     */
    BPlusTree.TreeRowClosure<IndexRow, IndexRow> filter(IndexRowComparator rowCmp) {
        // No need in the additional filter step for queries with 0 or 1 criteria.
        // Also skips filtering if the current search is unbounded (both boundaries equal to null).
        if (keyCond.length > 1 && !(lowerAllNulls && upperAllNulls))
            return new IndexQueryCriteriaClosure(this, rowCmp);

        return null;
    }

    /** */
    IndexKeyQueryCondition keyCondition(int idx) {
        if (idx < keyCond.length)
            return keyCond[idx];

        return null;
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
    @Nullable IndexPlainRowImpl lower() {
        if (lower == null && !lowerAllNulls)
            lower = new IndexPlainRowImpl(lowerBounds, null);

        return lower;
    }

    /** */
    @Nullable IndexPlainRowImpl upper() {
        if (upper == null && !upperAllNulls)
            upper = new IndexPlainRowImpl(upperBounds, null);

        return upper;
    }

    /**
     * Checks whether index thraversing should include boundary or not. Includes a boundary for unbounded searches, for
     * others it checks user criteria.
     *
     * @param lower {@code true} for lower bound and {@code false} for upper bound.
     * @return {@code true} for inclusive boundary, otherwise {@code false}.
     */
    boolean inclBoundary(boolean lower) {
        for (IndexKeyQueryCondition cond: keyCond) {
            RangeIndexQueryCriterion c = cond.range();

            if (c == null || (lower ? c.lower() : c.upper()) == null)
                break;

            if (!(lower ? c.lowerIncl() : c.upperIncl()))
                return false;
        }

        return true;
    }
}
