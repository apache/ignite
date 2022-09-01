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

import java.util.List;
import java.util.Set;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.cache.query.RangeIndexQueryCriterion;
import org.apache.ignite.internal.cache.query.index.sorted.IndexRow;
import org.apache.ignite.internal.cache.query.index.sorted.IndexRowComparator;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndexTree;
import org.apache.ignite.internal.cache.query.index.sorted.keys.IndexKey;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusIO;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.cache.query.index.sorted.IndexKeyType.JAVA_OBJECT;
import static org.apache.ignite.internal.cache.query.index.sorted.inline.types.NullableInlineIndexKeyType.CANT_BE_COMPARE;
import static org.apache.ignite.internal.cache.query.index.sorted.inline.types.NullableInlineIndexKeyType.COMPARE_UNSUPPORTED;

/**
 * Checks index rows for matching to specified index criteria.
 */
class IndexQueryCriteriaClosure implements BPlusTree.TreeRowClosure<IndexRow, IndexRow> {
    /** */
    private final IndexSingleRangeQuery qry;

    /** */
    private final IndexRowComparator rowCmp;

    /** */
    IndexQueryCriteriaClosure(IndexSingleRangeQuery qry, IndexRowComparator rowCmp) {
        this.qry = qry;
        this.rowCmp = rowCmp;
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
        int off = io.offset(idx);

        int fieldOff = 0;

        InlineIndexRow currRow = new InlineIndexRow(tree, io, pageAddr, idx);

        List<InlineIndexKeyType> keyTypes = tree.rowHandler().inlineIndexKeyTypes();

        for (int keyIdx = 0; keyIdx < tree.rowHandler().indexKeyDefinitions().size(); keyIdx++) {
            IndexKeyQueryCondition keyCond = qry.keyCondition(keyIdx);

            if (keyCond == null)
                return false;

            RangeIndexQueryCriterion c = keyCond.range();
            Set<IndexKey> inVals = keyCond.inVals();

            InlineIndexKeyType keyType = keyIdx < keyTypes.size() ? keyTypes.get(keyIdx) : null;

            boolean descOrder = keyCond.desc();

            int maxSize = tree.inlineSize() - fieldOff;

            if (inVals != null) {
                IndexKey key = null;

                if (keyType != null && keyType.type() != JAVA_OBJECT && keyType.inlinedFullValue(pageAddr, off + fieldOff))
                    key = keyType.get(pageAddr, off + fieldOff, maxSize);

                if (key == null) {
                    IndexRow row = io.getLookupRow(tree, pageAddr, idx);

                    key = row.key(keyIdx);
                }

                // Range boundaries were already checked for all IN values.
                return !inVals.contains(key);
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
