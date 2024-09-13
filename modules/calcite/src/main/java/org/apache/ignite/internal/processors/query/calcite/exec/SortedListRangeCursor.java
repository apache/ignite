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

package org.apache.ignite.internal.processors.query.calcite.exec;

import java.util.Comparator;
import java.util.List;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.jetbrains.annotations.Nullable;

/**
 * Cursor to navigate through a sorted list with duplicates.
 */
public class SortedListRangeCursor<Row> implements GridCursor<Row> {
    /** */
    private final Comparator<Row> comp;

    /** List of rows. */
    private final List<Row> rows;

    /** Upper bound. */
    private final Row upper;

    /** Include upper bound. */
    private final boolean includeUpper;

    /** Current row. */
    private Row row;

    /** Current index of list element. */
    private int idx;

    /**
     * @param comp Rows comparator.
     * @param rows List of rows.
     * @param lower Lower bound.
     * @param upper Upper bound.
     * @param lowerInclude {@code True} for inclusive lower bound.
     * @param upperInclude {@code True} for inclusive upper bound.
     */
    public SortedListRangeCursor(
        Comparator<Row> comp,
        List<Row> rows,
        @Nullable Row lower,
        @Nullable Row upper,
        boolean lowerInclude,
        boolean upperInclude
    ) {
        this.comp = comp;
        this.rows = rows;
        this.upper = upper;
        this.includeUpper = upperInclude;

        idx = lower == null ? 0 : lowerBound(rows, lower, lowerInclude);
    }

    /**
     * Searches the lower bound (skipping duplicates) using a binary search.
     *
     * @param rows List of rows.
     * @param bound Lower bound.
     * @return Lower bound position in the list.
     */
    private int lowerBound(List<Row> rows, Row bound, boolean includeBound) {
        int low = 0, high = rows.size() - 1, idx = -1;

        while (low <= high) {
            int mid = (high - low) / 2 + low;
            int compRes = comp.compare(rows.get(mid), bound);

            if (compRes > 0)
                high = mid - 1;
            else if (compRes == 0 && includeBound) {
                idx = mid;
                high = mid - 1;
            }
            else
                low = mid + 1;
        }

        return idx == -1 ? low : idx;
    }

    /** {@inheritDoc} */
    @Override public boolean next() {
        // Intentionally use `-comp.compare(rows.get(idx), upper)` here.
        // `InlineIndexTree#compareFullRows` that used here works correctly only for specific parameter order.
        // First parameter must be tree row and the second search row. See implementation, for details.
        if (idx == rows.size() || (upper != null && -comp.compare(rows.get(idx), upper) < (includeUpper ? 0 : 1)))
            return false;

        row = rows.get(idx++);

        return true;
    }

    /** {@inheritDoc} */
    @Override public Row get() {
        return row;
    }
}
