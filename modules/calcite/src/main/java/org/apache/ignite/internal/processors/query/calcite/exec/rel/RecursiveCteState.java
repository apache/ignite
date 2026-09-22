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

package org.apache.ignite.internal.processors.query.calcite.exec.rel;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.agg.GroupKey;
import org.apache.ignite.internal.processors.query.calcite.exec.tracker.RowTracker;
import org.apache.ignite.internal.util.GridUnsafe;
import org.jetbrains.annotations.Nullable;

/** Query-local current and next deltas of a recursive CTE. */
public class RecursiveCteState<Row> {
    /** Rows seen across all iterations, or null for UNION ALL. */
    private final @Nullable Set<GroupKey<Row>> seen;

    /** Row handler used for SQL grouping keys. */
    private final RowHandler<Row> hnd;

    /** Memory tracker for keys and their rows retained for duplicate elimination. */
    private final @Nullable RowTracker<GroupKey<Row>> seenMemoryTracker;

    /** Rows visible to the recursive table scan. */
    private List<Row> cur = Collections.emptyList();

    /** Rows produced by the active seed or recursive term. */
    private List<Row> next = new ArrayList<>();

    /** Memory tracker for rows in the current delta. */
    private RowTracker<Row> curMemoryTracker;

    /** Memory tracker for rows in the next delta. */
    private RowTracker<Row> nextMemoryTracker;

    /** */
    public RecursiveCteState(ExecutionContext<Row> ctx, boolean all) {
        seen = all ? null : new HashSet<>();
        hnd = ctx.rowHandler();
        seenMemoryTracker = all ? null : ctx.createNodeMemoryTracker(MemoryTrackingNode.HASH_MAP_ROW_OVERHEAD);
        curMemoryTracker = ctx.createNodeMemoryTracker(GridUnsafe.OBJ_REF_SIZE);
        nextMemoryTracker = ctx.createNodeMemoryTracker(GridUnsafe.OBJ_REF_SIZE);
    }

    /** Adds a new row to the next delta, returning false for duplicates in DISTINCT mode. */
    public boolean add(Row row) {
        if (seen != null) {
            GroupKey<Row> rowKey = GroupKey.of(row, hnd);

            if (!seen.add(rowKey))
                return false;

            seenMemoryTracker.onRowAdded(rowKey);
        }

        next.add(row);

        // DISTINCT already accounts for the row in seen; null charges only the delta's reference overhead.
        nextMemoryTracker.onRowAdded(seen == null ? row : null);

        return true;
    }

    /** Publishes the collected delta and prepares an empty buffer for the next iteration. */
    public void commit() {
        curMemoryTracker.reset();
        cur = next;
        next = new ArrayList<>();

        RowTracker<Row> tracker = curMemoryTracker;

        curMemoryTracker = nextMemoryTracker;
        nextMemoryTracker = tracker;
    }

    /** Current delta. */
    public Iterable<Row> current() {
        return () -> cur.iterator();
    }

    /** Returns whether the current delta is empty. */
    public boolean isEmpty() {
        return cur.isEmpty();
    }

    /** Clears all query-local rows. */
    public void clear() {
        cur = Collections.emptyList();
        next = new ArrayList<>();

        curMemoryTracker.reset();
        nextMemoryTracker.reset();

        if (seen != null) {
            seen.clear();
            seenMemoryTracker.reset();
        }
    }
}
