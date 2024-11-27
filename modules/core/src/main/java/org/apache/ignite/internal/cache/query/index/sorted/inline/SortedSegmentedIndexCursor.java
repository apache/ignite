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

package org.apache.ignite.internal.cache.query.index.sorted.inline;

import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.Queue;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.cache.query.index.SortOrder;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyDefinition;
import org.apache.ignite.internal.cache.query.index.sorted.IndexRow;
import org.apache.ignite.internal.cache.query.index.sorted.IndexRowComparator;
import org.apache.ignite.internal.cache.query.index.sorted.SortedIndexDefinition;
import org.apache.ignite.internal.util.lang.GridCursor;

/** Single cursor over multiple segments. The next value is chosen with the index row comparator. */
public class SortedSegmentedIndexCursor implements GridCursor<IndexRow> {
    /** Cursors over segments. */
    private final Queue<GridCursor<IndexRow>> cursors;

    /** Comparator to compare index rows. */
    private final Comparator<GridCursor<IndexRow>> cursorComp;

    /** */
    private IndexRow head;

    /** */
    public SortedSegmentedIndexCursor(GridCursor<IndexRow>[] cursors, SortedIndexDefinition idxDef) throws IgniteCheckedException {
        cursorComp = new Comparator<>() {
            private final IndexRowComparator rowComparator = idxDef.rowComparator();

            private final IndexKeyDefinition[] keyDefs =
                idxDef.indexKeyDefinitions().values().toArray(new IndexKeyDefinition[0]);

            @Override public int compare(GridCursor<IndexRow> o1, GridCursor<IndexRow> o2) {
                try {
                    int keysLen = o1.get().keysCount();

                    for (int i = 0; i < keysLen; i++) {
                        int cmp = rowComparator.compareRow(o1.get(), o2.get(), i);

                        if (cmp != 0) {
                            boolean desc = keyDefs[i].order().sortOrder() == SortOrder.DESC;

                            return desc ? -cmp : cmp;
                        }
                    }

                    return 0;
                }
                catch (IgniteCheckedException e) {
                    throw new IgniteException("Failed to sort remote index rows", e);
                }
            }
        };

        this.cursors = cursorsQueue(cursors);
    }

    /** {@inheritDoc} */
    @Override public boolean next() throws IgniteCheckedException {
        if (cursors.isEmpty())
            return false;

        GridCursor<IndexRow> c = cursors.poll();

        head = c.get();

        if (c.next())
            cursors.add(c);

        return true;
    }

    /** {@inheritDoc} */
    @Override public IndexRow get() throws IgniteCheckedException {
        return head;
    }

    /** */
    protected Queue<GridCursor<IndexRow>> cursorsQueue(GridCursor<IndexRow>[] cursors)
        throws IgniteCheckedException {
        PriorityQueue<GridCursor<IndexRow>> q = new PriorityQueue<>(cursors.length, cursorComp);

        for (GridCursor<IndexRow> c: cursors) {
            if (c.next())
                q.add(c);
        }

        return q;
    }
}
