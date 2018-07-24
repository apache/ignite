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

package org.apache.ignite.internal.processors.query.h2.twostep;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Cursor;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2PlainRowFactory;
import org.h2.engine.Session;
import org.h2.index.Cursor;
import org.h2.index.IndexType;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.result.SortOrder;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.TableFilter;
import org.h2.value.Value;

/**
 * Unsorted merge index.
 */
public final class GridMergeIndexUnsorted extends GridMergeIndex {
    /** */
    private static final IndexType TYPE = IndexType.createScan(false);

    /** */
    private final PollableQueue<GridResultPage> queue = new PollableQueue<>();

    /** */
    private final AtomicInteger activeSources = new AtomicInteger(-1);

    /** */
    private Iterator<Value[]> iter = Collections.emptyIterator();

    /**
     * @param ctx Context.
     * @param tbl  Table.
     * @param name Index name.
     */
    public GridMergeIndexUnsorted(GridKernalContext ctx, GridMergeTable tbl, String name) {
        super(ctx, tbl, name, TYPE, IndexColumn.wrap(tbl.getColumns()));
    }

    /**
     * @param ctx Context.
     * @return Dummy index instance.
     */
    public static GridMergeIndexUnsorted createDummy(GridKernalContext ctx) {
        return new GridMergeIndexUnsorted(ctx);
    }

    /**
     * @param ctx Context.
     */
    private GridMergeIndexUnsorted(GridKernalContext ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Override public void setSources(Map<ClusterNode, Integer> nodesSegments) {
        super.setSources(nodesSegments);

        int x = 0;

        for (Integer srcs : nodesSegments.values())
            x += srcs;

        assert x > 0: x;

        activeSources.set(x);
    }

    /** {@inheritDoc} */
    @Override public boolean fetchedAll() {
        int x = activeSources.get();

        assert x >= 0: x; // This method must not be called if the sources were not set.

        return x == 0 && queue.isEmpty();
    }

    /** {@inheritDoc} */
    @Override protected void addPage0(GridResultPage page) {
        assert page.rowsInPage() > 0 || page.isLast() || page.isFail();

        // Do not add empty page to avoid premature stream termination.
        if (page.rowsInPage() != 0 || page.isFail())
            queue.add(page);

        if (page.isLast()) {
            int x = activeSources.decrementAndGet();

            assert x >= 0: x;

            if (x == 0) // Always terminate with empty iterator.
                queue.add(createDummyLastPage(page));
        }
    }

    /** {@inheritDoc} */
    @Override public double getCost(Session ses, int[] masks, TableFilter[] filters, int filter, SortOrder sortOrder, HashSet<Column> allColumnsSet) {
        return getCostRangeIndex(masks, getRowCountApproximation(), filters, filter, sortOrder, true, allColumnsSet);
    }

    /** {@inheritDoc} */
    @Override protected Cursor findAllFetched(List<Row> fetched, SearchRow first, SearchRow last) {
        // This index is unsorted: have to ignore bounds.
        return new GridH2Cursor(fetched.iterator());
    }

    /** {@inheritDoc} */
    @Override protected Cursor findInStream(SearchRow first, SearchRow last) {
        // This index is unsorted: have to ignore bounds.
        return new FetchingCursor(null, null, new Iterator<Row>() {
            @Override public boolean hasNext() {
                iter = pollNextIterator(queue, iter);

                return iter.hasNext();
            }

            @Override public Row next() {
                return GridH2PlainRowFactory.create(iter.next());
            }

            @Override public void remove() {
                throw new UnsupportedOperationException();
            }
        });
    }

    /**
     */
    private static class PollableQueue<X> extends LinkedBlockingQueue<X> implements Pollable<X> {
        // No-op.
    }
}