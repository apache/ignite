/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.query.h2.twostep;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
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
    @Override public void setSources(Collection<ClusterNode> nodes, int segmentsCnt) {
        super.setSources(nodes, segmentsCnt);

        int x = nodes.size() * segmentsCnt;

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