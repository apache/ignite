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

import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Cursor;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2RowFactory;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
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
import org.jetbrains.annotations.Nullable;

import static java.util.Collections.emptyIterator;
import static org.apache.ignite.internal.processors.query.h2.opt.GridH2IndexBase.bubbleUp;

/**
 * Sorted index.
 */
public final class GridMergeIndexSorted extends GridMergeIndex {
    /** */
    private static final IndexType TYPE = IndexType.createNonUnique(false);

    /** */
    private final Comparator<RowStream> streamCmp = new Comparator<RowStream>() {
        @Override public int compare(RowStream o1, RowStream o2) {
            // Nulls at the beginning.
            if (o1 == null)
                return -1;

            if (o2 == null)
                return 1;

            return compareRows(o1.get(), o2.get());
        }
    };

    /** */
    private Map<UUID,RowStream[]> streamsMap;

    /** */
    private final Lock lock = new ReentrantLock();

    /** */
    private final Condition notEmpty = lock.newCondition();

    /** */
    private GridResultPage failPage;

    /** */
    private MergeStreamIterator it;

    /**
     * @param ctx Kernal context.
     * @param tbl Table.
     * @param name Index name,
     * @param cols Columns.
     */
    public GridMergeIndexSorted(
        GridKernalContext ctx,
        GridMergeTable tbl,
        String name,
        IndexColumn[] cols
    ) {
        super(ctx, tbl, name, TYPE, cols);
    }

    /** {@inheritDoc} */
    @Override public void setSources(Collection<ClusterNode> nodes, int segmentsCnt) {
        super.setSources(nodes, segmentsCnt);

        streamsMap = U.newHashMap(nodes.size());
        RowStream[] streams = new RowStream[nodes.size() * segmentsCnt];

        int i = 0;

        for (ClusterNode node : nodes) {
            RowStream[] segments = new RowStream[segmentsCnt];

            for (int s = 0; s < segmentsCnt; s++)
                streams[i++] = segments[s] = new RowStream();

            if (streamsMap.put(node.id(), segments) != null)
                throw new IllegalStateException();
        }

        it = new MergeStreamIterator(streams);
    }

    /** {@inheritDoc} */
    @Override public boolean fetchedAll() {
        return it.fetchedAll();
    }

    /** {@inheritDoc} */
    @Override protected void addPage0(GridResultPage page) {
        if (page.isFail()) {
            lock.lock();

            try {
                if (failPage == null) {
                    failPage = page;

                    notEmpty.signalAll();
                }
            }
            finally {
                lock.unlock();
            }
        }
        else {
            UUID src = page.source();

            streamsMap.get(src)[page.segmentId()].addPage(page);
        }
    }

    /** {@inheritDoc} */
    @Override protected void checkBounds(Row lastEvictedRow, SearchRow first, SearchRow last) {
        // If our last evicted fetched row was smaller than the given lower bound,
        // then we are ok. This is important for merge join to work.
        if (lastEvictedRow != null && first != null && compareRows(lastEvictedRow, first) < 0)
            return;

        super.checkBounds(lastEvictedRow, first, last);
    }

    /** {@inheritDoc} */
    @Override protected Cursor findAllFetched(List<Row> fetched, SearchRow first, SearchRow last) {
        Iterator<Row> iter;

        if (fetched.isEmpty())
            iter = emptyIterator();
        else if (first == null && last == null)
            iter = fetched.iterator();
        else {
            int low = first == null ? 0 : binarySearchRow(fetched, first, firstRowCmp, false);

            if (low == fetched.size())
                iter = emptyIterator();
            else {
                int high = last == null ? fetched.size() : binarySearchRow(fetched, last, lastRowCmp, false);

                iter = fetched.subList(low, high).iterator();
            }
        }

        return new GridH2Cursor(iter);
    }

    /** {@inheritDoc} */
    @Override public double getCost(Session ses, int[] masks, TableFilter[] filters, int filter, SortOrder sortOrder, HashSet<Column> allColumnsSet) {
        return getCostRangeIndex(masks, getRowCountApproximation(), filters, filter, sortOrder, false, allColumnsSet);
    }

    /** {@inheritDoc} */
    @Override protected Cursor findInStream(@Nullable SearchRow first, @Nullable SearchRow last) {
        return new FetchingCursor(first, last, it);
    }

    /**
     * Iterator merging multiple row streams.
     */
    private final class MergeStreamIterator implements Iterator<Row> {
        /** */
        private boolean first = true;

        /** */
        private volatile int off;

        /** */
        private boolean hasNext;

        /** */
        private final RowStream[] streams;

        /**
         * @param streams Streams.
         */
        MergeStreamIterator(RowStream[] streams) {
            assert !F.isEmpty(streams);

            this.streams = streams;
        }

        /**
         * @return {@code true} If fetched all.
         */
        private boolean fetchedAll() {
            return off == streams.length;
        }

        /**
         *
         */
        private void goFirst() {
            assert first;

            first = false;

            for (int i = 0; i < streams.length; i++) {
                RowStream s = streams[i];

                if (!s.next()) {
                    streams[i] = null;
                    off++; // Move left bound.
                }
            }

            if (off < streams.length)
                Arrays.sort(streams, streamCmp);
        }

        /**
         *
         */
        private void goNext() {
            if (off == streams.length)
                return; // All streams are done.

            if (streams[off].next())
                bubbleUp(streams, off, streamCmp);
            else
                streams[off++] = null; // Move left bound and nullify empty stream.
        }

        /** {@inheritDoc} */
        @Override public boolean hasNext() {
            if (hasNext)
                return true;

            if (first)
                goFirst();
            else
                goNext();

            return hasNext = off < streams.length;
        }

        /** {@inheritDoc} */
        @Override public Row next() {
            if (!hasNext())
                throw new NoSuchElementException();

            hasNext = false;

            return streams[off].get();
        }

        /** {@inheritDoc} */
        @Override public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Row stream.
     */
    private final class RowStream implements Pollable<GridResultPage> {
        /** */
        Iterator<Value[]> iter = emptyIterator();

        /** */
        Row cur;

        /** */
        GridResultPage nextPage;

        /**
         * @param page Page.
         */
        private void addPage(GridResultPage page) {
            assert !page.isFail();

            if (page.isLast() && page.rowsInPage() == 0)
                page = createDummyLastPage(page); // Terminate.

            lock.lock();

            try {
                // We can fetch the next page only when we have polled the previous one.
                assert nextPage == null;

                nextPage = page;

                notEmpty.signalAll();
            }
            finally {
                lock.unlock();
            }
        }

        /** {@inheritDoc} */
        @Override public GridResultPage poll(long timeout, TimeUnit unit) throws InterruptedException {
            long nanos = unit.toNanos(timeout);

            lock.lock();

            try {
                for (;;) {
                    if (failPage != null)
                        return failPage;

                    GridResultPage page = nextPage;

                    if (page != null) {
                        // isLast && !isDummyLast
                        nextPage = page.isLast() && page.response() != null
                            ? createDummyLastPage(page) : null; // Terminate with empty iterator.

                        return page;
                    }

                    if ((nanos = notEmpty.awaitNanos(nanos)) <= 0)
                        return null;
                }
            }
            finally {
                lock.unlock();
            }
        }

        /**
         * @return {@code true} If we successfully switched to the next row.
         */
        private boolean next() {
            cur = null;

            iter = pollNextIterator(this, iter);

            if (!iter.hasNext())
                return false;

            cur = GridH2RowFactory.create(iter.next());

            return true;
        }

        /**
         * @return Current row.
         */
        private Row get() {
            assert cur != null;

            return cur;
        }
    }
}
