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
import java.util.Collections;
import java.util.Comparator;
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
import org.apache.ignite.internal.processors.query.h2.H2Utils;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Cursor;
import org.apache.ignite.internal.processors.query.h2.opt.H2PlainRowFactory;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.h2.index.Cursor;
import org.h2.message.DbException;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.value.Value;
import org.jetbrains.annotations.Nullable;

import static java.util.Collections.emptyIterator;
import static java.util.Objects.requireNonNull;

/**
 * Sorted merge index.
 */
public class SortedReducer extends AbstractReducer {
    /** */
    @SuppressWarnings("ComparatorMethodParameterNotUsed")
    protected final Comparator<SearchRow> firstRowCmp = (rowInList, searchRow) -> {
        int res = compareRows(rowInList, searchRow);

        return res == 0 ? 1 : res;
    };

    /** */
    @SuppressWarnings("ComparatorMethodParameterNotUsed")
    protected final Comparator<SearchRow> lastRowCmp = (rowInList, searchRow) -> {
        int res = compareRows(rowInList, searchRow);

        return res == 0 ? -1 : res;
    };

    /** */
    private final Comparator<RowStream> streamCmp = (o1, o2) -> {
        if (o1 == o2) // both nulls
            return 0;

        if (o1 == null)
            return -1;

        if (o2 == null)
            return 1;

        return compareRows(o1.get(), o2.get());
    };

    /** */
    private final Lock lock = new ReentrantLock();

    /** */
    private final Condition notEmpty = lock.newCondition();

    /** */
    private final RowComparator rowComparator;

    /** */
    private Map<UUID, RowStream[]> streamsMap;

    /** */
    private ReduceResultPage failPage;

    /** */
    private MergeStreamIterator it;

    /**
     *  Constructor.
     *
     * @param ctx Kernal context.
     * @param rowComparator Row comparator.
     */
    public SortedReducer(GridKernalContext ctx, final RowComparator rowComparator) {
        super(ctx);

        this.rowComparator = rowComparator;

    }

    /**
     * Compare two rows.
     *
     * @param rowData the first row
     * @param compare the second row
     * @return 0 if both rows are equal, -1 if the first row is smaller,
     *         otherwise 1
     */
    private int compareRows(SearchRow rowData, SearchRow compare) {
        return rowComparator.compareRows(rowData, compare);
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
    @Override protected Cursor findInStream(@Nullable SearchRow first, @Nullable SearchRow last) {
        return new FetchingCursor(first, last, it);
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

    /**
     * @param lastEvictedRow Last evicted fetched row.
     * @param first Lower bound.
     * @param last Upper bound.
     */
    @Override protected void checkBounds(Row lastEvictedRow, SearchRow first, SearchRow last) {
        // If our last evicted fetched row was smaller than the given lower bound,
        // then we are ok. This is important for merge join to work.
        if (lastEvictedRow != null && first != null && compareRows(lastEvictedRow, first) < 0)
            return;

        super.checkBounds(lastEvictedRow, first, last);
    }

    /** {@inheritDoc} */
    @Override protected void addPage0(ReduceResultPage page) {
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

    /**
     * Iterator merging multiple row streams.
     */
    private final class MergeStreamIterator implements Iterator<Row> {
        /** */
        private boolean first = true;

        /** */
        private int off;

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
                H2Utils.bubbleUp(streams, off, streamCmp);
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
    private final class RowStream implements Pollable<ReduceResultPage> {
        /** */
        private Iterator<Value[]> iter = emptyIterator();

        /** */
        private Row cur;

        /** */
        private ReduceResultPage nextPage;

        /**
         * @param page Page.
         */
        private void addPage(ReduceResultPage page) {
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
        @Override public ReduceResultPage poll(long timeout, TimeUnit unit) throws InterruptedException {
            long nanos = unit.toNanos(timeout);

            lock.lock();

            try {
                for (;;) {
                    if (failPage != null)
                        return failPage;

                    ReduceResultPage page = nextPage;

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

            cur = H2PlainRowFactory.create(iter.next());

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

    /**
     * Fetching cursor.
     */
    private class FetchingCursor implements Cursor {
        /** */
        private Iterator<Row> stream;

        /** */
        private List<Row> rows;

        /** */
        private int cur;

        /** */
        private SearchRow first;

        /** */
        private SearchRow last;

        /** */
        private int lastFound = Integer.MAX_VALUE;

        /**
         * @param first Lower bound.
         * @param last Upper bound.
         * @param stream Stream of all the rows from remote nodes.
         */
         FetchingCursor(SearchRow first, SearchRow last, Iterator<Row> stream) {
            assert stream != null;

            // Initially we will use all the fetched rows, after we will switch to the last block.
            rows = fetched;

            this.stream = stream;
            this.first = first;
            this.last = last;

            if (haveBounds() && !rows.isEmpty())
                cur = findBounds();

            cur--; // Set current position before the first row.
        }

        /**
         * @return {@code true} If we have bounds.
         */
        private boolean haveBounds() {
            return first != null || last != null;
        }

        /**
         * @return Lower bound.
         */
        private int findBounds() {
            assert !rows.isEmpty() : "rows";

            int firstFound = cur;

            // Find the lower bound.
            if (first != null) {
                firstFound = binarySearchRow(rows, first, firstRowCmp, true);

                assert firstFound >= cur && firstFound <= rows.size() : "firstFound";

                if (firstFound == rows.size())
                    return firstFound; // The lower bound is greater than all the rows we have.

                first = null; // We have found the lower bound, do not need it anymore.
            }

            // Find the upper bound.
            if (last != null) {
                assert lastFound == Integer.MAX_VALUE : "lastFound";

                int lastFound0 = binarySearchRow(rows, last, lastRowCmp, true);

                // If the upper bound is too large we will ignore it.
                if (lastFound0 != rows.size())
                    lastFound = lastFound0;
            }

            return firstFound;
        }

        /**
         * Fetch rows from the stream.
         */
        private void fetchRows() {
            for (;;) {
                // Take the current last block and set the position after last.
                rows = fetched.lastBlock();
                cur = rows.size();

                // Fetch stream.
                while (stream.hasNext()) {
                    fetched.add(requireNonNull(stream.next()));

                    // Evict block if we've fetched too many rows.
                    if (fetched.size() == MAX_FETCH_SIZE) {
                        onBlockEvict(fetched.evictFirstBlock());

                        assert fetched.size() < MAX_FETCH_SIZE;
                    }

                    // No bounds -> no need to do binary search, can return the fetched row right away.
                    if (!haveBounds())
                        break;

                    // When the last block changed, it means that we've filled the current last block.
                    // We have fetched the needed number of rows for binary search.
                    if (fetched.lastBlock() != rows) {
                        assert fetched.lastBlock().isEmpty(); // The last row must be added to the previous block.

                        break;
                    }
                }

                if (cur == rows.size())
                    cur = Integer.MAX_VALUE; // We were not able to fetch anything. Done.
                else {
                    // Update fetched count.
                    if (haveBounds()) {
                        cur = findBounds();

                        if (cur == rows.size())
                            continue; // The lower bound is too large, continue fetching rows.
                    }
                }

                return;
            }
        }

        /** {@inheritDoc} */
        @Override public boolean next() {
            if (cur == Integer.MAX_VALUE)
                return false;

            if (++cur == rows.size())
                fetchRows();

            return cur < lastFound;
        }

        /** {@inheritDoc} */
        @Override public Row get() {
            return rows.get(cur);
        }

        /** {@inheritDoc} */
        @Override public SearchRow getSearchRow() {
            return get();
        }

        /** {@inheritDoc} */
        @Override public boolean previous() {
            // Should never be called.
            throw DbException.getUnsupportedException("previous");
        }
    }

    /**
     * @param rows Sorted rows list.
     * @param searchRow Search row.
     * @param cmp Comparator.
     * @param checkLast If we need to optimistically check the last row right away.
     * @return Insertion point for the search row.
     */
    protected static int binarySearchRow(
        List<Row> rows,
        SearchRow searchRow,
        Comparator<SearchRow> cmp,
        boolean checkLast
    ) {
        assert !rows.isEmpty();

        // Optimistically compare with the last row as a first step.
        if (checkLast) {
            int res = cmp.compare(last(rows), searchRow);

            assert res != 0; // Comparators must never return 0 here.

            if (res < 0)
                return rows.size(); // The search row is greater than the last row.
        }

        int res = Collections.binarySearch(rows, searchRow, cmp);

        assert res < 0 : res; // Comparator must never return 0.

        return -res - 1;
    }
}
