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

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.RandomAccess;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.CacheException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.h2.engine.Session;
import org.h2.index.BaseIndex;
import org.h2.index.Cursor;
import org.h2.index.IndexType;
import org.h2.message.DbException;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.result.SortOrder;
import org.h2.table.IndexColumn;
import org.h2.table.TableFilter;
import org.h2.value.Value;
import org.jetbrains.annotations.Nullable;

import static java.util.Collections.emptyIterator;
import static java.util.Objects.requireNonNull;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_SQL_MERGE_TABLE_MAX_SIZE;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_SQL_MERGE_TABLE_PREFETCH_SIZE;
import static org.apache.ignite.IgniteSystemProperties.getInteger;

/**
 * Merge index.
 */
public abstract class GridMergeIndex extends BaseIndex {
    /** */
    private static final int MAX_FETCH_SIZE = getInteger(IGNITE_SQL_MERGE_TABLE_MAX_SIZE, 10_000);

    /** */
    private static final int PREFETCH_SIZE = getInteger(IGNITE_SQL_MERGE_TABLE_PREFETCH_SIZE, 1024);

    /** */
    protected final Comparator<SearchRow> firstRowCmp = new Comparator<SearchRow>() {
        @Override public int compare(SearchRow rowInList, SearchRow searchRow) {
            int res = compareRows(rowInList, searchRow);

            return res == 0 ? 1 : res;
        }
    };

    /** */
    protected final Comparator<SearchRow> lastRowCmp = new Comparator<SearchRow>() {
        @Override public int compare(SearchRow rowInList, SearchRow searchRow) {
            int res = compareRows(rowInList, searchRow);

            return res == 0 ? -1 : res;
        }
    };

    /** All rows number. */
    private final AtomicInteger expRowsCnt = new AtomicInteger(0);

    /** Remaining rows per source node ID. */
    private Map<UUID, Counter[]> remainingRows;

    /** */
    private final AtomicBoolean lastSubmitted = new AtomicBoolean();

    /**
     * Will be r/w from query execution thread only, does not need to be threadsafe.
     */
    private final BlockList<Row> fetched;

    /** */
    private Row lastEvictedRow;

    /** */
    private volatile int fetchedCnt;

    /** */
    private final GridKernalContext ctx;

    /**
     * @param ctx Context.
     * @param tbl Table.
     * @param name Index name.
     * @param type Type.
     * @param cols Columns.
     */
    public GridMergeIndex(GridKernalContext ctx,
        GridMergeTable tbl,
        String name,
        IndexType type,
        IndexColumn[] cols
    ) {
        this(ctx);

        initBaseIndex(tbl, 0, name, cols, type);
    }

    /**
     * @param ctx Context.
     */
    protected GridMergeIndex(GridKernalContext ctx) {
        if (!U.isPow2(PREFETCH_SIZE)) {
            throw new IllegalArgumentException(IGNITE_SQL_MERGE_TABLE_PREFETCH_SIZE + " (" + PREFETCH_SIZE +
                ") must be positive and a power of 2.");
        }

        if (PREFETCH_SIZE >= MAX_FETCH_SIZE) {
            throw new IllegalArgumentException(IGNITE_SQL_MERGE_TABLE_PREFETCH_SIZE + " (" + PREFETCH_SIZE +
                ") must be less than " + IGNITE_SQL_MERGE_TABLE_MAX_SIZE + " (" + MAX_FETCH_SIZE + ").");
        }

        this.ctx = ctx;

        fetched = new BlockList<>(PREFETCH_SIZE);
    }

    /**
     * @return Return source nodes for this merge index.
     */
    public Set<UUID> sources() {
        return remainingRows.keySet();
    }

    /**
     * Fails index if any source node is left.
     */
    private void checkSourceNodesAlive() {
        for (UUID nodeId : sources()) {
            if (!ctx.discovery().alive(nodeId)) {
                fail(nodeId, null);

                return;
            }
        }
    }

    /**
     * @param nodeId Node ID.
     * @return {@code true} If this index needs data from the given source node.
     */
    public boolean hasSource(UUID nodeId) {
        return remainingRows.containsKey(nodeId);
    }

    /** {@inheritDoc} */
    @Override public long getRowCount(Session ses) {
        return expRowsCnt.get();
    }

    /** {@inheritDoc} */
    @Override public long getRowCountApproximation() {
        return getRowCount(null);
    }

    /**
     * Set source nodes.
     *
     * @param nodes Nodes.
     * @param segmentsCnt Index segments per table.
     */
    public void setSources(Collection<ClusterNode> nodes, int segmentsCnt) {
        assert remainingRows == null;

        remainingRows = U.newHashMap(nodes.size());

        for (ClusterNode node : nodes) {
            Counter[] counters = new Counter[segmentsCnt];

            for (int i = 0; i < segmentsCnt; i++)
                counters[i] = new Counter();

            if (remainingRows.put(node.id(), counters) != null)
                throw new IllegalStateException("Duplicate node id: " + node.id());

        }
    }

    /**
     * @param queue Queue to poll.
     * @return Next page.
     */
    private GridResultPage takeNextPage(BlockingQueue<GridResultPage> queue) {
        GridResultPage page;

        for (;;) {
            try {
                page = queue.poll(500, TimeUnit.MILLISECONDS);
            }
            catch (InterruptedException e) {
                throw new CacheException("Query execution was interrupted.", e);
            }

            if (page != null)
                break;

            checkSourceNodesAlive();
        }

        return page;
    }

    /**
     * @param queue Queue to poll.
     * @param iter Current iterator.
     * @return The same or new iterator.
     */
    protected final Iterator<Value[]> pollNextIterator(BlockingQueue<GridResultPage> queue, Iterator<Value[]> iter) {
        while (!iter.hasNext()) {
            GridResultPage page = takeNextPage(queue);

            if (page.isLast())
                return emptyIterator(); // We are done.

            fetchNextPage(page);

            iter = page.rows();
        }

        return iter;
    }

    /**
     * @param e Error.
     */
    public void fail(final CacheException e) {
        for (UUID nodeId0 : remainingRows.keySet()) {
            addPage0(new GridResultPage(null, nodeId0, null) {
                @Override public boolean isFail() {
                    return true;
                }

                @Override public void fetchNextPage() {
                    throw e;
                }
            });
        }
    }

    /**
     * @param nodeId Node ID.
     */
    public void fail(UUID nodeId, final CacheException e) {
        addPage0(new GridResultPage(null, nodeId, null) {
            @Override public boolean isFail() {
                return true;
            }

            @Override public void fetchNextPage() {
                if (e == null)
                    super.fetchNextPage();
                else
                    throw e;
            }
        });
    }

    /**
     * @param page Page.
     */
    public final void addPage(GridResultPage page) {
        int pageRowsCnt = page.rowsInPage();

        Counter cnt = remainingRows.get(page.source())[page.res.segmentId()];

        // RemainingRowsCount should be updated before page adding to avoid race
        // in GridMergeIndexUnsorted cursor iterator
        int remainingRowsCount;

        int allRows = page.response().allRows();

        if (allRows != -1) { // Only the first page contains allRows count and is allowed to init counter.
            assert cnt.state == State.UNINITIALIZED : "Counter is already initialized.";

            remainingRowsCount = cnt.addAndGet(allRows - pageRowsCnt);

            expRowsCnt.addAndGet(allRows);

            // Add page before setting initialized flag to avoid race condition with adding last page
            if (pageRowsCnt > 0)
                addPage0(page);

            // We need this separate flag to handle case when the first source contains only one page
            // and it will signal that all remaining counters are zero and fetch is finished.
            cnt.state = State.INITIALIZED;
        }
        else {
            remainingRowsCount = cnt.addAndGet(-pageRowsCnt);

            if (pageRowsCnt > 0)
                addPage0(page);
        }

        if (remainingRowsCount == 0) { // Result can be negative in case of race between messages, it is ok.
            if (cnt.state == State.UNINITIALIZED)
                return;

            // Guarantee that finished state possible only if counter is zero and all pages was added
            cnt.state = State.FINISHED;

            for (Counter[] cntrs : remainingRows.values()) { // Check all the sources.
                for(int i = 0; i < cntrs.length; i++) {
                    if (cntrs[i].state != State.FINISHED)
                        return;
                }
            }

            if (lastSubmitted.compareAndSet(false, true)) {
                addPage0(new GridResultPage(null, page.source(), null) {
                    @Override public boolean isLast() {
                        return true;
                    }
                });
            }
        }
    }

    /**
     * @param page Page.
     */
    protected abstract void addPage0(GridResultPage page);

    /**
     * @param page Page.
     */
    protected void fetchNextPage(GridResultPage page) {
        assert !page.isLast();

        if(page.isFail())
            page.fetchNextPage(); //rethrow exceptions

        assert page.res != null;

        Counter[] counters = remainingRows.get(page.source());

        int segId = page.res.segmentId();

        Counter counter = counters[segId];

        if (counter.get() != 0)
            page.fetchNextPage();
    }

    /** {@inheritDoc} */
    @Override public final Cursor find(Session ses, SearchRow first, SearchRow last) {
        checkBounds(lastEvictedRow, first, last);

        if (fetchedAll())
            return findAllFetched(fetched, first, last);

        return findInStream(first, last);
    }

    /**
     * @return {@code true} If we have fetched all the remote rows.
     */
    public boolean fetchedAll() {
        return fetchedCnt == expRowsCnt.get();
    }

    /**
     * @param lastEvictedRow Last evicted fetched row.
     * @param first Lower bound.
     * @param last Upper bound.
     */
    protected void checkBounds(Row lastEvictedRow, SearchRow first, SearchRow last) {
        if (lastEvictedRow != null)
            throw new IgniteException("Fetched result set was too large.");
    }

    /**
     * @param first Lower bound.
     * @param last Upper bound.
     * @return Cursor. Usually it must be {@link FetchingCursor} instance.
     */
    protected abstract Cursor findInStream(@Nullable SearchRow first, @Nullable SearchRow last);

    /**
     * @param fetched Fetched rows.
     * @param first Lower bound.
     * @param last Upper bound.
     * @return Cursor.
     */
    protected abstract Cursor findAllFetched(List<Row> fetched, @Nullable SearchRow first, @Nullable SearchRow last);

    /** {@inheritDoc} */
    @Override public void checkRename() {
        throw DbException.getUnsupportedException("rename");
    }

    /** {@inheritDoc} */
    @Override public void close(Session ses) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void add(Session ses, Row row) {
        throw DbException.getUnsupportedException("add");
    }

    /** {@inheritDoc} */
    @Override public void remove(Session ses, Row row) {
        throw DbException.getUnsupportedException("remove row");
    }

    /** {@inheritDoc} */
    @Override public double getCost(Session ses, int[] masks, TableFilter[] filters, int filter, SortOrder sortOrder) {
        return getCostRangeIndex(masks, getRowCountApproximation(), filters, filter, sortOrder, true);
    }

    /** {@inheritDoc} */
    @Override public void remove(Session ses) {
        throw DbException.getUnsupportedException("remove index");
    }

    /** {@inheritDoc} */
    @Override public void truncate(Session ses) {
        throw DbException.getUnsupportedException("truncate");
    }

    /** {@inheritDoc} */
    @Override public boolean canGetFirstOrLast() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public Cursor findFirstOrLast(Session ses, boolean first) {
        throw DbException.getUnsupportedException("findFirstOrLast");
    }

    /** {@inheritDoc} */
    @Override public boolean needRebuild() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public long getDiskSpaceUsed() {
        return 0;
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

        assert res < 0: res; // Comparator must never return 0.

        return -res - 1;
    }

    /**
     * @param evictedBlock Evicted block.
     */
    private void onBlockEvict(List<Row> evictedBlock) {
        assert evictedBlock.size() == PREFETCH_SIZE;

        // Remember the last row (it will be max row) from the evicted block.
        lastEvictedRow = requireNonNull(last(evictedBlock));
    }

    /**
     * @param l List.
     * @return Last element.
     */
    private static <Z> Z last(List<Z> l) {
        return l.get(l.size() - 1);
    }

    /**
     * Fetching cursor.
     */
    protected class FetchingCursor implements Cursor {
        /** */
        Iterator<Row> stream;

        /** */
        List<Row> rows;

        /** */
        int cur;

        /** */
        SearchRow first;

        /** */
        SearchRow last;

        /** */
        int lastFound = Integer.MAX_VALUE;

        /**
         * @param first Lower bound.
         * @param last Upper bound.
         * @param stream Stream of all the rows from remote nodes.
         */
        public FetchingCursor(SearchRow first, SearchRow last, Iterator<Row> stream) {
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
            assert !rows.isEmpty(): "rows";

            int firstFound = cur;

            // Find the lower bound.
            if (first != null) {
                firstFound = binarySearchRow(rows, first, firstRowCmp, true);

                assert firstFound >= cur && firstFound <= rows.size(): "firstFound";

                if (firstFound == rows.size())
                    return firstFound; // The lower bound is greater than all the rows we have.

                first = null; // We have found the lower bound, do not need it anymore.
            }

            // Find the upper bound.
            if (last != null) {
                assert lastFound == Integer.MAX_VALUE: "lastFound";

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
                    fetchedCnt += rows.size() - cur;

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

    /** */
    enum State {
        UNINITIALIZED, INITIALIZED, FINISHED
    }

    /**
     * Counter with initialization flag.
     */
    private static class Counter extends AtomicInteger {
        /** */
        volatile State state = State.UNINITIALIZED;
    }

    /**
     */
    private static final class BlockList<Z> extends AbstractList<Z> implements RandomAccess {
        /** */
        private final List<List<Z>> blocks;

        /** */
        private int size;

        /** */
        private final int maxBlockSize;

        /** */
        private final int shift;

        /** */
        private final int mask;

        /**
         * @param maxBlockSize Max block size.
         */
        private BlockList(int maxBlockSize) {
            assert U.isPow2(maxBlockSize);

            this.maxBlockSize = maxBlockSize;

            shift = Integer.numberOfTrailingZeros(maxBlockSize);
            mask = maxBlockSize - 1;

            blocks = new ArrayList<>();
            blocks.add(new ArrayList<Z>());
        }

        /** {@inheritDoc} */
        @Override public int size() {
            return size;
        }

        /** {@inheritDoc} */
        @Override public boolean add(Z z) {
            size++;

            List<Z> lastBlock = lastBlock();

            lastBlock.add(z);

            if (lastBlock.size() == maxBlockSize)
                blocks.add(new ArrayList<Z>());

            return true;
        }

        /** {@inheritDoc} */
        @Override public Z get(int idx) {
            return blocks.get(idx >>> shift).get(idx & mask);
        }

        /**
         * @return Last block.
         */
        private List<Z> lastBlock() {
            return last(blocks);
        }

        /**
         * @return Evicted block.
         */
        private List<Z> evictFirstBlock() {
            // Remove head block.
            List<Z> res = blocks.remove(0);

            size -= res.size();

            return res;
        }
    }
}