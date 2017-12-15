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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.RandomAccess;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import javax.cache.CacheException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.query.h2.twostep.messages.GridQueryNextPageResponse;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.h2.engine.Session;
import org.h2.index.BaseIndex;
import org.h2.index.Cursor;
import org.h2.index.IndexType;
import org.h2.message.DbException;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.table.IndexColumn;
import org.h2.value.Value;
import org.jetbrains.annotations.Nullable;

import static java.util.Objects.requireNonNull;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_SQL_MERGE_TABLE_MAX_SIZE;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_SQL_MERGE_TABLE_PREFETCH_SIZE;
import static org.apache.ignite.IgniteSystemProperties.getInteger;

/**
 * Merge index.
 */
@SuppressWarnings("AtomicFieldUpdaterIssues")
public abstract class GridMergeIndex extends BaseIndex {
    /** */
    private static final int MAX_FETCH_SIZE = getInteger(IGNITE_SQL_MERGE_TABLE_MAX_SIZE, 10_000);

    /** */
    private static final int PREFETCH_SIZE = getInteger(IGNITE_SQL_MERGE_TABLE_PREFETCH_SIZE, 1024);

    /** */
    private static final AtomicReferenceFieldUpdater<GridMergeIndex, ConcurrentMap> lastPagesUpdater =
        AtomicReferenceFieldUpdater.newUpdater(GridMergeIndex.class, ConcurrentMap.class, "lastPages");

    static {
        if (!U.isPow2(PREFETCH_SIZE)) {
            throw new IllegalArgumentException(IGNITE_SQL_MERGE_TABLE_PREFETCH_SIZE + " (" + PREFETCH_SIZE +
                ") must be positive and a power of 2.");
        }

        if (PREFETCH_SIZE >= MAX_FETCH_SIZE) {
            throw new IllegalArgumentException(IGNITE_SQL_MERGE_TABLE_PREFETCH_SIZE + " (" + PREFETCH_SIZE +
                ") must be less than " + IGNITE_SQL_MERGE_TABLE_MAX_SIZE + " (" + MAX_FETCH_SIZE + ").");
        }
    }

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

    /** Row source nodes. */
    private Set<UUID> sources;

    /** */
    private int pageSize;

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

    /** */
    private volatile ConcurrentMap<SourceKey, Integer> lastPages;

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
        this.ctx = ctx;

        fetched = new BlockList<>(PREFETCH_SIZE);
    }

    /**
     * @return Return source nodes for this merge index.
     */
    public Set<UUID> sources() {
        return sources;
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
        return sources.contains(nodeId);
    }

    /** {@inheritDoc} */
    @Override public long getRowCount(Session ses) {
        Cursor c = find(ses, null, null);

        long cnt = 0;

        while (c.next())
            cnt++;

        return cnt;
    }

    /** {@inheritDoc} */
    @Override public long getRowCountApproximation() {
        return 10_000;
    }

    /**
     * Set source nodes.
     *
     * @param nodes Nodes.
     * @param segmentsCnt Index segments per table.
     */
    public void setSources(Collection<ClusterNode> nodes, int segmentsCnt) {
        assert sources == null;

        sources = new HashSet<>();

        for (ClusterNode node : nodes) {
            if (!sources.add(node.id()))
                throw new IllegalStateException();
        }
    }

    /**
     * @param pageSize Page size.
     */
    public void setPageSize(int pageSize) {
        this.pageSize = pageSize;
    }

    /**
     * @param queue Queue to poll.
     * @return Next page.
     */
    private GridResultPage takeNextPage(Pollable<GridResultPage> queue) {
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
    protected final Iterator<Value[]> pollNextIterator(Pollable<GridResultPage> queue, Iterator<Value[]> iter) {
        if (!iter.hasNext()) {
            GridResultPage page = takeNextPage(queue);

            if (!page.isLast())
                page.fetchNextPage(); // Failed will throw an exception here.

            iter = page.rows();

            // The received iterator must be empty in the dummy last page or on failure.
            assert iter.hasNext() || page.isDummyLast() || page.isFail();
        }

        return iter;
    }

    /**
     * @param e Error.
     */
    public void fail(final CacheException e) {
        for (UUID nodeId : sources)
            fail(nodeId, e);
    }

    /**
     * @param nodeId Node ID.
     * @param e Exception.
     */
    public void fail(UUID nodeId, final CacheException e) {
        if (nodeId == null)
            nodeId = F.first(sources);

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
     * @param nodeId Node ID.
     * @param res Response.
     */
    private void initLastPages(UUID nodeId, GridQueryNextPageResponse res) {
        int allRows = res.allRows();

        // If the old protocol we send all rows number in the page 0, other pages have -1.
        // In the new protocol we do not know it and always have -1, except terminating page,
        // which has -2. Thus we have to init page counters only when we receive positive value
        // in the first page.
        if (allRows < 0 || res.page() != 0)
            return;

        ConcurrentMap<SourceKey,Integer> lp = lastPages;

        if (lp == null && !lastPagesUpdater.compareAndSet(this, null, lp = new ConcurrentHashMap<>()))
            lp = lastPages;

        assert pageSize > 0: pageSize;

        int lastPage = allRows == 0 ? 0 : (allRows - 1) / pageSize;

        assert lastPage >= 0: lastPage;

        if (lp.put(new SourceKey(nodeId, res.segmentId()), lastPage) != null)
            throw new IllegalStateException();
    }

    /**
     * @param page Page.
     */
    private void markLastPage(GridResultPage page) {
        GridQueryNextPageResponse res = page.response();

        if (!res.last()) {
            UUID nodeId = page.source();

            initLastPages(nodeId, res);

            ConcurrentMap<SourceKey,Integer> lp = lastPages;

            if (lp == null)
                return; // It was not initialized --> wait for last page flag.

            Integer lastPage = lp.get(new SourceKey(nodeId, res.segmentId()));

            if (lastPage == null)
                return; // This node may use the new protocol --> wait for last page flag.

            if (lastPage != res.page()) {
                assert lastPage > res.page();

                return; // This is not the last page.
            }
        }

        page.setLast(true);
    }

    /**
     * @param page Page.
     */
    public final void addPage(GridResultPage page) {
        markLastPage(page);
        addPage0(page);
    }

    /**
     * @param lastPage Real last page.
     * @return Created dummy page.
     */
    protected final GridResultPage createDummyLastPage(GridResultPage lastPage) {
        assert !lastPage.isDummyLast(); // It must be a real last page.

        return new GridResultPage(ctx, lastPage.source(), null).setLast(true);
    }

    /**
     * @param page Page.
     */
    protected abstract void addPage0(GridResultPage page);

    /** {@inheritDoc} */
    @Override public final Cursor find(Session ses, SearchRow first, SearchRow last) {
        checkBounds(lastEvictedRow, first, last);

        if (fetchedAll())
            return findAllFetched(fetched, first, last);

        return findInStream(first, last);
    }

    /**
     * @return {@code true} If we have fetched all the remote rows into a fetched list.
     */
    public abstract boolean fetchedAll();

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

    /**
     * Pollable.
     */
    protected static interface Pollable<E> {
        /**
         * @param timeout Timeout.
         * @param unit Time unit.
         * @return Polled value or {@code null} if none.
         * @throws InterruptedException If interrupted.
         */
        E poll(long timeout, TimeUnit unit) throws InterruptedException;
    }

    /**
     */
    private static class SourceKey {
        final UUID nodeId;

        /** */
        final int segment;

        /**
         * @param nodeId Node ID.
         * @param segment Segment.
         */
        SourceKey(UUID nodeId, int segment) {
            this.nodeId = nodeId;
            this.segment = segment;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            SourceKey sourceKey = (SourceKey)o;

            if (segment != sourceKey.segment) return false;
            return nodeId.equals(sourceKey.nodeId);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int result = nodeId.hashCode();
            result = 31 * result + segment;
            return result;
        }
    }
}