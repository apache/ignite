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

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
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
import org.h2.index.Cursor;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.value.Value;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static java.util.Objects.requireNonNull;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_SQL_MERGE_TABLE_MAX_SIZE;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_SQL_MERGE_TABLE_PREFETCH_SIZE;
import static org.apache.ignite.IgniteSystemProperties.getInteger;

/**
 * Base class for reducer of remote index lookup results.
 */
public abstract class AbstractReducer implements Reducer {
    /** */
    static final int MAX_FETCH_SIZE = getInteger(IGNITE_SQL_MERGE_TABLE_MAX_SIZE, 10_000);

    /** */
    static int prefetchSize = getInteger(IGNITE_SQL_MERGE_TABLE_PREFETCH_SIZE, 1024);

    static {
        if (!U.isPow2(prefetchSize)) {
            throw new IllegalArgumentException(IGNITE_SQL_MERGE_TABLE_PREFETCH_SIZE + " (" + prefetchSize +
                ") must be positive and a power of 2.");
        }

        if (prefetchSize >= MAX_FETCH_SIZE) {
            throw new IllegalArgumentException(IGNITE_SQL_MERGE_TABLE_PREFETCH_SIZE + " (" + prefetchSize +
                ") must be less than " + IGNITE_SQL_MERGE_TABLE_MAX_SIZE + " (" + MAX_FETCH_SIZE + ").");
        }
    }

    /** */
    @SuppressWarnings("rawtypes")
    private static final AtomicReferenceFieldUpdater<AbstractReducer, ConcurrentMap> LAST_PAGES_UPDATER =
        AtomicReferenceFieldUpdater.newUpdater(AbstractReducer.class, ConcurrentMap.class, "lastPages");

    /** */
    private final GridKernalContext ctx;

    /** DO NOT change name field of this field, updated through {@link #LAST_PAGES_UPDATER} */
    @SuppressWarnings("unused")
    private volatile ConcurrentMap<ReduceSourceKey, Integer> lastPages;

    /** Row source nodes. */
    protected Set<UUID> srcNodes;

    /** */
    private int pageSize;

    /**
     * Will be r/w from query execution thread only, does not need to be threadsafe.
     */
    protected final ReduceBlockList<Row> fetched;

    /** */
    private Row lastEvictedRow;

    /**
     * Constructor.
     *
     * @param ctx Kernal context.
     */
    AbstractReducer(GridKernalContext ctx) {
        this.ctx = ctx;

        fetched = new ReduceBlockList<>(prefetchSize);
    }

    /** {@inheritDoc} */
    @Override public void setSources(Collection<ClusterNode> nodes, int segmentsCnt) {
        assert srcNodes == null;

        srcNodes = new HashSet<>(nodes.size());

        for (ClusterNode node : nodes) {
            if (!srcNodes.add(node.id()))
                throw new IllegalStateException();
        }
    }

    /** {@inheritDoc} */
    @Override public Set<UUID> sources() {
        return srcNodes;
    }

    /** {@inheritDoc} */
    @Override public boolean hasSource(UUID nodeId) {
        return srcNodes.contains(nodeId);
    }

    /** {@inheritDoc} */
    @Override public void setPageSize(int pageSize) {
        this.pageSize = pageSize;
    }

    /** {@inheritDoc} */
    @Override public void onFailure(UUID nodeId, final CacheException e) {
        if (nodeId == null)
            nodeId = F.first(srcNodes);

        addPage0(new ReduceResultPage(null, nodeId, null) {
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

    /** {@inheritDoc} */
    @Override public final Cursor find(@Nullable SearchRow first, @Nullable SearchRow last) {
        checkBounds(lastEvictedRow, first, last);

        if (fetchedAll())
            return findAllFetched(fetched, first, last);

        return findInStream(first, last);
    }

    /**
     * @param first Row.
     * @param last Row.
     * @return Cursor over remote streams.
     */
    protected abstract Cursor findInStream(@Nullable SearchRow first, @Nullable SearchRow last);

    /**
     * @param fetched Fetched data.
     * @param first Row.
     * @param last Row.
     * @return Cursor over fetched data.
     */
    protected abstract Cursor findAllFetched(List<Row> fetched, @Nullable SearchRow first, @Nullable SearchRow last);

    /**
     * @param lastEvictedRow Last evicted fetched row.
     * @param first Lower bound.
     * @param last Upper bound.
     */
    protected void checkBounds(Row lastEvictedRow, SearchRow first, SearchRow last) {
        if (lastEvictedRow != null)
            throw new IgniteException("Fetched result set was too large. " +
                    IGNITE_SQL_MERGE_TABLE_MAX_SIZE + "(" + MAX_FETCH_SIZE + ") should be increased.");
    }

    /**
     * @param evictedBlock Evicted block.
     */
    protected void onBlockEvict(@NotNull List<Row> evictedBlock) {
        assert evictedBlock.size() == prefetchSize;

        // Remember the last row (it will be max row) from the evicted block.
        lastEvictedRow = requireNonNull(last(evictedBlock));
    }

    /**
     * @param l List.
     * @return Last element.
     */
    static <Z> Z last(List<Z> l) {
        return l.get(l.size() - 1);
    }


    /** {@inheritDoc} */
    @Override public void addPage(ReduceResultPage page) {
        markLastPage(page);
        addPage0(page);
    }

    /**
     * @param page Page.
     */
    protected abstract void addPage0(ReduceResultPage page);

    /**
     * Fails index if any source node is left.
     */
    private void checkSourceNodesAlive() {
        for (UUID nodeId : srcNodes) {
            if (!ctx.discovery().alive(nodeId)) {
                onFailure(nodeId, null);

                return;
            }
        }
    }

    /**
     * @param e Error.
     */
    public void fail(final CacheException e) {
        for (UUID nodeId : srcNodes)
            onFailure(nodeId, e);
    }

    /**
     * @param page Page.
     */
    private void markLastPage(ReduceResultPage page) {
        GridQueryNextPageResponse res = page.response();

        if (!res.last()) {
            UUID nodeId = page.source();

            initLastPages(nodeId, res);

            ConcurrentMap<ReduceSourceKey,Integer> lp = lastPages;

            if (lp == null)
                return; // It was not initialized --> wait for last page flag.

            Integer lastPage = lp.get(new ReduceSourceKey(nodeId, res.segmentId()));

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

        ConcurrentMap<ReduceSourceKey,Integer> lp = lastPages;

        if (lp == null && !LAST_PAGES_UPDATER.compareAndSet(this, null, lp = new ConcurrentHashMap<>()))
            lp = lastPages;

        assert pageSize > 0 : pageSize;

        int lastPage = allRows == 0 ? 0 : (allRows - 1) / pageSize;

        assert lastPage >= 0 : lastPage;

        if (lp.put(new ReduceSourceKey(nodeId, res.segmentId()), lastPage) != null)
            throw new IllegalStateException();
    }

    /**
     * @param lastPage Real last page.
     * @return Created dummy page.
     */
    protected final ReduceResultPage createDummyLastPage(ReduceResultPage lastPage) {
        assert !lastPage.isDummyLast(); // It must be a real last page.

        return new ReduceResultPage(ctx, lastPage.source(), null).setLast(true);
    }

    /**
     * @param queue Queue to poll.
     * @param iter Current iterator.
     * @return The same or new iterator.
     */
    protected final Iterator<Value[]> pollNextIterator(Pollable<ReduceResultPage> queue, Iterator<Value[]> iter) {
        if (!iter.hasNext()) {
            ReduceResultPage page = takeNextPage(queue);

            if (!page.isLast())
                page.fetchNextPage(); // Failed will throw an exception here.

            iter = page.rows();

            // The received iterator must be empty in the dummy last page or on failure.
            assert iter.hasNext() || page.isDummyLast() || page.isFail();
        }

        return iter;
    }

    /**
     * @param queue Queue to poll.
     * @return Next page.
     */
    private ReduceResultPage takeNextPage(Pollable<ReduceResultPage> queue) {
        ReduceResultPage page;

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
     * Pollable.
     */
    interface Pollable<E> {
        /**
         * @param timeout Timeout.
         * @param unit Time unit.
         * @return Polled value or {@code null} if none.
         * @throws InterruptedException If interrupted.
         */
        E poll(long timeout, TimeUnit unit) throws InterruptedException;
    }
}
