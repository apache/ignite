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

package org.apache.ignite.internal.processors.cache.query;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import javax.cache.CacheException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.tracing.MTC;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;

import static java.util.Collections.emptyIterator;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_SQL_MERGE_TABLE_MAX_SIZE;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_SQL_MERGE_TABLE_PREFETCH_SIZE;
import static org.apache.ignite.IgniteSystemProperties.getInteger;
import static org.apache.ignite.internal.processors.tracing.SpanTags.SQL_PAGE_ROWS;
import static org.apache.ignite.internal.processors.tracing.SpanType.SQL_PAGE_FETCH;
import static org.apache.ignite.internal.processors.tracing.SpanType.SQL_PAGE_WAIT;

/**
 * Ordered reducer of cache query results from remote nodes. There is the MergeSort algorithm under the hood.
 * Streams of query result pages by node are represented with class {@link NodePageStream}.
 */
public class OrderedReducer<T> implements Reducer<T> {
    /** */
    private static final int MAX_FETCH_SIZE = getInteger(IGNITE_SQL_MERGE_TABLE_MAX_SIZE, 10_000);

    /** */
    private static final int prefetchSize = getInteger(IGNITE_SQL_MERGE_TABLE_PREFETCH_SIZE, 1024);

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

    /** Compares head of streams to get lowest value at the moment. */
    // TODO: order: asc, desc.
    private final Comparator<NodePageStream> streamCmp;

    /** If {@code true} then there wasn't call {@link #hasNext()} on this reducer. */
    private boolean first = true;

    /** Prefetched next value. */
    // TODO: thread safety?
    private T nextVal;

    /** Kernal context. */
    private final GridKernalContext ctx;

    /** Row source nodes. */
    private Set<UUID> srcNodes;

    /** Map of Node ID to stream. */
    private Map<UUID, NodePageStream> streamsMap;

    /** Array of streams. */
    private NodePageStream[] streams;

    /** Query request ID. */
    private final long reqId;

    /**
     * Offset of current stream to get next value. This offset only increments if a stream is done.\
     * The array {@link #streams} is sorted to put stream with lowest value on the {@link #streamOff} offset.
     */
    private int streamOff;

    /** Fetcher of query result pages. */
    private final CacheQueryResultFetcher fetcher;

    /** This lock is shared between all streams. It guards {@link #failPage}. */
    private final Lock lock = new ReentrantLock();

    /** */
    private final Condition notEmpty = lock.newCondition();

    /** Global page that indicates that processing of request failed. */
    private CacheQueryResultPage<T> failPage;

    /** */
    OrderedReducer(GridKernalContext ctx, long reqId, CacheQueryResultFetcher fetcher, Comparator<T> rowCmp) {
        this.ctx = ctx;
        this.fetcher = fetcher;
        this.reqId = reqId;

        streamCmp = (o1, o2) -> {
            if (o1 == o2) // both nulls
                return 0;

            if (o1 == null)
                return -1;

            if (o2 == null)
                return 1;

            return rowCmp.compare(o1.head(), o2.head());
        };
    }

    /** {@inheritDoc} */
    @Override public boolean hasNext() {
        if (first)
            goFirst();
        else
            goNext();

        return !finished();
    }

    /** */
    @Override public Object sharedLock() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public T next() {
        if (nextVal == null) {
            // No hasNext() was invoked.
            if (!hasNext())
                throw new NoSuchElementException("No next value.");
        }

        T val = nextVal;

        nextVal = null;

        return val;
    }

    /**
     * On first run find order of streams by head value of every stream.
     */
    private void goFirst() {
        assert first;

        first = false;

        for (int i = 0; i < streams.length; i++) {
            NodePageStream s = streams[i];

            if (!s.hasNext()) {
                streams[i] = null;
                streamOff++; // Move left bound.
            }
            else {
                // Initialize with first value.
                s.next();
            }
        }

        if (finished())
            return;

        Arrays.sort(streams, streamCmp);

        nextVal = currentStream().head();
    }

    /**
     * Find stream with lowest value for current moment by head value of every stream.
     */
    private void goNext() {
        if (finished())
            return;

        if (currentStream().hasNext()) {
            currentStream().next();

            bubbleUp(streams, streamOff, streamCmp);
        }
        else
            streams[streamOff++] = null; // Move left bound and nullify empty stream.

        if (!finished())
            nextVal = currentStream().head();
    }

    /** {@inheritDoc} */
    public void setSources(Collection<ClusterNode> nodes) {
        assert srcNodes == null;

        srcNodes = new HashSet<>(nodes.size());
        streamsMap = U.newHashMap(nodes.size());
        streams = (NodePageStream[]) Array.newInstance(NodePageStream.class, nodes.size());

        int i = 0;

        for (ClusterNode node : nodes) {
            if (!srcNodes.add(node.id()))
                throw new IllegalStateException();

            streams[i] = new NodePageStream(node);

            streamsMap.put(node.id(), streams[i++]);
        }
    }

    // TODO: which threads add pages?
    /** {@inheritDoc} */
    @Override public void addPage(CacheQueryResultPage<T> page) {
        if (page.fail())
            failPage(page);
        else {
            UUID src = page.sourceNodeId();

            // Local node.
            if (src == null)
                src = ctx.localNodeId();

            streamsMap.get(src).addNextPage(page);
        }
    }

    /** {@inheritDoc} */
    @Override public void onLastPage() {
        lock.lock();

        try {
            srcNodes.clear();
            streamsMap.clear();

            nextVal = null;
            failPage = null;

        } finally {
            lock.unlock();
        }
    }

    // TODO: Maybe raise exception earlier. Handler node failure from other thread (ReduceQueryRyn failed).
    // TODO: where to invoke?
    /** */
    private void onFailure(UUID nodeId, final IgniteException e) {
        if (nodeId == null)
            nodeId = F.first(srcNodes);

        CacheQueryResultPage<T> page = new CacheQueryResultPage<>(null, nodeId, null);
        page.fail(e);

        addPage(page);
    }

    /**
     * @return Stream that deliver values in the current moment.
     */
    private NodePageStream currentStream() {
        if (!finished())
            return streams[streamOff];

        throw new NoSuchElementException();
    }

    /** Whether reducer handled all data. */
    private boolean finished() {
        return streamOff == streams.length;
    }

    /** {@inheritDoc} */
    // TODO: on node left?
    public boolean hasSource(UUID nodeId) {
        return srcNodes.contains(nodeId);
    }

    /**
     * Row stream iterator. Supports public methods:
     * 1. hasNext()
     * 2. next()
     * 3. head()
     *
     * It iterates over query result page.
     * It consumes pages that reducer add with addNextPage().
     * If nextPage isn't set then wait for it in the timeout range then wait.
     *
     */
    private class NodePageStream {
        /** Cluster node ID. */
        private final List<UUID> singleNode;

        /** Iterator over current page. */
        private Iterator<T> currPageIter = emptyIterator();

        /** Prefetched next pages. TODO: thread safety? */
        private LinkedList<CacheQueryResultPage<T>> nextPages = new LinkedList<>();

        /** Head value of current stream. */
        private T head;

        /**
         * @param node Node for this stream.
         */
        NodePageStream(ClusterNode node) {
            singleNode = Collections.singletonList(node.id());
        }

        /**
         * @param page Next page to stream.
         */
        void addNextPage(CacheQueryResultPage<T> page) {
            assert !page.fail();

            lock.lock();

            try {
                nextPages.add(page);

                notEmpty.signalAll();
            }
            finally {
                lock.unlock();
            }
        }

        /**
         * @return Next page or {@code null} if timeout exceed.
         */
        private CacheQueryResultPage<T> poll(long timeout, TimeUnit unit) throws InterruptedException {
            long nanos = unit.toNanos(timeout);

            lock.lock();

            try {
                while (true) {
                    if (failPage != null) {
                        if (failPage.exception() != null)
                            throw failPage.exception();

                        throw new CacheException("Failed to fetch data from node: " + singleNode.get(0));
                    }

                    CacheQueryResultPage<T> page = nextPages.poll();

                    if (page != null) {
                        // This is mark that this stream is done. Cleanup.
                        if (page.dummyLast()) {
                            nextPages = null;
                            head = null;
                        }

                        if (page.last() && !page.dummyLast())
                            nextPages.add(createDummyLastPage(page));

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
         * @return {@code true} If this stream has next row.
         */
        public boolean hasNext() {
            currPageIter = iterator(currPageIter);

            return currPageIter.hasNext();
        }

        /**
         * @return Next value from this stream.
         */
        public T next() {
            head = currPageIter.next();

            return head;
        }

        /**
         * Note, that this method should be invoked after {@link #next()}.
         *
         * @return Head of this stream. Can be invoked multiple times.
         */
        public T head() {
            assert head != null;

            return head;
        }

        /**
         * Takes next page. If currently stream doesn't have a page, then wait for it.
         *
         * @return Next page.
         */
        protected CacheQueryResultPage<T> takeNextPage() {
            try (MTC.TraceSurroundings ignored = MTC.support(ctx.tracing().create(SQL_PAGE_WAIT, MTC.span()))) {
                CacheQueryResultPage<T> page;

                for (; ; ) {
                    try {
                        page = poll(500, TimeUnit.MILLISECONDS);
                    }
                    catch (InterruptedException e) {
                        throw new CacheException("Query execution was interrupted.", e);
                    }

                    if (page != null)
                        break;

                    checkSourceNodesAlive();
                }

                // We trigger upload a new page in background to avoid waits.
                if (!page.fail() && !page.last()) {
                    try {
                        fetcher.fetchPages(reqId, singleNode, false);

                    } catch (Exception e) {
                        throw new CacheException("Query execution was interrupted.", e);
                    }
                }

                return page;
            }
        }

        /**
         * @param iter Current iterator.
         * @return The same or new iterator.
         */
        private Iterator<T> iterator(Iterator<T> iter) {
            if (!iter.hasNext()) {
                try (MTC.TraceSurroundings ignored = MTC.support(ctx.tracing().create(SQL_PAGE_FETCH, MTC.span()))) {
                    CacheQueryResultPage<T> page = takeNextPage();

                    iter = page.rows().iterator();

                    MTC.span().addTag(SQL_PAGE_ROWS, () -> Integer.toString(page.rowsInPage()));

                    // The received iterator must be empty in the dummy last page or on failure.
                    assert iter.hasNext() || page.dummyLast() || page.fail();
                }
            }

            return iter;
        }
    }

    /**
     * Dummy page marks stream as done. It contains empty iterator and the last flag.
     *
     * @param lastPage Real last page.
     * @return Created dummy page.
     */
    private CacheQueryResultPage createDummyLastPage(CacheQueryResultPage lastPage) {
        CacheQueryResultPage p = new CacheQueryResultPage<>(ctx, lastPage.sourceNodeId(), null);
        p.last(true);

        return p;
    }

    /**
     * Check if all nodes are alive.
     */
    private void checkSourceNodesAlive() {
        for (UUID nodeId: srcNodes) {
            if (!ctx.discovery().alive(nodeId)) {
                CacheQueryResultPage<T> page = new CacheQueryResultPage<>(null, nodeId, null);
                // TODO: exception
                page.fail(new IgniteException("Node with id " + nodeId + " is gone."));

                // TODO: throw exception instead?
                failPage = page;

                break;
            }
        }
    }

    /** Set fail page.*/
    public void failPage(CacheQueryResultPage<T> page) {
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

    /**
     * @param arr Array.
     * @param off Offset.
     * @param cmp Comparator.
     */
    public static <Z> void bubbleUp(Z[] arr, int off, Comparator<Z> cmp) {
        for (int i = off, last = arr.length - 1; i < last; i++) {
            if (cmp.compare(arr[i], arr[i + 1]) <= 0)
                break;

            U.swap(arr, i, i + 1);
        }
    }
}
