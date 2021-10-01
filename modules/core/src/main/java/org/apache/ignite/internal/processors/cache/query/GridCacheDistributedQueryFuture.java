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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteIllegalStateException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.query.reducer.CacheQueryReducer;
import org.apache.ignite.internal.processors.cache.query.reducer.MergeSortDistributedCacheQueryReducer;
import org.apache.ignite.internal.processors.cache.query.reducer.NodePage;
import org.apache.ignite.internal.processors.cache.query.reducer.UnsortedDistributedCacheQueryReducer;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.query.GridCacheQueryType.TEXT;

/**
 * Distributed query future.
 */
public class GridCacheDistributedQueryFuture<K, V, R> extends GridCacheQueryFutureAdapter<K, V, R> {
    /** */
    private final long reqId;

    /** Reducer of distributed cache query results. */
    private final CacheQueryReducer<R> reducer;

    /** Helps to send cache query requests to other nodes. */
    private final GridCacheDistributedQueryManager<K, V> qryMgr;

    /** Collection of streams. */
    private final Map<UUID, NodePageStream> streams;

    /** Collection of streams that don't deliver all pages yet. */
    private final Map<UUID, NodePageStream> remoteStreams;

    /** Count down this latch when every node responses on initial cache query request. */
    private final CountDownLatch firstPageLatch = new CountDownLatch(1);

    /**
     * This lock guards:
     * 1. Order of invocation query future {@code onDone} and while loop over {@link #streams} queues.
     * 2. Consistency of local and remote pages.
     */
    private final Object pagesLock = new Object();

    /** Set of nodes that deliver their first page. */
    private Set<UUID> rcvdFirstPage = new HashSet<>();

    /**
     * @param ctx Cache context.
     * @param reqId Request ID.
     * @param qry Query.
     */
    protected GridCacheDistributedQueryFuture(GridCacheContext<K, V> ctx, long reqId, GridCacheQueryBean qry,
        Collection<ClusterNode> nodes) {
        super(ctx, qry, false);

        assert reqId > 0;

        this.reqId = reqId;

        qryMgr = (GridCacheDistributedQueryManager<K, V>) ctx.queries();

        streams = new HashMap<>(nodes.size());

        for (ClusterNode node : nodes) {
            NodePageStream s = new NodePageStream(node.id());

            streams.put(node.id(), s);
        }

        remoteStreams = new ConcurrentHashMap<>(streams);

        reducer = qry.query().type() == TEXT ?
            new MergeSortDistributedCacheQueryReducer<R>(this::nextNodePage, textResultComparator, nodes)
            : new UnsortedDistributedCacheQueryReducer<>(this::nextNodePage, nodes);
    }

    /** {@inheritDoc} */
    @Override protected void cancelQuery() {
        release(false, null);

        List<UUID> nodes = new ArrayList<>();

        for (NodePageStream s : remoteStreams.values()) {
            s.cancel();

            nodes.add(s.nodeId);
        }

        remoteStreams.clear();

        try {
            GridCacheQueryRequest req = GridCacheQueryRequest.cancelRequest(cctx, reqId, fields());

            qryMgr.sendRequest(null, req, nodes);
        }
        catch (IgniteCheckedException e) {
            if (logger() != null)
                U.error(logger(), "Failed to send cancel request (will cancel query in any case).", e);
        }

        cctx.queries().onQueryFutureCanceled(reqId);

        clear();
    }

    /** {@inheritDoc} */
    @Override protected void onNodeLeft(UUID nodeId) {
        boolean qryNode = remoteStreams.containsKey(nodeId);

        if (qryNode) {
            onPage(nodeId, null,
                new ClusterTopologyCheckedException("Remote node has left topology: " + nodeId), true);
        }
    }

    /** {@inheritDoc} */
    @Override protected void onPageError(Throwable err) {
        onError(err);
    }

    /** {@inheritDoc} */
    @Override protected void onPage(UUID nodeId, Collection<R> data, boolean last) {
        synchronized (pagesLock) {
            if (rcvdFirstPage != null) {
                rcvdFirstPage.add(nodeId);

                if (rcvdFirstPage.size() == streams.size()) {
                    firstPageLatch.countDown();

                    rcvdFirstPage.clear();
                    rcvdFirstPage = null;
                }
            }

            NodePageStream stream = remoteStreams.get(nodeId);

            if (stream == null)
                return;

            stream.addPage(nodeId, data, last);

            if (last && (remoteStreams.remove(nodeId) != null) && remoteStreams.isEmpty())
                release(true, null);

            pagesLock.notifyAll();
        }
    }

    /** {@inheritDoc} */
    @Override public void awaitFirstItemAvailable() throws IgniteCheckedException {
        U.await(firstPageLatch);

        if (isDone() && error() != null)
            // Throw the exception if future failed.
            super.get();
    }

    /** {@inheritDoc} */
    @Override protected CacheQueryReducer<R> reducer() {
        return reducer;
    }

    /** {@inheritDoc} */
    @Override public Collection<R> get() throws IgniteCheckedException {
        return get0();
    }

    /** {@inheritDoc} */
    @Override public Collection<R> get(long timeout, TimeUnit unit) throws IgniteCheckedException {
        return get0();
    }

    /** {@inheritDoc} */
    @Override public Collection<R> getUninterruptibly() throws IgniteCheckedException {
        return get0();
    }

    /**
     * Completion of distributed query future depends on user that iterates over query result with lazy page loading.
     * Then {@link #get()} can lock on unpredictably long period of time. So we should avoid call it.
     */
    private Collection<R> get0() {
        throw new IgniteIllegalStateException("Unexpected lock on iterator over distributed cache query result.");
    }

    /** {@inheritDoc} */
    @Override void clear() {
        assert isDone() : this;

        GridCacheDistributedQueryManager<K, V> qryMgr = (GridCacheDistributedQueryManager<K, V>)cctx.queries();

        if (qryMgr != null)
            qryMgr.removeQueryFuture(reqId);
    }

    /** @return Request ID. */
    long requestId() {
        return reqId;
    }

    /** Compares rows for {@code TextQuery} results for ordering results in MergeSort reducer. */
    private static final Comparator textResultComparator = (c1, c2) ->
        Float.compare(((ScoredCacheEntry)c2).score(), ((ScoredCacheEntry)c1).score());

    /**
     * Releases owned resources.
     *
     * @param complete Whether to complete future.
     * @param err      Error.
     */
    private void release(boolean complete, Throwable err) {
        if (complete) {
            if (err != null)
                onDone(err);
            else
                onDone();
        }


        // Must release the latch after onDone() in order for a waiting thread to see an exception, if any.
        firstPageLatch.countDown();
    }

    /**
     * Send request to fetch new pages.
     *
     * @param node Node to send request.
     * @param all  Whether page will contain all data from node.
     */
    private void requestPages(UUID node, boolean all) {
        try {
            GridCacheQueryRequest req = GridCacheQueryRequest.pageRequest(cctx, reqId, query().query(), fields(), all);

            qryMgr.sendRequest(null, req, Collections.singletonList(node));
        }
        catch (IgniteCheckedException e) {
            onError(e);
        }
    }

    /** Handle receiving error page. */
    private void onError(Throwable err) {
        synchronized (pagesLock) {
            for (NodePageStream s : remoteStreams.values())
                s.onError();

            release(true, err);

            pagesLock.notifyAll();
        }
    }

    /**
     * Returns next node page for specified {@code nodeId} or {@code null} in case of error.
     */
    public @Nullable NodePage<R> nextNodePage(UUID nodeId) {
        try {
            return streams.get(nodeId).nextPage();
        }
        catch (IgniteCheckedException e) {
            onError(e);

            return null;
        }
    }

    /**
     * This class provides an interface {@link #nextPage()} that returns a {@link NodePage} of cache query result from
     * single node. Pages are stored in a queue, after polling a queue it tries to load a new page.
     */
    private class NodePageStream {
        /**
         * Queue of data of results pages.
         */
        private final Queue<NodePage<R>> queue = new LinkedList<>();

        /**
         * Node ID to stream pages
         */
        private final UUID nodeId;

        /**
         * {@code true} shows whether node responsed with cache query result pages. Flag will be set to {@code false}
         * before new page requests.
         */
        private volatile boolean rcvd;

        /**
         * Flags shows whether there are no available pages on query node.
         */
        private boolean noMorePages;

        /**
         *
         */
        NodePageStream(UUID nodeId) {
            this.nodeId = nodeId;
        }

        /**
         * Returns query result page. Load new pages after polling a queue. Wait for new page if currently there is no
         * available any.
         *
         * @return Query result page.
         * @throws IgniteCheckedException In case of error.
         */
        NodePage<R> nextPage() throws IgniteCheckedException {
            NodePage<R> page = null;

            while (page == null || !page.hasNext()) {
                // Check current page iterator.
                synchronized (pagesLock) {
                    page = queue.poll();

                    // If all pages are ready, then skip loading new pages.
                    if (noMorePages && queue.peek() == null) {
                        if (page == null)
                            page = new NodePage<>(nodeId, Collections.emptyList());

                        return page;
                    }
                }

                // Trigger loads next pages after polling queue.
                loadPage();

                // Wait for a page after triggering page loading.
                if (page == null) {
                    long timeout = query().query().timeout();

                    long waitTime = timeout == 0 ? Long.MAX_VALUE : endTime() - U.currentTimeMillis();

                    if (waitTime <= 0) {
                        page = new NodePage<>(nodeId, Collections.emptyList());

                        return page;
                    }

                    synchronized (pagesLock) {
                        try {
                            if (queue.isEmpty() && !noMorePages)
                                pagesLock.wait(waitTime);
                        }
                        catch (InterruptedException e) {
                            Thread.currentThread().interrupt();

                            throw new IgniteCheckedException("Query was interrupted: " + query().query(), e);
                        }
                    }
                }
            }

            if (page == null)
                page = new NodePage<>(nodeId, Collections.emptyList());

            return page;
        }

        /**
         * Add new query result page of data.
         */
        boolean addPage(UUID nodeId, Collection<R> data, boolean last) {
            assert Thread.holdsLock(pagesLock);

            queue.add(new NodePage<>(nodeId, data));

            if (nodeId != null)
                rcvd = true;

            if (last)
                noMorePages = true;

            return last;
        }

        /**
         * Callback on receiving page error.
         */
        void onError() {
            synchronized (pagesLock) {
                queue.add(new NodePage<>(nodeId, Collections.emptyList()));

                noMorePages = true;
            }
        }

        /**
         * Clear structures on cancel.
         */
        void cancel() {
            synchronized (pagesLock) {
                rcvd = false;
                queue.clear();

                noMorePages = true;
            }
        }

        /**
         * Trigger load new pages from all nodes that still have data.
         */
        private void loadPage() {
            boolean sendReq = false;

            synchronized (pagesLock) {
                if (noMorePages)
                    return;

                // Loads only queue is empty to avoid memory consumption on additional pages.
                if (!queue.isEmpty())
                    return;

                // Rcvd has to contain all nodes from subgrid, otherwise there are requested pages. So let's wait it first.
                if (rcvd) {
                    rcvd = false;

                    sendReq = true;
                }
            }

            if (sendReq)
                requestPages(nodeId, false);
        }
    }
}
