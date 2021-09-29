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

package org.apache.ignite.internal.processors.cache.query.reducer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
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
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.cache.query.DistributedCacheQueryReducer;
import org.apache.ignite.internal.processors.cache.query.GridCacheDistributedQueryManager;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryFutureAdapter;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryRequest;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Abstract class for distributed reducer implementations.
 */
abstract class AbstractDistributedCacheQueryReducer<R> implements DistributedCacheQueryReducer<R> {
    /** Collection of streams that don't deliver all pages yet. */
    private final Map<UUID, NodePageStream> remoteStreams;

    /** Set of nodes that deliver their first page. */
    private Set<UUID> rcvdFirstPage = new HashSet<>();

    /** Collection of streams. */
    protected final Map<UUID, NodePageStream> streams;

    /** Query request ID. */
    private final long reqId;

    /** Query future. */
    private final GridCacheQueryFutureAdapter<?, ?, ?> fut;

    /** Helps to send cache query requests to other nodes. */
    private final GridCacheDistributedQueryManager<?, ?> qryMgr;

    /**
     * Count down this latch when every node responses on initial cache query request.
     */
    private final CountDownLatch firstPageLatch = new CountDownLatch(1);

    /**
     * This lock guards:
     * 1. Order of invocation query future {@code onDone} and while loop over {@link #streams} queues.
     * 2. Consistency of local and remote pages.
     */
    private final Object pagesLock = new Object();

    /**
     * @param fut Cache query future.
     * @param reqId Cache query request ID.
     * @param qryMgr Distributed cache query manager.
     * @param nodes Collection of nodes this query applies to.
     */
    protected AbstractDistributedCacheQueryReducer(GridCacheQueryFutureAdapter<?, ?, ?> fut, long reqId,
        GridCacheDistributedQueryManager<?, ?> qryMgr, Collection<ClusterNode> nodes) {
        this.fut = fut;
        this.reqId = reqId;
        this.qryMgr = qryMgr;

        streams = new HashMap<>(nodes.size());

        for (ClusterNode node: nodes) {
            NodePageStream s = new NodePageStream(node.id());

            streams.put(node.id(), s);
        }

        remoteStreams = new ConcurrentHashMap<>(streams);
    }

    /**
     * Releases owned resources.
     *
     * @param complete Whether to complete future.
     * @param err Error.
     */
    private void release(boolean complete, Throwable err) {
        if (complete) {
            if (err != null)
                fut.onDone(err);
            else
                fut.onDone();
        }

        // Must release the latch after onDone() in order for a waiting thread to see an exception, if any.
        firstPageLatch.countDown();
    }

    /** {@inheritDoc} */
    @Override public void awaitInitialization() throws IgniteInterruptedCheckedException {
        U.await(firstPageLatch);
    }

    /**
     * Callback that invoked when all nodes response with initial page.
     */
    private void onFirstItemReady() {
        firstPageLatch.countDown();
    }

    /**
     * Send request to fetch new pages.
     *
     * @param nodes Collection of nodes to send request.
     * @param all Whether page will contain all data from node.
     */
    private void requestPages(Collection<UUID> nodes, boolean all) {
        try {
            GridCacheQueryRequest req = GridCacheQueryRequest.pageRequest(fut.cacheContext(), reqId, fut, all);

            qryMgr.sendRequest(null, req, nodes);

        } catch (IgniteCheckedException e) {
            onError(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void onError(Throwable err) {
        synchronized (pagesLock) {
            for (NodePageStream s: remoteStreams.values())
                s.onError();

            release(true, err);
        }
    }

    /** {@inheritDoc} */
    @Override public void cancel() {
        release(false, null);

        List<UUID> nodes = new ArrayList<>();

        for (NodePageStream s: remoteStreams.values()) {
            s.cancel();

            nodes.add(s.nodeId());
        }

        remoteStreams.clear();

        try {
            GridCacheQueryRequest req = GridCacheQueryRequest.cancelRequest(fut.cacheContext(), reqId, fut.fields());

            qryMgr.sendRequest(null, req, nodes);
        }
        catch (IgniteCheckedException e) {
            if (fut.logger() != null)
                U.error(fut.logger(), "Failed to send cancel request (will cancel query in any case).", e);
        }
    }

    /** {@inheritDoc} */
    @Override public void onPage(UUID nodeId, Collection<R> data, boolean last) {
        synchronized (pagesLock) {
            if (rcvdFirstPage != null) {
                rcvdFirstPage.add(nodeId);

                if (rcvdFirstPage.size() == streams.size()) {
                    onFirstItemReady();

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
    @Override public boolean remoteQueryNode(UUID nodeId) {
        return remoteStreams.containsKey(nodeId);
    }

    /**
     * This class provides an interface {@link #nextPage()} that returns a {@link NodePage} of cache query result
     * from single node. Pages are stored in a queue, after polling a queue it tries to load a new page.
     */
    protected class NodePageStream {
        /** Queue of data of results pages. */
        private final Queue<NodePage<R>> queue = new LinkedList<>();

        /** Node ID to stream pages */
        private final UUID nodeId;

        /**
         * List of nodes that respons with cache query result pages. This collection should be cleaned before sending new
         * cache query request.
         */
        private volatile boolean rcvd;

        /** Flags shows whether there are no available pages on query nodes. */
        private boolean noMorePages;

        /** */
        NodePageStream(UUID nodeId) {
            this.nodeId = nodeId;
        }

        /**
         * Returns query result page. Load new pages after polling a queue.
         * Wait for new page if currently there is no available any.
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
                    long timeout = fut.query().query().timeout();

                    long waitTime = timeout == 0 ? Long.MAX_VALUE : fut.endTime() - U.currentTimeMillis();

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

                            throw new IgniteCheckedException("Query was interrupted: " + fut.query().query(), e);
                        }
                    }
                }
            }

            if (page == null)
                page = new NodePage<>(nodeId, Collections.emptyList());

            return page;
        }

        /**
         * @return Node ID.
         */
        UUID nodeId() {
            return nodeId;
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

        /** Callback on receiving page error. */
        void onError() {
            synchronized (pagesLock) {
                queue.add(new NodePage<>(nodeId, Collections.emptyList()));

                noMorePages = true;
            }
        }

        /** Clear structures on cancel. */
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
            Collection<UUID> nodes = null;

            synchronized (pagesLock) {
                if (noMorePages)
                    return;

                // Loads only queue is empty to avoid memory consumption on additional pages.
                if (!queue.isEmpty())
                    return;

                // Rcvd has to contain all nodes from subgrid, otherwise there are requested pages. So let's wait it first.
                if (rcvd) {
                    rcvd = false;

                    nodes = Collections.singletonList(nodeId);
                }
            }

            if (nodes != null)
                requestPages(nodes, false);
        }
    }
}
