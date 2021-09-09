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

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Queue;
import java.util.UUID;
import java.util.function.BiConsumer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryAdapter;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * This class provides an interface {@link #nextPage()} that returns a {@link NodePage} of cache query result
 * from single node. Pages are stored in a queue, after polling a queue it tries to load a new page.
 */
class NodePageStream<R> {
    /** Queue of data of results pages. */
    private final Queue<NodePage<R>> queue = new LinkedList<>();

    /** Node ID to stream pages */
    private final UUID nodeId;

    /**
     * List of nodes that respons with cache query result pages. This collection should be cleaned before sending new
     * cache query request.
     */
    private volatile boolean rcvd;

    /**
     * This lock guards:
     * 1. Order of invocation query future {@code onDone} and while loop over {@link #queue}.
     * 2. Consistency of {@link #rcvd} and {@link #queue}.
     */
    private final Object queueLock;

    /** Query info. */
    private final GridCacheQueryAdapter<?> qry;

    /** Timestamp when a query fails with timeout. */
    private final long timeoutTime;

    /** Callback to request pages from query nodes. */
    private final BiConsumer<Collection<UUID>, Boolean> reqPages;

    /** Flags shows whether there are no available pages on query nodes. */
    private boolean noMorePages;

    /** */
    NodePageStream(GridCacheQueryAdapter<?> qry, Object queueLock, long timeoutTime,
        UUID nodeId, BiConsumer<Collection<UUID>, Boolean> reqPages) {

        this.queueLock = queueLock;
        this.qry = qry;
        this.timeoutTime = timeoutTime;
        this.nodeId = nodeId;

        this.reqPages = reqPages;
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
            synchronized (queueLock) {
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
                long timeout = qry.timeout();

                long waitTime = timeout == 0 ? Long.MAX_VALUE : timeoutTime - U.currentTimeMillis();

                if (waitTime <= 0) {
                    page = new NodePage<>(nodeId, Collections.emptyList());

                    return page;
                }

                synchronized (queueLock) {
                    try {
                        if (queue.isEmpty() && !noMorePages)
                            queueLock.wait(waitTime);
                    }
                    catch (InterruptedException e) {
                        Thread.currentThread().interrupt();

                        throw new IgniteCheckedException("Query was interrupted: " + qry, e);
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
        assert Thread.holdsLock(queueLock);

        queue.add(new NodePage<>(nodeId, data));

        if (nodeId != null)
            rcvd = true;

        if (last)
            noMorePages = true;

        return last;
    }

    /** Callback on receiving page error. */
    void onError() {
        synchronized (queueLock) {
            queue.add(new NodePage<>(nodeId, Collections.emptyList()));

            noMorePages = true;
        }
    }

    /** Clear structures on cancel. */
    void cancel() {
        synchronized (queueLock) {
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

        synchronized (queueLock) {
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
            reqPages.accept(nodes, false);
    }
}
