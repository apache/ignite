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
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryAdapter;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Iterator over pages stream. Pages are stored in a queue. After polling a queue try load a new page instead of it.
 */
class PageStream<R> {
    /** Queue of data of results pages. */
    protected final Queue<Collection<R>> queue = new LinkedList<>();

    /** Iterator over current page. */
    private Iterator<R> iter;

    /**
     * Dynamic collection of nodes that run this query. If a node finishes this query then remove it from this colleciton.
     */
    private final Collection<UUID> subgrid;

    /**
     * List of nodes that respons with cache query result pages. This collection should be cleaned before sending new
     * cache query request.
     */
    private final Collection<UUID> rcvd = new HashSet<>();

    /**
     * Lock that guards:
     * 1. Order of invocation query future {@code onDone} and while loop over {@link #queue}.
     * 2. Consistency of {@link #rcvd} and {@link #queue}.
     */
    private final Object queueLock;

    /** Query info. */
    protected final GridCacheQueryAdapter qry;

    /** Timestamp when a query fails with timeout. */
    private final long timeoutTime;

    /** Callback to request pages from query nodes. */
    private final BiConsumer<Collection<UUID>, Boolean> reqPages;

    /** Flags shows whether there are no available pages on query nodes. */
    private boolean noMorePages;

    /** */
    protected PageStream(GridCacheQueryAdapter qry, Object queueLock, long timeoutTime,
        Collection<UUID> subgrid, BiConsumer<Collection<UUID>, Boolean> reqPages) {

        this.queueLock = queueLock;
        this.qry = qry;
        this.timeoutTime = timeoutTime;
        this.subgrid = subgrid;

        this.reqPages = reqPages;
    }

    /**
     * @return {@code true} If this stream has next row, {@code false} otherwise.
     */
    public boolean hasNext() throws IgniteCheckedException {
        return streamIterator().hasNext();
    }

    /**
     * @return Next item from this stream.
     */
    public R next() throws IgniteCheckedException {
        return streamIterator().next();
    }

    /**
     * Add new query result page of data.
     */
    boolean addPage(UUID nodeId, Collection<R> data, boolean last) {
        assert Thread.holdsLock(queueLock);

        queue.add(data);

        boolean finished = last;

        if (nodeId != null) {
            rcvd.add(nodeId);

            finished = last && subgrid.remove(nodeId) && subgrid.isEmpty();
        }

        if (finished)
            noMorePages = true;

        return finished;
    }

    /** Callback on receiving page error. */
    void onError() {
        queue.add(Collections.emptyList());

        noMorePages = true;
    }

    /**
     * Returns iterator over a query result page. Load new pages after polling a queue.
     * Wait for new page if currently there is no any.
     *
     * @return Iterator over a query result page.
     * @throws IgniteCheckedException In case of error.
     */
    private Iterator<R> streamIterator() throws IgniteCheckedException {
        Iterator<R> it = null;

        while (it == null || !it.hasNext()) {
            Collection<R> c;

            // Check current page iterator.
            synchronized (queueLock) {
                it = iter;

                // Skip page loading.
                if (it != null && it.hasNext())
                    return it;

                // Prev iterator is done. Try get new iterator over next page.
                c = queue.poll();

                if (c != null)
                    it = iter = c.iterator();

                // If all pages are ready, then skip wait for new pages.
                if (noMorePages && queue.peek() == null) {
                    if (it == null)
                        it = Collections.<R>emptyList().iterator();

                    // Skip page loading.
                    return it;
                }
            }

            // Trigger loads next pages after polling queue.
            loadPage();

            // Wait for a page after triggering page loading.
            if (c == null) {
                long timeout = qry.timeout();

                long waitTime = timeout == 0 ? Long.MAX_VALUE : timeoutTime - U.currentTimeMillis();

                if (waitTime <= 0) {
                    it = Collections.<R>emptyList().iterator();

                    return it;
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

        return it;
    }

    /**
     * @return {@code true} if all nodes responses with page and currenlty there is no requested pages.
     */
    boolean allPagesReady() {
        return rcvd.containsAll(subgrid);
    }

    /**
     * Cancel all page requests.
     *
     * @param cancelQry Callback to send cancel requests to nodes.
     */
    void cancel(Consumer<Collection<UUID>> cancelQry) {
        Collection<UUID> nodes;

        synchronized (queueLock) {
            nodes = new ArrayList<>(subgrid);

            subgrid.clear();
            rcvd.clear();
            queue.clear();
            iter = null;

            noMorePages = true;
        }

        cancelQry.accept(nodes);
    }

    /**
     * Request pages with whole data from nodes.
     */
    void loadAll() {
        Collection<UUID> nodes = null;

        synchronized (queueLock) {
            if (!subgrid.isEmpty())
                nodes = new ArrayList<>(subgrid);
        }

        if (nodes != null)
            reqPages.accept(nodes, true);
    }

    /**
     * @param nodeId Node ID to check.
     * @return {@code true} if specified node still runs query.
     */
    boolean queryNode(UUID nodeId) {
        synchronized (queueLock) {
            return subgrid.contains(nodeId);
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
            if (rcvd.containsAll(subgrid)) {
                rcvd.clear();

                nodes = new ArrayList<>(subgrid);
            }
        }

        if (nodes != null)
            reqPages.accept(nodes, false);
    }
}
