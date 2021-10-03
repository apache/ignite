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
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * This class provides an interface {@link #nextPage()} that returns a {@link NodePage} of cache query result from
 * single node. A new page requests when previous page was fetched by class consumer.
 */
public class NodePageStream<R> {
    /** Node ID to stream pages */
    private final UUID nodeId;

    /** Flags shows whether there are no available pages on a query node. */
    private boolean noRemotePages;

    /** Last delivered page from the stream. */
    private NodePage<R> head;

    /** Promise to notify the stream consumer about delivering new page. */
    private CompletableFuture<UUID> pageReady = new CompletableFuture<>();

    /** This lock syncs {@link #head} and {@link #noRemotePages}. */
    private final Object pagesLock = new Object();

    /** Request pages action. */
    private final Runnable reqPages;

    /** */
    public NodePageStream(UUID nodeId, Runnable reqPages) {
        this.nodeId = nodeId;
        this.reqPages = reqPages;
    }

    /** */
    public UUID nodeId() {
        return nodeId;
    }

    /**
     * @return Future that will be completed when a new page delivered.
     */
    public CompletableFuture<UUID> pageReady() {
        return pageReady;
    }

    /**
     * Returns a last delivered page from this stream. Note, that this method has to be invoked after getting
     * the future provided with {@link #pageReady()}.
     *
     * @return Query result page.
     */
    public NodePage<R> nextPage() {
        boolean loadPage = false;

        NodePage<R> page;

        synchronized (pagesLock) {
            assert head != null;

            page = head;

            head = null;

            if (!noRemotePages)
                loadPage = true;
        }

        if (loadPage) {
            pageReady = new CompletableFuture<>();

            reqPages.run();
        }

        return page;
    }

    /**
     * Add new query result page of data.
     *
     * @param data Collection of query result items.
     * @param last Whether it is the last page from this node.
     */
    public void addPage(Collection<R> data, boolean last) {
        synchronized (pagesLock) {
            head = new NodePage<>(nodeId, data);

            if (last)
                noRemotePages = true;
        }

        pageReady.complete(nodeId);
    }

    /**
     * Cancel query on all nodes.
     */
    public void cancel() {
        synchronized (pagesLock) {
            head = new NodePage<>(nodeId, Collections.emptyList());

            noRemotePages = true;
        }

        pageReady.complete(nodeId);
    }

    /**
     * @return {@code true} if there are some undelivered page from the node, otherwise {@code false}.
     */
    public boolean hasRemotePages() {
        synchronized (pagesLock) {
            return !noRemotePages;
        }
    }

    /**
     * @return {@code true} if this stream delivers all query results from the node to a consumer.
     */
    public boolean closed() {
        synchronized (pagesLock) {
            return noRemotePages && (head == null);
        }
    }
}
