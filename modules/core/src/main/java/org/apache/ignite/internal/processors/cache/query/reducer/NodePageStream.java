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
import java.util.function.BiConsumer;
import org.apache.ignite.internal.processors.cache.query.GridCacheDistributedQueryManager;

/**
 * This class provides an interface {@link #nextPage()} that returns a {@link NodePage} of cache query result from
 * single node. A new page requests when previous page was fetched by class consumer.
 */
public class NodePageStream<R> {
    /** Node ID to stream pages */
    private final UUID nodeId;

    /** Flags shows whether there are no available pages on query node. */
    private boolean noRemotePages;

    /**
     *  Promise of next query result page. It's inited here as initial request is sent independetly in
     * {@link GridCacheDistributedQueryManager}.
     */
    private CompletableFuture<NodePage<R>> pendingPage = new CompletableFuture<>();

    /** This lock syncs {@link #pendingPage} and {@link #noRemotePages}. */
    private final Object pagesLock = new Object();

    /** Request pages. */
    private final BiConsumer<UUID, Boolean> reqPages;

    /** */
    public NodePageStream(UUID nodeId, BiConsumer<UUID, Boolean> reqPages) {
        this.nodeId = nodeId;
        this.reqPages = reqPages;
    }

    /** */
    public UUID nodeId() {
        return nodeId;
    }

    /**
     * Returns a future with query result page from query node.
     *
     * @return Query result page future.
     */
    public CompletableFuture<NodePage<R>> nextPage() {
        CompletableFuture<NodePage<R>> pageFut;

        boolean loadPage;

        synchronized (pagesLock) {
            if (pendingPage == null) {
                pageFut = new CompletableFuture<>();

                pageFut.complete(new NodePage<>(nodeId, Collections.emptyList()));

                return pageFut;
            }

            if (!pendingPage.isDone())
                return pendingPage;

            pageFut = pendingPage;

            pendingPage = noRemotePages ? null : new CompletableFuture<>();

            loadPage = pendingPage != null;
        }

        if (loadPage)
            reqPages.accept(nodeId, false);

        return pageFut;
    }

    /**
     * Add new query result page of data.
     *
     * @return Whether it's a last page for this node.
     */
    public boolean addPage(UUID nodeId, Collection<R> data, boolean last) {
        synchronized (pagesLock) {
            if (pendingPage == null)
                return true;

            pendingPage.complete(new NodePage<>(nodeId, data));

            if (last)
                noRemotePages = true;

            return last;
        }
    }

    /**
     * Cancel query on all nodes.
     */
    public void cancel() {
        synchronized (pagesLock) {
            if (pendingPage == null)
                return;

            if (!pendingPage.isDone())
                pendingPage.complete(new NodePage<>(nodeId, Collections.emptyList()));

            pendingPage = null;

            noRemotePages = true;
        }
    }

    /** */
    public boolean hasRemotePages() {
        synchronized (pagesLock) {
            return !noRemotePages;
        }
    }

    /** */
    public boolean closed() {
        synchronized (pagesLock) {
            return pendingPage == null;
        }
    }

}
