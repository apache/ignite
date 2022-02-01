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
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * This class provides an interface {@link #headPage()} that returns a future will be completed with {@link NodePage}
 * of cache query result from single node. A new page requests when previous page was fetched by class consumer.
 */
public class NodePageStream<R> {
    /** Node ID to stream pages. */
    private final UUID nodeId;

    /** Request pages action. */
    private final Runnable reqPages;

    /** Cancel remote pages action. */
    private final Runnable cancelPages;

    /** Flags shows whether there are available pages on a query node. */
    private boolean hasRemotePages = true;

    /** Promise to notify the stream consumer about delivering new page. */
    private CompletableFuture<NodePage<R>> head = new CompletableFuture<>();

    /** */
    public NodePageStream(UUID nodeId, Runnable reqPages, Runnable cancelPages) {
        this.nodeId = nodeId;
        this.reqPages = reqPages;
        this.cancelPages = cancelPages;
    }

    /** */
    public UUID nodeId() {
        return nodeId;
    }

    /**
     * Returns a last delivered page from this stream.
     *
     * @return Future that will be completed with query result page.
     */
    public synchronized CompletableFuture<NodePage<R>> headPage() {
        return head;
    }

    /**
     * Add new query result page of data.
     *
     * @param data Collection of query result items.
     * @param last Whether it is the last page from this node.
     */
    public synchronized void addPage(Collection<R> data, boolean last) {
        head.complete(new NodePage<R>(nodeId, data) {
            /** Flag shows whether the request for new page was triggered. */
            private boolean reqNext;

            /** {@inheritDoc} */
            @Override public boolean hasNext() {
                if (!reqNext) {
                    synchronized (NodePageStream.this) {
                        if (hasRemotePages) {
                            head = new CompletableFuture<>();

                            reqPages.run();
                        }
                        else
                            head = null;
                    }

                    reqNext = true;
                }

                return super.hasNext();
            }
        });

        if (last)
            hasRemotePages = false;
    }

    /**
     * Cancel query on all nodes.
     */
    public synchronized void cancel(Throwable err) {
        if (closed())
            return;

        head.completeExceptionally(err);

        cancelPages.run();

        hasRemotePages = false;
    }

    /**
     * @return {@code true} if there are some undelivered page from the node, otherwise {@code false}.
     */
    public synchronized boolean hasRemotePages() {
        return hasRemotePages;
    }

    /**
     * @return {@code true} if this stream delivers all query results from the node to a consumer.
     */
    public synchronized boolean closed() {
        return !hasRemotePages && (head == null);
    }
}
