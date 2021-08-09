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
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.cache.query.CacheQueryPageRequester;
import org.apache.ignite.internal.processors.cache.query.DistributedCacheQueryReducer;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Abstract class for reducer implementations. Controls global state of pages loading and receiving.
 */
abstract class AbstractDistributedCacheQueryReducer<R> implements DistributedCacheQueryReducer<R> {
    /** Query request ID. */
    protected final long reqId;

    /** Query future. */
    protected final GridCacheQueryFutureAdapter fut;

    /** Cache query page requester. */
    protected final CacheQueryPageRequester pageRequester;

    /**
     * Whether it is allowed to send cache query result requests to nodes.
     * It is set to {@code false} if query doesn't accept initial pages from all nodes, or query is finished or failed.
     */
    private volatile boolean loadAllowed;

    /**
     * Count down this latch when every node responses on initial cache query request.
     */
    private final CountDownLatch firstPageLatch = new CountDownLatch(1);

    /** */
    protected AbstractDistributedCacheQueryReducer(
        GridCacheQueryFutureAdapter fut, long reqId, CacheQueryPageRequester pageRequester) {

        this.fut = fut;
        this.reqId = reqId;
        this.pageRequester = pageRequester;
    }

    /** {@inheritDoc} */
    @Override public void onFinish() {
        loadAllowed = false;

        firstPageLatch.countDown();
    }

    /** {@inheritDoc} */
    @Override public void awaitInitialization() throws IgniteInterruptedCheckedException {
        U.await(firstPageLatch);
    }

    /**
     * Callback that invoked when all nodes response with initial page.
     */
    protected void onFirstItemReady() {
        loadAllowed = true;

        firstPageLatch.countDown();
    }

    /**
     * Send cancel request to specified nodes.
     *
     * @param nodes Collection of nodes to cancel this query.
     */
    protected void cancel(Collection<UUID> nodes) {
        pageRequester.cancelQuery(reqId, nodes, fut.fields());
    }

    /**
     * Send request to fetch new pages.
     *
     * @param nodes Collection of nodes to send request.
     * @param all Whether page will contain all data from node.
     */
    protected void requestPages(Collection<UUID> nodes, boolean all) {
        pageRequester.requestPages(reqId, fut, nodes, all);
    }

    /** */
    protected boolean loadAllowed() {
        return loadAllowed;
    }
}
