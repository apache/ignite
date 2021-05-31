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
import java.util.HashSet;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.P1;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

/**
 * Reducer of distributed query, fetch pages from remote nodes. All pages go in single page stream so no ordering is provided.
 */
class UnsortedDistributedCacheQueryReducer<R> extends AbstractCacheQueryReducer<R> implements DistributedCacheQueryReducer<R> {
    /**
     * Whether it is allowed to send cache query result requests to nodes.
     * It is set to {@code false} if a query finished or failed.
     */
    protected volatile boolean loadAllowed;

    /** Query request ID. */
    protected final long reqId;

    /**
     * Dynamic collection of nodes that run this query. If a node finishes this query then remove it from this colleciton.
     */
    protected final Collection<UUID> subgrid = new HashSet<>();

    /**
     * List of nodes that respons with cache query result pages. This collection should be cleaned before sending new
     * cache query request.
     */
    protected final Collection<UUID> rcvd = new HashSet<>();

    /** Requester of cache query result pages. */
    protected final CacheQueryPageRequester pageRequester;

    /** Cache context. */
    protected final GridCacheContext cctx;

    /** Count down this latch when every node responses on initial cache query request. */
    private final CountDownLatch firstPageLatch = new CountDownLatch(1);

    /** Single page stream. */
    private final PageStream pageStream;

    /** Query future. */
    protected final GridCacheQueryFutureAdapter fut;

    /**
     * @param reqId Cache query request ID.
     * @param pageRequester Provides a functionality to request pages from remote nodes.
     * @param nodes Collection of nodes this query applies to.
     */
    UnsortedDistributedCacheQueryReducer(GridCacheQueryFutureAdapter fut, long reqId, CacheQueryPageRequester pageRequester,
        Collection<ClusterNode> nodes) {
        super(fut);

        this.reqId = reqId;
        this.pageRequester = pageRequester;

        synchronized (queueLock()) {
            for (ClusterNode node : nodes)
                subgrid.add(node.id());
        }

        cctx = fut.cctx;

        pageStream = new PageStream();

        this.fut = fut;
    }

    /** {@inheritDoc} */
    @Override public boolean hasNext() throws IgniteCheckedException {
        return pageStream.hasNext();
    }

    /** {@inheritDoc} */
    @Override public R next() throws IgniteCheckedException {
        return pageStream.next();
    }

    /** {@inheritDoc} */
    @Override public void addPage(@Nullable UUID nodeId, Collection<R> data) {
        pageStream.addPage(data);
    }

    /** {@inheritDoc} */
    @Override public void onLastPage() {
        super.onLastPage();

        loadAllowed = false;

        firstPageLatch.countDown();
    }

    /** {@inheritDoc} */
    @Override public void cancel() {
        Collection<ClusterNode> allNodes = cctx.discovery().allNodes();
        Collection<ClusterNode> nodes;

        synchronized (queueLock()) {
            nodes = F.retain(allNodes, true,
                new P1<ClusterNode>() {
                    @Override public boolean apply(ClusterNode node) {
                        return !cctx.localNodeId().equals(node.id()) && subgrid.contains(node.id());
                    }
                }
            );

            rcvd.clear();
            subgrid.clear();
        }

        pageRequester.cancelQueryRequest(reqId, nodes, fut.fields());

        pageStream.clear();
    }

    /** {@inheritDoc} */
    @Override protected void loadPage() {
        assert !Thread.holdsLock(queueLock());

        Collection<UUID> nodes = null;

        synchronized (queueLock()) {
            // Loads only queue is empty to avoid memory consumption on additional pages.
            if (!pageStream.queue.isEmpty())
                return;

            if (loadAllowed && rcvd.containsAll(subgrid)) {
                rcvd.clear();

                nodes = new ArrayList<>(subgrid);
            }
        }

        if (nodes != null)
            pageRequester.requestPages(reqId, fut, nodes, false);
    }

    /** {@inheritDoc} */
    @Override public void loadAll() throws IgniteInterruptedCheckedException {
        assert !Thread.holdsLock(queueLock());

        U.await(firstPageLatch);

        Collection<UUID> nodes = null;

        synchronized (queueLock()) {
            if (loadAllowed && !subgrid.isEmpty())
                nodes = new ArrayList<>(subgrid);
        }

        if (nodes != null)
            pageRequester.requestPages(reqId, fut, nodes, true);
    }

    /** {@inheritDoc} */
    @Override public boolean onPage(@Nullable UUID nodeId, boolean last) {
        assert Thread.holdsLock(queueLock());

        if (nodeId == null)
            nodeId = cctx.localNodeId();

        rcvd.add(nodeId);

        if (!loadAllowed && rcvd.containsAll(subgrid) ) {
            firstPageLatch.countDown();
            loadAllowed = true;
        }

        return last && subgrid.remove(nodeId) && subgrid.isEmpty();
    }

    /** {@inheritDoc} */
    @Override public boolean onNodeLeft(UUID nodeId) {
        synchronized (queueLock()) {
            return subgrid.contains(nodeId);
        }
    }

    /** {@inheritDoc} */
    @Override public void awaitFirstItem() throws InterruptedException {
        firstPageLatch.await();
    }
}
