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
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.util.lang.GridPlainCallable;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.P1;
import org.apache.ignite.internal.util.typedef.internal.U;

/** Reducer of distributed query, fetch pages from remote nodes. */
class UnorderedDistributedReducer<R> extends UnorderedReducer<R> implements DistributedReducer<R> {
    /**
     * Whether it is allowed to send cache query result requests to nodes.
     * It is set to {@code false} if a query finished or failed.
     */
    private volatile boolean loadAllowed;

    /** Query request ID. */
    private final long reqId;

    /**
     * Dynamic collection of nodes that run this query. If a node finishes run this query then remove it from this colleciton.
     */
    private final Collection<UUID> subgrid = new HashSet<>();

    /**
     * List of nodes that send cache query result pages.
     *
     * The query node send a single request to every remote node. Fills this collection by receiving data.
     * When all nodes responsed, clean this colleciton. And fill it again after next request.
     */
    private final Collection<UUID> rcvd = new HashSet<>();

    /** */
    private final CacheQueryResultFetcher fetcher;

    /** */
    private final GridCacheContext cctx;

    /** */
    private final IgniteLogger log;

    /** Count down this latch when every node responses on initial cache query request. */
    private final CountDownLatch firstPageLatch = new CountDownLatch(1);

    /** Whether it is fields query or not. */
    private final boolean fields;

    /** */
    UnorderedDistributedReducer(GridCacheQueryFutureAdapter fut, long reqId, CacheQueryResultFetcher fetcher,
        Collection<ClusterNode> nodes) {
        super(fut);

        this.fields = fut.fields();
        this.reqId = reqId;
        this.fetcher = fetcher;

        synchronized (sharedLock()) {
            for (ClusterNode node : nodes)
                subgrid.add(node.id());
        }

        cctx = fut.cctx;

        log = cctx.kernalContext().config().getGridLogger();
    }

    /** {@inheritDoc} */
    @Override public void onLastPage() {
        super.onLastPage();

        loadAllowed = false;

        firstPageLatch.countDown();
    }

    /** {@inheritDoc} */
    @Override public void cancel() {
        final GridCacheQueryManager qryMgr = cctx.queries();

        assert qryMgr != null;

        try {
            Collection<ClusterNode> allNodes = cctx.discovery().allNodes();
            Collection<ClusterNode> nodes;

            synchronized (sharedLock()) {
                nodes = F.retain(allNodes, true,
                    new P1<ClusterNode>() {
                        @Override public boolean apply(ClusterNode node) {
                            return !cctx.localNodeId().equals(node.id()) && subgrid.contains(node.id());
                        }
                    }
                );

                subgrid.clear();
            }

            final GridCacheQueryRequest req = new GridCacheQueryRequest(cctx.cacheId(),
                reqId,
                fields,
                cctx.startTopologyVersion(),
                cctx.deploymentEnabled());

            // Process cancel query directly (without sending) for local node,
            cctx.closures().callLocalSafe(new GridPlainCallable<Object>() {
                @Override public Object call() {
                    qryMgr.processQueryRequest(cctx.localNodeId(), req);

                    return null;
                }
            });

            if (!nodes.isEmpty()) {
                for (ClusterNode node : nodes) {
                    try {
                        cctx.io().send(node, req, cctx.ioPolicy());
                    }
                    catch (IgniteCheckedException e) {
                        if (cctx.io().checkNodeLeft(node.id(), e, false)) {
                            if (log.isDebugEnabled())
                                log.debug("Failed to send cancel request, node failed: " + node);
                        }
                        else
                            U.error(log, "Failed to send cancel request [node=" + node + ']', e);
                    }
                }
            }
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to send cancel request (will cancel query in any case).", e);
        }
    }

    /** {@inheritDoc} */
    @Override protected void loadPage() {
        assert !Thread.holdsLock(sharedLock());

        Collection<UUID> nodes = null;

        synchronized (sharedLock()) {
            if (loadAllowed && rcvd.containsAll(subgrid)) {
                rcvd.clear();

                nodes = new ArrayList<>(subgrid);
            }
        }

        if (nodes != null)
            fetcher.fetchPages(reqId, nodes, false);
    }

    /** {@inheritDoc} */
    @Override public void loadAll() throws IgniteInterruptedCheckedException {
        assert !Thread.holdsLock(sharedLock());

        U.await(firstPageLatch);

        Collection<UUID> nodes = null;

        synchronized (sharedLock()) {
            if (loadAllowed && !subgrid.isEmpty())
                nodes = new ArrayList<>(subgrid);
        }

        if (nodes != null)
            fetcher.fetchPages(reqId, nodes, true);
    }

    /** {@inheritDoc} */
    @Override public boolean onPage(UUID nodeId, boolean last) {
        assert Thread.holdsLock(sharedLock());

        rcvd.add(nodeId);

        if (rcvd.containsAll(subgrid) && !loadAllowed) {
            firstPageLatch.countDown();
            loadAllowed = true;
        }

        return last && subgrid.remove(nodeId) && subgrid.isEmpty();
    }

    /** {@inheritDoc} */
    @Override public boolean onNodeLeft(UUID nodeId) {
        synchronized (sharedLock()) {
            return subgrid.contains(nodeId);
        }
    }

    /** {@inheritDoc} */
    @Override public void awaitFirstItem() throws InterruptedException {
        firstPageLatch.await();
    }
}
