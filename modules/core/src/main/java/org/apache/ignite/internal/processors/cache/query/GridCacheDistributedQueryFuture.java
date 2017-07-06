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
import java.util.HashSet;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.P1;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Distributed query future.
 */
public class GridCacheDistributedQueryFuture<K, V, R> extends GridCacheQueryFutureAdapter<K, V, R> {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private long reqId;

    /** */
    private final Collection<UUID> subgrid = new HashSet<>();

    /** */
    private final Collection<UUID> rcvd = new HashSet<>();

    /** */
    private CountDownLatch firstPageLatch = new CountDownLatch(1);

    /**
     * @param ctx Cache context.
     * @param reqId Request ID.
     * @param qry Query.
     * @param nodes Nodes.
     */
    @SuppressWarnings("unchecked")
    protected GridCacheDistributedQueryFuture(GridCacheContext<K, V> ctx, long reqId, GridCacheQueryBean qry,
        Iterable<ClusterNode> nodes) {
        super(ctx, qry, false);

        assert reqId > 0;

        this.reqId = reqId;

        GridCacheQueryManager<K, V> mgr = ctx.queries();

        assert mgr != null;

        synchronized (this) {
            for (ClusterNode node : nodes)
                subgrid.add(node.id());
        }
    }

    /** {@inheritDoc} */
    @Override protected void cancelQuery() throws IgniteCheckedException {
        final GridCacheQueryManager<K, V> qryMgr = cctx.queries();

        assert qryMgr != null;

        try {
            Collection<ClusterNode> allNodes = cctx.discovery().allNodes();
            Collection<ClusterNode> nodes;

            synchronized (this) {
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
                fields(),
                qryMgr.queryTopologyVersion(),
                cctx.deploymentEnabled());

            // Process cancel query directly (without sending) for local node,
            cctx.closures().callLocalSafe(new Callable<Object>() {
                @Override public Object call() throws Exception {
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

        qryMgr.onQueryFutureCanceled(reqId);

        clear();
    }

    /** {@inheritDoc} */
    @SuppressWarnings("NonPrivateFieldAccessedInSynchronizedContext")
    @Override protected void onNodeLeft(UUID nodeId) {
        boolean callOnPage;

        synchronized (this) {
            callOnPage = !loc && subgrid.contains(nodeId);
        }

        if (callOnPage)
            onPage(nodeId, Collections.emptyList(),
                new ClusterTopologyCheckedException("Remote node has left topology: " + nodeId), true);
    }

    /** {@inheritDoc} */
    @Override public void awaitFirstPage() throws IgniteCheckedException {
        try {
            firstPageLatch.await();

            if (isDone() && error() != null)
                // Throw the exception if future failed.
                get();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new IgniteInterruptedCheckedException(e);
        }
    }

    /** {@inheritDoc} */
    @Override protected boolean onPage(UUID nodeId, boolean last) {
        assert Thread.holdsLock(this);

        if (!loc) {
            rcvd.add(nodeId);

            if (rcvd.containsAll(subgrid))
                firstPageLatch.countDown();
        }

        boolean futFinish;

        if (last) {
            futFinish = loc || (subgrid.remove(nodeId) && subgrid.isEmpty());

            if (futFinish)
                firstPageLatch.countDown();
        }
        else
            futFinish = false;

        return futFinish;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("NonPrivateFieldAccessedInSynchronizedContext")
    @Override protected void loadPage() {
        assert !Thread.holdsLock(this);

        Collection<ClusterNode> nodes = null;

        synchronized (this) {
            if (!isDone() && rcvd.containsAll(subgrid)) {
                rcvd.clear();

                nodes = nodes();
            }
        }

        if (nodes != null)
            cctx.queries().loadPage(reqId, qry.query(), nodes, false);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("NonPrivateFieldAccessedInSynchronizedContext")
    @Override protected void loadAllPages() throws IgniteInterruptedCheckedException {
        assert !Thread.holdsLock(this);

        U.await(firstPageLatch);

        Collection<ClusterNode> nodes = null;

        synchronized (this) {
            if (!isDone() && !subgrid.isEmpty())
                nodes = nodes();
        }

        if (nodes != null)
            cctx.queries().loadPage(reqId, qry.query(), nodes, true);
    }

    /**
     * @return Nodes to send requests to.
     */
    private Collection<ClusterNode> nodes() {
        assert Thread.holdsLock(this);

        Collection<ClusterNode> nodes = new ArrayList<>(subgrid.size());

        for (UUID nodeId : subgrid) {
            ClusterNode node = cctx.discovery().node(nodeId);

            if (node != null)
                nodes.add(node);
        }

        return nodes;
    }

    /** {@inheritDoc} */
    @Override public boolean onDone(Collection<R> res, Throwable err) {
        boolean done = super.onDone(res, err);

        // Must release the lath after onDone() in order for a waiting thread to see an exception, if any.
        firstPageLatch.countDown();

        return done;
    }

    /** {@inheritDoc} */
    @Override public boolean onCancelled() {
        firstPageLatch.countDown();

        return super.onCancelled();
    }

    /** {@inheritDoc} */
    @Override public void onTimeout() {
        firstPageLatch.countDown();

        super.onTimeout();
    }

    /** {@inheritDoc} */
    @Override void clear() {
        assert isDone() : this;

        GridCacheDistributedQueryManager<K, V> qryMgr = (GridCacheDistributedQueryManager<K, V>)cctx.queries();

        if (qryMgr != null)
            qryMgr.removeQueryFuture(reqId);
    }
}
