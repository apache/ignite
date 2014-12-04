/* @java.file.header */

/*  _________        _____ __________________        _____
*  __  ____/___________(_)______  /__  ____/______ ____(_)_______
*  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
*  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
*  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
*/

package org.gridgain.grid.kernal.processors.cache.query;

import org.gridgain.grid.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

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
     * Required by {@link Externalizable}.
     */
    public GridCacheDistributedQueryFuture() {
        // No-op.
    }

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

        synchronized (mux) {
            for (ClusterNode node : nodes)
                subgrid.add(node.id());
        }
    }

    /** {@inheritDoc} */
    @Override protected void cancelQuery() throws GridException {
        final GridCacheQueryManager<K, V> qryMgr = cctx.queries();

        assert qryMgr != null;

        try {
            Collection<ClusterNode> allNodes = cctx.discovery().allNodes();
            Collection<ClusterNode> nodes;

            synchronized (mux) {
                nodes = F.retain(allNodes, true,
                    new P1<ClusterNode>() {
                        @Override public boolean apply(ClusterNode node) {
                            return !cctx.localNodeId().equals(node.id()) && subgrid.contains(node.id());
                        }
                    }
                );

                subgrid.clear();
            }

            final GridCacheQueryRequest<K, V> req = new GridCacheQueryRequest<>(cctx.cacheId(), reqId, fields());

            // Process cancel query directly (without sending) for local node,
            cctx.closures().callLocalSafe(new Callable<Object>() {
                @Override public Object call() throws Exception {
                    qryMgr.processQueryRequest(cctx.localNodeId(), req);

                    return null;
                }
            });

            if (!nodes.isEmpty()) {
                cctx.io().safeSend(nodes, req,
                    new P1<ClusterNode>() {
                        @Override public boolean apply(ClusterNode node) {
                            onNodeLeft(node.id());

                            return !isDone();
                        }
                    });
            }
        }
        catch (GridException e) {
            U.error(log, "Failed to send cancel request (will cancel query in any case).", e);
        }

        qryMgr.onQueryFutureCanceled(reqId);

        clear();
    }

    /** {@inheritDoc} */
    @SuppressWarnings("NonPrivateFieldAccessedInSynchronizedContext")
    @Override protected void onNodeLeft(UUID nodeId) {
        boolean callOnPage;

        synchronized (mux) {
            callOnPage = !loc && subgrid.contains(nodeId);
        }

        if (callOnPage)
            // We consider node departure as a reception of last empty
            // page from this node.
            onPage(nodeId, Collections.emptyList(), null, true);
    }

    /** {@inheritDoc} */
    @Override protected boolean onPage(UUID nodeId, boolean last) {
        assert Thread.holdsLock(mux);

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
        assert !Thread.holdsLock(mux);

        Collection<ClusterNode> nodes = null;

        synchronized (mux) {
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
    @Override protected void loadAllPages() throws GridInterruptedException {
        assert !Thread.holdsLock(mux);

        U.await(firstPageLatch);

        Collection<ClusterNode> nodes = null;

        synchronized (mux) {
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
        assert Thread.holdsLock(mux);

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
        firstPageLatch.countDown();

        return super.onDone(res, err);
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
        GridCacheDistributedQueryManager<K, V> qryMgr = (GridCacheDistributedQueryManager<K, V>)cctx.queries();

        assert qryMgr != null;

        qryMgr.removeQueryFuture(reqId);
    }
}
