/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.distributed.near;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheCompoundIdentityFuture;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.GridCacheMvccCandidate;
import org.apache.ignite.internal.processors.cache.GridCacheVersionedFuture;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedTxMapping;
import org.apache.ignite.internal.processors.cache.mvcc.MvccSnapshot;
import org.apache.ignite.internal.processors.cache.mvcc.MvccSnapshotResponseListener;
import org.apache.ignite.internal.processors.cache.mvcc.MvccTxInfo;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.timeout.GridTimeoutObjectAdapter;
import org.apache.ignite.internal.transactions.IgniteTxTimeoutCheckedException;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

/**
 * A future tracking requests for remote nodes transaction enlisting and locking
 * of entries produced with complex DML queries requiring reduce step.
 */
public class GridNearTxSelectForUpdateFuture extends GridCacheCompoundIdentityFuture<Long>
    implements GridCacheVersionedFuture<Long>, MvccSnapshotResponseListener {
    /** */
    private static final long serialVersionUID = 6931664882548658420L;

    /** Done field updater. */
    private static final AtomicIntegerFieldUpdater<GridNearTxSelectForUpdateFuture> DONE_UPD =
        AtomicIntegerFieldUpdater.newUpdater(GridNearTxSelectForUpdateFuture.class, "done");

    /** */
    @SuppressWarnings("unused")
    @GridToStringExclude
    private volatile int done;

    /** Cache context. */
    @GridToStringExclude
    private final GridCacheContext<?, ?> cctx;

    /** Transaction. */
    private final GridNearTxLocal tx;

    /** Mvcc future id. */
    private final IgniteUuid futId;

    /** Lock version. */
    private final GridCacheVersion lockVer;

    /** */
    private AffinityTopologyVersion topVer;

    /** */
    private final long timeout;

    /** Logger. */
    @GridToStringExclude
    private final IgniteLogger log;

    /** Timeout object. */
    @GridToStringExclude
    private LockTimeoutObject timeoutObj;

    /** Ids of mini futures. */
    private final Map<UUID, Integer> miniFutIds = new HashMap<>();

    /**
     * @param cctx Cache context.
     * @param tx Transaction.
     * @param timeout Timeout.
     */
    public GridNearTxSelectForUpdateFuture(GridCacheContext<?, ?> cctx,
        GridNearTxLocal tx,
        long timeout) {
        super(CU.longReducer());

        this.cctx = cctx;
        this.tx = tx;
        this.timeout = timeout;

        futId = IgniteUuid.randomUuid();
        lockVer = tx.xidVersion();

        log = cctx.logger(GridNearTxSelectForUpdateFuture.class);
    }

    /**
     * @return Cache context.
     */
    public GridCacheContext<?, ?> cache() {
        return cctx;
    }

    /**
     * @param node Node.
     */
    private void map(ClusterNode node) {
        GridDistributedTxMapping mapping = tx.mappings().get(node.id());

        if (mapping == null)
            tx.mappings().put(mapping = new GridDistributedTxMapping(node));

        mapping.markQueryUpdate();

        int futId = futuresCountNoLock();

        miniFutIds.put(node.id(), futId);

        add(new NodeFuture(node));
    }

    /**
     * Process result of query execution on given
     * @param nodeId Node id.
     * @param cnt Total rows counter on given node.
     * @param removeMapping Whether transaction mapping should be removed for node.
     * @param err Error.
     */
    public void onResult(UUID nodeId, Long cnt, boolean removeMapping, @Nullable Throwable err) {
        NodeFuture nodeFut = mapFuture(nodeId);

        if (nodeFut != null)
            nodeFut.onResult(cnt, removeMapping, err);
    }

    /** {@inheritDoc} */
    @Override public boolean onDone(@Nullable Long res, @Nullable Throwable err) {
        if (!DONE_UPD.compareAndSet(this, 0, 1))
            return false;

        cctx.tm().txContext(tx);

        if (err != null)
            tx.setRollbackOnly();

        if (!X.hasCause(err, IgniteTxTimeoutCheckedException.class) && tx.trackTimeout()) {
            // Need restore timeout before onDone is called and next tx operation can proceed.
            boolean add = tx.addTimeoutHandler();

            assert add;
        }

        if (super.onDone(res, err)) {
            // Clean up.
            cctx.mvcc().removeVersionedFuture(this);

            if (timeoutObj != null)
                cctx.time().removeTimeoutObject(timeoutObj);

            return true;
        }

        return false;
    }

    /**
     * Finds pending map node future by the given ID.
     *
     * @param nodeId Node id.
     * @return Map node future.
     */
    private NodeFuture mapFuture(UUID nodeId) {
        synchronized (this) {
            Integer idx = miniFutIds.get(nodeId);

            if (idx == null)
                throw new IllegalStateException("SELECT FOR UPDATE node future not found [nodeId=" + nodeId + "].");

            assert idx >= 0 && idx < futuresCountNoLock();

            IgniteInternalFuture<Long> fut = future(idx);

            if (!fut.isDone())
                return (NodeFuture)fut;
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override public GridCacheVersion version() {
        return lockVer;
    }

    /** {@inheritDoc} */
    @Override public boolean onOwnerChanged(GridCacheEntryEx entry, GridCacheMvccCandidate owner) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public IgniteUuid futureId() {
        return futId;
    }

    /** {@inheritDoc} */
    @Override public boolean onNodeLeft(UUID nodeId) {
        if (topVer == null)
            return false; // Local query, do nothing.

        for (IgniteInternalFuture<?> fut : futures()) {
            NodeFuture f = (NodeFuture)fut;

            if (f.node.id().equals(nodeId)) {
                if (log.isDebugEnabled())
                    log.debug("Found mini-future for left node [nodeId=" + nodeId + ", mini=" + f + ", fut=" +
                        this + ']');

                ClusterTopologyCheckedException topEx = new ClusterTopologyCheckedException("Failed to enlist keys " +
                    "(primary node left grid, retry transaction if possible) [node=" + nodeId + ']');

                topEx.retryReadyFuture(cctx.shared().nextAffinityReadyFuture(topVer));

                return f.onResult(0, false, topEx);
            }
        }

        if (log.isDebugEnabled())
            log.debug("Future does not have mapping for left node (ignoring) [nodeId=" + nodeId +
                ", fut=" + this + ']');

        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean trackable() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public void markNotTrackable() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onResponse(UUID crdId, MvccSnapshot res) {
        if (tx != null)
            tx.mvccInfo(new MvccTxInfo(crdId, res));
    }

    /** {@inheritDoc} */
    @Override public void onError(IgniteCheckedException e) {
        onDone(e);
    }

    /** {@inheritDoc} */
    @Override protected void logError(IgniteLogger log, String msg, Throwable e) {
        // no-op
    }

    /** {@inheritDoc} */
    @Override protected void logDebug(IgniteLogger log, String msg) {
        // no-op
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridNearTxSelectForUpdateFuture.class, this, super.toString());
    }

    /**
     * Initialize this future for distributed execution.
     * @param topVer Topology version.
     * @param nodes Nodes to run query on.
     */
    public synchronized void init(AffinityTopologyVersion topVer, Collection<ClusterNode> nodes) {
        doInit(topVer, nodes, false);
    }

    /**
     * Initialize this future for local execution.
     */
    public synchronized void initLocal() {
        doInit(null, Collections.singletonList(cctx.localNode()), true);
    }

    /**
     * Initialize this future for distributed or local execution.
     * @param topVer Topology version ({@code null} for local case).
     * @param nodes Nodes to run query on.
     * @param loc Local query flag.
     */
    private void doInit(@Nullable AffinityTopologyVersion topVer, Collection<ClusterNode> nodes, boolean loc) {
        assert !loc || (topVer == null && nodes.size() == 1 && nodes.iterator().next().isLocal());

        tx.init();

        if (initialized())
            throw new IllegalStateException("SELECT FOR UPDATE future has been initialized already.");

        if (tx.trackTimeout()) {
            if (!tx.removeTimeoutHandler()) {
                tx.finishFuture().listen(new IgniteInClosure<IgniteInternalFuture<IgniteInternalTx>>() {
                    @Override public void apply(IgniteInternalFuture<IgniteInternalTx> fut) {
                        IgniteTxTimeoutCheckedException err = new IgniteTxTimeoutCheckedException("Failed to " +
                            "acquire lock, transaction was rolled back on timeout [timeout=" + tx.timeout() +
                            ", tx=" + tx + ']');

                        onDone(err);
                    }
                });

                return;
            }
        }

        if (timeout > 0) {
            timeoutObj = new GridNearTxSelectForUpdateFuture.LockTimeoutObject();

            cctx.time().addTimeoutObject(timeoutObj);
        }

        boolean added = cctx.mvcc().addFuture(this);

        assert added : this;

        this.topVer = topVer;

        for (ClusterNode n : nodes)
            map(n);

        markInitialized();
    }

    /**
     * A future tracking a single MAP request to be enlisted in transaction and locked on data node.
     */
    private class NodeFuture extends GridFutureAdapter<Long> {
        /** */
        private boolean completed;

        /** Node ID. */
        @GridToStringExclude
        private final ClusterNode node;

        /**
         * @param node Cluster node.
         *
         */
        private NodeFuture(ClusterNode node) {
            this.node = node;
        }

        /**
         * @return Node.
         */
        public ClusterNode node() {
            return node;
        }

        /**
         * @param cnt Total rows counter on given node.
         * @param removeMapping Whether transaction mapping should be removed for node.
         * @param err Exception.
         * @return {@code True} if future was completed by this call.
         */
        public boolean onResult(long cnt, boolean removeMapping, Throwable err) {
            synchronized (this) {
                if (completed)
                    return false;

                completed = true;
            }

            if (X.hasCause(err, ClusterTopologyCheckedException.class) || removeMapping) {
                assert tx.mappings().get(node.id()).empty();

                tx.removeMapping(node.id());
            }
            else if (err == null && cnt > 0) {
                if (node.isLocal())
                    tx.colocatedLocallyMapped(true);
                else
                    tx.hasRemoteLocks(true);
            }

            return onDone(cnt, err);
        }
    }

    /**
     * Lock request timeout object.
     */
    private class LockTimeoutObject extends GridTimeoutObjectAdapter {
        /**
         * Default constructor.
         */
        LockTimeoutObject() {
            super(timeout);
        }

        /** {@inheritDoc} */
        @Override public void onTimeout() {
            if (log.isDebugEnabled())
                log.debug("Timed out waiting for lock response: " + this);

            onDone(new IgniteTxTimeoutCheckedException("Failed to acquire lock within provided timeout for " +
                "transaction [timeout=" + tx.timeout() + ", tx=" + tx + ']'));
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(LockTimeoutObject.class, this);
        }
    }
}
