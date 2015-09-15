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

package org.apache.ignite.internal.processors.cache.distributed.dht;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.processors.cache.GridCacheFuture;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedTxMapping;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.transactions.IgniteTxHeuristicCheckedException;
import org.apache.ignite.internal.transactions.IgniteTxRollbackCheckedException;
import org.apache.ignite.internal.util.future.GridCompoundIdentityFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.C1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.transactions.TransactionState.COMMITTING;

/**
 *
 */
public final class GridDhtTxFinishFuture<K, V> extends GridCompoundIdentityFuture<IgniteInternalTx>
    implements GridCacheFuture<IgniteInternalTx> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Logger reference. */
    private static final AtomicReference<IgniteLogger> logRef = new AtomicReference<>();

    /** Logger. */
    private static IgniteLogger log;

    /** Context. */
    private GridCacheSharedContext<K, V> cctx;

    /** Future ID. */
    private IgniteUuid futId;

    /** Transaction. */
    @GridToStringExclude
    private GridDhtTxLocalAdapter tx;

    /** Commit flag. */
    private boolean commit;

    /** Error. */
    @GridToStringExclude
    private AtomicReference<Throwable> err = new AtomicReference<>(null);

    /** DHT mappings. */
    private Map<UUID, GridDistributedTxMapping> dhtMap;

    /** Near mappings. */
    private Map<UUID, GridDistributedTxMapping> nearMap;

    /** Trackable flag. */
    private boolean trackable = true;

    /**
     * @param cctx Context.
     * @param tx Transaction.
     * @param commit Commit flag.
     */
    public GridDhtTxFinishFuture(GridCacheSharedContext<K, V> cctx, GridDhtTxLocalAdapter tx, boolean commit) {
        super(cctx.kernalContext(), F.<IgniteInternalTx>identityReducer(tx));

        this.cctx = cctx;
        this.tx = tx;
        this.commit = commit;

        dhtMap = tx.dhtMap();
        nearMap = tx.nearMap();

        futId = IgniteUuid.randomUuid();

        if (log == null)
            log = U.logger(cctx.kernalContext(), logRef, GridDhtTxFinishFuture.class);
    }

    /** {@inheritDoc} */
    @Override public IgniteUuid futureId() {
        return futId;
    }

    /** {@inheritDoc} */
    @Override public GridCacheVersion version() {
        return tx.xidVersion();
    }

    /**
     * @return Involved nodes.
     */
    @Override public Collection<? extends ClusterNode> nodes() {
        return
            F.viewReadOnly(futures(), new IgniteClosure<IgniteInternalFuture<?>, ClusterNode>() {
                @Nullable @Override public ClusterNode apply(IgniteInternalFuture<?> f) {
                    if (isMini(f))
                        return ((MiniFuture)f).node();

                    return cctx.discovery().localNode();
                }
            });
    }

    /** {@inheritDoc} */
    @Override public boolean onNodeLeft(UUID nodeId) {
        for (IgniteInternalFuture<?> fut : futures())
            if (isMini(fut)) {
                MiniFuture f = (MiniFuture)fut;

                if (f.node().id().equals(nodeId)) {
                    f.onResult(new ClusterTopologyCheckedException("Remote node left grid (will retry): " + nodeId));

                    return true;
                }
            }

        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean trackable() {
        return trackable;
    }

    /** {@inheritDoc} */
    @Override public void markNotTrackable() {
        trackable = false;
    }

    /**
     * @param e Error.
     */
    public void onError(Throwable e) {
        if (err.compareAndSet(null, e)) {
            boolean marked = tx.setRollbackOnly();

            if (e instanceof IgniteTxRollbackCheckedException) {
                if (marked) {
                    try {
                        tx.rollback();
                    }
                    catch (IgniteCheckedException ex) {
                        U.error(log, "Failed to automatically rollback transaction: " + tx, ex);
                    }
                }
            }
            else if (tx.isSystemInvalidate()) { // Invalidate remote transactions on heuristic error.
                finish();

                try {
                    get();
                }
                catch (IgniteTxHeuristicCheckedException ignore) {
                    // Future should complete with GridCacheTxHeuristicException.
                }
                catch (IgniteCheckedException err) {
                    U.error(log, "Failed to invalidate transaction: " + tx, err);
                }
            }

            onComplete();
        }
    }

    /**
     * @param nodeId Sender.
     * @param res Result.
     */
    public void onResult(UUID nodeId, GridDhtTxFinishResponse res) {
        if (!isDone()) {
            for (IgniteInternalFuture<IgniteInternalTx> fut : futures()) {
                if (isMini(fut)) {
                    MiniFuture f = (MiniFuture)fut;

                    if (f.futureId().equals(res.miniId())) {
                        assert f.node().id().equals(nodeId);

                        f.onResult(res);
                    }
                }
            }
        }
    }

    /** {@inheritDoc} */
    @Override public boolean onDone(IgniteInternalTx tx, Throwable err) {
        if (initialized() || err != null) {
            if (this.tx.onePhaseCommit() && (this.tx.state() == COMMITTING))
                this.tx.tmFinish(err == null);

            Throwable e = this.err.get();

            if (super.onDone(tx, e != null ? e : err)) {
                // Always send finish reply.
                this.tx.sendFinishReply(commit, error());

                // Don't forget to clean up.
                cctx.mvcc().removeFuture(this);

                return true;
            }
        }

        return false;
    }

    /**
     * @param f Future.
     * @return {@code True} if mini-future.
     */
    private boolean isMini(IgniteInternalFuture<?> f) {
        return f.getClass().equals(MiniFuture.class);
    }

    /**
     * Completeness callback.
     */
    private void onComplete() {
        onDone(tx, err.get());
    }

    /**
     * Initializes future.
     */
    @SuppressWarnings({"SimplifiableIfStatement", "IfMayBeConditional"})
    public void finish() {
        boolean sync;

        if (!F.isEmpty(dhtMap) || !F.isEmpty(nearMap))
            sync = finish(dhtMap, nearMap);
        else if (!commit && !F.isEmpty(tx.lockTransactionNodes()))
            sync = rollbackLockTransactions(tx.lockTransactionNodes());
        else
            // No backup or near nodes to send commit message to (just complete then).
            sync = false;

        markInitialized();

        if (!sync)
            onComplete();
    }

    /**
     * @param nodes Nodes.
     * @return {@code True} in case there is at least one synchronous {@code MiniFuture} to wait for.
     */
    private boolean rollbackLockTransactions(Collection<ClusterNode> nodes) {
        assert !commit;
        assert !F.isEmpty(nodes);

        if (tx.onePhaseCommit())
            return false;

        boolean sync = commit ? tx.syncCommit() : tx.syncRollback();

        if (tx.explicitLock())
            sync = true;

        boolean res = false;

        for (ClusterNode n : nodes) {
            assert !n.isLocal();

            MiniFuture fut = new MiniFuture(n);

            add(fut); // Append new future.

            GridDhtTxFinishRequest req = new GridDhtTxFinishRequest(
                tx.nearNodeId(),
                futId,
                fut.futureId(),
                tx.topologyVersion(),
                tx.xidVersion(),
                tx.commitVersion(),
                tx.threadId(),
                tx.isolation(),
                commit,
                tx.isInvalidate(),
                tx.system(),
                tx.ioPolicy(),
                tx.isSystemInvalidate(),
                sync,
                sync,
                tx.completedBase(),
                tx.committedVersions(),
                tx.rolledbackVersions(),
                tx.pendingVersions(),
                tx.size(),
                tx.subjectId(),
                tx.taskNameHash());

            try {
                cctx.io().send(n, req, tx.ioPolicy());

                if (sync)
                    res = true;
                else
                    fut.onDone();
            }
            catch (IgniteCheckedException e) {
                // Fail the whole thing.
                if (e instanceof ClusterTopologyCheckedException)
                    fut.onResult((ClusterTopologyCheckedException)e);
                else
                    fut.onResult(e);
            }
        }

        return res;
    }

    /**
     * @param dhtMap DHT map.
     * @param nearMap Near map.
     * @return {@code True} in case there is at least one synchronous {@code MiniFuture} to wait for.
     */
    private boolean finish(Map<UUID, GridDistributedTxMapping> dhtMap,
        Map<UUID, GridDistributedTxMapping> nearMap) {
        if (tx.onePhaseCommit())
            return false;

        boolean sync = commit ? tx.syncCommit() : tx.syncRollback();

        if (tx.explicitLock())
            sync = true;

        boolean res = false;

        // Create mini futures.
        for (GridDistributedTxMapping dhtMapping : dhtMap.values()) {
            ClusterNode n = dhtMapping.node();

            assert !n.isLocal();

            GridDistributedTxMapping nearMapping = nearMap.get(n.id());

            if (dhtMapping.empty() && nearMapping != null && nearMapping.empty())
                // Nothing to send.
                continue;

            MiniFuture fut = new MiniFuture(dhtMapping, nearMapping);

            add(fut); // Append new future.

            GridDhtTxFinishRequest req = new GridDhtTxFinishRequest(
                tx.nearNodeId(),
                futId,
                fut.futureId(),
                tx.topologyVersion(),
                tx.xidVersion(),
                tx.commitVersion(),
                tx.threadId(),
                tx.isolation(),
                commit,
                tx.isInvalidate(),
                tx.system(),
                tx.ioPolicy(),
                tx.isSystemInvalidate(),
                sync,
                sync,
                tx.completedBase(),
                tx.committedVersions(),
                tx.rolledbackVersions(),
                tx.pendingVersions(),
                tx.size(),
                tx.subjectId(),
                tx.taskNameHash());

            req.writeVersion(tx.writeVersion() != null ? tx.writeVersion() : tx.xidVersion());

            try {
                cctx.io().send(n, req, tx.ioPolicy());

                if (sync)
                    res = true;
                else
                    fut.onDone();
            }
            catch (IgniteCheckedException e) {
                // Fail the whole thing.
                if (e instanceof ClusterTopologyCheckedException)
                    fut.onResult((ClusterTopologyCheckedException)e);
                else
                    fut.onResult(e);
            }
        }

        for (GridDistributedTxMapping nearMapping : nearMap.values()) {
            if (!dhtMap.containsKey(nearMapping.node().id())) {
                if (nearMapping.empty())
                    // Nothing to send.
                    continue;

                MiniFuture fut = new MiniFuture(null, nearMapping);

                add(fut); // Append new future.

                GridDhtTxFinishRequest req = new GridDhtTxFinishRequest(
                    tx.nearNodeId(),
                    futId,
                    fut.futureId(),
                    tx.topologyVersion(),
                    tx.xidVersion(),
                    tx.commitVersion(),
                    tx.threadId(),
                    tx.isolation(),
                    commit,
                    tx.isInvalidate(),
                    tx.system(),
                    tx.ioPolicy(),
                    tx.isSystemInvalidate(),
                    sync,
                    sync,
                    tx.completedBase(),
                    tx.committedVersions(),
                    tx.rolledbackVersions(),
                    tx.pendingVersions(),
                    tx.size(),
                    tx.subjectId(),
                    tx.taskNameHash());

                req.writeVersion(tx.writeVersion());

                try {
                    cctx.io().send(nearMapping.node(), req, tx.ioPolicy());

                    if (sync)
                        res = true;
                    else
                        fut.onDone();
                }
                catch (IgniteCheckedException e) {
                    // Fail the whole thing.
                    if (e instanceof ClusterTopologyCheckedException)
                        fut.onResult((ClusterTopologyCheckedException)e);
                    else
                        fut.onResult(e);
                }
            }
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        Collection<String> futs = F.viewReadOnly(futures(), new C1<IgniteInternalFuture<?>, String>() {
            @SuppressWarnings("unchecked")
            @Override public String apply(IgniteInternalFuture<?> f) {
                return "[node=" + ((MiniFuture)f).node().id() +
                    ", loc=" + ((MiniFuture)f).node().isLocal() +
                    ", done=" + f.isDone() + "]";
            }
        });

        return S.toString(GridDhtTxFinishFuture.class, this,
            "innerFuts", futs,
            "super", super.toString());
    }

    /**
     * Mini-future for get operations. Mini-futures are only waiting on a single
     * node as opposed to multiple nodes.
     */
    private class MiniFuture extends GridFutureAdapter<IgniteInternalTx> {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private final IgniteUuid futId = IgniteUuid.randomUuid();

        /** DHT mapping. */
        @GridToStringInclude
        private GridDistributedTxMapping dhtMapping;

        /** Near mapping. */
        @GridToStringInclude
        private GridDistributedTxMapping nearMapping;

        /** */
        @GridToStringInclude
        private ClusterNode node;

        /**
         * @param node Node.
         */
        private MiniFuture(ClusterNode node) {
            this.node = node;
        }

        /**
         * @param dhtMapping Mapping.
         * @param nearMapping nearMapping.
         */
        MiniFuture(GridDistributedTxMapping dhtMapping, GridDistributedTxMapping nearMapping) {
            assert dhtMapping == null || nearMapping == null || dhtMapping.node().equals(nearMapping.node());

            this.dhtMapping = dhtMapping;
            this.nearMapping = nearMapping;
        }

        /**
         * @return Future ID.
         */
        IgniteUuid futureId() {
            return futId;
        }

        /**
         * @return Node ID.
         */
        public ClusterNode node() {
            return node != null ? node : dhtMapping != null ? dhtMapping.node() : nearMapping.node();
        }

        /**
         * @param e Error.
         */
        void onResult(Throwable e) {
            if (log.isDebugEnabled())
                log.debug("Failed to get future result [fut=" + this + ", err=" + e + ']');

            // Fail.
            onDone(e);
        }

        /**
         * @param e Node failure.
         */
        void onResult(ClusterTopologyCheckedException e) {
            if (log.isDebugEnabled())
                log.debug("Remote node left grid while sending or waiting for reply (will ignore): " + this);

            // If node left, then there is nothing to commit on it.
            onDone(tx);
        }

        /**
         * @param res Result callback.
         */
        void onResult(GridDhtTxFinishResponse res) {
            if (log.isDebugEnabled())
                log.debug("Transaction synchronously completed on node [node=" + node() + ", res=" + res + ']');

            onDone();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(MiniFuture.class, this, "done", isDone(), "cancelled", isCancelled(), "err", error());
        }
    }
}
