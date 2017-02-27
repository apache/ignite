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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.processors.cache.GridCacheFuture;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedTxMapping;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxEntry;
import org.apache.ignite.internal.util.future.GridCompoundIdentityFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.C1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;

import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.PRIMARY_SYNC;
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

    /** Error updater. */
    private static final AtomicReferenceFieldUpdater<GridDhtTxFinishFuture, Throwable> ERR_UPD =
        AtomicReferenceFieldUpdater.newUpdater(GridDhtTxFinishFuture.class, Throwable.class, "err");

    /** Logger. */
    private static IgniteLogger log;

    /** Logger. */
    private static IgniteLogger msgLog;

    /** Context. */
    private GridCacheSharedContext<K, V> cctx;

    /** Future ID. */
    private final IgniteUuid futId;

    /** Transaction. */
    @GridToStringExclude
    private GridDhtTxLocalAdapter tx;

    /** Commit flag. */
    private boolean commit;

    /** Error. */
    @SuppressWarnings("UnusedDeclaration")
    @GridToStringExclude
    private volatile Throwable err;

    /** DHT mappings. */
    private Map<UUID, GridDistributedTxMapping> dhtMap;

    /** Near mappings. */
    private Map<UUID, GridDistributedTxMapping> nearMap;

    /**
     * @param cctx Context.
     * @param tx Transaction.
     * @param commit Commit flag.
     */
    public GridDhtTxFinishFuture(GridCacheSharedContext<K, V> cctx, GridDhtTxLocalAdapter tx, boolean commit) {
        super(F.<IgniteInternalTx>identityReducer(tx));

        this.cctx = cctx;
        this.tx = tx;
        this.commit = commit;

        dhtMap = tx.dhtMap();
        nearMap = tx.nearMap();

        futId = IgniteUuid.randomUuid();

        if (log == null) {
            msgLog = cctx.txFinishMessageLogger();
            log = U.logger(cctx.kernalContext(), logRef, GridDhtTxFinishFuture.class);
        }
    }

    /**
     * @return Transaction.
     */
    public GridDhtTxLocalAdapter tx() {
        return tx;
    }

    /** {@inheritDoc} */
    @Override public IgniteUuid futureId() {
        return futId;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public boolean onNodeLeft(UUID nodeId) {
        for (IgniteInternalFuture<?> fut : futures())
            if (isMini(fut)) {
                MiniFuture f = (MiniFuture)fut;

                if (f.node().id().equals(nodeId)) {
                    f.onNodeLeft();

                    return true;
                }
            }

        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean trackable() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public void markNotTrackable() {
        assert false;
    }

    /**
     * @param e Error.
     */
    public void rollbackOnError(Throwable e) {
        assert e != null;

        if (ERR_UPD.compareAndSet(this, null, e)) {
            tx.setRollbackOnly();

            finish(false);
        }
    }

    /**
     * @param nodeId Sender.
     * @param res Result.
     */
    public void onResult(UUID nodeId, GridDhtTxFinishResponse res) {
        if (!isDone()) {
            boolean found = false;

            for (IgniteInternalFuture<IgniteInternalTx> fut : futures()) {
                if (isMini(fut)) {
                    MiniFuture f = (MiniFuture)fut;

                    if (f.futureId().equals(res.miniId())) {
                        found = true;

                        assert f.node().id().equals(nodeId);

                        f.onResult(res);
                    }
                }
            }

            if (!found) {
                if (msgLog.isDebugEnabled()) {
                    msgLog.debug("DHT finish fut, failed to find mini future [txId=" + tx.nearXidVersion() +
                        ", dhtTxId=" + tx.xidVersion() +
                        ", node=" + nodeId +
                        ", res=" + res +
                        ", fut=" + this + ']');
                }
            }
        }
        else {
            if (msgLog.isDebugEnabled()) {
                msgLog.debug("DHT finish fut, failed to find mini future [txId=" + tx.nearXidVersion() +
                    ", dhtTxId=" + tx.xidVersion() +
                    ", node=" + nodeId +
                    ", res=" + res +
                    ", fut=" + this + ']');
            }
        }
    }

    /** {@inheritDoc} */
    @Override public boolean onDone(IgniteInternalTx tx, Throwable err) {
        if (initialized() || err != null) {
            Throwable e = this.err;

            if (this.tx.onePhaseCommit() && (this.tx.state() == COMMITTING)) {
                try {
                    this.tx.tmFinish(err == null);
                }
                catch (IgniteCheckedException finishErr) {
                    U.error(log, "Failed to finish tx: " + tx, e);

                    if (e == null)
                        e = finishErr;
                }
            }

            if (commit && e == null)
                e = this.tx.commitError();

            Throwable finishErr = e != null ? e : err;

            if (super.onDone(tx, finishErr)) {
                if (finishErr == null)
                    finishErr = this.tx.commitError();

                if (this.tx.syncMode() != PRIMARY_SYNC)
                    this.tx.sendFinishReply(finishErr);

                // Don't forget to clean up.
                cctx.mvcc().removeFuture(futId);

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
        onDone(tx, err);
    }

    /**
     * Initializes future.
     *
     * @param commit Commit flag.
     */
    @SuppressWarnings({"SimplifiableIfStatement", "IfMayBeConditional"})
    public void finish(boolean commit) {
        boolean sync;

        if (!F.isEmpty(dhtMap) || !F.isEmpty(nearMap))
            sync = finish(commit, dhtMap, nearMap);
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
        assert !F.isEmpty(nodes);

        if (tx.onePhaseCommit())
            return false;

        boolean sync = tx.syncMode() == FULL_SYNC;

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
                false,
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
                tx.taskNameHash(),
                tx.activeCachesDeploymentEnabled(),
                false,
                false);

            try {
                cctx.io().send(n, req, tx.ioPolicy());

                if (msgLog.isDebugEnabled()) {
                    msgLog.debug("DHT finish fut, sent request lock tx [txId=" + tx.nearXidVersion() +
                        ", dhtTxId=" + tx.xidVersion() +
                        ", node=" + n.id() + ']');
                }

                if (sync)
                    res = true;
                else
                    fut.onDone();
            }
            catch (IgniteCheckedException e) {
                // Fail the whole thing.
                if (e instanceof ClusterTopologyCheckedException)
                    fut.onNodeLeft((ClusterTopologyCheckedException)e);
                else {
                    if (msgLog.isDebugEnabled()) {
                        msgLog.debug("DHT finish fut, failed to send request lock tx [txId=" + tx.nearXidVersion() +
                            ", dhtTxId=" + tx.xidVersion() +
                            ", node=" + n.id() +
                            ", err=" + e + ']');
                    }

                    fut.onResult(e);
                }
            }
        }

        return res;
    }

    /**
     * @param commit Commit flag.
     * @param dhtMap DHT map.
     * @param nearMap Near map.
     * @return {@code True} in case there is at least one synchronous {@code MiniFuture} to wait for.
     */
    private boolean finish(boolean commit,
        Map<UUID, GridDistributedTxMapping> dhtMap,
        Map<UUID, GridDistributedTxMapping> nearMap) {
        if (tx.onePhaseCommit())
            return false;

        boolean sync = tx.syncMode() == FULL_SYNC;

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

            Collection<Long> updCntrs = new ArrayList<>(dhtMapping.entries().size());

            for (IgniteTxEntry e : dhtMapping.entries())
                updCntrs.add(e.updateCounter());

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
                tx.taskNameHash(),
                tx.activeCachesDeploymentEnabled(),
                updCntrs,
                false,
                false);

            req.writeVersion(tx.writeVersion() != null ? tx.writeVersion() : tx.xidVersion());

            try {
                cctx.io().send(n, req, tx.ioPolicy());

                if (msgLog.isDebugEnabled()) {
                    msgLog.debug("DHT finish fut, sent request dht [txId=" + tx.nearXidVersion() +
                        ", dhtTxId=" + tx.xidVersion() +
                        ", node=" + n.id() + ']');
                }

                if (sync)
                    res = true;
                else
                    fut.onDone();
            }
            catch (IgniteCheckedException e) {
                // Fail the whole thing.
                if (e instanceof ClusterTopologyCheckedException)
                    fut.onNodeLeft((ClusterTopologyCheckedException)e);
                else {
                    if (msgLog.isDebugEnabled()) {
                        msgLog.debug("DHT finish fut, failed to send request dht [txId=" + tx.nearXidVersion() +
                            ", dhtTxId=" + tx.xidVersion() +
                            ", node=" + n.id() +
                            ", err=" + e + ']');
                    }

                    fut.onResult(e);
                }
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
                    tx.taskNameHash(),
                    tx.activeCachesDeploymentEnabled(),
                    false,
                    false);

                req.writeVersion(tx.writeVersion());

                try {
                    cctx.io().send(nearMapping.node(), req, tx.ioPolicy());

                    if (msgLog.isDebugEnabled()) {
                        msgLog.debug("DHT finish fut, sent request near [txId=" + tx.nearXidVersion() +
                            ", dhtTxId=" + tx.xidVersion() +
                            ", node=" + nearMapping.node().id() + ']');
                    }

                    if (sync)
                        res = true;
                    else
                        fut.onDone();
                }
                catch (IgniteCheckedException e) {
                    // Fail the whole thing.
                    if (e instanceof ClusterTopologyCheckedException)
                        fut.onNodeLeft((ClusterTopologyCheckedException)e);
                    else {
                        if (msgLog.isDebugEnabled()) {
                            msgLog.debug("DHT finish fut, failed to send request near [txId=" + tx.nearXidVersion() +
                                ", dhtTxId=" + tx.xidVersion() +
                                ", node=" + nearMapping.node().id() +
                                ", err=" + e + ']');
                        }

                        fut.onResult(e);
                    }
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
            "xidVer", tx.xidVersion(),
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
        void onNodeLeft(ClusterTopologyCheckedException e) {
            onNodeLeft();
        }

        /**
         */
        void onNodeLeft() {
            if (msgLog.isDebugEnabled()) {
                msgLog.debug("DHT finish fut, mini future node left [txId=" + tx.nearXidVersion() +
                    ", dhtTxId=" + tx.xidVersion() +
                    ", node=" + node().id() + ']');
            }

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
