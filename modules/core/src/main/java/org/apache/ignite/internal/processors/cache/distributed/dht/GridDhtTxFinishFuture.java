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

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.cluster.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.cache.distributed.*;
import org.apache.ignite.internal.processors.cache.transactions.*;
import org.apache.ignite.internal.processors.cache.version.*;
import org.apache.ignite.internal.transactions.*;
import org.apache.ignite.internal.util.future.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.*;

import static org.apache.ignite.internal.managers.communication.GridIoPolicy.*;
import static org.apache.ignite.transactions.IgniteTxState.*;

/**
 *
 */
public final class GridDhtTxFinishFuture<K, V> extends GridCompoundIdentityFuture<IgniteInternalTx>
    implements GridCacheFuture<IgniteInternalTx> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Logger reference. */
    private static final AtomicReference<IgniteLogger> logRef = new AtomicReference<>();

    /** Context. */
    private GridCacheSharedContext<K, V> cctx;

    /** Future ID. */
    private IgniteUuid futId;

    /** Transaction. */
    @GridToStringExclude
    private GridDhtTxLocalAdapter<K, V> tx;

    /** Commit flag. */
    private boolean commit;

    /** Logger. */
    private IgniteLogger log;

    /** Error. */
    @GridToStringExclude
    private AtomicReference<Throwable> err = new AtomicReference<>(null);

    /** DHT mappings. */
    private Map<UUID, GridDistributedTxMapping<K, V>> dhtMap;

    /** Near mappings. */
    private Map<UUID, GridDistributedTxMapping<K, V>> nearMap;

    /** Trackable flag. */
    private boolean trackable = true;

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public GridDhtTxFinishFuture() {
        // No-op.
    }

    /**
     * @param cctx Context.
     * @param tx Transaction.
     * @param commit Commit flag.
     */
    public GridDhtTxFinishFuture(GridCacheSharedContext<K, V> cctx, GridDhtTxLocalAdapter<K, V> tx, boolean commit) {
        super(cctx.kernalContext(), F.<IgniteInternalTx>identityReducer(tx));

        assert cctx != null;

        this.cctx = cctx;
        this.tx = tx;
        this.commit = commit;

        dhtMap = tx.dhtMap();
        nearMap = tx.nearMap();

        futId = IgniteUuid.randomUuid();

        log = U.logger(ctx, logRef, GridDhtTxFinishFuture.class);
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
    public void onResult(UUID nodeId, GridDhtTxFinishResponse<K, V> res) {
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
                this.tx.tmCommit();

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
     * Completes this future.
     */
    void complete() {
        onComplete();
    }

    /**
     * Initializes future.
     */
    public void finish() {
        if (!F.isEmpty(dhtMap) || !F.isEmpty(nearMap)) {
            boolean sync = finish(dhtMap, nearMap);

            markInitialized();

            if (!sync)
                onComplete();
        }
        else {
            markInitialized();

            // No backup or near nodes to send commit message to (just complete then).
            onComplete();
        }
    }

    /**
     * @param dhtMap DHT map.
     * @param nearMap Near map.
     * @return {@code True} in case there is at least one synchronous {@code MiniFuture} to wait for.
     */
    private boolean finish(Map<UUID, GridDistributedTxMapping<K, V>> dhtMap,
        Map<UUID, GridDistributedTxMapping<K, V>> nearMap) {
        boolean res = false;

        boolean sync = commit ? tx.syncCommit() : tx.syncRollback();

        // Create mini futures.
        for (GridDistributedTxMapping<K, V> dhtMapping : dhtMap.values()) {
            ClusterNode n = dhtMapping.node();

            assert !n.isLocal();

            GridDistributedTxMapping<K, V> nearMapping = nearMap.get(n.id());

            if (dhtMapping.empty() && nearMapping != null && nearMapping.empty())
                // Nothing to send.
                continue;

            MiniFuture fut = new MiniFuture(dhtMapping, nearMapping);

            add(fut); // Append new future.

            GridDhtTxFinishRequest<K, V> req = new GridDhtTxFinishRequest<>(
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
                tx.isSystemInvalidate(),
                tx.syncCommit(),
                tx.syncRollback(),
                tx.completedBase(),
                tx.committedVersions(),
                tx.rolledbackVersions(),
                tx.pendingVersions(),
                tx.size(),
                tx.pessimistic() ? dhtMapping.writes() : null,
                tx.pessimistic() && nearMapping != null ? nearMapping.writes() : null,
                tx.recoveryWrites(),
                tx.onePhaseCommit(),
                tx.groupLockKey(),
                tx.subjectId(),
                tx.taskNameHash());

            if (!tx.pessimistic()) {
                int idx = 0;

                for (IgniteTxEntry<K, V> e : dhtMapping.writes())
                    req.ttl(idx++, e.ttl());

                if (nearMapping != null) {
                    idx = 0;

                    for (IgniteTxEntry<K, V> e : nearMapping.writes())
                        req.nearTtl(idx++, e.ttl());
                }
            }

            if (tx.onePhaseCommit())
                req.writeVersion(tx.writeVersion());

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

        for (GridDistributedTxMapping<K, V> nearMapping : nearMap.values()) {
            if (!dhtMap.containsKey(nearMapping.node().id())) {
                if (nearMapping.empty())
                    // Nothing to send.
                    continue;

                MiniFuture fut = new MiniFuture(null, nearMapping);

                add(fut); // Append new future.

                GridDhtTxFinishRequest<K, V> req = new GridDhtTxFinishRequest<>(
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
                    tx.isSystemInvalidate(),
                    tx.syncCommit(),
                    tx.syncRollback(),
                    tx.completedBase(),
                    tx.committedVersions(),
                    tx.rolledbackVersions(),
                    tx.pendingVersions(),
                    tx.size(),
                    null,
                    tx.pessimistic() ? nearMapping.writes() : null,
                    tx.recoveryWrites(),
                    tx.onePhaseCommit(),
                    tx.groupLockKey(),
                    tx.subjectId(),
                    tx.taskNameHash());

                if (!tx.pessimistic()) {
                    int idx = 0;

                    for (IgniteTxEntry<K, V> e : nearMapping.writes())
                        req.nearTtl(idx++, e.ttl());
                }

                if (tx.onePhaseCommit())
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
        return S.toString(GridDhtTxFinishFuture.class, this, super.toString());
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
        private GridDistributedTxMapping<K, V> dhtMapping;

        /** Near mapping. */
        @GridToStringInclude
        private GridDistributedTxMapping<K, V> nearMapping;

        /**
         * Empty constructor required for {@link Externalizable}.
         */
        public MiniFuture() {
            // No-op.
        }

        /**
         * @param dhtMapping Mapping.
         * @param nearMapping nearMapping.
         */
        MiniFuture(GridDistributedTxMapping<K, V> dhtMapping, GridDistributedTxMapping<K, V> nearMapping) {
            super(cctx.kernalContext());

            assert dhtMapping == null || nearMapping == null || dhtMapping.node() == nearMapping.node();

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
            return dhtMapping != null ? dhtMapping.node() : nearMapping.node();
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
        void onResult(GridDhtTxFinishResponse<K, V> res) {
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
