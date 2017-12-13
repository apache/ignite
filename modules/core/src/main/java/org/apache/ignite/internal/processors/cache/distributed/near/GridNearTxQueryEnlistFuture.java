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

package org.apache.ignite.internal.processors.cache.distributed.near;

import java.util.Collection;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.processors.affinity.AffinityAssignment;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheStoppedException;
import org.apache.ignite.internal.processors.cache.GridCacheCompoundIdentityFuture;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.GridCacheMvccCandidate;
import org.apache.ignite.internal.processors.cache.GridCacheVersionedFuture;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedTxMapping;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTopologyFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxQueryEnlistFuture;
import org.apache.ignite.internal.processors.cache.mvcc.CacheCoordinatorsProcessor;
import org.apache.ignite.internal.processors.cache.mvcc.MvccCoordinator;
import org.apache.ignite.internal.processors.cache.mvcc.MvccCoordinatorVersion;
import org.apache.ignite.internal.processors.cache.mvcc.MvccResponseListener;
import org.apache.ignite.internal.processors.cache.mvcc.TxMvccInfo;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.timeout.GridTimeoutObjectAdapter;
import org.apache.ignite.internal.transactions.IgniteTxTimeoutCheckedException;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

/**
 * Cache lock future.
 */
@SuppressWarnings("ForLoopReplaceableByForEach")
public class GridNearTxQueryEnlistFuture extends GridCacheCompoundIdentityFuture<Long>
    implements GridCacheVersionedFuture<Long>, MvccResponseListener {

    /** Transaction. */
    private final GridNearTxLocal tx;

    /** Initiated thread id. */
    private final long threadId;

    /** Mvcc future id. */
    private final IgniteUuid futId;

    /** Lock version. */
    private final GridCacheVersion lockVer;

    /** Involved cache ids. */
    private final int[] cacheIds;

    /** Partitions. */
    private final int[] parts;

    /** Schema name. */
    private final String schema;

    /** Query string. */
    private final String qry;

    /** Query parameters. */
    private final Object[] params;

    /** Flags. */
    private final int flags;

    /** Fetch page size. */
    private final int pageSize;

    /** Timeout. */
    private final long timeout;

    /** Mvcc version. */
    private MvccCoordinatorVersion mvccVer;

    /** Mapped topology version. */
    @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
    private AffinityTopologyVersion topVer;

    /** Cache context. */
    @GridToStringExclude
    private final GridCacheContext<?, ?> cctx;

    /** Logger. */
    @GridToStringExclude
    private final IgniteLogger log;

    /** Timeout object. */
    @GridToStringExclude
    private LockTimeoutObject timeoutObj;

    /**
     * @param cctx Cache context.
     * @param tx Transaction.
     * @param cacheIds Involved cache ids.
     * @param parts Partitions.
     * @param schema Schema name.
     * @param qry Query string.
     * @param params Query parameters.
     * @param flags Flags.
     * @param pageSize Fetch page size.
     * @param timeout Timeout.
     */
    protected GridNearTxQueryEnlistFuture(
        GridCacheContext<?, ?> cctx, GridNearTxLocal tx, int[] cacheIds, int[] parts, String schema, String qry,
        Object[] params, int flags, int pageSize, long timeout) {
        super(CU.longReducer());

        this.cctx = cctx;
        this.tx = tx;
        this.cacheIds = cacheIds;
        this.parts = parts;
        this.schema = schema;
        this.qry = qry;
        this.params = params;
        this.flags = flags;
        this.pageSize = pageSize;
        this.timeout = timeout;

        threadId = tx.threadId();

        lockVer = tx.xidVersion();

        mvccVer = tx.mvccInfo() != null ? tx.mvccInfo().version() : null;

        futId = IgniteUuid.randomUuid();

        log = cctx.logger(GridNearTxQueryEnlistFuture.class);
    }

    /**
     *
     */
    public void map() {
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
            timeoutObj = new LockTimeoutObject();

            cctx.time().addTimeoutObject(timeoutObj);
        }

        boolean added = cctx.mvcc().addFuture(this);

        assert added : this;

        // Obtain the topology version to use.
        long threadId = Thread.currentThread().getId();

        AffinityTopologyVersion topVer = cctx.mvcc().lastExplicitLockTopologyVersion(threadId);

        // If there is another system transaction in progress, use it's topology version to prevent deadlock.
        if (topVer == null && tx.system())
            topVer = cctx.tm().lockedTopologyVersion(threadId, tx);

        if (topVer != null)
            tx.topologyVersion(topVer);

        if (topVer == null)
            topVer = tx.topologyVersionSnapshot();

        if (topVer != null) {
            for (GridDhtTopologyFuture fut : cctx.shared().exchange().exchangeFutures()) {
                if (fut.exchangeDone() && fut.topologyVersion().equals(topVer)) {
                    Throwable err = fut.validateCache(cctx, false, false, null, null);

                    if (err != null) {
                        onDone(err);

                        return;
                    }

                    break;
                }
            }

            if (this.topVer == null)
                this.topVer = topVer;

            map(false, true);

            markInitialized();

            return;
        }

        mapOnTopology(false);
    }

    /**
     * @param remap Remap flag.
     */
    private void mapOnTopology(final boolean remap) {
        cctx.topology().readLock();

        try {
            if (cctx.topology().stopping()) {
                onDone(new CacheStoppedException(cctx.name()));

                return;
            }

            GridDhtTopologyFuture fut = cctx.topologyVersionFuture();

            if (fut.isDone()) {
                Throwable err = fut.validateCache(cctx, false, false, null, null);

                if (err != null) {
                    onDone(err);

                    return;
                }

                AffinityTopologyVersion topVer = fut.topologyVersion();

                if (tx != null)
                    tx.topologyVersion(topVer);

                if (this.topVer == null)
                    this.topVer = topVer;

                map(remap, false);
            }
            else {
                fut.listen(new CI1<IgniteInternalFuture<AffinityTopologyVersion>>() {
                    @Override public void apply(IgniteInternalFuture<AffinityTopologyVersion> fut) {
                        try {
                            fut.get();

                            mapOnTopology(remap);
                        }
                        catch (IgniteCheckedException e) {
                            onDone(e);
                        }
                        finally {
                            cctx.shared().txContextReset();
                        }
                    }
                });
            }
        }
        finally {
            cctx.topology().readUnlock();
        }
    }

    /**
     * @param remap Remap flag.
     * @param topLocked Topology locked flag.
     */
    private void map(final boolean remap, final boolean topLocked) { // TODO remap.
        if (cctx.mvccEnabled() && mvccVer == null) {
            MvccCoordinator mvccCrd = cctx.affinity().mvccCoordinator(topVer);

            if (mvccCrd == null) {
                onDone(CacheCoordinatorsProcessor.noCoordinatorError(topVer));

                return;
            }

            if (cctx.localNodeId().equals(mvccCrd.nodeId())) {
                MvccCoordinatorVersion mvccVer = cctx.shared().coordinators().requestTxCounterOnCoordinator(lockVer);

                onMvccResponse(cctx.localNodeId(), mvccVer);
            }
            else {
                cctx.shared().coordinators().requestTxCounter(mvccCrd, this, lockVer)
                    .listen(new IgniteInClosure<IgniteInternalFuture<MvccCoordinatorVersion>>() {
                        @Override public void apply(IgniteInternalFuture<MvccCoordinatorVersion> fut) {
                            if (fut.error() == null)
                                // proceed mapping.
                                map(remap, topLocked);
                        }
                    });

                return;
            }
        }

        final AffinityAssignment assignment = cctx.affinity().assignment(topVer);

        Collection<ClusterNode> primary;

        IgniteTxMappings m = tx.mappings();

        if (parts != null) {
            primary = U.newHashSet(parts.length);

            for (int i = 0; i < parts.length; i++) {
                ClusterNode pNode = assignment.get(parts[i]).get(0);

                primary.add(pNode);

                GridDistributedTxMapping mapping = m.get(pNode.id());

                if (mapping == null)
                    m.put(mapping = new GridDistributedTxMapping(pNode));

                mapping.markQueryUpdate();
            }
        }
        else {
            primary = assignment.primaryPartitionNodes();

            for (ClusterNode pNode : primary) {
                GridDistributedTxMapping mapping = m.get(pNode.id());

                if (mapping == null)
                    m.put(mapping = new GridDistributedTxMapping(pNode));

                mapping.markQueryUpdate();
            }
        }

        boolean locallyMapped = primary.contains(cctx.localNode());

        if (locallyMapped)
            add(new MiniFuture(cctx.localNode()));

        MiniFuture mini = null;

        try {
            int idx = locallyMapped ? 1 : 0;
            boolean first = true;
            boolean clientFirst = false;

            for (ClusterNode node : F.view(primary, F.remoteNodes(cctx.localNodeId()))) {
                add(mini = new MiniFuture(node));

                if (first) {
                    clientFirst = cctx.localNode().isClient() && !topLocked && !tx.hasRemoteLocks();

                    first = false;
                }

                GridNearTxQueryEnlistRequest req = new GridNearTxQueryEnlistRequest(
                    cctx.cacheId(),
                    threadId,
                    futId,
                    ++idx,
                    tx.subjectId(),
                    topVer,
                    lockVer,
                    mvccVer,
                    cacheIds,
                    parts,
                    schema,
                    qry,
                    params,
                    flags,
                    pageSize,
                    timeout,
                    tx.taskNameHash(),
                    clientFirst
                );

                cctx.io().send(node.id(), req, cctx.ioPolicy());
            }

            if (locallyMapped) {
                final MiniFuture localMini = mini = miniFuture(-1);

                final GridNearTxQueryEnlistRequest req = new GridNearTxQueryEnlistRequest(
                    cctx.cacheId(),
                    threadId,
                    futId,
                    -1,
                    tx.subjectId(),
                    topVer,
                    lockVer,
                    mvccVer,
                    cacheIds,
                    parts,
                    schema,
                    qry,
                    params,
                    flags,
                    pageSize,
                    timeout,
                    tx.taskNameHash(),
                    false
                );

                GridDhtTxQueryEnlistFuture fut = new GridDhtTxQueryEnlistFuture(
                    cctx.localNode().id(),
                    req.version(),
                    req.topologyVersion(),
                    req.mvccVersion(),
                    req.threadId(),
                    req.futureId(),
                    req.miniId(),
                    tx,
                    req.cacheIds(),
                    req.partitions(),
                    req.schemaName(),
                    req.query(),
                    req.parameters(),
                    req.flags(),
                    req.pageSize(),
                    req.timeout(),
                    cctx);

                fut.listen(new CI1<IgniteInternalFuture<GridNearTxQueryEnlistResponse>>() {
                    @Override public void apply(IgniteInternalFuture<GridNearTxQueryEnlistResponse> fut) {
                        assert fut.error() != null || fut.result() != null : fut;

                        try {
                            localMini.onResult(fut.result(), fut.error());
                        }
                        finally {
                            cctx.io().onMessageProcessed(req);
                        }
                    }
                });

                fut.map();
            }
        }
        catch (Throwable e) {
            mini.onResult(null, e);

            if (e instanceof Error)
                throw (Error)e;
        }

        markInitialized();
    }

    @Override public GridCacheVersion version() {
        return lockVer;
    }

    @Override public boolean onOwnerChanged(GridCacheEntryEx entry, GridCacheMvccCandidate owner) {
        return false;
    }

    @Override public IgniteUuid futureId() {
        return futId;
    }

    /**
     * @param nodeId Left node ID
     * @return {@code True} if node was in the list.
     */
    @Override public synchronized boolean onNodeLeft(UUID nodeId) {
        for (IgniteInternalFuture<?> fut : futures()) {
            MiniFuture f = (MiniFuture)fut;

            if (f.node.id().equals(nodeId)) {
                if (log.isDebugEnabled())
                    log.debug("Found mini-future for left node [nodeId=" + nodeId + ", mini=" + f + ", fut=" +
                        this + ']');

                return f.onResult(null, newTopologyException(nodeId));
            }
        }

        if (log.isDebugEnabled())
            log.debug("Future does not have mapping for left node (ignoring) [nodeId=" + nodeId +
                ", fut=" + this + ']');

        return false;
    }

    @Override public boolean trackable() {
        return true;
    }

    @Override public void markNotTrackable() {
        // No-op;
    }

    @Override public void onMvccResponse(UUID crdId, MvccCoordinatorVersion res) {
        mvccVer = res;

        if (tx != null)
            tx.mvccInfo(new TxMvccInfo(crdId, res));
    }

    @Override public void onMvccError(IgniteCheckedException e) {
        onDone(e);
    }

    /** {@inheritDoc} */
    @Override public boolean onDone(@Nullable Long res, @Nullable Throwable err) {
        if (super.onDone(res, err)) {

            if (timeoutObj != null)
                cctx.time().removeTimeoutObject(timeoutObj);

            return true;
        }

        return false;
    }

    /**
     * Finds pending mini future by the given mini ID.
     *
     * @param miniId Mini ID to find.
     * @return Mini future.
     */
    private MiniFuture miniFuture(int miniId) {
         synchronized (this) {
            int idx = Math.abs(miniId) - 1;

            assert idx >= 0 && idx < futuresCountNoLock();

            IgniteInternalFuture<Long> fut = future(idx);

            if (!fut.isDone())
                return (MiniFuture)fut;
        }

        return null;
    }

    /**
     * Creates new topology exception for cases when primary node leaves grid during mapping.
     *
     * @param nodeId Node ID.
     * @return Topology exception with user-friendly message.
     */
    private ClusterTopologyCheckedException newTopologyException(UUID nodeId) {
        ClusterTopologyCheckedException topEx = new ClusterTopologyCheckedException("Failed to enlist keys " +
            "(primary node left grid, retry transaction if possible) [node=" + nodeId + ']');

        topEx.retryReadyFuture(cctx.shared().nextAffinityReadyFuture(topVer));

        return topEx;
    }

    /**
     * @param nodeId Sender node id.
     * @param res Response.
     */
    public void onResult(UUID nodeId, GridNearTxQueryEnlistResponse res) {
        MiniFuture mini = miniFuture(res.miniId());

        if (mini != null)
            mini.onResult(res, null);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridNearTxQueryEnlistFuture.class, this, super.toString());
    }

    /** */
    private class MiniFuture extends GridFutureAdapter<Long> {
        /** */
        private boolean completed;

        /** Node ID. */
        @GridToStringExclude
        private final ClusterNode node;

        /**
         * @param node Cluster node.
         */
        private MiniFuture(ClusterNode node) {
            this.node = node;
        }

        /**
         * @param res Response.
         * @param err Exception.
         * @return {@code True} if future was completed by this call.
         */
        public boolean onResult(GridNearTxQueryEnlistResponse res, Throwable err) {
            assert res != null || err != null : this;

            synchronized (this) {
                if (completed)
                    return false;

                completed = true;
            }

            if (X.hasCause(err, ClusterTopologyCheckedException.class)
                || (res != null && res.removeMapping())) {
                assert tx.mappings().get(node.id()).empty();

                tx.removeMapping(node.id());
            }

            return err != null ? onDone(err) : onDone(res.result(), res.error());
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
            String msg = "Timed out waiting for lock response: " + this;

            if (log.isDebugEnabled())
                log.debug(msg);

            onDone(new IgniteTxTimeoutCheckedException(msg));
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(LockTimeoutObject.class, this);
        }
    }
}
