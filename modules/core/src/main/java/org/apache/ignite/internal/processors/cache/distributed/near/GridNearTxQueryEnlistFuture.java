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
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.processors.affinity.AffinityAssignment;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedTxMapping;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxQueryEnlistFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Cache lock future.
 */
@SuppressWarnings("ForLoopReplaceableByForEach")
public class GridNearTxQueryEnlistFuture extends GridNearTxAbstractEnlistFuture {
    /** */
    private static final long serialVersionUID = -2155104765461405820L;
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
        super(cctx, tx, timeout);

        this.cacheIds = cacheIds;
        this.parts = parts;
        this.schema = schema;
        this.qry = qry;
        this.params = params;
        this.flags = flags;
        this.pageSize = pageSize;
    }

    /**
     * @param topLocked Topology locked flag.
     */
    protected void map(final boolean topLocked) {
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
                    mvccSnapshot,
                    cacheIds,
                    parts,
                    schema,
                    qry,
                    params,
                    flags,
                    pageSize,
                    remainingTime(),
                    tx.remainingTime(),
                    tx.taskNameHash(),
                    clientFirst
                );

                cctx.io().send(node.id(), req, cctx.ioPolicy());
            }

            if (locallyMapped) {
                final MiniFuture localMini = mini = miniFuture(-1);

                assert localMini != null;

                GridDhtTxQueryEnlistFuture fut = new GridDhtTxQueryEnlistFuture(
                    cctx.localNode().id(),
                    lockVer,
                    topVer,
                    mvccSnapshot,
                    threadId,
                    futId,
                    -1,
                    tx,
                    cacheIds,
                    parts,
                    schema,
                    qry,
                    params,
                    flags,
                    pageSize,
                    remainingTime(),
                    cctx);

                fut.listen(new CI1<IgniteInternalFuture<GridNearTxQueryEnlistResponse>>() {
                    @Override public void apply(IgniteInternalFuture<GridNearTxQueryEnlistResponse> fut) {
                        assert fut.error() != null || fut.result() != null : fut;

                        try {
                            localMini.onResult(fut.result(), fut.error());
                        }
                        finally {
                            CU.unwindEvicts(cctx);
                        }
                    }
                });

                fut.init();
            }
        }
        catch (Throwable e) {
            assert mini != null;

            mini.onResult(null, e);

            if (e instanceof Error)
                throw (Error)e;
        }

        markInitialized();
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

            if (err == null && res.error() != null)
                err = res.error();

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
            else if (res.result() > 0) {
                if (node.isLocal())
                    tx.colocatedLocallyMapped(true);
                else
                    tx.hasRemoteLocks(true);
            }

            return err != null ? onDone(err) : onDone(res.result(), res.error());
        }
    }
}
