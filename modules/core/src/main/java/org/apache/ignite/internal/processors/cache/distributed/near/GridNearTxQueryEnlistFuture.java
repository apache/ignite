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

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.cluster.ClusterTopologyServerNotFoundException;
import org.apache.ignite.internal.processors.affinity.AffinityAssignment;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheMessage;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedTxMapping;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxQueryEnlistFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.managers.communication.GridIoPolicy.QUERY_POOL;
import static org.apache.ignite.internal.processors.cache.distributed.dht.NearTxQueryEnlistResultHandler.createResponse;

/**
 * Cache lock future.
 */
@SuppressWarnings("ForLoopReplaceableByForEach")
public class GridNearTxQueryEnlistFuture extends GridNearTxQueryAbstractEnlistFuture {
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
    @Override protected void map(final boolean topLocked) {
        try {
            Map<ClusterNode, IntArrayHolder> map; boolean locallyMapped = false;

            AffinityAssignment assignment = cctx.affinity().assignment(topVer);

            if (parts != null) {
                map = U.newHashMap(parts.length);

                for (int i = 0; i < parts.length; i++) {
                    ClusterNode pNode = assignment.get(parts[i]).get(0);

                    map.computeIfAbsent(pNode, n -> new IntArrayHolder()).add(parts[i]);

                    updateMappings(pNode);

                    if (!locallyMapped && pNode.isLocal())
                        locallyMapped = true;
                }
            }
            else {
                Set<ClusterNode> nodes = assignment.primaryPartitionNodes();

                map = U.newHashMap(nodes.size());

                for (ClusterNode pNode : nodes) {
                    map.put(pNode, null);

                    updateMappings(pNode);

                    if (!locallyMapped && pNode.isLocal())
                        locallyMapped = true;
                }
            }

            if (map.isEmpty())
                throw new ClusterTopologyServerNotFoundException("Failed to find data nodes for cache (all partition " +
                    "nodes left the grid). [fut=" + toString() + ']');

            int idx = 0; boolean first = true, clientFirst = false;

            GridDhtTxQueryEnlistFuture localFut = null;

            for (Map.Entry<ClusterNode, IntArrayHolder> entry : map.entrySet()) {
                MiniFuture mini; ClusterNode node = entry.getKey(); IntArrayHolder parts = entry.getValue();

                add(mini = new MiniFuture(node));

                if (node.isLocal()) {
                    localFut = new GridDhtTxQueryEnlistFuture(
                        cctx.localNode().id(),
                        lockVer,
                        mvccSnapshot,
                        threadId,
                        futId,
                        -(++idx), // The common tx logic expects non-zero mini-future ids (negative local and positive non-local).
                        tx,
                        cacheIds,
                        parts == null ? null : parts.array(),
                        schema,
                        qry,
                        params,
                        flags,
                        pageSize,
                        remainingTime(),
                        cctx);

                    updateLocalFuture(localFut);

                    localFut.listen(new CI1<IgniteInternalFuture<Long>>() {
                        @Override public void apply(IgniteInternalFuture<Long> fut) {
                            assert fut.error() != null || fut.result() != null : fut;

                            try {
                                clearLocalFuture((GridDhtTxQueryEnlistFuture)fut);

                                GridNearTxQueryEnlistResponse res = fut.error() == null ? createResponse(fut) : null;

                                mini.onResult(res, fut.error());
                            }
                            catch (IgniteCheckedException e) {
                                mini.onResult(null, e);
                            }
                            finally {
                                CU.unwindEvicts(cctx);
                            }
                        }
                    });
                }
                else {
                    if (first) {
                        clientFirst = cctx.localNode().isClient() && !topLocked && !tx.hasRemoteLocks();

                        first = false;
                    }

                    GridNearTxQueryEnlistRequest req = new GridNearTxQueryEnlistRequest(
                        cctx.cacheId(),
                        threadId,
                        futId,
                        ++idx, // The common tx logic expects non-zero mini-future ids (negative local and positive non-local).
                        tx.subjectId(),
                        topVer,
                        lockVer,
                        mvccSnapshot,
                        cacheIds,
                        parts == null ? null : parts.array(),
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

                    sendRequest(req, node.id());
                }
            }

            markInitialized();

            if (localFut != null)
                localFut.init();
        }
        catch (Throwable e) {
            onDone(e);

            if (e instanceof Error)
                throw (Error)e;
        }
    }

    /**
     *
     * @param req Request.
     * @param nodeId Remote node ID.
     * @throws IgniteCheckedException if failed to send.
     */
    private void sendRequest(GridCacheMessage req, UUID nodeId) throws IgniteCheckedException {
        cctx.io().send(nodeId, req, QUERY_POOL); // Process query requests in query pool.
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
    private synchronized MiniFuture miniFuture(int miniId) {
        IgniteInternalFuture<Long> fut = future(Math.abs(miniId) - 1);

        return !fut.isDone() ? (MiniFuture)fut : null;
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
    @Override public Set<UUID> pendingResponseNodes() {
        if (initialized() && !isDone()) {
            return futures().stream()
                .map(MiniFuture.class::cast)
                .filter(mini -> !mini.isDone())
                .map(mini -> mini.node.id())
                .collect(Collectors.toSet());
        }

        return Collections.emptySet();
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

            if (res != null && res.removeMapping()) {
                GridDistributedTxMapping m = tx.mappings().get(node.id());

                assert m != null && m.empty();

                tx.removeMapping(node.id());

                if (node.isLocal())
                    tx.colocatedLocallyMapped(false);
            }
            else if (res != null) {
                tx.mappings().get(node.id()).addBackups(res.newDhtNodes());

                if (res.result() > 0 && !node.isLocal())
                    tx.hasRemoteLocks(true);
            }

            return err != null ? onDone(err) : onDone(res.result(), res.error());
        }
    }

    /** */
    private static class IntArrayHolder {
        /** */
        private int[] array;

        /** */
        private int size;

        /** */
        void add(int i) {
            if (array == null)
                array = new int[4];

            if (array.length == size)
                array = Arrays.copyOf(array, size << 1);

            array[size++] = i;
        }

        /** */
        public int[] array() {
            if (array == null)
                return null;
            else if (size == array.length)
                return array;
            else
                return Arrays.copyOf(array, size);
        }
    }
}
