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
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * This class is responsible for sending request for query result pages to remote nodes.
 */
public abstract class CacheQueryResultFetcher {
    /**
     * Map (requestId -> query future) where request id is unique for all requests per query.
     * This map is populated by query manager.
     */
    private final ConcurrentMap<Long, GridCacheDistributedQueryFuture<?, ?, ?>> qryFuts;

    /** Cache context. */
    private final GridCacheContext cctx;

    /** Ignite logger. */
    private final IgniteLogger log;

    /** */
    CacheQueryResultFetcher(
        final GridCacheContext cctx,
        final ConcurrentMap<Long, GridCacheDistributedQueryFuture<?, ?, ?>> qryFuts) {

        this.qryFuts = qryFuts;
        this.cctx = cctx;
        this.log = cctx.kernalContext().config().getGridLogger();
    }

    /**
     * Send initial query request to specified nodes.
     *
     * @param reqId Request (cache query) ID.
     * @param fut Cache query future, contains query info.
     * @param nodes Collection of nodes to send a request.
     */
    public void initFetchPages(long reqId, GridCacheDistributedQueryFuture fut, Collection<ClusterNode> nodes) throws IgniteCheckedException {
        GridCacheQueryBean bean = fut.query();
        GridCacheQueryAdapter qry = bean.query();

        boolean deployFilterOrTransformer = (qry.scanFilter() != null || qry.transform() != null)
            && cctx.gridDeploy().enabled();

        GridCacheQueryRequest req = new GridCacheQueryRequest(
            cctx.cacheId(),
            reqId,
            cctx.name(),
            qry.type(),
            fut.fields(),
            qry.clause(),
            qry.limit(),
            qry.queryClassName(),
            qry.scanFilter(),
            qry.partition(),
            bean.reducer(),
            qry.transform(),
            qry.pageSize(),
            qry.includeBackups(),
            bean.arguments(),
            qry.includeMetadata(),
            qry.keepBinary(),
            qry.subjectId(),
            qry.taskHash(),
            cctx.startTopologyVersion(),
            qry.mvccSnapshot(),
            // Force deployment anyway if scan query is used.
            cctx.deploymentEnabled() || deployFilterOrTransformer,
            qry.isDataPageScanEnabled());

        sendRequest(fut, req, nodes);
    }

    /**
     * Send request for fetching query result pages to specified nodes.
     *
     * @param reqId Request (cache query) ID.
     * @param nodes Collection of nodes to send a request.
     * @param all If {@code true} then request for all pages, otherwise for single only.
     */
    public void fetchPages(long reqId, Collection<UUID> nodes, boolean all) {
        GridCacheDistributedQueryFuture fut = qryFuts.get(reqId);
        GridCacheQueryAdapter qry = fut.query().query();

        GridCacheQueryRequest req = new GridCacheQueryRequest(
            cctx.cacheId(),
            reqId,
            cctx.name(),
            qry.pageSize(),
            qry.includeBackups(),
            fut.fields(),
            all,
            qry.keepBinary(),
            qry.subjectId(),
            qry.taskHash(),
            cctx.startTopologyVersion(),
            // Force deployment anyway if scan query is used.
            cctx.deploymentEnabled() || (qry.scanFilter() != null && cctx.gridDeploy().enabled()),
            qry.isDataPageScanEnabled());

        try {
            Collection<ClusterNode> n = new ArrayList<>();
            for (UUID id: nodes)
                n.add(cctx.node(id));

            sendRequest(fut, req, n);

        } catch (IgniteCheckedException e) {
            fut.onDone(e);
        }
    }

    /**
     * Cancel query request, so no new pages will be sent.
     *
     * @param reqId Query request ID.
     * @param nodes Collection of nodes to send the cancel request.
     * @param fieldsQry Whether query is a fields query.
     *
     */
    public void cancelQueryRequest(long reqId, Collection<ClusterNode> nodes, boolean fieldsQry) {
        final GridCacheQueryManager qryMgr = cctx.queries();

        assert qryMgr != null;

        try {
            final GridCacheQueryRequest req = new GridCacheQueryRequest(cctx.cacheId(),
                reqId,
                fieldsQry,
                cctx.startTopologyVersion(),
                cctx.deploymentEnabled());

            // Process cancel query directly (without sending) for local node.
            sendLocal(req);

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

    /**
     * Sends query request.
     *
     * @param req Request.
     * @param nodes Nodes.
     * @throws IgniteCheckedException In case of error.
     */
    private void sendRequest(
        final GridCacheDistributedQueryFuture fut,
        final GridCacheQueryRequest req,
        Collection<ClusterNode> nodes
    ) throws IgniteCheckedException {
        assert req != null;
        assert nodes != null;

        final UUID locNodeId = cctx.localNodeId();

        ClusterNode locNode = null;

        Collection<ClusterNode> rmtNodes = null;

        for (ClusterNode n : nodes) {
            if (n.id().equals(locNodeId))
                locNode = n;
            else {
                if (rmtNodes == null)
                    rmtNodes = new ArrayList<>(nodes.size());

                rmtNodes.add(n);
            }
        }

        // Request should be sent to remote nodes before the query is processed on the local node.
        // For example, a remote reducer has a state, we should not serialize and then send
        // the reducer changed by the local node.
        if (!F.isEmpty(rmtNodes)) {
            for (ClusterNode node : rmtNodes) {
                try {
                    cctx.io().send(node, req, GridIoPolicy.QUERY_POOL);
                }
                catch (IgniteCheckedException e) {
                    if (cctx.io().checkNodeLeft(node.id(), e, true)) {
                        fut.onNodeLeft(node.id());

                        if (fut.isDone())
                            return;
                    }
                    else
                        throw e;
                }
            }
        }

        if (locNode != null)
            sendLocal(req);
    }

    /** Send and handle request to local node. */
    protected abstract void sendLocal(GridCacheQueryRequest req);
}
