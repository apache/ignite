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
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

/**
 * This class is responsible for sending request for query result pages to remote nodes.
 */
public class CacheQueryPageRequester {
    /** Cache context. */
    private final GridCacheContext<?, ?> cctx;

    /** Ignite logger. */
    private final IgniteLogger log;

    /** Local handler of cache query request. */
    private final Consumer<GridCacheQueryRequest> sendLoc;

    /**
     * @param cctx Cache context.
     * @param sendLoc Local handler of cache query request.
     */
    CacheQueryPageRequester(GridCacheContext<?, ?> cctx, Consumer<GridCacheQueryRequest> sendLoc) {
        this.cctx = cctx;
        this.sendLoc = sendLoc;

        log = cctx.kernalContext().config().getGridLogger();
    }

    /**
     * Send initial query request to specified nodes.
     *
     * @param reqId Request (cache query) ID.
     * @param fut Cache query future, contains query info.
     * @param nodes Collection of nodes to send a request.
     */
    public void initRequestPages(long reqId, GridCacheDistributedQueryFuture<?, ?, ?> fut,
        Collection<ClusterNode> nodes) throws IgniteCheckedException {
        GridCacheQueryBean bean = fut.query();
        GridCacheQueryAdapter<?> qry = bean.query();

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
            qry.taskHash(),
            cctx.startTopologyVersion(),
            qry.mvccSnapshot(),
            // Force deployment anyway if scan query is used.
            cctx.deploymentEnabled() || deployFilterOrTransformer,
            qry.isDataPageScanEnabled());

        List<UUID> sendNodes = new ArrayList<>();

        for (ClusterNode n: nodes)
            sendNodes.add(n.id());

        sendRequest(fut, req, sendNodes);
    }

    /**
     * Send request for fetching query result pages to specified nodes.
     *
     * @param reqId Request (cache query) ID.
     * @param nodes Collection of nodes to send a request.
     * @param all If {@code true} then request for all pages, otherwise for single only.
     */
    public void requestPages(long reqId, GridCacheQueryFutureAdapter<?, ?, ?> fut, Collection<UUID> nodes, boolean all) {
        GridCacheQueryAdapter<?> qry = fut.query().query();

        GridCacheQueryRequest req = new GridCacheQueryRequest(
            cctx.cacheId(),
            reqId,
            cctx.name(),
            qry.pageSize(),
            qry.includeBackups(),
            fut.fields(),
            all,
            qry.keepBinary(),
            qry.taskHash(),
            cctx.startTopologyVersion(),
            // Force deployment anyway if scan query is used.
            cctx.deploymentEnabled() || (qry.scanFilter() != null && cctx.gridDeploy().enabled()),
            qry.isDataPageScanEnabled());

        try {
            sendRequest(fut, req, nodes);

        } catch (IgniteCheckedException e) {
            fut.onDone(e);
        }
    }

    /**
     * Send cancel query request, so no new pages will be sent.
     *
     * @param reqId Query request ID.
     * @param nodes Collection of nodes to send the cancel request.
     * @param fieldsQry Whether query is a fields query.
     *
     */
    public void cancelQuery(long reqId, Collection<UUID> nodes, boolean fieldsQry) {
        GridCacheQueryManager<?, ?> qryMgr = cctx.queries();

        assert qryMgr != null;

        try {
            GridCacheQueryRequest req = new GridCacheQueryRequest(cctx.cacheId(),
                reqId,
                fieldsQry,
                cctx.startTopologyVersion(),
                cctx.deploymentEnabled());

            sendRequest(null, req, nodes);
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to send cancel request (will cancel query in any case).", e);
        }
    }

    /**
     * Sends query request.
     *
     * @param fut Cache query future. {@code null} in case of cancel request.
     * @param req Request.
     * @param nodes Nodes.
     * @throws IgniteCheckedException In case of error.
     */
    private void sendRequest(
        @Nullable GridCacheQueryFutureAdapter<?, ?, ?> fut,
        GridCacheQueryRequest req,
        Collection<UUID> nodes
    ) throws IgniteCheckedException {
        assert req != null;
        assert nodes != null;

        UUID locNodeId = cctx.localNodeId();

        boolean loc = false;

        for (UUID nodeId : nodes) {
            if (nodeId.equals(locNodeId))
                loc = true;
            else {
                if (req.cancel())
                    sendNodeCancelRequest(nodeId, req);
                else if (!sendNodePageRequest(nodeId, req, fut))
                    return;
            }
        }

        if (loc)
            sendLoc.accept(req);
    }

    /** */
    private void sendNodeCancelRequest(UUID nodeId, GridCacheQueryRequest req) throws IgniteCheckedException {
        try {
            cctx.io().send(nodeId, req, GridIoPolicy.QUERY_POOL);
        }
        catch (IgniteCheckedException e) {
            if (cctx.io().checkNodeLeft(nodeId, e, false)) {
                if (log.isDebugEnabled())
                    log.debug("Failed to send cancel request, node failed: " + nodeId);
            }
            else
                U.error(log, "Failed to send cancel request [node=" + nodeId + ']', e);
        }
    }

    /**
     * @return {@code true} if succeed to send request, {@code false} otherwise.
     */
    private boolean sendNodePageRequest(UUID nodeId, GridCacheQueryRequest req, GridCacheQueryFutureAdapter<?, ?, ?> fut)
        throws IgniteCheckedException {
        try {
            cctx.io().send(nodeId, req, GridIoPolicy.QUERY_POOL);

            return true;
        }
        catch (IgniteCheckedException e) {
            if (cctx.io().checkNodeLeft(nodeId, e, true)) {
                fut.onNodeLeft(nodeId);

                return false;
            }
            else
                throw e;
        }
    }
}
