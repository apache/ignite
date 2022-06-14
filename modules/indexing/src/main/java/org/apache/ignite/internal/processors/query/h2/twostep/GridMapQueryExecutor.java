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

package org.apache.ignite.internal.processors.query.h2.twostep;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.cache.CacheException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.query.QueryCancelledException;
import org.apache.ignite.cache.query.QueryRetryException;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.CacheQueryExecutedEvent;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.GridTopic;
import org.apache.ignite.internal.metric.IoStatisticsHolder;
import org.apache.ignite.internal.metric.IoStatisticsQueryHelper;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.mvcc.MvccSnapshot;
import org.apache.ignite.internal.processors.cache.query.CacheQueryType;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryType;
import org.apache.ignite.internal.processors.cache.query.GridCacheSqlQuery;
import org.apache.ignite.internal.processors.query.GridQueryCancel;
import org.apache.ignite.internal.processors.query.h2.H2PooledConnection;
import org.apache.ignite.internal.processors.query.h2.H2StatementCache;
import org.apache.ignite.internal.processors.query.h2.H2Utils;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.processors.query.h2.MapH2QueryInfo;
import org.apache.ignite.internal.processors.query.h2.UpdateResult;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2RetryException;
import org.apache.ignite.internal.processors.query.h2.opt.QueryContext;
import org.apache.ignite.internal.processors.query.h2.opt.QueryContextRegistry;
import org.apache.ignite.internal.processors.query.h2.opt.join.DistributedJoinContext;
import org.apache.ignite.internal.processors.query.h2.twostep.messages.GridQueryCancelRequest;
import org.apache.ignite.internal.processors.query.h2.twostep.messages.GridQueryFailResponse;
import org.apache.ignite.internal.processors.query.h2.twostep.messages.GridQueryNextPageRequest;
import org.apache.ignite.internal.processors.query.h2.twostep.messages.GridQueryNextPageResponse;
import org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2DmlRequest;
import org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2DmlResponse;
import org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2QueryRequest;
import org.apache.ignite.internal.processors.tracing.MTC;
import org.apache.ignite.internal.processors.tracing.MTC.TraceSurroundings;
import org.apache.ignite.internal.processors.tracing.Span;
import org.apache.ignite.internal.processors.tracing.SpanType;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.indexing.IndexingQueryFilter;
import org.h2.api.ErrorCode;
import org.h2.jdbc.JdbcResultSet;
import org.h2.value.Value;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.events.EventType.EVT_CACHE_QUERY_EXECUTED;
import static org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndexImpl.calculateSegment;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.QUERY_POOL;
import static org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2QueryRequest.isDataPageScanEnabled;
import static org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2ValueMessageFactory.toMessages;
import static org.apache.ignite.internal.processors.tracing.SpanTags.ERROR;
import static org.apache.ignite.internal.processors.tracing.SpanTags.SQL_PAGE_ROWS;
import static org.apache.ignite.internal.processors.tracing.SpanTags.SQL_QRY_TEXT;
import static org.apache.ignite.internal.processors.tracing.SpanType.SQL_NEXT_PAGE_REQ;
import static org.apache.ignite.internal.processors.tracing.SpanType.SQL_PAGE_PREPARE;
import static org.apache.ignite.internal.processors.tracing.SpanType.SQL_QRY_CANCEL_REQ;
import static org.apache.ignite.internal.processors.tracing.SpanType.SQL_QRY_EXEC_REQ;

/**
 * Map query executor.
 */
@SuppressWarnings("ForLoopReplaceableByForEach")
public class GridMapQueryExecutor {
    /** */
    private IgniteLogger log;

    /** */
    private GridKernalContext ctx;

    /** */
    private IgniteH2Indexing h2;

    /** Query context registry. */
    private QueryContextRegistry qryCtxRegistry;

    /** */
    private ConcurrentMap<UUID, MapNodeResults> qryRess = new ConcurrentHashMap<>();

    /**
     * @param ctx Context.
     * @param h2 H2 Indexing.
     * @throws IgniteCheckedException If failed.
     */
    public void start(final GridKernalContext ctx, IgniteH2Indexing h2) throws IgniteCheckedException {
        this.ctx = ctx;
        this.h2 = h2;

        qryCtxRegistry = h2.queryContextRegistry();

        log = ctx.log(GridMapQueryExecutor.class);
    }

    /**
     * Node left event handling method..
     * @param evt Discovery event.
     */
    public void onNodeLeft(DiscoveryEvent evt) {
        UUID nodeId = evt.eventNode().id();

        qryCtxRegistry.clearSharedOnRemoteNodeStop(nodeId);

        MapNodeResults nodeRess = qryRess.remove(nodeId);

        if (nodeRess == null)
            return;

        nodeRess.cancelAll();
    }

    /**
     * Stop query map executor, cleanup resources.
     */
    public void stop() {
        for (MapNodeResults res : qryRess.values())
            res.cancelAll();
    }

    /**
     * @param node Node.
     * @param msg Message.
     */
    public void onCancel(ClusterNode node, GridQueryCancelRequest msg) {
        try (TraceSurroundings ignored = MTC.support(ctx.tracing().create(SQL_QRY_CANCEL_REQ, MTC.span()))) {
            long qryReqId = msg.queryRequestId();

            MapNodeResults nodeRess = resultsForNode(node.id());

            boolean clear = qryCtxRegistry.clearShared(node.id(), qryReqId);

            if (!clear) {
                nodeRess.onCancel(qryReqId);

                qryCtxRegistry.clearShared(node.id(), qryReqId);
            }

            nodeRess.cancelRequest(qryReqId);
        }
    }

    /**
     * @param nodeId Node ID.
     * @return Results for node.
     */
    private MapNodeResults resultsForNode(UUID nodeId) {
        MapNodeResults nodeRess = qryRess.get(nodeId);

        if (nodeRess == null) {
            nodeRess = new MapNodeResults(nodeId);

            MapNodeResults old = qryRess.putIfAbsent(nodeId, nodeRess);

            if (old != null)
                nodeRess = old;
        }

        return nodeRess;
    }

    /**
     * @param node Node.
     * @param req Query request.
     * @throws IgniteCheckedException On error.
     */
    public void onQueryRequest(final ClusterNode node, final GridH2QueryRequest req) throws IgniteCheckedException {
        int[] qryParts = req.queryPartitions();

        final Map<UUID, int[]> partsMap = req.partitions();

        final int[] parts = qryParts == null ? (partsMap == null ? null : partsMap.get(ctx.localNodeId())) : qryParts;

        boolean distributedJoins = req.isFlagSet(GridH2QueryRequest.FLAG_DISTRIBUTED_JOINS);
        boolean enforceJoinOrder = req.isFlagSet(GridH2QueryRequest.FLAG_ENFORCE_JOIN_ORDER);
        boolean explain = req.isFlagSet(GridH2QueryRequest.FLAG_EXPLAIN);
        boolean replicated = req.isFlagSet(GridH2QueryRequest.FLAG_REPLICATED);
        final boolean lazy = req.isFlagSet(GridH2QueryRequest.FLAG_LAZY);
        boolean treatReplicatedAsPartitioned = req.isFlagSet(GridH2QueryRequest.FLAG_REPLICATED_AS_PARTITIONED);

        try {
            Boolean dataPageScanEnabled = req.isDataPageScanEnabled();

            final List<Integer> cacheIds = req.caches();

            final int parallelism = explain || replicated || F.isEmpty(cacheIds) ? 1 :
                CU.firstPartitioned(ctx.cache().context(), cacheIds).config().getQueryParallelism();

            BitSet segments = new BitSet(parallelism);

            if (parts != null) {
                for (int i = 0; i < parts.length; i++)
                    segments.set(calculateSegment(parallelism, parts[i]));
            }
            else
                segments.set(0, parallelism);

            final Object[] params = req.parameters();

            final int timeout = req.timeout() > 0 || req.explicitTimeout()
                ? req.timeout()
                : (int)h2.distributedConfiguration().defaultQueryTimeout();

            int firstSegment = segments.nextSetBit(0);
            int segment = firstSegment;
            while ((segment = segments.nextSetBit(segment + 1)) != -1) {
                assert !F.isEmpty(cacheIds);

                final int segment0 = segment;

                Span span = MTC.span();

                ctx.closure().runLocal(
                    () -> {
                        try (TraceSurroundings ignored = MTC.supportContinual(span)) {
                            onQueryRequest0(node,
                                req.queryId(),
                                req.requestId(),
                                segment0,
                                req.schemaName(),
                                req.queries(),
                                cacheIds,
                                req.topologyVersion(),
                                partsMap,
                                parts,
                                req.pageSize(),
                                distributedJoins,
                                enforceJoinOrder,
                                false,
                                timeout,
                                params,
                                lazy,
                                req.mvccSnapshot(),
                                dataPageScanEnabled,
                                treatReplicatedAsPartitioned
                            );
                        }
                        catch (Throwable e) {
                            sendError(node, req.requestId(), e);
                        }
                    },
                    QUERY_POOL);
            }

            onQueryRequest0(node,
                req.queryId(),
                req.requestId(),
                firstSegment,
                req.schemaName(),
                req.queries(),
                cacheIds,
                req.topologyVersion(),
                partsMap,
                parts,
                req.pageSize(),
                distributedJoins,
                enforceJoinOrder,
                replicated,
                timeout,
                params,
                lazy,
                req.mvccSnapshot(),
                dataPageScanEnabled,
                treatReplicatedAsPartitioned
            );
        }
        catch (Throwable e) {
            sendError(node, req.requestId(), e);
        }
    }

    /**
     * @param node Node authored request.
     * @param qryId Query ID.
     * @param reqId Request ID.
     * @param segmentId index segment ID.
     * @param schemaName Schema name.
     * @param qrys Queries to execute.
     * @param cacheIds Caches which will be affected by these queries.
     * @param topVer Topology version.
     * @param partsMap Partitions map for unstable topology.
     * @param parts Explicit partitions for current node.
     * @param pageSize Page size.
     * @param distributedJoins Query distributed join mode.
     * @param enforceJoinOrder Enforce join order H2 flag.
     * @param replicated Replicated only flag.
     * @param timeout Query timeout.
     * @param params Query parameters.
     * @param lazy Streaming flag.
     * @param mvccSnapshot MVCC snapshot.
     * @param dataPageScanEnabled If data page scan is enabled.
     */
    private void onQueryRequest0(
        final ClusterNode node,
        final long qryId,
        final long reqId,
        final int segmentId,
        final String schemaName,
        final Collection<GridCacheSqlQuery> qrys,
        final List<Integer> cacheIds,
        final AffinityTopologyVersion topVer,
        final Map<UUID, int[]> partsMap,
        final int[] parts,
        final int pageSize,
        final boolean distributedJoins,
        final boolean enforceJoinOrder,
        final boolean replicated,
        final int timeout,
        final Object[] params,
        boolean lazy,
        @Nullable final MvccSnapshot mvccSnapshot,
        Boolean dataPageScanEnabled,
        boolean treatReplicatedAsPartitioned
    ) {
        boolean performanceStatsEnabled = ctx.performanceStatistics().enabled();

        if (performanceStatsEnabled)
            IoStatisticsQueryHelper.startGatheringQueryStatistics();

        // Prepare to run queries.
        GridCacheContext<?, ?> mainCctx = mainCacheContext(cacheIds);

        MapNodeResults nodeRess = resultsForNode(node.id());

        MapQueryResults qryResults = null;

        PartitionReservation reserved = null;

        QueryContext qctx = null;

        // We don't use try with resources on purpose - the catch block must also be executed in the context of this span.
        TraceSurroundings trace = MTC.support(ctx.tracing()
            .create(SQL_QRY_EXEC_REQ, MTC.span())
            .addTag(SQL_QRY_TEXT, () ->
                qrys.stream().map(GridCacheSqlQuery::query).collect(Collectors.joining("; "))));

        try {
            if (topVer != null) {
                // Reserve primary for topology version or explicit partitions.
                reserved = h2.partitionReservationManager().reservePartitions(
                    cacheIds,
                    topVer,
                    parts,
                    node.id(),
                    reqId
                );

                if (reserved.failed()) {
                    sendRetry(node, reqId, segmentId, reserved.error());

                    return;
                }
            }

            // Prepare query context.
            DistributedJoinContext distributedJoinCtx = null;

            if (distributedJoins && !replicated) {
                distributedJoinCtx = new DistributedJoinContext(
                    topVer,
                    partsMap,
                    node.id(),
                    reqId,
                    segmentId,
                    pageSize
                );
            }

            qctx = new QueryContext(
                segmentId,
                h2.backupFilter(topVer, parts, treatReplicatedAsPartitioned),
                distributedJoinCtx,
                mvccSnapshot,
                reserved,
                true);

            qryResults = new MapQueryResults(h2, reqId, qrys.size(), mainCctx, lazy, qctx);

            // qctx is set, we have to release reservations inside of it.
            reserved = null;

            if (distributedJoinCtx != null)
                qryCtxRegistry.setShared(node.id(), reqId, qctx);

            if (nodeRess.put(reqId, segmentId, qryResults) != null)
                throw new IllegalStateException();

            if (nodeRess.cancelled(reqId)) {
                qryCtxRegistry.clearShared(node.id(), reqId);

                nodeRess.cancelRequest(reqId);

                throw new QueryCancelledException();
            }

            // Run queries.
            int qryIdx = 0;

            boolean evt = mainCctx != null && mainCctx.events().isRecordable(EVT_CACHE_QUERY_EXECUTED);

            for (GridCacheSqlQuery qry : qrys) {
                H2PooledConnection conn = h2.connections().connection(schemaName);

                H2Utils.setupConnection(
                    conn,
                    qctx,
                    distributedJoins,
                    enforceJoinOrder,
                    lazy
                );

                MapQueryResult res = new MapQueryResult(h2, mainCctx, node.id(), qry, params, conn, log);

                qryResults.addResult(qryIdx, res);

                try {
                    res.lock();

                    // Ensure we are on the target node for this replicated query.
                    if (qry.node() == null || (segmentId == 0 && qry.node().equals(ctx.localNodeId()))) {
                        String sql = qry.query();
                        Collection<Object> params0 = F.asList(qry.parameters(params));

                        PreparedStatement stmt = conn.prepareStatement(sql, H2StatementCache.queryFlags(
                            distributedJoins,
                            enforceJoinOrder));

                        H2Utils.bindParameters(stmt, params0);

                        MapH2QueryInfo qryInfo = new MapH2QueryInfo(stmt, qry.query(), node.id(), qryId, reqId, segmentId);

                        ResultSet rs = h2.executeSqlQueryWithTimer(
                            stmt,
                            conn,
                            sql,
                            timeout,
                            qryResults.queryCancel(qryIdx),
                            dataPageScanEnabled,
                            qryInfo);

                        if (evt) {
                            ctx.event().record(new CacheQueryExecutedEvent<>(
                                node,
                                "SQL query executed.",
                                EVT_CACHE_QUERY_EXECUTED,
                                CacheQueryType.SQL.name(),
                                mainCctx.name(),
                                null,
                                qry.query(),
                                null,
                                null,
                                params,
                                node.id(),
                                null));
                        }

                        assert rs instanceof JdbcResultSet : rs.getClass();

                        if (qryResults.cancelled()) {
                            rs.close();

                            throw new QueryCancelledException();
                        }

                        res.openResult(rs, qryInfo);

                        final GridQueryNextPageResponse msg = prepareNextPage(
                            nodeRess,
                            node,
                            qryResults,
                            qryIdx,
                            segmentId,
                            pageSize,
                            dataPageScanEnabled
                        );

                        if (msg != null)
                            sendNextPage(node, msg);
                    }
                    else {
                        assert !qry.isPartitioned();

                        qryResults.closeResult(qryIdx);
                    }

                    qryIdx++;
                }
                finally {
                    try {
                        res.unlockTables();
                    }
                    finally {
                        res.unlock();
                    }
                }
            } // for map queries

            if (!lazy)
                qryResults.releaseQueryContext();
        }
        catch (Throwable e) {
            if (qryResults != null) {
                nodeRess.remove(reqId, segmentId, qryResults);

                qryResults.close();

                // If a query is cancelled before execution is started partitions have to be released.
                if (!lazy || !qryResults.isAllClosed())
                    qryResults.releaseQueryContext();
            }
            else
                releaseReservations(qctx);

            if (e instanceof QueryCancelledException)
                sendError(node, reqId, e);
            else {
                SQLException sqlEx = X.cause(e, SQLException.class);

                if (sqlEx != null && sqlEx.getErrorCode() == ErrorCode.STATEMENT_WAS_CANCELED)
                    sendQueryCancel(node, reqId);
                else {
                    GridH2RetryException retryErr = X.cause(e, GridH2RetryException.class);

                    if (retryErr != null) {
                        final String retryCause = String.format(
                            "Failed to execute non-collocated query (will retry) [localNodeId=%s, rmtNodeId=%s, reqId=%s, " +
                                "errMsg=%s]", ctx.localNodeId(), node.id(), reqId, retryErr.getMessage()
                        );

                        sendRetry(node, reqId, segmentId, retryCause);
                    }
                    else {
                        QueryRetryException qryRetryErr = X.cause(e, QueryRetryException.class);

                        if (qryRetryErr != null)
                            sendError(node, reqId, qryRetryErr);
                        else {
                            if (e instanceof Error) {
                                U.error(log, "Failed to execute local query.", e);

                                throw (Error)e;
                            }

                            U.warn(log, "Failed to execute local query.", e);

                            sendError(node, reqId, e);
                        }
                    }
                }
            }
        }
        finally {
            if (reserved != null)
                reserved.release();

            if (trace != null)
                trace.close();

            if (performanceStatsEnabled) {
                IoStatisticsHolder stat = IoStatisticsQueryHelper.finishGatheringQueryStatistics();

                if (stat.logicalReads() > 0 || stat.physicalReads() > 0) {
                    ctx.performanceStatistics().queryReads(
                        GridCacheQueryType.SQL_FIELDS,
                        node.id(),
                        reqId,
                        stat.logicalReads(),
                        stat.physicalReads());
                }
            }
        }
    }

    /**
     * @param cacheIds Cache ids.
     * @return Id of the first cache in list, or {@code null} if list is empty.
     */
    private GridCacheContext mainCacheContext(List<Integer> cacheIds) {
        return !F.isEmpty(cacheIds) ? ctx.cache().context().cacheContext(cacheIds.get(0)) : null;
    }

    /**
     * Releases reserved partitions.
     *
     * @param qctx Query context.
     */
    private void releaseReservations(QueryContext qctx) {
        if (qctx != null) {
            if (qctx.distributedJoinContext() == null)
                qctx.clearContext(false);
        }
    }

    /**
     * @param node Node.
     * @param req DML request.
     */
    public void onDmlRequest(final ClusterNode node, final GridH2DmlRequest req) {
        int[] parts = req.queryPartitions();

        List<Integer> cacheIds = req.caches();

        long reqId = req.requestId();

        AffinityTopologyVersion topVer = req.topologyVersion();

        PartitionReservation reserved = null;

        MapNodeResults nodeResults = resultsForNode(node.id());

        // We don't use try with resources on purpose - the catch block must also be executed in the context of this span.
        TraceSurroundings trace = MTC.support(ctx.tracing()
            .create(SpanType.SQL_DML_QRY_EXEC_REQ, MTC.span())
            .addTag(SQL_QRY_TEXT, req::query));

        try {
            reserved = h2.partitionReservationManager().reservePartitions(
                cacheIds,
                topVer,
                parts,
                node.id(),
                reqId
            );

            if (reserved.failed()) {
                U.error(log, "Failed to reserve partitions for DML request. [localNodeId=" + ctx.localNodeId() +
                    ", nodeId=" + node.id() + ", reqId=" + req.requestId() + ", cacheIds=" + cacheIds +
                    ", topVer=" + topVer + ", parts=" + Arrays.toString(parts) + ']');

                sendUpdateResponse(node, reqId, null,
                    "Failed to reserve partitions for DML request. " + reserved.error());

                return;
            }

            IndexingQueryFilter filter = h2.backupFilter(topVer, parts);

            GridQueryCancel cancel = nodeResults.putUpdate(reqId);

            SqlFieldsQuery fldsQry = new SqlFieldsQuery(req.query());

            if (req.parameters() != null)
                fldsQry.setArgs(req.parameters());

            fldsQry.setEnforceJoinOrder(req.isFlagSet(GridH2QueryRequest.FLAG_ENFORCE_JOIN_ORDER));
            fldsQry.setPageSize(req.pageSize());
            fldsQry.setLocal(true);

            if (req.timeout() > 0 || req.explicitTimeout())
                fldsQry.setTimeout(req.timeout(), TimeUnit.MILLISECONDS);

            boolean local = true;

            final boolean replicated = req.isFlagSet(GridH2QueryRequest.FLAG_REPLICATED);

            if (!replicated && !F.isEmpty(cacheIds) &&
                CU.firstPartitioned(ctx.cache().context(), cacheIds).config().getQueryParallelism() > 1) {
                fldsQry.setDistributedJoins(true);

                local = false;
            }

            UpdateResult updRes = h2.executeUpdateOnDataNode(req.schemaName(), fldsQry, filter, cancel, local);

            GridCacheContext<?, ?> mainCctx =
                !F.isEmpty(cacheIds) ? ctx.cache().context().cacheContext(cacheIds.get(0)) : null;

            boolean evt = local && mainCctx != null && mainCctx.events().isRecordable(EVT_CACHE_QUERY_EXECUTED);

            if (evt) {
                ctx.event().record(new CacheQueryExecutedEvent<>(
                    node,
                    "SQL query executed.",
                    EVT_CACHE_QUERY_EXECUTED,
                    CacheQueryType.SQL.name(),
                    mainCctx.name(),
                    null,
                    req.query(),
                    null,
                    null,
                    req.parameters(),
                    node.id(),
                    null));
            }

            sendUpdateResponse(node, reqId, updRes, null);
        }
        catch (Exception e) {
            MTC.span().addTag(ERROR, e::getMessage);

            U.error(log, "Error processing dml request. [localNodeId=" + ctx.localNodeId() +
                ", nodeId=" + node.id() + ", req=" + req + ']', e);

            sendUpdateResponse(node, reqId, null, e.getMessage());
        }
        finally {
            if (reserved != null)
                reserved.release();

            nodeResults.removeUpdate(reqId);

            if (trace != null)
                trace.close();
        }
    }

    /**
     * @param node Node.
     * @param qryReqId Query request ID.
     */
    private void sendQueryCancel(ClusterNode node, long qryReqId) {
        sendError(node, qryReqId, new QueryCancelledException());
    }

    /**
     * @param node Node.
     * @param qryReqId Query request ID.
     * @param err Error.
     */
    private void sendError(ClusterNode node, long qryReqId, Throwable err) {
        try {
            MTC.span().addTag(ERROR, err::getMessage);

            GridQueryFailResponse msg = new GridQueryFailResponse(qryReqId, err);

            if (node.isLocal()) {
                if (err instanceof QueryCancelledException) {
                    String errMsg = "Failed to run cancelled map query on local node: [localNodeId="
                        + node.id() + ", reqId=" + qryReqId + ']';

                    if (log.isDebugEnabled())
                        U.warn(log, errMsg, err);
                    else if (log.isInfoEnabled())
                        log.info(errMsg);
                }
                else
                    U.error(log, "Failed to run map query on local node.", err);

                h2.reduceQueryExecutor().onFail(node, msg);
            }
            else
                ctx.io().sendToGridTopic(node, GridTopic.TOPIC_QUERY, msg, QUERY_POOL);
        }
        catch (Exception e) {
            e.addSuppressed(err);

            String messageForLog = "Failed to send error message";

            if (node.isClient()) {
                if (log.isDebugEnabled())
                    log.debug(messageForLog + U.nl() + X.getFullStackTrace(e));
            }
            else
                U.error(log, messageForLog, e);
        }
    }

    /**
     * Sends update response for DML request.
     *
     * @param node Node.
     * @param reqId Request id.
     * @param updResult Update result.
     * @param error Error message.
     */
    @SuppressWarnings("deprecation")
    private void sendUpdateResponse(ClusterNode node, long reqId, UpdateResult updResult, String error) {
        try {
            GridH2DmlResponse rsp = new GridH2DmlResponse(reqId, updResult == null ? 0 : updResult.counter(),
                updResult == null ? null : updResult.errorKeys(), error);

            if (log.isDebugEnabled())
                log.debug("Sending: [localNodeId=" + ctx.localNodeId() + ", node=" + node.id() + ", msg=" + rsp + "]");

            if (node.isLocal())
                h2.reduceQueryExecutor().onDmlResponse(node, rsp);
            else {
                rsp.marshall(ctx.config().getMarshaller());

                ctx.io().sendToGridTopic(node, GridTopic.TOPIC_QUERY, rsp, QUERY_POOL);
            }
        }
        catch (Exception e) {
            U.error(log, "Failed to send message.", e);
        }
    }

    /**
     * @param node Node.
     * @param req Request.
     */
    public void onNextPageRequest(final ClusterNode node, final GridQueryNextPageRequest req) {
        try (TraceSurroundings ignored = MTC.support(ctx.tracing().create(SQL_NEXT_PAGE_REQ, MTC.span()))) {
            long reqId = req.queryRequestId();

            final MapNodeResults nodeRess = qryRess.get(node.id());

            if (nodeRess == null) {
                sendError(node, reqId, new CacheException("No node result found for request: " + req));

                return;
            }
            else if (nodeRess.cancelled(reqId)) {
                sendQueryCancel(node, reqId);

                return;
            }

            final MapQueryResults qryResults = nodeRess.get(reqId, req.segmentId());

            if (qryResults == null)
                sendError(node, reqId, new CacheException("No query result found for request: " + req));
            else if (qryResults.cancelled())
                sendQueryCancel(node, reqId);
            else {
                try {
                    MapQueryResult res = qryResults.result(req.query());

                    assert res != null;

                    try {
                        // Session isn't set for lazy=false queries.
                        // Also session == null when result already closed.
                        res.lock();
                        res.lockTables();
                        res.checkTablesVersions();

                        Boolean dataPageScanEnabled = isDataPageScanEnabled(req.getFlags());

                        GridQueryNextPageResponse msg = prepareNextPage(
                            nodeRess,
                            node,
                            qryResults,
                            req.query(),
                            req.segmentId(),
                            req.pageSize(),
                            dataPageScanEnabled);

                        if (msg != null)
                            sendNextPage(node, msg);
                    }
                    finally {
                        try {
                            res.unlockTables();
                        }
                        finally {
                            res.unlock();
                        }
                    }
                }
                catch (Exception e) {
                    QueryRetryException retryEx = X.cause(e, QueryRetryException.class);

                    if (retryEx != null)
                        sendError(node, reqId, retryEx);
                    else {
                        SQLException sqlEx = X.cause(e, SQLException.class);

                        if (sqlEx != null && sqlEx.getErrorCode() == ErrorCode.STATEMENT_WAS_CANCELED)
                            sendQueryCancel(node, reqId);
                        else
                            sendError(node, reqId, e);
                    }

                    qryResults.cancel();
                }
            }
        }
    }

    /**
     * @param nodeRess Results.
     * @param node Node.
     * @param qr Query results.
     * @param qry Query.
     * @param segmentId Index segment ID.
     * @param pageSize Page size.
     * @param dataPageScanEnabled If data page scan is enabled.
     * @return Next page.
     * @throws IgniteCheckedException If failed.
     */
    private GridQueryNextPageResponse prepareNextPage(
        MapNodeResults nodeRess,
        ClusterNode node,
        MapQueryResults qr,
        int qry,
        int segmentId,
        int pageSize,
        Boolean dataPageScanEnabled) throws IgniteCheckedException {
        try (TraceSurroundings ignored = MTC.support(ctx.tracing().create(SQL_PAGE_PREPARE, MTC.span()))) {
            MapQueryResult res = qr.result(qry);

            assert res != null;

            if (res.closed())
                return null;

            int page = res.page();

            List<Value[]> rows = new ArrayList<>(Math.min(64, pageSize));

            boolean last = res.fetchNextPage(rows, pageSize, dataPageScanEnabled);

            if (last) {
                qr.closeResult(qry);

                if (qr.isAllClosed()) {
                    nodeRess.remove(qr.queryRequestId(), segmentId, qr);

                    // Clear context, release reservations
                    if (qr.isLazy())
                        qr.releaseQueryContext();
                }
            }

            boolean loc = node.isLocal();

            GridQueryNextPageResponse msg = new GridQueryNextPageResponse(qr.queryRequestId(), segmentId, qry, page,
                page == 0 ? res.rowCount() : -1,
                res.columnCount(),
                loc ? null : toMessages(rows, new ArrayList<>(res.columnCount()), res.columnCount()),
                loc ? rows : null,
                last);

            MTC.span().addTag(SQL_PAGE_ROWS, () -> String.valueOf(rows.size()));

            return msg;
        }
    }

    /**
     * @param node Node.
     * @param msg Message to send.
     */
    private void sendNextPage(@NotNull ClusterNode node, @NotNull GridQueryNextPageResponse msg) {
        assert msg != null;

        try {
            if (node.isLocal())
                h2.reduceQueryExecutor().onNextPage(node, msg);
            else
                ctx.io().sendToGridTopic(node, GridTopic.TOPIC_QUERY, msg, QUERY_POOL);
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to send message.", e);

            throw new IgniteException(e);
        }
    }

    /**
     * @param node Node.
     * @param reqId Request ID.
     * @param segmentId Index segment ID.
     * @param retryCause Description of the retry cause.
     */
    private void sendRetry(ClusterNode node, long reqId, int segmentId, String retryCause) {
        try {
            boolean loc = node.isLocal();

            GridQueryNextPageResponse msg = new GridQueryNextPageResponse(reqId, segmentId,
            /*qry*/0, /*page*/0, /*allRows*/0, /*cols*/1,
                loc ? null : Collections.emptyList(),
                loc ? Collections.<Value[]>emptyList() : null,
                false);

            msg.retry(h2.readyTopologyVersion());
            msg.retryCause(retryCause);

            if (loc)
                h2.reduceQueryExecutor().onNextPage(node, msg);
            else
                ctx.io().sendToGridTopic(node, GridTopic.TOPIC_QUERY, msg, QUERY_POOL);
        }
        catch (Exception e) {
            U.warn(log, "Failed to send retry message: " + e.getMessage());
        }
    }
}
