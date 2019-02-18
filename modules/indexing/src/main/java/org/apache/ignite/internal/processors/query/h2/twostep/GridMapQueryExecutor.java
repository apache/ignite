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
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
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
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.GridTopic;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.managers.communication.GridMessageListener;
import org.apache.ignite.internal.managers.eventstorage.GridLocalEventListener;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.CompoundLockFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTransactionalCacheAdapter;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxLocalAdapter;
import org.apache.ignite.internal.processors.cache.mvcc.MvccSnapshot;
import org.apache.ignite.internal.processors.cache.mvcc.MvccUtils;
import org.apache.ignite.internal.processors.cache.query.CacheQueryType;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryMarshallable;
import org.apache.ignite.internal.processors.cache.query.GridCacheSqlQuery;
import org.apache.ignite.internal.processors.query.GridQueryCancel;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.h2.H2ConnectionWrapper;
import org.apache.ignite.internal.processors.query.h2.H2Utils;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.processors.query.h2.ResultSetEnlistFuture;
import org.apache.ignite.internal.processors.query.h2.UpdateResult;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2RetryException;
import org.apache.ignite.internal.processors.query.h2.opt.QueryContext;
import org.apache.ignite.internal.processors.query.h2.opt.QueryContextRegistry;
import org.apache.ignite.internal.processors.query.h2.opt.join.DistributedJoinContext;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlQueryParser;
import org.apache.ignite.internal.processors.query.h2.twostep.messages.GridQueryCancelRequest;
import org.apache.ignite.internal.processors.query.h2.twostep.messages.GridQueryFailResponse;
import org.apache.ignite.internal.processors.query.h2.twostep.messages.GridQueryNextPageRequest;
import org.apache.ignite.internal.processors.query.h2.twostep.messages.GridQueryNextPageResponse;
import org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2DmlRequest;
import org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2DmlResponse;
import org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2QueryRequest;
import org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2SelectForUpdateTxDetails;
import org.apache.ignite.internal.util.GridSpinBusyLock;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.spi.indexing.IndexingQueryFilter;
import org.h2.api.ErrorCode;
import org.h2.command.Prepared;
import org.h2.jdbc.JdbcResultSet;
import org.h2.jdbc.JdbcSQLException;
import org.h2.value.Value;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.events.EventType.EVT_CACHE_QUERY_EXECUTED;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.QUERY_POOL;
import static org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2QueryRequest.isDataPageScanEnabled;
import static org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2ValueMessageFactory.toMessages;

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

    /** */
    private final GridSpinBusyLock busyLock;

    /**
     * @param busyLock Busy lock.
     */
    public GridMapQueryExecutor(GridSpinBusyLock busyLock) {
        this.busyLock = busyLock;
    }

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

        ctx.event().addLocalEventListener(new GridLocalEventListener() {
            @Override public void onEvent(final Event evt) {
                UUID nodeId = ((DiscoveryEvent)evt).eventNode().id();

                qryCtxRegistry.clearSharedOnRemoteNodeStop(nodeId);

                MapNodeResults nodeRess = qryRess.remove(nodeId);

                if (nodeRess == null)
                    return;

                nodeRess.cancelAll();
            }
        }, EventType.EVT_NODE_FAILED, EventType.EVT_NODE_LEFT);

        ctx.io().addMessageListener(GridTopic.TOPIC_QUERY, new GridMessageListener() {
            @SuppressWarnings("deprecation")
            @Override public void onMessage(UUID nodeId, Object msg, byte plc) {
                if (!busyLock.enterBusy())
                    return;

                try {
                    if (msg instanceof GridCacheQueryMarshallable)
                        ((GridCacheQueryMarshallable)msg).unmarshall(ctx.config().getMarshaller(), ctx);

                    GridMapQueryExecutor.this.onMessage(nodeId, msg);
                }
                finally {
                    busyLock.leaveBusy();
                }
            }
        });
    }

    /**
     * Stop query map executor, cleanup resources.
     */
    public void stop() {
        for (MapNodeResults res : qryRess.values())
            res.cancelAll();
    }

    /**
     * @param nodeId Node ID.
     * @param msg Message.
     */
    public void onMessage(UUID nodeId, Object msg) {
        try {
            assert msg != null;

            ClusterNode node = ctx.discovery().node(nodeId);

            if (node == null)
                return; // Node left, ignore.

            boolean processed = true;

            if (msg instanceof GridH2QueryRequest)
                onQueryRequest(node, (GridH2QueryRequest)msg);
            else if (msg instanceof GridQueryNextPageRequest)
                onNextPageRequest(node, (GridQueryNextPageRequest)msg);
            else if (msg instanceof GridQueryCancelRequest)
                onCancel(node, (GridQueryCancelRequest)msg);
            else if (msg instanceof GridH2DmlRequest)
                onDmlRequest(node, (GridH2DmlRequest)msg);
            else
                processed = false;

            if (processed && log.isDebugEnabled())
                log.debug("Processed request: " + nodeId + "->" + ctx.localNodeId() + " " + msg);
        }
        catch(Throwable th) {
            U.error(log, "Failed to process message: " + msg, th);
        }
    }

    /**
     * @param node Node.
     * @param msg Message.
     */
    private void onCancel(ClusterNode node, GridQueryCancelRequest msg) {
        long qryReqId = msg.queryRequestId();

        MapNodeResults nodeRess = resultsForNode(node.id());

        boolean clear = qryCtxRegistry.clearShared(node.id(), qryReqId);

        if (!clear) {
            nodeRess.onCancel(qryReqId);

            qryCtxRegistry.clearShared(node.id(), qryReqId);
        }

        nodeRess.cancelRequest(qryReqId);
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
    private void onQueryRequest(final ClusterNode node, final GridH2QueryRequest req) throws IgniteCheckedException {
        int[] qryParts = req.queryPartitions();

        final Map<UUID,int[]> partsMap = req.partitions();

        final int[] parts = qryParts == null ? partsMap == null ? null : partsMap.get(ctx.localNodeId()) : qryParts;

        final GridDhtTxLocalAdapter tx;

        GridH2SelectForUpdateTxDetails txReq = req.txDetails();

        boolean distributedJoins = req.isFlagSet(GridH2QueryRequest.FLAG_DISTRIBUTED_JOINS);
        boolean enforceJoinOrder = req.isFlagSet(GridH2QueryRequest.FLAG_ENFORCE_JOIN_ORDER);
        boolean explain = req.isFlagSet(GridH2QueryRequest.FLAG_EXPLAIN);
        boolean replicated = req.isFlagSet(GridH2QueryRequest.FLAG_REPLICATED);
        final boolean lazy = req.isFlagSet(GridH2QueryRequest.FLAG_LAZY) && txReq == null;

        Boolean dataPageScanEnabled = req.isDataPageScanEnabled();

        final List<Integer> cacheIds = req.caches();

        int segments = explain || replicated || F.isEmpty(cacheIds) ? 1 :
            CU.firstPartitioned(ctx.cache().context(), cacheIds).config().getQueryParallelism();

        final Object[] params = req.parameters();

        try {
            if (txReq != null) {
                // Prepare to run queries.
                GridCacheContext<?, ?> mainCctx = mainCacheContext(cacheIds);

                if (mainCctx == null || mainCctx.atomic() || !mainCctx.mvccEnabled() || cacheIds.size() != 1)
                    throw new IgniteSQLException("SELECT FOR UPDATE is supported only for queries " +
                        "that involve single transactional cache.");

                GridDhtTransactionalCacheAdapter txCache = (GridDhtTransactionalCacheAdapter)mainCctx.cache();

                if (!node.isLocal()) {
                    tx = txCache.initTxTopologyVersion(
                        node.id(),
                        node,
                        txReq.version(),
                        txReq.futureId(),
                        txReq.miniId(),
                        txReq.firstClientRequest(),
                        req.topologyVersion(),
                        txReq.threadId(),
                        txReq.timeout(),
                        txReq.subjectId(),
                        txReq.taskNameHash(),
                        req.mvccSnapshot());
                }
                else {
                    tx = MvccUtils.tx(ctx, txReq.version());

                    assert tx != null;
                }
            }
            else
                tx = null;
        }
        catch (IgniteException | IgniteCheckedException e) {
            // send error if TX was not initialized properly.
            releaseReservations();

            U.error(log, "Failed to execute local query.", e);

            sendError(node, req.requestId(), e);

            return;
        }

        AtomicInteger runCntr;
        CompoundLockFuture lockFut;

        if (txReq != null && segments > 1) {
            runCntr = new AtomicInteger(segments);
            lockFut = new CompoundLockFuture(segments, tx);

            lockFut.init();
        }
        else {
            runCntr = null;
            lockFut = null;
        }

        for (int i = 1; i < segments; i++) {
            assert !F.isEmpty(cacheIds);

            final int segment = i;

            ctx.closure().callLocal(
                new Callable<Void>() {
                    @Override public Void call() {
                        onQueryRequest0(node,
                            req.requestId(),
                            segment,
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
                            req.timeout(),
                            params,
                            lazy,
                            req.mvccSnapshot(),
                            tx,
                            txReq,
                            lockFut,
                            runCntr,
                            dataPageScanEnabled);

                        return null;
                    }
                },
                QUERY_POOL);
        }

        onQueryRequest0(node,
            req.requestId(),
            0,
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
            req.timeout(),
            params,
            lazy,
            req.mvccSnapshot(),
            tx,
            txReq,
            lockFut,
            runCntr,
            dataPageScanEnabled);
    }

    /**
     * @param node Node authored request.
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
     * @param tx Transaction.
     * @param txDetails TX details, if it's a {@code FOR UPDATE} request, or {@code null}.
     * @param lockFut Lock future.
     * @param runCntr Counter which counts remaining queries in case segmented index is used.
     * @param dataPageScanEnabled If data page scan is enabled.
     */
    private void onQueryRequest0(
        final ClusterNode node,
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
        @Nullable final GridDhtTxLocalAdapter tx,
        @Nullable final GridH2SelectForUpdateTxDetails txDetails,
        @Nullable final CompoundLockFuture lockFut,
        @Nullable final AtomicInteger runCntr,
        Boolean dataPageScanEnabled) {
        // In presence of TX, we also must always have matching details.
        assert tx == null || txDetails != null;

        assert !lazy || txDetails == null : "Lazy execution of SELECT FOR UPDATE queries is not supported.";

        boolean inTx = (tx != null);

        // Prepare to run queries.
        GridCacheContext<?, ?> mainCctx = mainCacheContext(cacheIds);

        MapNodeResults nodeRess = resultsForNode(node.id());

        MapQueryResults qryResults = null;

        PartitionReservation reserved = null;

        try {
            // We want to reserve only in not SELECT FOR UPDATE case -
            // otherwise, their state is protected by locked topology.
            if (topVer != null && txDetails == null) {
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

            QueryContext qctx = new QueryContext(
                segmentId,
                h2.backupFilter(topVer, parts),
                distributedJoinCtx,
                mvccSnapshot,
                reserved
            );

            qryResults = new MapQueryResults(h2, reqId, qrys.size(), mainCctx, inTx, lazy, qctx);

            // qctx is set, we have to release reservations inside of it.
            reserved = null;

            qryCtxRegistry.setThreadLocal(qctx);

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
                H2ConnectionWrapper connWrp = h2.connections().connectionForThread();

                H2Utils.setupConnection(
                    connWrp.connection(schemaName),
                    distributedJoins,
                    enforceJoinOrder,
                    lazy
                );

                MapQueryResult res = new MapQueryResult(h2, mainCctx, node.id(), qry, params, connWrp, log);

                qryResults.addResult(qryIdx, res);

                try {
                    res.lock();

                    boolean removeMapping = false;
                    ResultSet rs = null;

                    // If we are not the target node for this replicated query, just ignore it.
                    if (qry.node() == null || (segmentId == 0 && qry.node().equals(ctx.localNodeId()))) {
                        String sql = qry.query();
                        Collection<Object> params0 = F.asList(qry.parameters(params));

                        PreparedStatement stmt;

                        try {
                            stmt = h2.connections().prepareStatement(connWrp.connection(), sql);
                        }
                        catch (SQLException e) {
                            throw new IgniteCheckedException("Failed to parse SQL query: " + sql, e);
                        }

                        Prepared p = GridSqlQueryParser.prepared(stmt);

                        if (GridSqlQueryParser.isForUpdateQuery(p)) {
                            sql = GridSqlQueryParser.rewriteQueryForUpdateIfNeeded(p, inTx);
                            stmt = h2.connections().prepareStatement(connWrp.connection(), sql);
                        }

                        H2Utils.bindParameters(stmt, params0);

                        int opTimeout = IgniteH2Indexing.operationTimeout(timeout, tx);

                        rs = h2.executeSqlQueryWithTimer(
                            stmt,
                            connWrp.connection(),
                            sql,
                            params0,
                            opTimeout,
                            qryResults.queryCancel(qryIdx),
                            dataPageScanEnabled);

                        int count = rs.getMetaData().getColumnCount();

                        while (rs.next()) {
                            StringBuilder s = new StringBuilder();

                            for (int i = 1; i <= count; i ++)
                                s.append(rs.getObject(i)).append(" ");

                            s.append("\n");

                            log.warning("qryId:" + qr.queryRequestId() + ", columns: " + s.toString());
                        }

                        rs.beforeFirst();

                        if (inTx) {
                            ResultSetEnlistFuture enlistFut = ResultSetEnlistFuture.future(
                                ctx.localNodeId(),
                                txDetails.version(),
                                mvccSnapshot,
                                txDetails.threadId(),
                                IgniteUuid.randomUuid(),
                                txDetails.miniId(),
                                parts,
                                tx,
                                opTimeout,
                                mainCctx,
                                rs
                            );

                            if (lockFut != null)
                                lockFut.register(enlistFut);

                            enlistFut.init();

                            enlistFut.get();

                            rs.beforeFirst();
                        }

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
                    }

                    res.openResult(rs);

                    if (qryResults.cancelled())
                        throw new QueryCancelledException();

                    if (inTx) {
                        if (tx.dht() && (runCntr == null || runCntr.decrementAndGet() == 0)) {
                            if (removeMapping = tx.empty() && !tx.queryEnlisted())
                                ctx.cache().context().tm().forgetTx(tx);
                        }
                    }

                    final GridQueryNextPageResponse msg = prepareNextPage(
                        nodeRess,
                        node,
                        qryResults,
                        qryIdx,
                        segmentId,
                        pageSize,
                        removeMapping,
                        dataPageScanEnabled
                    );

                    // Send the first page.
                    if (lockFut == null)
                        sendNextPage(node, msg);
                    else {
                        if (msg != null) {
                            lockFut.listen(new IgniteInClosure<IgniteInternalFuture<Void>>() {
                                @Override public void apply(IgniteInternalFuture<Void> future) {
                                    try {
                                        if (node.isLocal())
                                            h2.reduceQueryExecutor().onMessage(ctx.localNodeId(), msg);
                                        else
                                            ctx.io().sendToGridTopic(node, GridTopic.TOPIC_QUERY, msg, QUERY_POOL);
                                    }
                                    catch (Exception e) {
                                        U.error(log, e);
                                    }
                                }
                            });
                        }
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
            }
            else
                releaseReservations();

            if (e instanceof QueryCancelledException)
                sendError(node, reqId, e);
            else {
                JdbcSQLException sqlEx = X.cause(e, JdbcSQLException.class);

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
                            U.error(log, "Failed to execute local query.", e);

                            sendError(node, reqId, e);

                            if (e instanceof Error)
                                throw (Error)e;
                        }
                    }
                }
            }
        }
        finally {
            if (reserved != null)
                reserved.release();

            qryCtxRegistry.clearThreadLocal();
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
     */
    private void releaseReservations() {
        QueryContext qctx = qryCtxRegistry.getThreadLocal();

        if (qctx != null) { // No-op if already released.
            qryCtxRegistry.clearThreadLocal();

            if (qctx.distributedJoinContext() == null)
                qctx.clearContext(false);
        }
    }

    /**
     * @param node Node.
     * @param req DML request.
     */
    private void onDmlRequest(final ClusterNode node, final GridH2DmlRequest req) {
        int[] parts = req.queryPartitions();

        List<Integer> cacheIds = req.caches();

        long reqId = req.requestId();

        AffinityTopologyVersion topVer = req.topologyVersion();

        PartitionReservation reserved = null;

        MapNodeResults nodeResults = resultsForNode(node.id());

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
            fldsQry.setTimeout(req.timeout(), TimeUnit.MILLISECONDS);
            fldsQry.setPageSize(req.pageSize());
            fldsQry.setLocal(true);
            fldsQry.setDataPageScanEnabled(req.isDataPageScanEnabled());

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
            U.error(log, "Error processing dml request. [localNodeId=" + ctx.localNodeId() +
                ", nodeId=" + node.id() + ", req=" + req + ']', e);

            sendUpdateResponse(node, reqId, null, e.getMessage());
        }
        finally {
            if (reserved != null)
                reserved.release();

            nodeResults.removeUpdate(reqId);
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
            GridQueryFailResponse msg = new GridQueryFailResponse(qryReqId, err);

            if (node.isLocal()) {
                U.error(log, "Failed to run map query on local node.", err);

                h2.reduceQueryExecutor().onMessage(ctx.localNodeId(), msg);
            }
            else
                ctx.io().sendToGridTopic(node, GridTopic.TOPIC_QUERY, msg, QUERY_POOL);
        }
        catch (Exception e) {
            e.addSuppressed(err);

            U.error(log, "Failed to send error message.", e);
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
                h2.reduceQueryExecutor().onMessage(ctx.localNodeId(), rsp);
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
    private void onNextPageRequest(final ClusterNode node, final GridQueryNextPageRequest req) {
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
        else
            try {
                QueryContext qctxReduce = qryCtxRegistry.getThreadLocal();

                if (qctxReduce != null)
                    qryCtxRegistry.clearThreadLocal();

                qryCtxRegistry.setThreadLocal(qryResults.queryContext());

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
                        false,
                        dataPageScanEnabled);

                    sendNextPage(node, msg);
                }
                finally {
                    qryCtxRegistry.clearThreadLocal();

                    if (qctxReduce != null)
                        qryCtxRegistry.setThreadLocal(qctxReduce);

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
                    JdbcSQLException sqlEx = X.cause(e, JdbcSQLException.class);

                    if (sqlEx != null && sqlEx.getErrorCode() == ErrorCode.STATEMENT_WAS_CANCELED)
                        sendQueryCancel(node, reqId);
                    else
                        sendError(node, reqId, e);
                }

                qryResults.cancel();
            }
    }

    /**
     * @param nodeRess Results.
     * @param node Node.
     * @param qr Query results.
     * @param qry Query.
     * @param segmentId Index segment ID.
     * @param pageSize Page size.
     * @param removeMapping Remove mapping flag.
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
        boolean removeMapping,
        Boolean dataPageScanEnabled) throws IgniteCheckedException {
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

        // In case of SELECT FOR UPDATE the last columns is _KEY,
        // we can't retrieve them for an arbitrary row otherwise.
        int colsCnt = !qr.isForUpdate() ? res.columnCount() : res.columnCount() - 1;

        GridQueryNextPageResponse msg = new GridQueryNextPageResponse(qr.queryRequestId(), segmentId, qry, page,
            page == 0 ? res.rowCount() : -1,
            colsCnt,
            loc ? null : toMessages(rows, new ArrayList<>(res.columnCount()), colsCnt),
            loc ? rows : null,
            last);

        msg.removeMapping(removeMapping);

        return msg;
    }

    /**
     * @param node Node.
     * @param msg Message to send.
     */
    private void sendNextPage(ClusterNode node, GridQueryNextPageResponse msg) {
        try {
            if (msg != null) {
                if (node.isLocal())
                    h2.reduceQueryExecutor().onMessage(ctx.localNodeId(), msg);
                else
                    ctx.io().sendToGridTopic(node, GridTopic.TOPIC_QUERY, msg, QUERY_POOL);
            }
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
                h2.reduceQueryExecutor().onMessage(ctx.localNodeId(), msg);
            else
                ctx.io().sendToGridTopic(node, GridTopic.TOPIC_QUERY, msg, QUERY_POOL);
        }
        catch (Exception e) {
            U.warn(log, "Failed to send retry message: " + e.getMessage());
        }
    }
}
