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
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import javax.cache.CacheException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteClientDisconnectedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.query.Query;
import org.apache.ignite.cache.query.QueryCancelledException;
import org.apache.ignite.cache.query.QueryRetryException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.GridTopic;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.mvcc.MvccQueryTracker;
import org.apache.ignite.internal.processors.cache.query.GridCacheSqlQuery;
import org.apache.ignite.internal.processors.cache.query.GridCacheTwoStepQuery;
import org.apache.ignite.internal.processors.query.GridQueryCacheObjectsIterator;
import org.apache.ignite.internal.processors.query.GridQueryCancel;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.h2.H2FieldsIterator;
import org.apache.ignite.internal.processors.query.h2.H2PooledConnection;
import org.apache.ignite.internal.processors.query.h2.H2Utils;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.processors.query.h2.ReduceH2QueryInfo;
import org.apache.ignite.internal.processors.query.h2.UpdateResult;
import org.apache.ignite.internal.processors.query.h2.dml.DmlDistributedUpdateRun;
import org.apache.ignite.internal.processors.query.h2.opt.QueryContext;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlSortColumn;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlType;
import org.apache.ignite.internal.processors.query.h2.twostep.messages.GridQueryCancelRequest;
import org.apache.ignite.internal.processors.query.h2.twostep.messages.GridQueryFailResponse;
import org.apache.ignite.internal.processors.query.h2.twostep.messages.GridQueryNextPageRequest;
import org.apache.ignite.internal.processors.query.h2.twostep.messages.GridQueryNextPageResponse;
import org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2DmlRequest;
import org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2DmlResponse;
import org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2QueryRequest;
import org.apache.ignite.internal.transactions.IgniteTxAlreadyCompletedCheckedException;
import org.apache.ignite.internal.util.typedef.C2;
import org.apache.ignite.internal.util.typedef.CIX2;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiClosure;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.transactions.TransactionAlreadyCompletedException;
import org.apache.ignite.transactions.TransactionException;
import org.h2.command.ddl.CreateTableData;
import org.h2.engine.Session;
import org.h2.index.Index;
import org.h2.table.Column;
import org.h2.util.IntArray;
import org.h2.value.Value;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static java.util.Collections.singletonList;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_SQL_RETRY_TIMEOUT;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.checkActive;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.tx;
import static org.apache.ignite.internal.processors.cache.query.GridCacheSqlQuery.EMPTY_PARAMS;
import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlQuerySplitter.mergeTableIdentifier;

/**
 * Reduce query executor.
 */
@SuppressWarnings("IfMayBeConditional")
public class GridReduceQueryExecutor {
    /** Default retry timeout. */
    public static final long DFLT_RETRY_TIMEOUT = 30_000L;

    /** */
    private static final String MERGE_INDEX_UNSORTED = "merge_scan";

    /** */
    private static final String MERGE_INDEX_SORTED = "merge_sorted";

    /** */
    private GridKernalContext ctx;

    /** */
    private IgniteH2Indexing h2;

    /** */
    private IgniteLogger log;

    /** */
    private final AtomicLong qryIdGen = new AtomicLong();

    /** */
    private final ConcurrentMap<Long, ReduceQueryRun> runs = new ConcurrentHashMap<>();

    /** Contexts of running DML requests. */
    private final ConcurrentMap<Long, DmlDistributedUpdateRun> updRuns = new ConcurrentHashMap<>();

    /** */
    private volatile List<ReduceTableWrapper> fakeTbls = Collections.emptyList();

    /** */
    private final Lock fakeTblsLock = new ReentrantLock();

    /** */
    private final CIX2<ClusterNode,Message> locNodeHnd = new CIX2<ClusterNode,Message>() {
        @Override public void applyx(ClusterNode locNode, Message msg) {
            assert msg instanceof GridQueryNextPageRequest || msg instanceof GridH2QueryRequest ||
                msg instanceof GridH2DmlRequest || msg instanceof GridQueryCancelRequest : msg.getClass();

            h2.onMessage(locNode.id(), msg);
        }
    };

    /** Default query timeout. */
    private final long dfltQueryTimeout = IgniteSystemProperties.getLong(IGNITE_SQL_RETRY_TIMEOUT, DFLT_RETRY_TIMEOUT);

    /** Partition mapper. */
    private ReducePartitionMapper mapper;

    /**
     * @param ctx Context.
     * @param h2 H2 Indexing.
     * @throws IgniteCheckedException If failed.
     */
    public void start(final GridKernalContext ctx, final IgniteH2Indexing h2) throws IgniteCheckedException {
        this.ctx = ctx;
        this.h2 = h2;

        log = ctx.log(GridReduceQueryExecutor.class);

        mapper = new ReducePartitionMapper(ctx, log);
    }

    /**
     * Node left event handling method..
     * @param evt Discovery event.
     */
    public void onNodeLeft(DiscoveryEvent evt) {
        UUID nodeId = evt.eventNode().id();

        for (ReduceQueryRun r : runs.values()) {
            for (Reducer idx : r.reducers()) {
                if (idx.hasSource(nodeId)) {
                    handleNodeLeft(r, nodeId);

                    break;
                }
            }
        }

        for (DmlDistributedUpdateRun r : updRuns.values())
            r.handleNodeLeft(nodeId);
    }

    /**
     * @param r Query run.
     * @param nodeId Left node ID.
     */
    private void handleNodeLeft(ReduceQueryRun r, UUID nodeId) {
        r.setStateOnNodeLeave(nodeId, h2.readyTopologyVersion());
    }

    /**
     * @param node Node.
     * @param msg Message.
     */
    public void onFail(ClusterNode node, GridQueryFailResponse msg) {
        ReduceQueryRun r = runs.get(msg.queryRequestId());

        fail(r, node.id(), msg.error(), msg.failCode());
    }

    /**
     * @param r Query run.
     * @param nodeId Failed node ID.
     * @param msg Error message.
     */
    private void fail(ReduceQueryRun r, UUID nodeId, String msg, byte failCode) {
        if (r != null) {
            CacheException e;

            String mapperFailedMsg = "Failed to execute map query on remote node [nodeId=" + nodeId +
                ", errMsg=" + msg + ']';

            if (failCode == GridQueryFailResponse.CANCELLED_BY_ORIGINATOR)
                e = new CacheException(mapperFailedMsg, new QueryCancelledException());
            else if (failCode == GridQueryFailResponse.RETRY_QUERY)
                e = new CacheException(mapperFailedMsg, new QueryRetryException(msg));
            else
                e = new CacheException(mapperFailedMsg);

            r.setStateOnException(nodeId, e);
        }
    }

    /**
     * @param node Node.
     * @param msg Message.
     */
    public void onNextPage(final ClusterNode node, final GridQueryNextPageResponse msg) {
        final long qryReqId = msg.queryRequestId();
        final int qry = msg.query();
        final int seg = msg.segmentId();

        final ReduceQueryRun r = runs.get(qryReqId);

        if (r == null) // Already finished with error or canceled.
            return;

        final int pageSize = r.pageSize();

        Reducer idx = r.reducers().get(msg.query());

        ReduceResultPage page;

        try {
            page = new ReduceResultPage(ctx, node.id(), msg) {
                @Override public void fetchNextPage() {
                    if (r.hasErrorOrRetry()) {
                        if (r.exception() != null)
                            throw r.exception();

                        assert r.retryCause() != null;

                        throw new CacheException(r.retryCause());
                    }

                    try {
                        GridQueryNextPageRequest msg0 = new GridQueryNextPageRequest(qryReqId, qry, seg, pageSize,
                            (byte)GridH2QueryRequest.setDataPageScanEnabled(0, r.isDataPageScanEnabled()));

                        if (node.isLocal())
                            h2.mapQueryExecutor().onNextPageRequest(node, msg0);
                        else
                            ctx.io().sendToGridTopic(node, GridTopic.TOPIC_QUERY, msg0, GridIoPolicy.QUERY_POOL);
                    }
                    catch (IgniteCheckedException e) {
                        throw new CacheException("Failed to fetch data from node: " + node.id(), e);
                    }
                }
            };
        }
        catch (Exception e) {
            U.error(log, "Error in message.", e);

            fail(r, node.id(), "Error in message.", GridQueryFailResponse.GENERAL_ERROR);

            return;
        }

        idx.addPage(page);

        if (msg.retry() != null)
            r.setStateOnRetry(node.id(), msg.retry(), msg.retryCause());
        else if (msg.page() == 0) // Count down only on each first page received.
            r.onFirstPage();
    }

    /**
     * @param cacheId Cache ID.
     * @return Cache context.
     */
    private GridCacheContext<?,?> cacheContext(Integer cacheId) {
        GridCacheContext<?, ?> cctx = ctx.cache().context().cacheContext(cacheId);

        if (cctx == null)
            throw new CacheException(String.format("Cache not found on local node (was concurrently destroyed?) " +
                "[cacheId=%d]", cacheId));

        return cctx;
    }

    /**
     * @param schemaName Schema name.
     * @param qry Query.
     * @param keepBinary Keep binary.
     * @param enforceJoinOrder Enforce join order of tables.
     * @param timeoutMillis Timeout in milliseconds.
     * @param cancel Query cancel.
     * @param params Query parameters.
     * @param parts Partitions.
     * @param lazy Lazy execution flag.
     * @param mvccTracker Query tracker.
     * @param dataPageScanEnabled If data page scan is enabled.
     * @param pageSize Page size.
     * @return Rows iterator.
     */
    @SuppressWarnings({"BusyWait", "IfMayBeConditional"})
    public Iterator<List<?>> query(
        String schemaName,
        final GridCacheTwoStepQuery qry,
        boolean keepBinary,
        boolean enforceJoinOrder,
        int timeoutMillis,
        GridQueryCancel cancel,
        Object[] params,
        int[] parts,
        boolean lazy,
        MvccQueryTracker mvccTracker,
        Boolean dataPageScanEnabled,
        int pageSize
    ) {
        assert !qry.mvccEnabled() || mvccTracker != null;

        if (pageSize <= 0)
            pageSize = Query.DFLT_PAGE_SIZE;

        // If explicit partitions are set, but there are no real tables, ignore.
        if (!qry.hasCacheIds() && parts != null)
            parts = null;

        // Partitions are not supported for queries over all replicated caches.
        if (parts != null && qry.isReplicatedOnly())
            throw new CacheException("Partitions are not supported for replicated caches");

        try {
            if (qry.mvccEnabled())
                checkActive(tx(ctx));
        }
        catch (IgniteTxAlreadyCompletedCheckedException e) {
            throw new TransactionAlreadyCompletedException(e.getMessage(), e);
        }

        final boolean singlePartMode = parts != null && parts.length == 1;

        if (F.isEmpty(params))
            params = EMPTY_PARAMS;

        List<Integer> cacheIds = qry.cacheIds();

        List<GridCacheSqlQuery> mapQueries = prepareMapQueries(qry, params, singlePartMode);

        final boolean skipMergeTbl = !qry.explain() && qry.skipMergeTable() || singlePartMode;

        final int segmentsPerIndex = qry.explain() || qry.isReplicatedOnly() ? 1 :
            mapper.findFirstPartitioned(cacheIds).config().getQueryParallelism();

        final long retryTimeout = retryTimeout(timeoutMillis);
        final long qryStartTime = U.currentTimeMillis();

        ReduceQueryRun lastRun = null;

        for (int attempt = 0;; attempt++) {
            ensureQueryNotCancelled(cancel);

            if (attempt > 0) {
                throttleOnRetry(lastRun, qryStartTime, retryTimeout, attempt);

                ensureQueryNotCancelled(cancel);
            }

            AffinityTopologyVersion topVer = h2.readyTopologyVersion();

            // Check if topology has changed while retrying on locked topology.
            if (h2.serverTopologyChanged(topVer) && ctx.cache().context().lockedTopologyVersion(null) != null) {
                throw new CacheException(new TransactionException("Server topology is changed during query " +
                    "execution inside a transaction. It's recommended to rollback and retry transaction."));
            }

            ReducePartitionMapResult mapping = createMapping(qry, parts, cacheIds, topVer);

            if (mapping == null) // Can't map query.
                continue; // Retry.

            final Collection<ClusterNode> nodes = mapping.nodes();

            assert !F.isEmpty(nodes);

            H2PooledConnection conn = h2.connections().connection(schemaName);

            final long qryReqId = qryIdGen.incrementAndGet();

            boolean retry = false;
            boolean release = true;

            try {
                final ReduceQueryRun r = createReduceQueryRun(conn, mapQueries, nodes,
                    pageSize, segmentsPerIndex, skipMergeTbl, qry.explain(), dataPageScanEnabled);

                runs.put(qryReqId, r);

                try {
                    cancel.add(() -> send(nodes, new GridQueryCancelRequest(qryReqId), null, true));

                    GridH2QueryRequest req = new GridH2QueryRequest()
                        .requestId(qryReqId)
                        .topologyVersion(topVer)
                        .pageSize(pageSize)
                        .caches(qry.cacheIds())
                        .tables(qry.distributedJoins() ? qry.tables() : null)
                        .partitions(convert(mapping.partitionsMap()))
                        .queries(mapQueries)
                        .parameters(params)
                        .flags(queryFlags(qry, enforceJoinOrder, lazy, dataPageScanEnabled))
                        .timeout(timeoutMillis)
                        .schemaName(schemaName);

                    if (mvccTracker != null)
                        req.mvccSnapshot(mvccTracker.snapshot());

                    final C2<ClusterNode, Message, Message> spec =
                        parts == null ? null : new ReducePartitionsSpecializer(mapping.queryPartitionsMap());

                    if (send(nodes, req, spec, false)) {
                        awaitAllReplies(r, nodes, cancel);

                        if (r.hasErrorOrRetry()) {
                            CacheException err = r.exception();

                            if (err != null) {
                                if (err.getCause() instanceof IgniteClientDisconnectedException)
                                    throw err;

                                if (QueryUtils.wasCancelled(err))
                                    throw new QueryCancelledException(); // Throw correct exception.

                                throw err;
                            }
                            else {
                                retry = true;

                                // If remote node asks us to retry then we have outdated full partition map.
                                h2.awaitForReadyTopologyVersion(r.retryTopologyVersion());
                            }
                        }
                    }
                    else // Send failed.
                        retry = true;

                    if (retry) {
                        lastRun = runs.remove(qryReqId);
                        assert lastRun != null;

                        continue;
                    }
                    else {
                        Iterator<List<?>> resIter;

                        if (skipMergeTbl) {
                            resIter = new ReduceIndexIterator(this,
                                nodes,
                                r,
                                qryReqId,
                                qry.distributedJoins(),
                                mvccTracker);

                            release = false;

                            U.close(conn, log);
                        }
                        else {
                            ensureQueryNotCancelled(cancel);

                            QueryContext qctx = new QueryContext(
                                0,
                                null,
                                null,
                                null,
                                null,
                                true
                            );

                            H2Utils.setupConnection(conn, qctx, false, enforceJoinOrder);

                            if (qry.explain())
                                return explainPlan(conn, qry, params);

                            GridCacheSqlQuery rdc = qry.reduceQuery();

                            final PreparedStatement stmt = conn.prepareStatementNoCache(rdc.query());

                            H2Utils.bindParameters(stmt, F.asList(rdc.parameters(params)));

                            ReduceH2QueryInfo qryInfo = new ReduceH2QueryInfo(stmt, qry.originalSql(), qryReqId);

                            ResultSet res = h2.executeSqlQueryWithTimer(stmt,
                                conn,
                                rdc.query(),
                                timeoutMillis,
                                cancel,
                                dataPageScanEnabled,
                                qryInfo
                            );

                            resIter = new H2FieldsIterator(res, mvccTracker, conn, r.pageSize(), log, h2, qryInfo);

                            conn = null;

                            mvccTracker = null; // To prevent callback inside finally block;
                        }

                        return new GridQueryCacheObjectsIterator(resIter, h2.objectContext(), keepBinary);
                    }
                }
                catch (IgniteCheckedException | RuntimeException e) {
                    release = true;

                    if (e instanceof CacheException) {
                        if (QueryUtils.wasCancelled(e))
                            throw new CacheException("Failed to run reduce query locally.",
                                new QueryCancelledException());

                        throw (CacheException)e;
                    }

                    Throwable cause = e;

                    if (e instanceof IgniteCheckedException) {
                        Throwable disconnectedErr =
                            ((IgniteCheckedException)e).getCause(IgniteClientDisconnectedException.class);

                        if (disconnectedErr != null)
                            cause = disconnectedErr;
                    }

                    throw new CacheException("Failed to run reduce query locally. " + cause.getMessage(), cause);
                }
                finally {
                    if (release) {
                        releaseRemoteResources(nodes, r, qryReqId, qry.distributedJoins(), mvccTracker);

                        if (!skipMergeTbl) {
                            for (int i = 0, mapQrys = mapQueries.size(); i < mapQrys; i++)
                                fakeTable(null, i).innerTable(null); // Drop all merge tables.
                        }
                    }
                }
            }
            finally {
                if (conn != null && (retry || release))
                    U.close(conn, log);
            }
        }
    }

    /**
     * Wait on retry.
     *
     * @param lastRun Previous query run.
     * @param startTime Query start time.
     * @param retryTimeout Query retry timeout.
     * @param timeoutMultiplier Timeout multiplier.
     */
    private void throttleOnRetry(
        @Nullable ReduceQueryRun lastRun,
        long startTime,
        long retryTimeout,
        int timeoutMultiplier) {
        if (retryTimeout > 0 && (U.currentTimeMillis() - startTime > retryTimeout)) {
            // There are few cases when 'retryCause' can be undefined, so we should throw exception with proper message here.
            if (lastRun == null || lastRun.retryCause() == null)
                throw new CacheException("Failed to map SQL query to topology during timeout: " + retryTimeout + "ms");

            UUID retryNodeId = lastRun.retryNodeId();
            String retryCause = lastRun.retryCause();

            throw new CacheException("Failed to map SQL query to topology on data node [dataNodeId=" + retryNodeId +
                ", msg=" + retryCause + ']');
        }

        try {
            Thread.sleep(Math.min(10_000, timeoutMultiplier * 10)); // Wait for exchange.
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new CacheException("Query was interrupted.", e);
        }
    }

    /**
     * Check if query is cancelled.
     *
     * @param cancel Query cancel object.
     * @throws CacheException If query was cancelled.
     */
    private void ensureQueryNotCancelled(GridQueryCancel cancel) {
        if (Thread.currentThread().isInterrupted())
            throw new CacheException(new IgniteInterruptedCheckedException("Query was interrupted."));

        try {
            cancel.checkCancelled();
        }
        catch (QueryCancelledException cancelEx) {
            throw new CacheException("Failed to run reduce query locally. " + cancelEx.getMessage(), cancelEx);
        }

        if (ctx.clientDisconnected()) {
            throw new CacheException("Query was cancelled, client node disconnected.",
                new IgniteClientDisconnectedException(ctx.cluster().clientReconnectFuture(),
                    "Client node disconnected."));
        }
    }

    /**
     * Prepare map queries.
     * @param qry TwoStep query.
     * @param params Query parameters.
     * @param singlePartMode Single partition mode flag.
     * @return List of map queries.
     */
    @NotNull private List<GridCacheSqlQuery> prepareMapQueries(
        GridCacheTwoStepQuery qry,
        Object[] params,
        boolean singlePartMode) {
        List<GridCacheSqlQuery> mapQueries;

        {
            if (singlePartMode)
                mapQueries = prepareMapQueryForSinglePartition(qry, params);
            else {
                mapQueries = new ArrayList<>(qry.mapQueries().size());

                // Copy queries here because node ID will be changed below.
                for (GridCacheSqlQuery mapQry : qry.mapQueries()) {
                    final GridCacheSqlQuery copy = mapQry.copy();

                    mapQueries.add(copy);

                    if (qry.explain())
                        copy.query("EXPLAIN " + mapQry.query()).parameterIndexes(mapQry.parameterIndexes());
                }
            }
        }
        return mapQueries;
    }

    /**
     * Query run factory method.
     *
     * @param conn H2 connection.
     * @param mapQueries Map queries.
     * @param nodes Target nodes.
     * @param pageSize Page size.
     * @param segmentsPerIndex Segments per-index.
     * @param skipMergeTbl Skip merge table flag.
     * @param explain Explain query flag.
     * @param dataPageScanEnabled DataPage scan enabled flag.
     * @return Reduce query run.
     */
    @NotNull private ReduceQueryRun createReduceQueryRun(
        H2PooledConnection conn,
        List<GridCacheSqlQuery> mapQueries,
        Collection<ClusterNode> nodes,
        int pageSize,
        int segmentsPerIndex,
        boolean skipMergeTbl,
        boolean explain,
        Boolean dataPageScanEnabled) {

        final ReduceQueryRun r = new ReduceQueryRun(
            mapQueries.size(),
            pageSize,
            dataPageScanEnabled
        );

        int tblIdx = 0;
        int replicatedQrysCnt = 0;
        for (GridCacheSqlQuery mapQry : mapQueries) {
            Reducer reducer;

            if (skipMergeTbl)
                reducer = UnsortedReducer.createDummy(ctx);
            else {
                ReduceTable tbl;

                try {
                    tbl = createMergeTable(conn, mapQry, explain);
                }
                catch (IgniteCheckedException e) {
                    throw new IgniteException(e);
                }

                reducer = tbl.getReducer();

                fakeTable(conn, tblIdx++).innerTable(tbl);
            }

            // If the query has only replicated tables, we have to run it on a single node only.
            if (!mapQry.isPartitioned()) {
                ClusterNode node = F.rand(nodes);

                mapQry.node(node.id());

                replicatedQrysCnt++;

                reducer.setSources(singletonList(node), 1); // Replicated tables can have only 1 segment.
            }
            else
                reducer.setSources(nodes, segmentsPerIndex);

            reducer.setPageSize(r.pageSize());

            r.reducers().add(reducer);
        }

        r.init( (r.reducers().size() - replicatedQrysCnt) * nodes.size() * segmentsPerIndex + replicatedQrysCnt);

        return r;
    }

    /**
     * Build query flags.
     *
     * @return Query flags.
     */
    private int queryFlags(GridCacheTwoStepQuery qry,
        boolean enforceJoinOrder,
        boolean lazy,
        Boolean dataPageScanEnabled) {
        if (qry.distributedJoins())
            enforceJoinOrder = true;

        return GridH2QueryRequest.queryFlags(qry.distributedJoins(),
            enforceJoinOrder, lazy, qry.isReplicatedOnly(),
            qry.explain(), dataPageScanEnabled);
    }

    /**
     * Create mapping for query.
     *
     * @param qry Query.
     * @param parts Partitions.
     * @param cacheIds Cache ids.
     * @param topVer Topology version.
     * @return
     */
    private ReducePartitionMapResult createMapping(GridCacheTwoStepQuery qry,
        @Nullable int[] parts,
        List<Integer> cacheIds,
        AffinityTopologyVersion topVer) {
        if (qry.isLocalSplit() || !qry.hasCacheIds())
            return new ReducePartitionMapResult(singletonList(ctx.discovery().localNode()), null, null);
        else {
            ReducePartitionMapResult nodesParts =
                mapper.nodesForPartitions(cacheIds, topVer, parts, qry.isReplicatedOnly());

            Collection<ClusterNode> nodes = nodesParts.nodes();

            if (F.isEmpty(nodes))
                return null;

            if (qry.explain() || qry.isReplicatedOnly()) {
                ClusterNode locNode = ctx.discovery().localNode();

                // Always prefer local node if possible.
                if (nodes.contains(locNode))
                    nodes = singletonList(locNode);
                else {
                    // Select random data node to run query on a replicated data or
                    // get EXPLAIN PLAN from a single node.
                    nodes = singletonList(F.rand(nodes));
                }

                return new ReducePartitionMapResult(nodes, nodesParts.partitionsMap(), nodesParts.queryPartitionsMap());
            }

            return nodesParts;
        }
    }

    /**
     *
     * @param schemaName Schema name.
     * @param cacheIds Cache ids.
     * @param selectQry Select query.
     * @param params SQL parameters.
     * @param enforceJoinOrder Enforce join order of tables.
     * @param pageSize Page size.
     * @param timeoutMillis Timeout.
     * @param parts Partitions.
     * @param isReplicatedOnly Whether query uses only replicated caches.
     * @param cancel Cancel state.
     * @return Update result, or {@code null} when some map node doesn't support distributed DML.
     */
    @SuppressWarnings("IfMayBeConditional")
    public UpdateResult update(
        String schemaName,
        List<Integer> cacheIds,
        String selectQry,
        Object[] params,
        boolean enforceJoinOrder,
        int pageSize,
        int timeoutMillis,
        final int[] parts,
        boolean isReplicatedOnly,
        GridQueryCancel cancel
    ) {
        AffinityTopologyVersion topVer = h2.readyTopologyVersion();

        ReducePartitionMapResult nodesParts =
            mapper.nodesForPartitions(cacheIds, topVer, parts, isReplicatedOnly);

        Collection<ClusterNode> nodes = nodesParts.nodes();

        if (F.isEmpty(nodes))
            throw new CacheException("Failed to determine nodes participating in the update. " +
                "Explanation (Retry update once topology recovers).");

        if (isReplicatedOnly) {
            ClusterNode locNode = ctx.discovery().localNode();

            if (nodes.contains(locNode))
                nodes = singletonList(locNode);
            else
                nodes = singletonList(F.rand(nodes));
        }

        for (ClusterNode n : nodes) {
            if (!n.version().greaterThanEqual(2, 3, 0)) {
                log.warning("Server-side DML optimization is skipped because map node does not support it. " +
                    "Falling back to normal DML. [node=" + n.id() + ", v=" + n.version() + "].");

                return null;
            }
        }

        final long reqId = qryIdGen.incrementAndGet();

        final DmlDistributedUpdateRun r = new DmlDistributedUpdateRun(nodes.size());

        int flags = enforceJoinOrder ? GridH2QueryRequest.FLAG_ENFORCE_JOIN_ORDER : 0;

        if (isReplicatedOnly)
            flags |= GridH2QueryRequest.FLAG_REPLICATED;

        GridH2DmlRequest req = new GridH2DmlRequest()
            .requestId(reqId)
            .topologyVersion(topVer)
            .caches(cacheIds)
            .schemaName(schemaName)
            .query(selectQry)
            .pageSize(pageSize)
            .parameters(params)
            .timeout(timeoutMillis)
            .flags(flags);

        updRuns.put(reqId, r);

        boolean release = false;

        try {
            Map<ClusterNode, IntArray> partsMap = (nodesParts.queryPartitionsMap() != null) ?
                nodesParts.queryPartitionsMap() : nodesParts.partitionsMap();

            ReducePartitionsSpecializer partsSpec = (parts == null) ? null :
                new ReducePartitionsSpecializer(partsMap);

            final Collection<ClusterNode> finalNodes = nodes;

            cancel.add(() -> {
                r.future().onCancelled();

                send(finalNodes, new GridQueryCancelRequest(reqId), null, true);
            });

            // send() logs the debug message
            if (send(nodes, req, partsSpec, false))
                return r.future().get();

            throw new CacheException("Failed to send update request to participating nodes.");
        }
        catch (IgniteCheckedException | RuntimeException e) {
            release = true;

            U.error(log, "Error during update [localNodeId=" + ctx.localNodeId() + "]", e);

            throw new CacheException("Failed to run SQL update query. " + e.getMessage(), e);
        }
        finally {
            if (release)
                send(nodes, new GridQueryCancelRequest(reqId), null, false);

            if (!updRuns.remove(reqId, r))
                U.warn(log, "Update run was already removed: " + reqId);
        }
    }

    /**
     * Process response for DML request.
     *
     * @param node Node.
     * @param msg Message.
     */
    public void onDmlResponse(final ClusterNode node, GridH2DmlResponse msg) {
        try {
            long reqId = msg.requestId();

            DmlDistributedUpdateRun r = updRuns.get(reqId);

            if (r == null) {
                U.warn(log, "Unexpected dml response (will ignore). [localNodeId=" + ctx.localNodeId() + ", nodeId=" +
                    node.id() + ", msg=" + msg.toString() + ']');

                return;
            }

            r.handleResponse(node.id(), msg);
        }
        catch (Exception e) {
            U.error(log, "Error in dml response processing. [localNodeId=" + ctx.localNodeId() + ", nodeId=" +
                node.id() + ", msg=" + msg.toString() + ']', e);
        }
    }

    /**
     * Release remote resources if needed.
     *
     * @param nodes Query nodes.
     * @param r Query run.
     * @param qryReqId Query id.
     * @param distributedJoins Distributed join flag.
     * @param mvccTracker MVCC tracker.
     */
    void releaseRemoteResources(Collection<ClusterNode> nodes, ReduceQueryRun r, long qryReqId,
        boolean distributedJoins, MvccQueryTracker mvccTracker) {
        if (distributedJoins)
            send(nodes, new GridQueryCancelRequest(qryReqId), null, true);

        for (Reducer idx : r.reducers()) {
            if (!idx.fetchedAll()) {
                if (!distributedJoins) // cancel request has been already sent for distributed join.
                    send(nodes, new GridQueryCancelRequest(qryReqId), null, true);

                r.setStateOnException(ctx.localNodeId(),
                    new CacheException("Query is canceled.", new QueryCancelledException()));

                break;
            }
        }

        if (!runs.remove(qryReqId, r))
            U.warn(log, "Query run was already removed: " + qryReqId);
        else if (mvccTracker != null)
            mvccTracker.onDone();
    }

    /**
     * @param r Query run.
     * @param nodes Nodes to check periodically if they alive.
     * @param cancel Query cancel.
     * @throws IgniteInterruptedCheckedException If interrupted.
     * @throws QueryCancelledException On query cancel.
     */
    private void awaitAllReplies(ReduceQueryRun r, Collection<ClusterNode> nodes, GridQueryCancel cancel)
        throws IgniteInterruptedCheckedException, QueryCancelledException {
        while (!r.tryMapToSources(500, TimeUnit.MILLISECONDS)) {

            cancel.checkCancelled();

            for (ClusterNode node : nodes) {
                if (!ctx.discovery().alive(node)) {
                    handleNodeLeft(r, node.id());

                    assert r.mapped();

                    return;
                }
            }
        }
    }

    /**
     * Gets or creates new fake table for index.
     *
     * @param c Connection.
     * @param idx Index of table.
     * @return Table.
     */
    private ReduceTableWrapper fakeTable(H2PooledConnection c, int idx) {
        List<ReduceTableWrapper> tbls = fakeTbls;

        assert tbls.size() >= idx;

        if (tbls.size() == idx) { // If table for such index does not exist, create one.
            fakeTblsLock.lock();

            try {
                if ((tbls = fakeTbls).size() == idx) { // Double check inside of lock.
                    ReduceTableWrapper tbl = ReduceTableEngine.create(c.connection(), idx);

                    List<ReduceTableWrapper> newTbls = new ArrayList<>(tbls.size() + 1);

                    newTbls.addAll(tbls);
                    newTbls.add(tbl);

                    fakeTbls = tbls = newTbls;
                }
            }
            finally {
                fakeTblsLock.unlock();
            }
        }

        return tbls.get(idx);
    }

    /**
     * @param c Connection.
     * @param qry Query.
     * @param params Query parameters.
     * @return Cursor for plans.
     * @throws IgniteCheckedException if failed.
     */
    private Iterator<List<?>> explainPlan(H2PooledConnection c, GridCacheTwoStepQuery qry, Object[] params)
        throws IgniteCheckedException {
        List<List<?>> lists = new ArrayList<>(qry.mapQueries().size() + 1);

        for (int i = 0, mapQrys = qry.mapQueries().size(); i < mapQrys; i++) {
            ResultSet rs =
                h2.executeSqlQueryWithTimer(
                    c,
                    "SELECT PLAN FROM " + mergeTableIdentifier(i),
                    null,
                    0,
                    null,
                    null,
                    null);

            lists.add(F.asList(getPlan(rs)));
        }

        int tblIdx = 0;

        for (GridCacheSqlQuery mapQry : qry.mapQueries()) {
            ReduceTable tbl = createMergeTable(c, mapQry, false);

            fakeTable(c, tblIdx++).innerTable(tbl);
        }

        GridCacheSqlQuery rdc = qry.reduceQuery();

        ResultSet rs = h2.executeSqlQueryWithTimer(c,
            "EXPLAIN " + rdc.query(),
            F.asList(rdc.parameters(params)),
            0,
            null,
            null, null);

        lists.add(F.asList(getPlan(rs)));

        return lists.iterator();
    }

    /**
     * @param rs Result set.
     * @return Plan.
     * @throws IgniteCheckedException If failed.
     */
    private String getPlan(ResultSet rs) throws IgniteCheckedException {
        try {
            if (!rs.next())
                throw new IllegalStateException();

            return rs.getString(1);
        }
        catch (SQLException e) {
            throw new IgniteCheckedException(e);
        }
        finally {
            U.closeQuiet(rs);
        }
    }

    /**
     * @param nodes Nodes.
     * @param msg Message.
     * @param specialize Optional closure to specialize message for each node.
     * @param runLocParallel Run local handler in parallel thread.
     * @return {@code true} If all messages sent successfully.
     */
    public boolean send(
        Collection<ClusterNode> nodes,
        Message msg,
        @Nullable IgniteBiClosure<ClusterNode, Message, Message> specialize,
        boolean runLocParallel
    ) {
        if (log.isDebugEnabled())
            log.debug("Sending: [msg=" + msg + ", nodes=" + nodes + ", specialize=" + specialize + "]");

        return h2.send(GridTopic.TOPIC_QUERY,
            GridTopic.TOPIC_QUERY.ordinal(),
            nodes,
            msg,
            specialize,
            locNodeHnd,
            GridIoPolicy.QUERY_POOL,
            runLocParallel
        );
    }

    /**
     * @param ints Ints.
     * @return Array.
     */
    public static int[] toArray(IntArray ints) {
        int[] res = new int[ints.size()];

        ints.toArray(res);

        return res;
    }

    /**
     * @param m Map.
     * @return Converted map.
     */
    private static Map<UUID, int[]> convert(Map<ClusterNode, IntArray> m) {
        if (m == null)
            return null;

        Map<UUID, int[]> res = U.newHashMap(m.size());

        for (Map.Entry<ClusterNode,IntArray> entry : m.entrySet())
            res.put(entry.getKey().id(), toArray(entry.getValue()));

        return res;
    }

    /**
     * @param conn Connection.
     * @param qry Query.
     * @param explain Explain.
     * @return Table.
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings("unchecked")
    private ReduceTable createMergeTable(H2PooledConnection conn, GridCacheSqlQuery qry, boolean explain)
        throws IgniteCheckedException {
        try {
            Session ses = H2Utils.session(conn);

            CreateTableData data = new CreateTableData();

            data.tableName = "T___";
            data.schema = ses.getDatabase().getSchema(ses.getCurrentSchemaName());
            data.create = true;

            if (!explain) {
                LinkedHashMap<String,?> colsMap = qry.columns();

                assert colsMap != null;

                ArrayList<Column> cols = new ArrayList<>(colsMap.size());

                for (Map.Entry<String,?> e : colsMap.entrySet()) {
                    String alias = e.getKey();
                    GridSqlType type = (GridSqlType)e.getValue();

                    assert !F.isEmpty(alias);

                    Column col0;

                    if (type == GridSqlType.UNKNOWN) {
                        // Special case for parameter being set at the top of the query (e.g. SELECT ? FROM ...).
                        // Re-map it to STRING in the same way it is done in H2, because any argument can be cast
                        // to string.
                        col0 = new Column(alias, Value.STRING);
                    }
                    else {
                        col0 = new Column(
                            alias,
                            type.type(),
                            type.precision(),
                            type.scale(),
                            type.displaySize()
                        );
                    }

                    cols.add(col0);
                }

                data.columns = cols;
            }
            else
                data.columns = planColumns();

            boolean sortedIndex = !F.isEmpty(qry.sortColumns());

            ReduceTable tbl = new ReduceTable(data);

            ArrayList<Index> idxs = new ArrayList<>(2);

            if (explain) {
                idxs.add(new UnsortedReduceIndexAdapter(ctx, tbl,
                    sortedIndex ? MERGE_INDEX_SORTED : MERGE_INDEX_UNSORTED));
            }
            else if (sortedIndex) {
                List<GridSqlSortColumn> sortCols = (List<GridSqlSortColumn>)qry.sortColumns();

                SortedReduceIndexAdapter sortedMergeIdx = new SortedReduceIndexAdapter(ctx, tbl, MERGE_INDEX_SORTED,
                    GridSqlSortColumn.toIndexColumns(tbl, sortCols));

                idxs.add(ReduceTable.createScanIndex(sortedMergeIdx));
                idxs.add(sortedMergeIdx);
            }
            else
                idxs.add(new UnsortedReduceIndexAdapter(ctx, tbl, MERGE_INDEX_UNSORTED));

            tbl.indexes(idxs);

            return tbl;
        }
        catch (Exception e) {
            throw new IgniteCheckedException(e);
        }
    }

    /**
     * @return Columns.
     */
    private static ArrayList<Column> planColumns() {
        ArrayList<Column> res = new ArrayList<>(1);

        res.add(new Column("PLAN", Value.STRING));

        return res;
    }

    /**
     * @param reconnectFut Reconnect future.
     */
    public void onDisconnected(IgniteFuture<?> reconnectFut) {
        CacheException err = new CacheException("Query was cancelled, client node disconnected.",
            new IgniteClientDisconnectedException(reconnectFut, "Client node disconnected."));

        for (Map.Entry<Long, ReduceQueryRun> e : runs.entrySet())
            e.getValue().disconnected(err);

        for (DmlDistributedUpdateRun r: updRuns.values())
            r.handleDisconnect(err);
    }

    /**
     * @param qryTimeout Query timeout.
     * @return Query retry timeout.
     */
    private long retryTimeout(long qryTimeout) {
        if (qryTimeout > 0)
            return qryTimeout;

        return dfltQueryTimeout;
    }

    /**
     * Prepare map query based on original sql.
     *
     * @param qry Two step query.
     * @param params Query parameters.
     * @return Updated map query list with one map query.
     */
    private List<GridCacheSqlQuery> prepareMapQueryForSinglePartition(GridCacheTwoStepQuery qry, Object[] params) {
        boolean hasSubQries = false;

        for (GridCacheSqlQuery mapQry : qry.mapQueries()) {
            if (mapQry.hasSubQueries()) {
                hasSubQries = true;

                break;
            }
        }

        GridCacheSqlQuery originalQry = new GridCacheSqlQuery(qry.originalSql());

        if (!F.isEmpty(params)) {
            int[] paramIdxs = new int[params.length];

            for (int i = 0; i < params.length; i++)
                paramIdxs[i] = i;

            originalQry.parameterIndexes(paramIdxs);
        }

        originalQry.partitioned(true);

        originalQry.hasSubQueries(hasSubQries);

        return Collections.singletonList(originalQry);
    }
}
