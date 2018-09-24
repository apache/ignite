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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import javax.cache.CacheException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteClientDisconnectedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.query.QueryCancelledException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.GridTopic;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.internal.managers.communication.GridMessageListener;
import org.apache.ignite.internal.managers.eventstorage.GridLocalEventListener;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxSelectForUpdateFuture;
import org.apache.ignite.internal.processors.cache.distributed.near.TxTopologyVersionFuture;
import org.apache.ignite.internal.processors.cache.mvcc.MvccQueryTracker;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryMarshallable;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryType;
import org.apache.ignite.internal.processors.cache.query.GridCacheSqlQuery;
import org.apache.ignite.internal.processors.cache.query.GridCacheTwoStepQuery;
import org.apache.ignite.internal.processors.query.GridQueryCacheObjectsIterator;
import org.apache.ignite.internal.processors.query.GridQueryCancel;
import org.apache.ignite.internal.processors.query.GridRunningQueryInfo;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.h2.H2FieldsIterator;
import org.apache.ignite.internal.processors.query.h2.H2Utils;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.processors.query.h2.UpdateResult;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2QueryContext;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlSortColumn;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlType;
import org.apache.ignite.internal.processors.query.h2.twostep.messages.GridQueryCancelRequest;
import org.apache.ignite.internal.processors.query.h2.twostep.messages.GridQueryFailResponse;
import org.apache.ignite.internal.processors.query.h2.twostep.messages.GridQueryNextPageRequest;
import org.apache.ignite.internal.processors.query.h2.twostep.messages.GridQueryNextPageResponse;
import org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2DmlRequest;
import org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2DmlResponse;
import org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2QueryRequest;
import org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2SelectForUpdateTxDetails;
import org.apache.ignite.internal.util.GridIntIterator;
import org.apache.ignite.internal.util.GridIntList;
import org.apache.ignite.internal.util.GridSpinBusyLock;
import org.apache.ignite.internal.util.typedef.C2;
import org.apache.ignite.internal.util.typedef.CIX2;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiClosure;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.transactions.TransactionException;
import org.h2.command.ddl.CreateTableData;
import org.h2.engine.Session;
import org.h2.index.Index;
import org.h2.jdbc.JdbcConnection;
import org.h2.table.Column;
import org.h2.util.IntArray;
import org.h2.value.Value;
import org.jetbrains.annotations.Nullable;

import static java.util.Collections.singletonList;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_SQL_RETRY_TIMEOUT;
import static org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion.NONE;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.checkActive;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.mvccEnabled;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.tx;
import static org.apache.ignite.internal.processors.cache.query.GridCacheSqlQuery.EMPTY_PARAMS;
import static org.apache.ignite.internal.processors.query.h2.opt.DistributedJoinMode.OFF;
import static org.apache.ignite.internal.processors.query.h2.opt.GridH2QueryType.REDUCE;
import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlQuerySplitter.mergeTableIdentifier;

/**
 * Reduce query executor.
 */
public class GridReduceQueryExecutor {
    /** Default retry timeout. */
    public static final long DFLT_RETRY_TIMEOUT = 30_000L;

    /** */
    private static final String MERGE_INDEX_UNSORTED = "merge_scan";

    /** */
    private static final String MERGE_INDEX_SORTED = "merge_sorted";

    /** */
    private static final Set<ClusterNode> UNMAPPED_PARTS = Collections.emptySet();

    /** */
    private GridKernalContext ctx;

    /** */
    private IgniteH2Indexing h2;

    /** */
    private IgniteLogger log;

    /** */
    private final AtomicLong qryIdGen;

    /** */
    private final ConcurrentMap<Long, ReduceQueryRun> runs = new ConcurrentHashMap<>();

    /** Contexts of running DML requests. */
    private final ConcurrentMap<Long, DistributedUpdateRun> updRuns = new ConcurrentHashMap<>();

    /** */
    private volatile List<GridThreadLocalTable> fakeTbls = Collections.emptyList();

    /** */
    private final Lock fakeTblsLock = new ReentrantLock();

    /** */
    private final GridSpinBusyLock busyLock;

    /** */
    private final CIX2<ClusterNode,Message> locNodeHnd = new CIX2<ClusterNode,Message>() {
        @Override public void applyx(ClusterNode locNode, Message msg) {
            h2.mapQueryExecutor().onMessage(locNode.id(), msg);
        }
    };

    /**
     * @param qryIdGen Query ID generator.
     * @param busyLock Busy lock.
     */
    public GridReduceQueryExecutor(AtomicLong qryIdGen, GridSpinBusyLock busyLock) {
        this.qryIdGen = qryIdGen;
        this.busyLock = busyLock;
    }

    /**
     * @param ctx Context.
     * @param h2 H2 Indexing.
     * @throws IgniteCheckedException If failed.
     */
    public void start(final GridKernalContext ctx, final IgniteH2Indexing h2) throws IgniteCheckedException {
        this.ctx = ctx;
        this.h2 = h2;

        log = ctx.log(GridReduceQueryExecutor.class);

        ctx.io().addMessageListener(GridTopic.TOPIC_QUERY, new GridMessageListener() {
            @SuppressWarnings("deprecation")
            @Override public void onMessage(UUID nodeId, Object msg, byte plc) {
                if (!busyLock.enterBusy())
                    return;

                try {
                    if (msg instanceof GridCacheQueryMarshallable)
                        ((GridCacheQueryMarshallable)msg).unmarshall(ctx.config().getMarshaller(), ctx);

                    GridReduceQueryExecutor.this.onMessage(nodeId, msg);
                }
                finally {
                    busyLock.leaveBusy();
                }
            }
        });

        ctx.event().addLocalEventListener(new GridLocalEventListener() {
            @Override public void onEvent(final Event evt) {
                UUID nodeId = ((DiscoveryEvent)evt).eventNode().id();

                for (ReduceQueryRun r : runs.values()) {
                    for (GridMergeIndex idx : r.indexes()) {
                        if (idx.hasSource(nodeId)) {
                            handleNodeLeft(r, nodeId);

                            break;
                        }
                    }
                }

                for (DistributedUpdateRun r : updRuns.values())
                    r.handleNodeLeft(nodeId);

            }
        }, EventType.EVT_NODE_FAILED, EventType.EVT_NODE_LEFT);
    }

    /**
     * @param r Query run.
     * @param nodeId Left node ID.
     */
    private void handleNodeLeft(ReduceQueryRun r, UUID nodeId) {
        r.setStateOnNodeLeave(nodeId, h2.readyTopologyVersion());
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

            if (msg instanceof GridQueryNextPageResponse)
                onNextPage(node, (GridQueryNextPageResponse)msg);
            else if (msg instanceof GridQueryFailResponse)
                onFail(node, (GridQueryFailResponse)msg);
            else if (msg instanceof GridH2DmlResponse)
                onDmlResponse(node, (GridH2DmlResponse)msg);
            else
                processed = false;

            if (processed && log.isDebugEnabled())
                log.debug("Processed response: " + nodeId + "->" + ctx.localNodeId() + " " + msg);
        }
        catch(Throwable th) {
            U.error(log, "Failed to process message: " + msg, th);
        }
    }

    /**
     * @param node Node.
     * @param msg Message.
     */
    private void onFail(ClusterNode node, GridQueryFailResponse msg) {
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
            CacheException e = new CacheException("Failed to execute map query on remote node [nodeId=" + nodeId +
                ", errMsg=" + msg + ']');

            if (failCode == GridQueryFailResponse.CANCELLED_BY_ORIGINATOR)
                e.addSuppressed(new QueryCancelledException());

            r.setStateOnException(nodeId, e);
        }
    }

    /**
     * @param node Node.
     * @param msg Message.
     */
    private void onNextPage(final ClusterNode node, final GridQueryNextPageResponse msg) {
        final long qryReqId = msg.queryRequestId();
        final int qry = msg.query();
        final int seg = msg.segmentId();

        final ReduceQueryRun r = runs.get(qryReqId);

        if (r == null) // Already finished with error or canceled.
            return;

        final int pageSize = r.pageSize();

        GridMergeIndex idx = r.indexes().get(msg.query());

        GridResultPage page;

        try {
            page = new GridResultPage(ctx, node.id(), msg) {
                @Override public void fetchNextPage() {
                    if (r.hasErrorOrRetry()) {
                        if (r.exception() != null)
                            throw r.exception();

                        assert r.retryCause() != null;

                        throw new CacheException(r.retryCause());
                    }

                    try {
                        GridQueryNextPageRequest msg0 = new GridQueryNextPageRequest(qryReqId, qry, seg, pageSize);

                        if (node.isLocal())
                            h2.mapQueryExecutor().onMessage(ctx.localNodeId(), msg0);
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
        else if (msg.page() == 0) {
            // Do count down on each first page received.
            r.latch().countDown();

            GridNearTxSelectForUpdateFuture sfuFut = r.selectForUpdateFuture();

            if (sfuFut != null)
                sfuFut.onResult(node.id(), (long)msg.allRows(), msg.removeMapping(), null);
        }
    }

    /**
     * @param cacheIds Cache IDs.
     * @return {@code true} If preloading is active.
     */
    private boolean isPreloadingActive(List<Integer> cacheIds) {
        for (Integer cacheId : cacheIds) {
            if (null == cacheContext(cacheId))
                throw new CacheException(String.format("Cache not found on local node [cacheId=%d]", cacheId));

            if (hasMovingPartitions(cacheContext(cacheId)))
                return true;
        }

        return false;
    }

    /**
     * @param cctx Cache context.
     * @return {@code True} If cache has partitions in {@link GridDhtPartitionState#MOVING} state.
     */
    private boolean hasMovingPartitions(GridCacheContext<?, ?> cctx) {
        assert cctx != null;

        return !cctx.isLocal() && cctx.topology().hasMovingPartitions();
    }

    /**
     * @param cacheId Cache ID.
     * @return Cache context.
     */
    private GridCacheContext<?,?> cacheContext(Integer cacheId) {
        return ctx.cache().context().cacheContext(cacheId);
    }

    /**
     * @param topVer Topology version.
     * @param cctx Cache context.
     * @param parts Partitions.
     */
    private Map<ClusterNode, IntArray> stableDataNodesMap(AffinityTopologyVersion topVer,
        final GridCacheContext<?, ?> cctx, @Nullable final int[] parts) {

        Map<ClusterNode, IntArray> mapping = new HashMap<>();

        // Explicit partitions mapping is not applicable to replicated cache.
        if (cctx.isReplicated()) {
            for (ClusterNode clusterNode : cctx.affinity().assignment(topVer).nodes())
                mapping.put(clusterNode, null);

            return mapping;
        }

        List<List<ClusterNode>> assignment = cctx.affinity().assignment(topVer).assignment();

        boolean needPartsFilter = parts != null;

        GridIntIterator iter = needPartsFilter ? new GridIntList(parts).iterator() :
            U.forRange(0, cctx.affinity().partitions());

        while(iter.hasNext()) {
            int partId = iter.next();

            List<ClusterNode> partNodes = assignment.get(partId);

            if (!partNodes.isEmpty()) {
                ClusterNode prim = partNodes.get(0);

                if (!needPartsFilter) {
                    mapping.put(prim, null);

                    continue;
                }

                IntArray partIds = mapping.get(prim);

                if (partIds == null) {
                    partIds = new IntArray();

                    mapping.put(prim, partIds);
                }

                partIds.add(partId);
            }
        }

        return mapping;
    }

    /**
     * Load failed partition reservation.
     *
     * @param msg Message.
     */
    private void logRetry(String msg) {
        log.info(msg);
    }

    /**
     * @param isReplicatedOnly If we must only have replicated caches.
     * @param topVer Topology version.
     * @param cacheIds Participating cache IDs.
     * @param parts Partitions.
     * @param qryId Query ID.
     * @return Data nodes or {@code null} if repartitioning started and we need to retry.
     */
    private Map<ClusterNode, IntArray> stableDataNodes(boolean isReplicatedOnly, AffinityTopologyVersion topVer,
        List<Integer> cacheIds, int[] parts, long qryId) {
        GridCacheContext<?, ?> cctx = cacheContext(cacheIds.get(0));

        // If the first cache is not partitioned, find it (if it's present) and move it to index 0.
        if (!cctx.isPartitioned()) {
            for (int cacheId = 1; cacheId < cacheIds.size(); cacheId++) {
                GridCacheContext<?, ?> currCctx = cacheContext(cacheIds.get(cacheId));

                if (currCctx.isPartitioned()) {
                    Collections.swap(cacheIds, 0, cacheId);

                    cctx = currCctx;

                    break;
                }
            }
        }

        Map<ClusterNode, IntArray> map = stableDataNodesMap(topVer, cctx, parts);

        Set<ClusterNode> nodes = map.keySet();

        if (F.isEmpty(map))
            throw new CacheException("Failed to find data nodes for cache: " + cctx.name());

        for (int i = 1; i < cacheIds.size(); i++) {
            GridCacheContext<?,?> extraCctx = cacheContext(cacheIds.get(i));

            String extraCacheName = extraCctx.name();

            if (extraCctx.isLocal())
                continue; // No consistency guaranties for local caches.

            if (isReplicatedOnly && !extraCctx.isReplicated())
                throw new CacheException("Queries running on replicated cache should not contain JOINs " +
                    "with partitioned tables [replicatedCache=" + cctx.name() +
                    ", partitionedCache=" + extraCacheName + "]");

            Set<ClusterNode> extraNodes = stableDataNodesMap(topVer, extraCctx, parts).keySet();

            if (F.isEmpty(extraNodes))
                throw new CacheException("Failed to find data nodes for cache: " + extraCacheName);

            boolean disjoint;

            if (extraCctx.isReplicated()) {
                if (isReplicatedOnly) {
                    nodes.retainAll(extraNodes);

                    disjoint = map.isEmpty();
                }
                else
                    disjoint = !extraNodes.containsAll(nodes);
            }
            else
                disjoint = !extraNodes.equals(nodes);

            if (disjoint) {
                if (isPreloadingActive(cacheIds)) {
                    logRetry("Failed to calculate nodes for SQL query (got disjoint node map during rebalance) " +
                        "[qryId=" + qryId + ", affTopVer=" + topVer + ", cacheIds=" + cacheIds +
                        ", parts=" + (parts == null ? "[]" : Arrays.toString(parts)) +
                        ", replicatedOnly=" + isReplicatedOnly + ", lastCache=" + extraCctx.name() +
                        ", lastCacheId=" + extraCctx.cacheId() + ']');

                    return null; // Retry.
                }
                else
                    throw new CacheException("Caches have distinct sets of data nodes [cache1=" + cctx.name() +
                        ", cache2=" + extraCacheName + "]");
            }
        }

        return map;
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
     * @return Rows iterator.
     */
    public Iterator<List<?>> query(
        String schemaName,
        final GridCacheTwoStepQuery qry,
        boolean keepBinary,
        boolean enforceJoinOrder,
        int timeoutMillis,
        GridQueryCancel cancel,
        Object[] params,
        final int[] parts,
        boolean lazy,
        MvccQueryTracker mvccTracker) {
        assert !qry.mvccEnabled() || mvccTracker != null;

        if (F.isEmpty(params))
            params = EMPTY_PARAMS;

        final boolean isReplicatedOnly = qry.isReplicatedOnly();

        long retryTimeout = retryTimeout(timeoutMillis);

        final long startTime = U.currentTimeMillis();

        ReduceQueryRun lastRun = null;

        for (int attempt = 0;; attempt++) {
            if (attempt > 0 && retryTimeout > 0 && (U.currentTimeMillis() - startTime > retryTimeout)) {
                UUID retryNodeId = lastRun.retryNodeId();
                String retryCause = lastRun.retryCause();

                assert !F.isEmpty(retryCause);

                throw new CacheException("Failed to map SQL query to topology on data node [dataNodeId=" + retryNodeId +
                    ", msg=" + retryCause + ']');
            }

            if (attempt != 0) {
                try {
                    Thread.sleep(attempt * 10); // Wait for exchange.
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();

                    throw new CacheException("Query was interrupted.", e);
                }
            }

            long qryReqId = qryIdGen.incrementAndGet();

            List<Integer> cacheIds = qry.cacheIds();

            boolean mvccEnabled = mvccEnabled(ctx);

            final GridNearTxLocal curTx = mvccEnabled ? checkActive(tx(ctx)) : null;

            final GridNearTxSelectForUpdateFuture sfuFut;

            final boolean clientFirst;

            AffinityTopologyVersion topVer;

            if (qry.forUpdate()) {
                // Indexing should have started TX at this point for FOR UPDATE query.
                assert mvccEnabled && curTx != null;

                try {
                    TxTopologyVersionFuture topFut = new TxTopologyVersionFuture(curTx, mvccTracker.context());

                    topVer = topFut.get();

                    clientFirst = topFut.clientFirst();
                }
                catch (IgniteCheckedException e) {
                    throw new IgniteSQLException("Failed to map SELECT FOR UPDATE query on topology.", e);
                }

                sfuFut = new GridNearTxSelectForUpdateFuture(mvccTracker.context(), curTx, timeoutMillis);
            }
            else {
                sfuFut = null;

                clientFirst = false;

                topVer = h2.readyTopologyVersion();

                // Check if topology has changed while retrying on locked topology.
                if (h2.serverTopologyChanged(topVer) && ctx.cache().context().lockedTopologyVersion(null) != null) {
                    throw new CacheException(new TransactionException("Server topology is changed during query " +
                        "execution inside a transaction. It's recommended to rollback and retry transaction."));
                }
            }

            final ReduceQueryRun r = new ReduceQueryRun(qryReqId, qry.originalSql(), schemaName,
                h2.connectionForSchema(schemaName), qry.mapQueries().size(), qry.pageSize(),
                U.currentTimeMillis(), sfuFut, cancel);

            Collection<ClusterNode> nodes;

            // Explicit partition mapping for unstable topology.
            Map<ClusterNode, IntArray> partsMap = null;

            // Explicit partitions mapping for query.
            Map<ClusterNode, IntArray> qryMap = null;

            // Partitions are not supported for queries over all replicated caches.
            if (parts != null) {
                boolean replicatedOnly = true;

                for (Integer cacheId : cacheIds) {
                    if (!cacheContext(cacheId).isReplicated()) {
                        replicatedOnly = false;

                        break;
                    }
                }

                if (replicatedOnly)
                    throw new CacheException("Partitions are not supported for replicated caches");
            }

            if (qry.isLocal())
                nodes = singletonList(ctx.discovery().localNode());
            else {
                NodesForPartitionsResult nodesParts =
                    nodesForPartitions(cacheIds, topVer, parts, isReplicatedOnly, qryReqId);

                nodes = nodesParts.nodes();
                partsMap = nodesParts.partitionsMap();
                qryMap = nodesParts.queryPartitionsMap();

                if (nodes == null) {
                    if (sfuFut != null)
                        sfuFut.onDone(0L, null);

                    continue; // Retry.
                }

                assert !nodes.isEmpty();

                if (isReplicatedOnly || qry.explain()) {
                    ClusterNode locNode = ctx.discovery().localNode();

                    // Always prefer local node if possible.
                    if (nodes.contains(locNode))
                        nodes = singletonList(locNode);
                    else {
                        // Select random data node to run query on a replicated data or
                        // get EXPLAIN PLAN from a single node.
                        nodes = singletonList(F.rand(nodes));
                    }
                }
            }

            if (sfuFut != null && !sfuFut.isFailed())
                sfuFut.init(topVer, nodes);

            int tblIdx = 0;

            final boolean skipMergeTbl = !qry.explain() && qry.skipMergeTable();

            final int segmentsPerIndex = qry.explain() || isReplicatedOnly ? 1 :
                findFirstPartitioned(cacheIds).config().getQueryParallelism();

            int replicatedQrysCnt = 0;

            final Collection<ClusterNode> finalNodes = nodes;

            for (GridCacheSqlQuery mapQry : qry.mapQueries()) {
                GridMergeIndex idx;

                if (!skipMergeTbl) {
                    GridMergeTable tbl;

                    try {
                        tbl = createMergeTable(r.connection(), mapQry, qry.explain());
                    }
                    catch (IgniteCheckedException e) {
                        throw new IgniteException(e);
                    }

                    idx = tbl.getMergeIndex();

                    fakeTable(r.connection(), tblIdx++).innerTable(tbl);
                }
                else
                    idx = GridMergeIndexUnsorted.createDummy(ctx);

                // If the query has only replicated tables, we have to run it on a single node only.
                if (!mapQry.isPartitioned()) {
                    ClusterNode node = F.rand(nodes);

                    mapQry.node(node.id());

                    replicatedQrysCnt++;

                    idx.setSources(singletonList(node), 1); // Replicated tables can have only 1 segment.
                }
                else
                    idx.setSources(nodes, segmentsPerIndex);

                idx.setPageSize(r.pageSize());

                r.indexes().add(idx);
            }

            r.latch(new CountDownLatch(isReplicatedOnly ? 1 :
                (r.indexes().size() - replicatedQrysCnt) * nodes.size() * segmentsPerIndex + replicatedQrysCnt));

            runs.put(qryReqId, r);

            boolean release = true;

            try {
                cancel.checkCancelled();

                if (ctx.clientDisconnected()) {
                    throw new CacheException("Query was cancelled, client node disconnected.",
                        new IgniteClientDisconnectedException(ctx.cluster().clientReconnectFuture(),
                            "Client node disconnected."));
                }

                List<GridCacheSqlQuery> mapQrys = qry.mapQueries();

                if (qry.explain()) {
                    mapQrys = new ArrayList<>(qry.mapQueries().size());

                    for (GridCacheSqlQuery mapQry : qry.mapQueries())
                        mapQrys.add(new GridCacheSqlQuery("EXPLAIN " + mapQry.query())
                            .parameterIndexes(mapQry.parameterIndexes()));
                }

                final boolean distributedJoins = qry.distributedJoins();

                final long qryReqId0 = qryReqId;

                cancel.set(new Runnable() {
                    @Override public void run() {
                        send(finalNodes, new GridQueryCancelRequest(qryReqId0), null, false);
                    }
                });

                boolean retry = false;

                // Always enforce join order on map side to have consistent behavior.
                int flags = GridH2QueryRequest.FLAG_ENFORCE_JOIN_ORDER;

                if (distributedJoins)
                    flags |= GridH2QueryRequest.FLAG_DISTRIBUTED_JOINS;

                if (qry.isLocal())
                    flags |= GridH2QueryRequest.FLAG_IS_LOCAL;

                if (qry.explain())
                    flags |= GridH2QueryRequest.FLAG_EXPLAIN;

                if (isReplicatedOnly)
                    flags |= GridH2QueryRequest.FLAG_REPLICATED;

                if (lazy && mapQrys.size() == 1)
                    flags |= GridH2QueryRequest.FLAG_LAZY;

                GridH2QueryRequest req = new GridH2QueryRequest()
                    .requestId(qryReqId)
                    .topologyVersion(topVer)
                    .pageSize(r.pageSize())
                    .caches(qry.cacheIds())
                    .tables(distributedJoins ? qry.tables() : null)
                    .partitions(convert(partsMap))
                    .queries(mapQrys)
                    .parameters(params)
                    .flags(flags)
                    .timeout(timeoutMillis)
                    .schemaName(schemaName);

                if (curTx != null && curTx.mvccSnapshot() != null)
                    req.mvccSnapshot(curTx.mvccSnapshot());
                else if (mvccTracker != null)
                    req.mvccSnapshot(mvccTracker.snapshot());

                final C2<ClusterNode, Message, Message> pspec =
                    (parts == null ? null : new ExplicitPartitionsSpecializer(qryMap));

                final C2<ClusterNode, Message, Message> spec;

                if (qry.forUpdate()) {
                    final AtomicInteger cnt = new AtomicInteger();

                    spec = new C2<ClusterNode, Message, Message>() {
                        @Override public Message apply(ClusterNode clusterNode, Message msg) {
                            assert msg instanceof GridH2QueryRequest;

                            GridH2QueryRequest res = pspec != null ? (GridH2QueryRequest)pspec.apply(clusterNode, msg) :
                                new GridH2QueryRequest((GridH2QueryRequest)msg);

                            GridH2SelectForUpdateTxDetails txReq = new GridH2SelectForUpdateTxDetails(
                                curTx.threadId(),
                                IgniteUuid.randomUuid(),
                                cnt.incrementAndGet(),
                                curTx.subjectId(),
                                curTx.xidVersion(),
                                curTx.taskNameHash(),
                                clientFirst,
                                curTx.remainingTime());

                            res.txDetails(txReq);

                            return res;
                        }
                    };
                }
                else
                    spec = pspec;

                if (send(nodes, req, spec, false)) {
                    awaitAllReplies(r, nodes, cancel);

                    if (r.hasErrorOrRetry()) {
                        CacheException err = r.exception();

                        if (err != null) {
                            if (err.getCause() instanceof IgniteClientDisconnectedException)
                                throw err;

                            if (wasCancelled(err))
                                throw new QueryCancelledException(); // Throw correct exception.

                            throw err;
                        }
                        else {
                            retry = true;

                            // On-the-fly topology change must not be possible in FOR UPDATE case.
                            assert sfuFut == null;

                            // If remote node asks us to retry then we have outdated full partition map.
                            h2.awaitForReadyTopologyVersion(r.retryTopologyVersion());
                        }
                    }
                }
                else // Send failed.
                    retry = true;

                Iterator<List<?>> resIter = null;

                if (!retry) {
                    if (skipMergeTbl) {
                        resIter = new GridMergeIndexIterator(this,
                            finalNodes,
                            r,
                            qryReqId,
                            qry.distributedJoins(),
                            mvccTracker);

                        release = false;
                    }
                    else {
                        cancel.checkCancelled();

                        UUID locNodeId = ctx.localNodeId();

                        H2Utils.setupConnection(r.connection(), false, enforceJoinOrder);

                        GridH2QueryContext.set(new GridH2QueryContext(locNodeId, locNodeId, qryReqId, REDUCE)
                            .pageSize(r.pageSize()).distributedJoinMode(OFF));

                        try {
                            if (qry.explain())
                                return explainPlan(r.connection(), qry, params);

                            GridCacheSqlQuery rdc = qry.reduceQuery();

                            ResultSet res = h2.executeSqlQueryWithTimer(r.connection(),
                                rdc.query(),
                                F.asList(rdc.parameters(params)),
                                false, // The statement will cache some extra thread local objects.
                                timeoutMillis,
                                cancel);

                            resIter = new H2FieldsIterator(res, mvccTracker, false);

                            mvccTracker = null; // To prevent callback inside finally block;
                        }
                        finally {
                            GridH2QueryContext.clearThreadLocal();
                        }
                    }
                }
                else {
                    assert r != null;
                    lastRun=r;

                    if (Thread.currentThread().isInterrupted())
                        throw new IgniteInterruptedCheckedException("Query was interrupted.");

                    if (sfuFut != null)
                        sfuFut.onDone(0L);

                    continue;
                }

                if (sfuFut != null)
                    sfuFut.get();

                return new GridQueryCacheObjectsIterator(resIter, h2.objectContext(), keepBinary);
            }
            catch (IgniteCheckedException | RuntimeException e) {
                release = true;

                U.closeQuiet(r.connection());

                CacheException resEx = null;

                if (e instanceof CacheException) {
                    if (wasCancelled((CacheException)e))
                        resEx = new  CacheException("Failed to run reduce query locally.",
                            new QueryCancelledException());
                    else
                        resEx = (CacheException)e;
                }

                if (resEx != null) {
                    if (sfuFut != null)
                        sfuFut.onDone(resEx);

                    throw resEx;
                }

                Throwable cause = e;

                if (e instanceof IgniteCheckedException) {
                    Throwable disconnectedErr =
                        ((IgniteCheckedException)e).getCause(IgniteClientDisconnectedException.class);

                    if (disconnectedErr != null)
                        cause = disconnectedErr;
                }

                resEx = new CacheException("Failed to run reduce query locally.", cause);

                if (sfuFut != null)
                    sfuFut.onDone(resEx);

                throw resEx;
            }
            finally {
                if (release) {
                    releaseRemoteResources(finalNodes, r, qryReqId, qry.distributedJoins(), mvccTracker);

                    if (!skipMergeTbl) {
                        for (int i = 0, mapQrys = qry.mapQueries().size(); i < mapQrys; i++)
                            fakeTable(null, i).innerTable(null); // Drop all merge tables.
                    }
                }
            }
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

        final long reqId = qryIdGen.incrementAndGet();

        NodesForPartitionsResult nodesParts = nodesForPartitions(cacheIds, topVer, parts, isReplicatedOnly, reqId);

        final GridRunningQueryInfo qryInfo = new GridRunningQueryInfo(reqId, selectQry, GridCacheQueryType.SQL_FIELDS,
            schemaName, U.currentTimeMillis(), cancel, false);

        Collection<ClusterNode> nodes = nodesParts.nodes();

        if (nodes == null)
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

        final DistributedUpdateRun r = new DistributedUpdateRun(nodes.size(), qryInfo);

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

            ExplicitPartitionsSpecializer partsSpec = (parts == null) ? null :
                new ExplicitPartitionsSpecializer(partsMap);

            final Collection<ClusterNode> finalNodes = nodes;

            cancel.set(new Runnable() {
                @Override public void run() {
                    r.future().onCancelled();

                    send(finalNodes, new GridQueryCancelRequest(reqId), null, false);
                }
            });

            // send() logs the debug message
            if (send(nodes, req, partsSpec, false))
                return r.future().get();

            throw new CacheException("Failed to send update request to participating nodes.");
        }
        catch (IgniteCheckedException | RuntimeException e) {
            release = true;

            U.error(log, "Error during update [localNodeId=" + ctx.localNodeId() + "]", e);

            throw new CacheException("Failed to run update. " + e.getMessage(), e);
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
    private void onDmlResponse(final ClusterNode node, GridH2DmlResponse msg) {
        try {
            long reqId = msg.requestId();

            DistributedUpdateRun r = updRuns.get(reqId);

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
     * @param cacheIds Cache IDs.
     * @return The first partitioned cache context.
     */
    private GridCacheContext<?,?> findFirstPartitioned(List<Integer> cacheIds) {
        for (int i = 0; i < cacheIds.size(); i++) {
            GridCacheContext<?, ?> cctx = cacheContext(cacheIds.get(i));

            if (i == 0 && cctx.isLocal())
                throw new CacheException("Cache is LOCAL: " + cctx.name());

            if (!cctx.isReplicated() && !cctx.isLocal())
                return cctx;
        }

        throw new IllegalStateException("Failed to find partitioned cache.");
    }

    /**
     * Returns true if the exception is triggered by query cancel.
     *
     * @param e Exception.
     * @return {@code true} if exception is caused by cancel.
     */
    private boolean wasCancelled(CacheException e) {
        return X.hasSuppressed(e, QueryCancelledException.class);
    }

    /**
     * Release remote resources if needed.
     *
     * @param nodes Query nodes.
     * @param r Query run.
     * @param qryReqId Query id.
     * @param distributedJoins Distributed join flag.
     */
    public void releaseRemoteResources(Collection<ClusterNode> nodes, ReduceQueryRun r, long qryReqId,
        boolean distributedJoins, MvccQueryTracker mvccTracker) {
        if (mvccTracker != null)
            mvccTracker.onDone();

        // For distributedJoins need always send cancel request to cleanup resources.
        if (distributedJoins)
            send(nodes, new GridQueryCancelRequest(qryReqId), null, false);
        else {
            for (GridMergeIndex idx : r.indexes()) {
                if (!idx.fetchedAll()) {
                    send(nodes, new GridQueryCancelRequest(qryReqId), null, false);

                    break;
                }
            }
        }

        if (!runs.remove(qryReqId, r))
            U.warn(log, "Query run was already removed: " + qryReqId);
    }

    /**
     * @param r Query run.
     * @param nodes Nodes to check periodically if they alive.
     * @param cancel Query cancel.
     * @throws IgniteInterruptedCheckedException If interrupted.
     */
    private void awaitAllReplies(ReduceQueryRun r, Collection<ClusterNode> nodes, GridQueryCancel cancel)
        throws IgniteInterruptedCheckedException, QueryCancelledException {
        while (!U.await(r.latch(), 500, TimeUnit.MILLISECONDS)) {

            cancel.checkCancelled();

            for (ClusterNode node : nodes) {
                if (!ctx.discovery().alive(node)) {
                    handleNodeLeft(r, node.id());

                    assert r.latch().getCount() == 0;

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
    private GridThreadLocalTable fakeTable(Connection c, int idx) {
        List<GridThreadLocalTable> tbls = fakeTbls;

        assert tbls.size() >= idx;

        if (tbls.size() == idx) { // If table for such index does not exist, create one.
            fakeTblsLock.lock();

            try {
                if ((tbls = fakeTbls).size() == idx) { // Double check inside of lock.
                    try (Statement stmt = c.createStatement()) {
                        stmt.executeUpdate("CREATE TABLE " + mergeTableIdentifier(idx) +
                            "(fake BOOL) ENGINE \"" + GridThreadLocalTable.Engine.class.getName() + '"');
                    }
                    catch (SQLException e) {
                        throw new IllegalStateException(e);
                    }

                    List<GridThreadLocalTable> newTbls = new ArrayList<>(tbls.size() + 1);

                    newTbls.addAll(tbls);
                    newTbls.add(GridThreadLocalTable.Engine.getCreated());

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
     * Calculates data nodes for replicated caches on unstable topology.
     *
     * @param cacheIds Cache IDs.
     * @param qryId Query ID.
     * @return Collection of all data nodes owning all the caches or {@code null} for retry.
     */
    private Collection<ClusterNode> replicatedUnstableDataNodes(List<Integer> cacheIds, long qryId) {
        int i = 0;

        GridCacheContext<?, ?> cctx = cacheContext(cacheIds.get(i++));

        // The main cache is allowed to be partitioned.
        if (!cctx.isReplicated()) {
            assert cacheIds.size() > 1: "no extra replicated caches with partitioned main cache";

            // Just replace the main cache with the first one extra.
            cctx = cacheContext(cacheIds.get(i++));

            assert cctx.isReplicated(): "all the extra caches must be replicated here";
        }

        Set<ClusterNode> nodes = replicatedUnstableDataNodes(cctx, qryId);

        if (F.isEmpty(nodes))
            return null; // Retry.

        for (;i < cacheIds.size(); i++) {
            GridCacheContext<?, ?> extraCctx = cacheContext(cacheIds.get(i));

            if (extraCctx.isLocal())
                continue;

            if (!extraCctx.isReplicated())
                throw new CacheException("Queries running on replicated cache should not contain JOINs " +
                    "with tables in partitioned caches [replicatedCache=" + cctx.name() + ", " +
                    "partitionedCache=" + extraCctx.name() + "]");

            Set<ClusterNode> extraOwners = replicatedUnstableDataNodes(extraCctx, qryId);

            if (F.isEmpty(extraOwners))
                return null; // Retry.

            nodes.retainAll(extraOwners);

            if (nodes.isEmpty()) {
                logRetry("Failed to calculate nodes for SQL query (got disjoint node map for REPLICATED caches " +
                    "during rebalance) [qryId=" + qryId + ", cacheIds=" + cacheIds +
                    ", lastCache=" + extraCctx.name() + ", lastCacheId=" + extraCctx.cacheId() + ']');

                return null; // Retry.
            }
        }

        return nodes;
    }

    /**
     * @param grpId Cache group ID.
     * @param topVer Topology version.
     * @return Collection of data nodes.
     */
    private Collection<ClusterNode> dataNodes(int grpId, AffinityTopologyVersion topVer) {
        Collection<ClusterNode> res = ctx.discovery().cacheGroupAffinityNodes(grpId, topVer);

        return res != null ? res : Collections.<ClusterNode>emptySet();
    }

    /**
     * Collects all the nodes owning all the partitions for the given replicated cache.
     *
     * @param cctx Cache context.
     * @param qryId Query ID.
     * @return Owning nodes or {@code null} if we can't find owners for some partitions.
     */
    private Set<ClusterNode> replicatedUnstableDataNodes(GridCacheContext<?,?> cctx, long qryId) {
        assert cctx.isReplicated() : cctx.name() + " must be replicated";

        String cacheName = cctx.name();

        Set<ClusterNode> dataNodes = new HashSet<>(dataNodes(cctx.groupId(), NONE));

        if (dataNodes.isEmpty())
            throw new CacheException("Failed to find data nodes for cache: " + cacheName);

        // Find all the nodes owning all the partitions for replicated cache.
        for (int p = 0, parts = cctx.affinity().partitions(); p < parts; p++) {
            List<ClusterNode> owners = cctx.topology().owners(p);

            if (F.isEmpty(owners)) {
                logRetry("Failed to calculate nodes for SQL query (partition of a REPLICATED cache has no owners) [" +
                    "qryId=" + qryId + ", cacheName=" + cctx.name() + ", cacheId=" + cctx.cacheId() +
                    ", part=" + p + ']');

                return null; // Retry.
            }

            dataNodes.retainAll(owners);

            if (dataNodes.isEmpty()) {
                logRetry("Failed to calculate nodes for SQL query (partitions of a REPLICATED has no common owners) [" +
                    "qryId=" + qryId + ", cacheName=" + cctx.name() + ", cacheId=" + cctx.cacheId() +
                    ", lastPart=" + p + ']');

                return null; // Retry.
            }
        }

        return dataNodes;
    }

    /**
     * Calculates partition mapping for partitioned cache on unstable topology.
     *
     * @param cacheIds Cache IDs.
     * @param qryId Query ID.
     * @return Partition mapping or {@code null} if we can't calculate it due to repartitioning and we need to retry.
     */
    @SuppressWarnings("unchecked")
    private Map<ClusterNode, IntArray> partitionedUnstableDataNodes(List<Integer> cacheIds, long qryId) {
        // If the main cache is replicated, just replace it with the first partitioned.
        GridCacheContext<?,?> cctx = findFirstPartitioned(cacheIds);

        final int partsCnt = cctx.affinity().partitions();

        if (cacheIds.size() > 1) { // Check correct number of partitions for partitioned caches.
            for (Integer cacheId : cacheIds) {
                GridCacheContext<?, ?> extraCctx = cacheContext(cacheId);

                if (extraCctx.isReplicated() || extraCctx.isLocal())
                    continue;

                int parts = extraCctx.affinity().partitions();

                if (parts != partsCnt)
                    throw new CacheException("Number of partitions must be the same for correct collocation [cache1=" +
                        cctx.name() + ", parts1=" + partsCnt + ", cache2=" + extraCctx.name() +
                        ", parts2=" + parts + "]");
            }
        }

        Set<ClusterNode>[] partLocs = new Set[partsCnt];

        // Fill partition locations for main cache.
        for (int p = 0; p < partsCnt; p++) {
            List<ClusterNode> owners = cctx.topology().owners(p);

            if (F.isEmpty(owners)) {
                // Handle special case: no mapping is configured for a partition.
                if (F.isEmpty(cctx.affinity().assignment(NONE).get(p))) {
                    partLocs[p] = UNMAPPED_PARTS; // Mark unmapped partition.

                    continue;
                }
                else if (!F.isEmpty(dataNodes(cctx.groupId(), NONE))) {
                    logRetry("Failed to calculate nodes for SQL query (partition has no owners, but corresponding " +
                        "cache group has data nodes) [qryId=" + qryId + ", cacheIds=" + cacheIds +
                        ", cacheName=" + cctx.name() + ", cacheId=" + cctx.cacheId() + ", part=" + p +
                        ", cacheGroupId=" + cctx.groupId() + ']');

                    return null; // Retry.
                }

                throw new CacheException("Failed to find data nodes [cache=" + cctx.name() + ", part=" + p + "]");
            }

            partLocs[p] = new HashSet<>(owners);
        }

        if (cacheIds.size() > 1) {
            // Find owner intersections for each participating partitioned cache partition.
            // We need this for logical collocation between different partitioned caches with the same affinity.
            for (Integer cacheId : cacheIds) {
                GridCacheContext<?, ?> extraCctx = cacheContext(cacheId);

                // This is possible if we have replaced a replicated cache with a partitioned one earlier.
                if (cctx == extraCctx)
                    continue;

                if (extraCctx.isReplicated() || extraCctx.isLocal())
                    continue;

                for (int p = 0, parts = extraCctx.affinity().partitions(); p < parts; p++) {
                    List<ClusterNode> owners = extraCctx.topology().owners(p);

                    if (partLocs[p] == UNMAPPED_PARTS)
                        continue; // Skip unmapped partitions.

                    if (F.isEmpty(owners)) {
                        if (!F.isEmpty(dataNodes(extraCctx.groupId(), NONE))) {
                            logRetry("Failed to calculate nodes for SQL query (partition has no owners, but " +
                                "corresponding cache group has data nodes) [qryId=" + qryId +
                                ", cacheIds=" + cacheIds + ", cacheName=" + extraCctx.name() +
                                ", cacheId=" + extraCctx.cacheId() + ", part=" + p +
                                ", cacheGroupId=" + extraCctx.groupId() + ']');

                            return null; // Retry.
                        }

                        throw new CacheException("Failed to find data nodes [cache=" + extraCctx.name() +
                            ", part=" + p + "]");
                    }

                    if (partLocs[p] == null)
                        partLocs[p] = new HashSet<>(owners);
                    else {
                        partLocs[p].retainAll(owners); // Intersection of owners.

                        if (partLocs[p].isEmpty()) {
                            logRetry("Failed to calculate nodes for SQL query (caches have no common data nodes for " +
                                "partition) [qryId=" + qryId + ", cacheIds=" + cacheIds +
                                ", lastCacheName=" + extraCctx.name() + ", lastCacheId=" + extraCctx.cacheId() +
                                ", part=" + p + ']');

                            return null; // Intersection is empty -> retry.
                        }
                    }
                }
            }

            // Filter nodes where not all the replicated caches loaded.
            for (Integer cacheId : cacheIds) {
                GridCacheContext<?, ?> extraCctx = cacheContext(cacheId);

                if (!extraCctx.isReplicated())
                    continue;

                Set<ClusterNode> dataNodes = replicatedUnstableDataNodes(extraCctx, qryId);

                if (F.isEmpty(dataNodes))
                    return null; // Retry.

                int part = 0;

                for (Set<ClusterNode> partLoc : partLocs) {
                    if (partLoc == UNMAPPED_PARTS)
                        continue; // Skip unmapped partition.

                    partLoc.retainAll(dataNodes);

                    if (partLoc.isEmpty()) {
                        logRetry("Failed to calculate nodes for SQL query (caches have no common data nodes for " +
                            "partition) [qryId=" + qryId + ", cacheIds=" + cacheIds +
                            ", lastReplicatedCacheName=" + extraCctx.name() +
                            ", lastReplicatedCacheId=" + extraCctx.cacheId() + ", part=" + part + ']');

                        return null; // Retry.
                    }

                    part++;
                }
            }
        }

        // Collect the final partitions mapping.
        Map<ClusterNode, IntArray> res = new HashMap<>();

        // Here partitions in all IntArray's will be sorted in ascending order, this is important.
        for (int p = 0; p < partLocs.length; p++) {
            Set<ClusterNode> pl = partLocs[p];

            // Skip unmapped partitions.
            if (pl == UNMAPPED_PARTS)
                continue;

            assert !F.isEmpty(pl) : pl;

            ClusterNode n = pl.size() == 1 ? F.first(pl) : F.rand(pl);

            IntArray parts = res.get(n);

            if (parts == null)
                res.put(n, parts = new IntArray());

            parts.add(p);
        }

        return res;
    }

    /**
     * @param c Connection.
     * @param qry Query.
     * @param params Query parameters.
     * @return Cursor for plans.
     * @throws IgniteCheckedException if failed.
     */
    private Iterator<List<?>> explainPlan(JdbcConnection c, GridCacheTwoStepQuery qry, Object[] params)
        throws IgniteCheckedException {
        List<List<?>> lists = new ArrayList<>();

        for (int i = 0, mapQrys = qry.mapQueries().size(); i < mapQrys; i++) {
            ResultSet rs =
                h2.executeSqlQueryWithTimer(c, "SELECT PLAN FROM " + mergeTableIdentifier(i), null, false, 0, null);

            lists.add(F.asList(getPlan(rs)));
        }

        int tblIdx = 0;

        for (GridCacheSqlQuery mapQry : qry.mapQueries()) {
            GridMergeTable tbl = createMergeTable(c, mapQry, false);

            fakeTable(c, tblIdx++).innerTable(tbl);
        }

        GridCacheSqlQuery rdc = qry.reduceQuery();

        ResultSet rs = h2.executeSqlQueryWithTimer(c,
            "EXPLAIN " + rdc.query(),
            F.asList(rdc.parameters(params)),
            false,
            0,
            null);

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
            runLocParallel);
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
     * Evaluates nodes and nodes to partitions map given a list of cache ids, topology version and partitions.
     *
     * @param cacheIds Cache ids.
     * @param topVer Topology version.
     * @param parts Partitions array.
     * @param isReplicatedOnly Allow only replicated caches.
     * @param qryId Query ID.
     * @return Result.
     */
    private NodesForPartitionsResult nodesForPartitions(List<Integer> cacheIds, AffinityTopologyVersion topVer,
        int[] parts, boolean isReplicatedOnly, long qryId) {
        Collection<ClusterNode> nodes = null;
        Map<ClusterNode, IntArray> partsMap = null;
        Map<ClusterNode, IntArray> qryMap = null;

        if (isPreloadingActive(cacheIds)) {
            if (isReplicatedOnly)
                nodes = replicatedUnstableDataNodes(cacheIds, qryId);
            else {
                partsMap = partitionedUnstableDataNodes(cacheIds, qryId);

                if (partsMap != null) {
                    qryMap = narrowForQuery(partsMap, parts);

                    nodes = qryMap == null ? null : qryMap.keySet();
                }
            }
        }
        else {
            qryMap = stableDataNodes(isReplicatedOnly, topVer, cacheIds, parts, qryId);

            if (qryMap != null)
                nodes = qryMap.keySet();
        }

        return new NodesForPartitionsResult(nodes, partsMap, qryMap);
    }

    /**
     * @param conn Connection.
     * @param qry Query.
     * @param explain Explain.
     * @return Table.
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings("unchecked")
    private GridMergeTable createMergeTable(JdbcConnection conn, GridCacheSqlQuery qry, boolean explain)
        throws IgniteCheckedException {
        try {
            Session ses = (Session)conn.getSession();

            CreateTableData data  = new CreateTableData();

            data.tableName = "T___";
            data.schema = ses.getDatabase().getSchema(ses.getCurrentSchemaName());
            data.create = true;

            if (!explain) {
                LinkedHashMap<String,?> colsMap = qry.columns();

                assert colsMap != null;

                ArrayList<Column> cols = new ArrayList<>(colsMap.size());

                for (Map.Entry<String,?> e : colsMap.entrySet()) {
                    String alias = e.getKey();
                    GridSqlType t = (GridSqlType)e.getValue();

                    assert !F.isEmpty(alias);

                    Column c = new Column(alias, t.type(), t.precision(), t.scale(), t.displaySize());

                    cols.add(c);
                }

                data.columns = cols;
            }
            else
                data.columns = planColumns();

            boolean sortedIndex = !F.isEmpty(qry.sortColumns());

            GridMergeTable tbl = new GridMergeTable(data);

            ArrayList<Index> idxs = new ArrayList<>(2);

            if (explain) {
                idxs.add(new GridMergeIndexUnsorted(ctx, tbl,
                    sortedIndex ? MERGE_INDEX_SORTED : MERGE_INDEX_UNSORTED));
            }
            else if (sortedIndex) {
                List<GridSqlSortColumn> sortCols = (List<GridSqlSortColumn>)qry.sortColumns();

                GridMergeIndexSorted sortedMergeIdx = new GridMergeIndexSorted(ctx, tbl, MERGE_INDEX_SORTED,
                    GridSqlSortColumn.toIndexColumns(tbl, sortCols));

                idxs.add(GridMergeTable.createScanIndex(sortedMergeIdx));
                idxs.add(sortedMergeIdx);
            }
            else
                idxs.add(new GridMergeIndexUnsorted(ctx, tbl, MERGE_INDEX_UNSORTED));

            tbl.indexes(idxs);

            return tbl;
        }
        catch (Exception e) {
            U.closeQuiet(conn);

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

        for (DistributedUpdateRun r: updRuns.values())
            r.handleDisconnect(err);
    }

    /**
     * Collect queries that already running more than specified duration.
     *
     * @param duration Duration to check.
     * @return Collection of IDs and statements of long running queries.
     */
    public Collection<GridRunningQueryInfo> longRunningQueries(long duration) {
        Collection<GridRunningQueryInfo> res = new ArrayList<>();

        long curTime = U.currentTimeMillis();

        for (ReduceQueryRun run : runs.values()) {
            if (run.queryInfo().longQuery(curTime, duration))
                res.add(run.queryInfo());
        }

        for (DistributedUpdateRun upd: updRuns.values()) {
            if (upd.queryInfo().longQuery(curTime, duration))
                res.add(upd.queryInfo());
        }

        return res;
    }

    /**
     * Cancel specified queries.
     *
     * @param queries Queries IDs to cancel.
     */
    public void cancelQueries(Collection<Long> queries) {
        for (Long qryId : queries) {
            ReduceQueryRun run = runs.get(qryId);

            if (run != null)
                run.queryInfo().cancel();
            else {
                DistributedUpdateRun upd = updRuns.get(qryId);

                if (upd != null)
                    upd.queryInfo().cancel();
            }
        }
    }

    /** */
    private Map<ClusterNode, IntArray> narrowForQuery(Map<ClusterNode, IntArray> partsMap, int[] parts) {
        if (parts == null)
            return partsMap;

        Map<ClusterNode, IntArray> cp = U.newHashMap(partsMap.size());

        for (Map.Entry<ClusterNode, IntArray> entry : partsMap.entrySet()) {
            IntArray filtered = new IntArray(parts.length);

            IntArray orig = entry.getValue();

            for (int i = 0; i < orig.size(); i++) {
                int p = orig.get(i);

                if (Arrays.binarySearch(parts, p) >= 0)
                    filtered.add(p);
            }

            if (filtered.size() > 0)
                cp.put(entry.getKey(), filtered);
        }

        return cp.isEmpty() ? null : cp;
    }

    /**
     * @param qryTimeout Query timeout.
     * @return Query retry timeout.
     */
    private static long retryTimeout(long qryTimeout) {
        if (qryTimeout > 0)
            return qryTimeout;

        return IgniteSystemProperties.getLong(IGNITE_SQL_RETRY_TIMEOUT, DFLT_RETRY_TIMEOUT);
    }

    /** */
    private static class ExplicitPartitionsSpecializer implements C2<ClusterNode, Message, Message> {
        /** Partitions map. */
        private final Map<ClusterNode, IntArray> partsMap;

        /**
         * @param partsMap Partitions map.
         */
        public ExplicitPartitionsSpecializer(Map<ClusterNode, IntArray> partsMap) {
            this.partsMap = partsMap;
        }

        /** {@inheritDoc} */
        @Override public Message apply(ClusterNode node, Message msg) {
            if (msg instanceof GridH2QueryRequest) {
                GridH2QueryRequest rq = new GridH2QueryRequest((GridH2QueryRequest)msg);

                rq.queryPartitions(toArray(partsMap.get(node)));

                return rq;
            } else if (msg instanceof GridH2DmlRequest) {
                GridH2DmlRequest rq = new GridH2DmlRequest((GridH2DmlRequest)msg);

                rq.queryPartitions(toArray(partsMap.get(node)));

                return rq;
            }

            return msg;
        }
    }

    /**
     * Result of nodes to partitions mapping for a query or update.
     */
    static class NodesForPartitionsResult {
        /** */
        final Collection<ClusterNode> nodes;

        /** */
        final Map<ClusterNode, IntArray> partsMap;

        /** */
        final Map<ClusterNode, IntArray> qryMap;

        /** */
        NodesForPartitionsResult(Collection<ClusterNode> nodes, Map<ClusterNode, IntArray> partsMap,
            Map<ClusterNode, IntArray> qryMap) {
            this.nodes = nodes;
            this.partsMap = partsMap;
            this.qryMap = qryMap;
        }

        /**
         * @return Collection of nodes a message shall be sent to.
         */
        Collection<ClusterNode> nodes() {
            return nodes;
        }

        /**
         * @return Maps a node to partition array.
         */
        Map<ClusterNode, IntArray> partitionsMap() {
            return partsMap;
        }

        /**
         * @return Maps a node to partition array.
         */
        Map<ClusterNode, IntArray> queryPartitionsMap() {
            return qryMap;
        }
    }
}
