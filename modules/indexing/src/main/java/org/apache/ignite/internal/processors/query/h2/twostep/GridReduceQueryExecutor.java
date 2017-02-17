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

import java.lang.reflect.Constructor;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
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
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import javax.cache.CacheException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteClientDisconnectedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
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
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryMarshallable;
import org.apache.ignite.internal.processors.cache.query.GridCacheSqlQuery;
import org.apache.ignite.internal.processors.cache.query.GridCacheTwoStepQuery;
import org.apache.ignite.internal.processors.query.GridRunningQueryInfo;
import org.apache.ignite.internal.processors.query.GridQueryCacheObjectsIterator;
import org.apache.ignite.internal.processors.query.GridQueryCancel;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2QueryContext;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlQuerySplitter;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlType;
import org.apache.ignite.internal.processors.query.h2.twostep.messages.GridQueryCancelRequest;
import org.apache.ignite.internal.processors.query.h2.twostep.messages.GridQueryFailResponse;
import org.apache.ignite.internal.processors.query.h2.twostep.messages.GridQueryNextPageRequest;
import org.apache.ignite.internal.processors.query.h2.twostep.messages.GridQueryNextPageResponse;
import org.apache.ignite.internal.processors.query.h2.twostep.messages.GridQueryRequest;
import org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2QueryRequest;
import org.apache.ignite.internal.util.GridSpinBusyLock;
import org.apache.ignite.internal.util.typedef.CIX2;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiClosure;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.h2.command.ddl.CreateTableData;
import org.h2.engine.Session;
import org.h2.index.Cursor;
import org.h2.jdbc.JdbcConnection;
import org.h2.jdbc.JdbcResultSet;
import org.h2.jdbc.JdbcStatement;
import org.h2.result.ResultInterface;
import org.h2.result.Row;
import org.h2.table.Column;
import org.h2.util.IntArray;
import org.h2.value.Value;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentHashMap8;

import static org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion.NONE;
import static org.apache.ignite.internal.processors.cache.query.GridCacheQueryType.SQL_FIELDS;
import static org.apache.ignite.internal.processors.query.h2.opt.GridH2QueryType.REDUCE;

/**
 * Reduce query executor.
 */
public class GridReduceQueryExecutor {
    /** Thread pool to process query messages. */
    public static final byte QUERY_POOL = GridIoPolicy.SYSTEM_POOL;

    /** */
    private static final IgniteProductVersion DISTRIBUTED_JOIN_SINCE = IgniteProductVersion.fromString("1.7.0");

    /** */
    private GridKernalContext ctx;

    /** */
    private IgniteH2Indexing h2;

    /** */
    private IgniteLogger log;

    /** */
    private final AtomicLong qryIdGen;

    /** */
    private final ConcurrentMap<Long, QueryRun> runs = new ConcurrentHashMap8<>();

    /** */
    private volatile List<GridThreadLocalTable> fakeTbls = Collections.emptyList();

    /** */
    private final Lock fakeTblsLock = new ReentrantLock();

    /** */
    private static final Constructor<JdbcResultSet> CONSTRUCTOR;

    /**
     * Init constructor.
     */
    static {
        try {
            CONSTRUCTOR = JdbcResultSet.class.getDeclaredConstructor(
                JdbcConnection.class,
                JdbcStatement.class,
                ResultInterface.class,
                Integer.TYPE,
                Boolean.TYPE,
                Boolean.TYPE,
                Boolean.TYPE
            );

            CONSTRUCTOR.setAccessible(true);
        }
        catch (NoSuchMethodException e) {
            throw new IllegalStateException("Check H2 version in classpath.", e);
        }
    }

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
            @Override public void onMessage(UUID nodeId, Object msg) {
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

                for (QueryRun r : runs.values()) {
                    for (GridMergeIndex idx : r.idxs) {
                        if (idx.hasSource(nodeId)) {
                            handleNodeLeft(r, nodeId);

                            break;
                        }
                    }
                }
            }
        }, EventType.EVT_NODE_FAILED, EventType.EVT_NODE_LEFT);
    }

    /**
     * @param r Query run.
     * @param nodeId Left node ID.
     */
    private void handleNodeLeft(QueryRun r, UUID nodeId) {
        // Will attempt to retry. If reduce query was started it will fail on next page fetching.
        retry(r, h2.readyTopologyVersion(), nodeId);
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
        QueryRun r = runs.get(msg.queryRequestId());

        fail(r, node.id(), msg.error(), msg.failCode());
    }

    /**
     * @param r Query run.
     * @param nodeId Failed node ID.
     * @param msg Error message.
     */
    private void fail(QueryRun r, UUID nodeId, String msg, byte failCode) {
        if (r != null) {
            CacheException e = new CacheException("Failed to execute map query on the node: " + nodeId + ", " + msg);

            if (failCode == GridQueryFailResponse.CANCELLED_BY_ORIGINATOR)
                e.addSuppressed(new QueryCancelledException());

            r.state(e, nodeId);
        }
    }

    /**
     * @param node Node.
     * @param msg Message.
     */
    private void onNextPage(final ClusterNode node, GridQueryNextPageResponse msg) {
        final long qryReqId = msg.queryRequestId();
        final int qry = msg.query();

        final QueryRun r = runs.get(qryReqId);

        if (r == null) // Already finished with error or canceled.
            return;

        final int pageSize = r.pageSize;

        GridMergeIndex idx = r.idxs.get(msg.query());

        GridResultPage page;

        try {
            page = new GridResultPage(ctx, node.id(), msg) {
                @Override public void fetchNextPage() {
                    Object errState = r.state.get();

                    if (errState != null) {
                        CacheException err0 = errState instanceof CacheException ? (CacheException)errState : null;

                        if (err0 != null && err0.getCause() instanceof IgniteClientDisconnectedException)
                            throw err0;

                        CacheException e = new CacheException("Failed to fetch data from node: " + node.id());

                        if (err0 != null)
                            e.addSuppressed(err0);

                        throw e;
                    }

                    try {
                        GridQueryNextPageRequest msg0 = new GridQueryNextPageRequest(qryReqId, qry, pageSize);

                        if (node.isLocal())
                            h2.mapQueryExecutor().onMessage(ctx.localNodeId(), msg0);
                        else
                            ctx.io().send(node, GridTopic.TOPIC_QUERY, msg0, QUERY_POOL);
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
            retry(r, msg.retry(), node.id());
        else if (msg.allRows() != -1) // Only the first page contains row count.
            r.latch.countDown();
    }

    /**
     * @param r Query run.
     * @param retryVer Retry version.
     * @param nodeId Node ID.
     */
    private void retry(QueryRun r, AffinityTopologyVersion retryVer, UUID nodeId) {
        r.state(retryVer, nodeId);
    }

    /**
     * @param cctx Cache context for main space.
     * @param extraSpaces Extra spaces.
     * @return {@code true} If preloading is active.
     */
    private boolean isPreloadingActive(final GridCacheContext<?, ?> cctx, List<Integer> extraSpaces) {
        if (hasMovingPartitions(cctx))
            return true;

        if (extraSpaces != null) {
            for (int i = 0; i < extraSpaces.size(); i++) {
                if (hasMovingPartitions(cacheContext(extraSpaces.get(i))))
                    return true;
            }
        }

        return false;
    }

    /**
     * @param cctx Cache context.
     * @return {@code True} If cache has partitions in {@link GridDhtPartitionState#MOVING} state.
     */
    private boolean hasMovingPartitions(GridCacheContext<?, ?> cctx) {
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
     * @param cctx Cache context for main space.
     * @param extraSpaces Extra spaces.
     * @return Data nodes or {@code null} if repartitioning started and we need to retry.
     */
    private Collection<ClusterNode> stableDataNodes(
        AffinityTopologyVersion topVer,
        final GridCacheContext<?, ?> cctx,
        List<Integer> extraSpaces
    ) {
        Set<ClusterNode> nodes = new HashSet<>(cctx.affinity().assignment(topVer).primaryPartitionNodes());

        if (F.isEmpty(nodes))
            throw new CacheException("Failed to find data nodes for cache: " + cctx.name());

        if (!F.isEmpty(extraSpaces)) {
            for (int i = 0; i < extraSpaces.size(); i++) {
                GridCacheContext<?,?> extraCctx = cacheContext(extraSpaces.get(i));

                String extraSpace = extraCctx.name();

                if (extraCctx.isLocal())
                    continue; // No consistency guaranties for local caches.

                if (cctx.isReplicated() && !extraCctx.isReplicated())
                    throw new CacheException("Queries running on replicated cache should not contain JOINs " +
                        "with partitioned tables [rCache=" + cctx.name() + ", pCache=" + extraSpace + "]");

                Collection<ClusterNode> extraNodes = extraCctx.affinity().assignment(topVer).primaryPartitionNodes();

                if (F.isEmpty(extraNodes))
                    throw new CacheException("Failed to find data nodes for cache: " + extraSpace);

                if (cctx.isReplicated() && extraCctx.isReplicated()) {
                    nodes.retainAll(extraNodes);

                    if (nodes.isEmpty()) {
                        if (isPreloadingActive(cctx, extraSpaces))
                            return null; // Retry.
                        else
                            throw new CacheException("Caches have distinct sets of data nodes [cache1=" + cctx.name() +
                                ", cache2=" + extraSpace + "]");
                    }
                }
                else if (!cctx.isReplicated() && extraCctx.isReplicated()) {
                    if (!extraNodes.containsAll(nodes))
                        if (isPreloadingActive(cctx, extraSpaces))
                            return null; // Retry.
                        else
                            throw new CacheException("Caches have distinct sets of data nodes [cache1=" + cctx.name() +
                                ", cache2=" + extraSpace + "]");
                }
                else if (!cctx.isReplicated() && !extraCctx.isReplicated()) {
                    if (extraNodes.size() != nodes.size() || !nodes.containsAll(extraNodes))
                        if (isPreloadingActive(cctx, extraSpaces))
                            return null; // Retry.
                        else
                            throw new CacheException("Caches have distinct sets of data nodes [cache1=" + cctx.name() +
                                ", cache2=" + extraSpace + "]");
                }
                else
                    throw new IllegalStateException();
            }
        }

        return nodes;
    }

    /**
     * @param cctx Cache context.
     * @param qry Query.
     * @param keepPortable Keep portable.
     * @param enforceJoinOrder Enforce join order of tables.
     * @param timeoutMillis Timeout in milliseconds.
     * @param cancel Query cancel.
     * @return Rows iterator.
     */
    public Iterator<List<?>> query(
        GridCacheContext<?, ?> cctx,
        GridCacheTwoStepQuery qry,
        boolean keepPortable,
        boolean enforceJoinOrder,
        int timeoutMillis,
        GridQueryCancel cancel
    ) {
        for (int attempt = 0;; attempt++) {
            if (attempt != 0) {
                try {
                    Thread.sleep(attempt * 10); // Wait for exchange.
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();

                    throw new CacheException("Query was interrupted.", e);
                }
            }

            final long qryReqId = qryIdGen.incrementAndGet();

            final String space = cctx.name();

            final QueryRun r = new QueryRun(qryReqId, qry.originalSql(), space,
                h2.connectionForSpace(space), qry.mapQueries().size(), qry.pageSize(),
                U.currentTimeMillis(), cancel);

            AffinityTopologyVersion topVer = h2.readyTopologyVersion();

            List<Integer> extraSpaces = qry.extraCaches();

            Collection<ClusterNode> nodes;

            // Explicit partition mapping for unstable topology.
            Map<ClusterNode, IntArray> partsMap = null;

            if (isPreloadingActive(cctx, extraSpaces)) {
                if (cctx.isReplicated())
                    nodes = replicatedUnstableDataNodes(cctx, extraSpaces);
                else {
                    partsMap = partitionedUnstableDataNodes(cctx, extraSpaces);

                    nodes = partsMap == null ? null : partsMap.keySet();
                }
            }
            else
                nodes = stableDataNodes(topVer, cctx, extraSpaces);

            if (nodes == null)
                continue; // Retry.

            assert !nodes.isEmpty();

            if (cctx.isReplicated() || qry.explain()) {
                assert qry.explain() || !nodes.contains(ctx.discovery().localNode()) :
                    "We must be on a client node.";

                // Select random data node to run query on a replicated data or get EXPLAIN PLAN from a single node.
                nodes = Collections.singleton(F.rand(nodes));
            }

            final Collection<ClusterNode> finalNodes = nodes;

            int tblIdx = 0;

            final boolean skipMergeTbl = !qry.explain() && qry.skipMergeTable();

            for (GridCacheSqlQuery mapQry : qry.mapQueries()) {
                GridMergeIndex idx;

                if (!skipMergeTbl) {
                    GridMergeTable tbl;

                    try {
                        tbl = createMergeTable(r.conn, mapQry, qry.explain());
                    }
                    catch (IgniteCheckedException e) {
                        throw new IgniteException(e);
                    }

                    idx = tbl.getScanIndex(null);

                    fakeTable(r.conn, tblIdx++).innerTable(tbl);
                }
                else
                    idx = GridMergeIndexUnsorted.createDummy(ctx);

                idx.setSources(nodes);

                r.idxs.add(idx);
            }

            r.latch = new CountDownLatch(r.idxs.size() * nodes.size());

            runs.put(qryReqId, r);

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
                        mapQrys.add(new GridCacheSqlQuery("EXPLAIN " + mapQry.query(), mapQry.parameters()));
                }

                IgniteProductVersion minNodeVer = cctx.shared().exchange().minimumNodeVersion(topVer);

                final boolean oldStyle = minNodeVer.compareToIgnoreTimestamp(DISTRIBUTED_JOIN_SINCE) < 0;
                final boolean distributedJoins = qry.distributedJoins();

                cancel.set(new Runnable() {
                    @Override public void run() {
                        send(finalNodes, new GridQueryCancelRequest(qryReqId), null, false);
                    }
                });

                boolean retry = false;

                if (oldStyle && distributedJoins)
                    throw new CacheException("Failed to enable distributed joins. Topology contains older data nodes.");

                if (send(nodes,
                    oldStyle ?
                        new GridQueryRequest(qryReqId,
                            r.pageSize,
                            space,
                            mapQrys,
                            topVer,
                            extraSpaces(space, qry.spaces()),
                            null,
                            timeoutMillis) :
                        new GridH2QueryRequest()
                            .requestId(qryReqId)
                            .topologyVersion(topVer)
                            .pageSize(r.pageSize)
                            .caches(qry.caches())
                            .tables(distributedJoins ? qry.tables() : null)
                            .partitions(convert(partsMap))
                            .queries(mapQrys)
                            .flags(distributedJoins ? GridH2QueryRequest.FLAG_DISTRIBUTED_JOINS : 0)
                            .timeout(timeoutMillis),
                    oldStyle && partsMap != null ? new ExplicitPartitionsSpecializer(partsMap) : null,
                    distributedJoins)
                    ) {
                    awaitAllReplies(r, nodes);

                    cancel.checkCancelled();

                    Object state = r.state.get();

                    if (state != null) {
                        if (state instanceof CacheException) {
                            CacheException err = (CacheException)state;

                            if (err.getCause() instanceof IgniteClientDisconnectedException)
                                throw err;

                            if (wasCancelled(err))
                                throw new QueryCancelledException(); // Throw correct exception.

                            throw new CacheException("Failed to run map query remotely.", err);
                        }

                        if (state instanceof AffinityTopologyVersion) {
                            retry = true;

                            // If remote node asks us to retry then we have outdated full partition map.
                            h2.awaitForReadyTopologyVersion((AffinityTopologyVersion)state);
                        }
                    }
                }
                else // Send failed.
                    retry = true;

                Iterator<List<?>> resIter = null;

                if (!retry) {
                    if (skipMergeTbl) {
                        List<List<?>> res = new ArrayList<>();

                        assert r.idxs.size() == 1 : r.idxs;

                        GridMergeIndex idx = r.idxs.get(0);

                        Cursor cur = idx.findInStream(null, null);

                        while (cur.next()) {
                            Row row = cur.get();

                            int cols = row.getColumnCount();

                            List<Object> resRow = new ArrayList<>(cols);

                            for (int c = 0; c < cols; c++)
                                resRow.add(row.getValue(c).getObject());

                            res.add(resRow);
                        }

                        resIter = res.iterator();
                    }
                    else {
                        cancel.checkCancelled();

                        UUID locNodeId = ctx.localNodeId();

                        h2.setupConnection(r.conn, false, enforceJoinOrder);

                        GridH2QueryContext.set(new GridH2QueryContext(locNodeId, locNodeId, qryReqId, REDUCE)
                            .pageSize(r.pageSize).distributedJoins(false));

                        try {
                            if (qry.explain())
                                return explainPlan(r.conn, space, qry);

                            GridCacheSqlQuery rdc = qry.reduceQuery();

                            ResultSet res = h2.executeSqlQueryWithTimer(space,
                                r.conn,
                                rdc.query(),
                                F.asList(rdc.parameters()),
                                false,
                                timeoutMillis,
                                cancel);

                            resIter = new IgniteH2Indexing.FieldsIterator(res);
                        }
                        finally {
                            GridH2QueryContext.clearThreadLocal();
                        }
                    }
                }

                if (retry) {
                    if (Thread.currentThread().isInterrupted())
                        throw new IgniteInterruptedCheckedException("Query was interrupted.");

                    continue;
                }

                return new GridQueryCacheObjectsIterator(resIter, cctx, keepPortable);
            }
            catch (IgniteCheckedException | RuntimeException e) {
                U.closeQuiet(r.conn);

                if (e instanceof CacheException) {
                    if (wasCancelled((CacheException)e))
                        throw new CacheException("Failed to run reduce query locally.", new QueryCancelledException());

                    throw (CacheException)e;
                }

                Throwable cause = e;

                if (e instanceof IgniteCheckedException) {
                    Throwable disconnectedErr =
                        ((IgniteCheckedException)e).getCause(IgniteClientDisconnectedException.class);

                    if (disconnectedErr != null)
                        cause = disconnectedErr;
                }

                throw new CacheException("Failed to run reduce query locally.", cause);
            }
            finally {
                // Make sure any activity related to current attempt is cancelled.
                cancelRemoteQueriesIfNeeded(nodes, r, qryReqId, qry.distributedJoins());

                if (!runs.remove(qryReqId, r))
                    U.warn(log, "Query run was already removed: " + qryReqId);

                if (!skipMergeTbl) {
                    for (int i = 0, mapQrys = qry.mapQueries().size(); i < mapQrys; i++)
                        fakeTable(null, i).innerTable(null); // Drop all merge tables.
                }
            }
        }
    }

    /**
     * @param idxs Merge indexes.
     * @return {@code true} If all remote data was fetched.
     */
    private static boolean allIndexesFetched(List<GridMergeIndex> idxs) {
        for (int i = 0; i <  idxs.size(); i++) {
            if (!idxs.get(i).fetchedAll())
                return false;
        }

        return true;
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
     * @param nodes Query nodes.
     * @param r Query run.
     * @param qryReqId Query id.
     * @param distributedJoins Distributed join flag.
     */
    private void cancelRemoteQueriesIfNeeded(Collection<ClusterNode> nodes,
        QueryRun r,
        long qryReqId,
        boolean distributedJoins)
    {
        // For distributedJoins need always send cancel request to cleanup resources.
        if (distributedJoins)
            send(nodes, new GridQueryCancelRequest(qryReqId), null, false);
        else {
            for (GridMergeIndex idx : r.idxs) {
                if (!idx.fetchedAll()) {
                    send(nodes, new GridQueryCancelRequest(qryReqId), null, false);

                    break;
                }
            }
        }
    }

    /**
     * @param r Query run.
     * @param nodes Nodes to check periodically if they alive.
     * @throws IgniteInterruptedCheckedException If interrupted.
     */
    private void awaitAllReplies(QueryRun r, Collection<ClusterNode> nodes)
        throws IgniteInterruptedCheckedException {
        while (!U.await(r.latch, 500, TimeUnit.MILLISECONDS)) {
            for (ClusterNode node : nodes) {
                if (!ctx.discovery().alive(node)) {
                    handleNodeLeft(r, node.id());

                    assert r.latch.getCount() == 0;

                    return;
                }
            }
        }
    }

    /**
     * @param idx Table index.
     * @return Table name.
     */
    private static String table(int idx) {
        return GridSqlQuerySplitter.table(idx).getSQL();
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
                        stmt.executeUpdate("CREATE TABLE " + table(idx) +
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
     * @param cctx Cache context for main space.
     * @param extraSpaces Extra spaces.
     * @return Collection of all data nodes owning all the caches or {@code null} for retry.
     */
    private Collection<ClusterNode> replicatedUnstableDataNodes(final GridCacheContext<?, ?> cctx,
        List<Integer> extraSpaces) {
        assert cctx.isReplicated() : cctx.name() + " must be replicated";

        Set<ClusterNode> nodes = replicatedUnstableDataNodes(cctx);

        if (F.isEmpty(nodes))
            return null; // Retry.

        if (!F.isEmpty(extraSpaces)) {
            for (int i = 0; i < extraSpaces.size(); i++) {
                GridCacheContext<?, ?> extraCctx = cacheContext(extraSpaces.get(i));

                if (extraCctx.isLocal())
                    continue;

                if (!extraCctx.isReplicated())
                    throw new CacheException("Queries running on replicated cache should not contain JOINs " +
                        "with tables in partitioned caches [rCache=" + cctx.name() + ", pCache=" + extraCctx.name() + "]");

                Set<ClusterNode> extraOwners = replicatedUnstableDataNodes(extraCctx);

                if (F.isEmpty(extraOwners))
                    return null; // Retry.

                nodes.retainAll(extraOwners);

                if (nodes.isEmpty())
                    return null; // Retry.
            }
        }

        return nodes;
    }

    /**
     * @param space Cache name.
     * @param topVer Topology version.
     * @return Collection of data nodes.
     */
    private Collection<ClusterNode> dataNodes(String space, AffinityTopologyVersion topVer) {
        Collection<ClusterNode> res = ctx.discovery().cacheAffinityNodes(space, topVer);

        return res != null ? res : Collections.<ClusterNode>emptySet();
    }

    /**
     * Collects all the nodes owning all the partitions for the given replicated cache.
     *
     * @param cctx Cache context.
     * @return Owning nodes or {@code null} if we can't find owners for some partitions.
     */
    private Set<ClusterNode> replicatedUnstableDataNodes(GridCacheContext<?,?> cctx) {
        assert cctx.isReplicated() : cctx.name() + " must be replicated";

        String space = cctx.name();

        Set<ClusterNode> dataNodes = new HashSet<>(dataNodes(space, NONE));

        if (dataNodes.isEmpty())
            throw new CacheException("Failed to find data nodes for cache: " + space);

        // Find all the nodes owning all the partitions for replicated cache.
        for (int p = 0, parts = cctx.affinity().partitions(); p < parts; p++) {
            List<ClusterNode> owners = cctx.topology().owners(p);

            if (F.isEmpty(owners))
                return null; // Retry.

            dataNodes.retainAll(owners);

            if (dataNodes.isEmpty())
                return null; // Retry.
        }

        return dataNodes;
    }

    /**
     * Calculates partition mapping for partitioned cache on unstable topology.
     *
     * @param cctx Cache context for main space.
     * @param extraSpaces Extra spaces.
     * @return Partition mapping or {@code null} if we can't calculate it due to repartitioning and we need to retry.
     */
    @SuppressWarnings("unchecked")
    private Map<ClusterNode, IntArray> partitionedUnstableDataNodes(final GridCacheContext<?,?> cctx,
        List<Integer> extraSpaces) {
        assert !cctx.isReplicated() && !cctx.isLocal() : cctx.name() + " must be partitioned";

        final int partsCnt = cctx.affinity().partitions();

        if (extraSpaces != null) { // Check correct number of partitions for partitioned caches.
            for (int i = 0; i < extraSpaces.size(); i++) {
                GridCacheContext<?, ?> extraCctx = cacheContext(extraSpaces.get(i));

                if (extraCctx.isReplicated() || extraCctx.isLocal())
                    continue;

                int parts = extraCctx.affinity().partitions();

                if (parts != partsCnt)
                    throw new CacheException("Number of partitions must be the same for correct collocation [cache1=" +
                        cctx.name() + ", parts1=" + partsCnt + ", cache2=" + extraCctx.name() + ", parts2=" + parts + "]");
            }
        }

        Set<ClusterNode>[] partLocs = new Set[partsCnt];

        // Fill partition locations for main cache.
        for (int p = 0, parts =  cctx.affinity().partitions(); p < parts; p++) {
            List<ClusterNode> owners = cctx.topology().owners(p);

            if (F.isEmpty(owners)) {
                if (!F.isEmpty(dataNodes(cctx.name(), NONE)))
                    return null; // Retry.

                throw new CacheException("Failed to find data nodes [cache=" + cctx.name() + ", part=" + p + "]");
            }

            partLocs[p] = new HashSet<>(owners);
        }

        if (extraSpaces != null) {
            // Find owner intersections for each participating partitioned cache partition.
            // We need this for logical collocation between different partitioned caches with the same affinity.
            for (int i = 0; i < extraSpaces.size(); i++) {
                GridCacheContext<?, ?> extraCctx = cacheContext(extraSpaces.get(i));

                if (extraCctx.isReplicated() || extraCctx.isLocal())
                    continue;

                for (int p = 0, parts =  extraCctx.affinity().partitions(); p < parts; p++) {
                    List<ClusterNode> owners = extraCctx.topology().owners(p);

                    if (F.isEmpty(owners)) {
                        if (!F.isEmpty(dataNodes(extraCctx.name(), NONE)))
                            return null; // Retry.

                        throw new CacheException("Failed to find data nodes [cache=" + extraCctx.name() + ", part=" + p + "]");
                    }

                    if (partLocs[p] == null)
                        partLocs[p] = new HashSet<>(owners);
                    else {
                        partLocs[p].retainAll(owners); // Intersection of owners.

                        if (partLocs[p].isEmpty())
                            return null; // Intersection is empty -> retry.
                    }
                }
            }

            // Filter nodes where not all the replicated caches loaded.
            for (int i = 0; i < extraSpaces.size(); i++) {
                GridCacheContext<?,?> extraCctx = cacheContext(extraSpaces.get(i));

                if (!extraCctx.isReplicated())
                    continue;

                Set<ClusterNode> dataNodes = replicatedUnstableDataNodes(extraCctx);

                if (F.isEmpty(dataNodes))
                    return null; // Retry.

                for (Set<ClusterNode> partLoc : partLocs) {
                    partLoc.retainAll(dataNodes);

                    if (partLoc.isEmpty())
                        return null; // Retry.
                }
            }
        }

        // Collect the final partitions mapping.
        Map<ClusterNode, IntArray> res = new HashMap<>();

        // Here partitions in all IntArray's will be sorted in ascending order, this is important.
        for (int p = 0; p < partLocs.length; p++) {
            Set<ClusterNode> pl = partLocs[p];

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
     * @param mainSpace Main space.
     * @param allSpaces All spaces.
     * @return List of all extra spaces or {@code null} if none.
     */
    private List<String> extraSpaces(String mainSpace, Collection<String> allSpaces) {
        if (F.isEmpty(allSpaces) || (allSpaces.size() == 1 && allSpaces.contains(mainSpace)))
            return null;

        ArrayList<String> res = new ArrayList<>(allSpaces.size());

        for (String space : allSpaces) {
            if (!F.eq(space, mainSpace))
                res.add(space);
        }

        return res;
    }

    /**
     * @param c Connection.
     * @param space Space.
     * @param qry Query.
     * @return Cursor for plans.
     * @throws IgniteCheckedException if failed.
     */
    private Iterator<List<?>> explainPlan(JdbcConnection c, String space, GridCacheTwoStepQuery qry)
        throws IgniteCheckedException {
        List<List<?>> lists = new ArrayList<>();

        for (int i = 0, mapQrys = qry.mapQueries().size(); i < mapQrys; i++) {
            ResultSet rs = h2.executeSqlQueryWithTimer(space, c, "SELECT PLAN FROM " + table(i), null, false, 0, null);

            lists.add(F.asList(getPlan(rs)));
        }

        int tblIdx = 0;

        for (GridCacheSqlQuery mapQry : qry.mapQueries()) {
            GridMergeTable tbl = createMergeTable(c, mapQry, false);

            fakeTable(c, tblIdx++).innerTable(tbl);
        }

        GridCacheSqlQuery rdc = qry.reduceQuery();

        ResultSet rs = h2.executeSqlQueryWithTimer(space,
            c,
            "EXPLAIN " + rdc.query(),
            F.asList(rdc.parameters()),
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
    private boolean send(
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
            QUERY_POOL,
            runLocParallel);
    }

    /**
     * @param msg Message to copy.
     * @param node Node.
     * @param partsMap Partitions map.
     * @return Copy of message with partitions set.
     */
    private Message copy(Message msg, ClusterNode node, Map<ClusterNode,IntArray> partsMap) {
        assert partsMap != null;

        GridQueryRequest res = new GridQueryRequest((GridQueryRequest)msg);

        IntArray parts = partsMap.get(node);

        assert parts != null : node;

        res.partitions(toArray(parts));

        return res;
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

            return new GridMergeTable(data, ctx);
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

        for (Map.Entry<Long, QueryRun> e : runs.entrySet())
            e.getValue().disconnected(err);
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

        for (QueryRun run : runs.values()) {
            if (run.qry.longQuery(curTime, duration))
                res.add(run.qry);
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
            QueryRun run = runs.get(qryId);

            if (run != null)
                run.qry.cancel();
        }
    }

    /**
     * Query run.
     */
    private static class QueryRun {
        /** */
        private final GridRunningQueryInfo qry;

        /** */
        private final List<GridMergeIndex> idxs;

        /** */
        private CountDownLatch latch;

        /** */
        private final JdbcConnection conn;

        /** */
        private final int pageSize;

        /** Can be either CacheException in case of error or AffinityTopologyVersion to retry if needed. */
        private final AtomicReference<Object> state = new AtomicReference<>();

        /**
         * @param id Query ID.
         * @param qry Query text.
         * @param cache Cache where query was executed.
         * @param conn Connection.
         * @param idxsCnt Number of indexes.
         * @param pageSize Page size.
         * @param startTime Start time.
         * @param cancel Query cancel handler.
         */
        private QueryRun(Long id, String qry, String cache, Connection conn, int idxsCnt, int pageSize, long startTime, GridQueryCancel cancel) {
            this.qry = new GridRunningQueryInfo(id, qry, SQL_FIELDS, cache, startTime, cancel, false);
            this.conn = (JdbcConnection)conn;
            this.idxs = new ArrayList<>(idxsCnt);
            this.pageSize = pageSize > 0 ? pageSize : GridCacheTwoStepQuery.DFLT_PAGE_SIZE;
        }

        /**
         * @param o Fail state object.
         * @param nodeId Node ID.
         */
        void state(Object o, @Nullable UUID nodeId) {
            assert o != null;
            assert o instanceof CacheException || o instanceof AffinityTopologyVersion : o.getClass();

            if (!state.compareAndSet(null, o))
                return;

            while (latch.getCount() != 0) // We don't need to wait for all nodes to reply.
                latch.countDown();

            for (GridMergeIndex idx : idxs) // Fail all merge indexes.
                idx.fail(nodeId, o instanceof CacheException ? (CacheException) o : null);
        }

        /**
         * @param e Error.
         */
        void disconnected(CacheException e) {
            if (!state.compareAndSet(null, e))
                return;

            while (latch.getCount() != 0) // We don't need to wait for all nodes to reply.
                latch.countDown();

            for (GridMergeIndex idx : idxs) // Fail all merge indexes.
                idx.fail(e);
        }
    }

    /**
     *
     */
    private class ExplicitPartitionsSpecializer implements IgniteBiClosure<ClusterNode,Message,Message> {
        /** */
        private final Map<ClusterNode,IntArray> partsMap;

        /**
         * @param partsMap Partitions map.
         */
        private ExplicitPartitionsSpecializer(Map<ClusterNode,IntArray> partsMap) {
            this.partsMap = partsMap;
        }

        /** {@inheritDoc} */
        @Override public Message apply(ClusterNode n, Message msg) {
            return copy(msg, n, partsMap);
        }
    }
}
