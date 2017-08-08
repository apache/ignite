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
import java.util.AbstractCollection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import javax.cache.CacheException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.query.QueryCancelledException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.CacheQueryExecutedEvent;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.GridTopic;
import org.apache.ignite.internal.managers.communication.GridMessageListener;
import org.apache.ignite.internal.managers.eventstorage.GridLocalEventListener;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionsReservation;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridReservable;
import org.apache.ignite.internal.processors.cache.query.CacheQueryType;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryMarshallable;
import org.apache.ignite.internal.processors.cache.query.GridCacheSqlQuery;
import org.apache.ignite.internal.processors.cache.query.QueryTable;
import org.apache.ignite.internal.processors.query.h2.H2Utils;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.processors.query.h2.opt.DistributedJoinMode;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2QueryContext;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2RetryException;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.processors.query.h2.twostep.messages.GridQueryCancelRequest;
import org.apache.ignite.internal.processors.query.h2.twostep.messages.GridQueryFailResponse;
import org.apache.ignite.internal.processors.query.h2.twostep.messages.GridQueryNextPageRequest;
import org.apache.ignite.internal.processors.query.h2.twostep.messages.GridQueryNextPageResponse;
import org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2QueryRequest;
import org.apache.ignite.internal.util.GridSpinBusyLock;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.h2.jdbc.JdbcResultSet;
import org.h2.value.Value;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentHashMap8;

import static org.apache.ignite.events.EventType.EVT_CACHE_QUERY_EXECUTED;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.QUERY_POOL;
import static org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion.NONE;
import static org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionState.OWNING;
import static org.apache.ignite.internal.processors.query.h2.opt.DistributedJoinMode.OFF;
import static org.apache.ignite.internal.processors.query.h2.opt.DistributedJoinMode.distributedJoinMode;
import static org.apache.ignite.internal.processors.query.h2.opt.GridH2QueryType.MAP;
import static org.apache.ignite.internal.processors.query.h2.opt.GridH2QueryType.REPLICATED;
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

    /** */
    private ConcurrentMap<UUID, MapNodeResults> qryRess = new ConcurrentHashMap8<>();

    /** */
    private final GridSpinBusyLock busyLock;

    /** */
    private final ConcurrentMap<MapReservationKey, GridReservable> reservations = new ConcurrentHashMap8<>();

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

        log = ctx.log(GridMapQueryExecutor.class);

        final UUID locNodeId = ctx.localNodeId();

        ctx.event().addLocalEventListener(new GridLocalEventListener() {
            @Override public void onEvent(final Event evt) {
                UUID nodeId = ((DiscoveryEvent)evt).eventNode().id();

                GridH2QueryContext.clearAfterDeadNode(locNodeId, nodeId);

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

        boolean clear = GridH2QueryContext.clear(ctx.localNodeId(), node.id(), qryReqId, MAP);

        if (!clear) {
            nodeRess.onCancel(qryReqId);

            GridH2QueryContext.clear(ctx.localNodeId(), node.id(), qryReqId, MAP);
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
            nodeRess = new MapNodeResults();

            MapNodeResults old = qryRess.putIfAbsent(nodeId, nodeRess);

            if (old != null)
                nodeRess = old;
        }

        return nodeRess;
    }

    /**
     * @param cctx Cache context.
     * @param p Partition ID.
     * @return Partition.
     */
    private GridDhtLocalPartition partition(GridCacheContext<?, ?> cctx, int p) {
        return cctx.topology().localPartition(p, NONE, false);
    }

    /**
     * @param cacheIds Cache IDs.
     * @param topVer Topology version.
     * @param explicitParts Explicit partitions list.
     * @param reserved Reserved list.
     * @return {@code true} If all the needed partitions successfully reserved.
     * @throws IgniteCheckedException If failed.
     */
    private boolean reservePartitions(
        @Nullable List<Integer> cacheIds,
        AffinityTopologyVersion topVer,
        final int[] explicitParts,
        List<GridReservable> reserved
    ) throws IgniteCheckedException {
        assert topVer != null;

        if (F.isEmpty(cacheIds))
            return true;

        Collection<Integer> partIds = wrap(explicitParts);

        for (int i = 0; i < cacheIds.size(); i++) {
            GridCacheContext<?, ?> cctx = ctx.cache().context().cacheContext(cacheIds.get(i));

            if (cctx == null) // Cache was not found, probably was not deployed yet.
                return false;

            if (cctx.isLocal() || !cctx.rebalanceEnabled())
                continue;

            // For replicated cache topology version does not make sense.
            final MapReservationKey grpKey = new MapReservationKey(cctx.name(), cctx.isReplicated() ? null : topVer);

            GridReservable r = reservations.get(grpKey);

            if (explicitParts == null && r != null) { // Try to reserve group partition if any and no explicits.
                if (r != MapReplicatedReservation.INSTANCE) {
                    if (!r.reserve())
                        return false; // We need explicit partitions here -> retry.

                    reserved.add(r);
                }
            }
            else { // Try to reserve partitions one by one.
                int partsCnt = cctx.affinity().partitions();

                if (cctx.isReplicated()) { // Check all the partitions are in owning state for replicated cache.
                    if (r == null) { // Check only once.
                        for (int p = 0; p < partsCnt; p++) {
                            GridDhtLocalPartition part = partition(cctx, p);

                            // We don't need to reserve partitions because they will not be evicted in replicated caches.
                            if (part == null || part.state() != OWNING)
                                return false;
                        }

                        // Mark that we checked this replicated cache.
                        reservations.putIfAbsent(grpKey, MapReplicatedReservation.INSTANCE);
                    }
                }
                else { // Reserve primary partitions for partitioned cache (if no explicit given).
                    if (explicitParts == null)
                        partIds = cctx.affinity().primaryPartitions(ctx.localNodeId(), topVer);

                    for (int partId : partIds) {
                        GridDhtLocalPartition part = partition(cctx, partId);

                        if (part == null || part.state() != OWNING || !part.reserve())
                            return false;

                        reserved.add(part);

                        // Double check that we are still in owning state and partition contents are not cleared.
                        if (part.state() != OWNING)
                            return false;
                    }

                    if (explicitParts == null) {
                        // We reserved all the primary partitions for cache, attempt to add group reservation.
                        GridDhtPartitionsReservation grp = new GridDhtPartitionsReservation(topVer, cctx, "SQL");

                        if (grp.register(reserved.subList(reserved.size() - partIds.size(), reserved.size()))) {
                            if (reservations.putIfAbsent(grpKey, grp) != null)
                                throw new IllegalStateException("Reservation already exists.");

                            grp.onPublish(new CI1<GridDhtPartitionsReservation>() {
                                @Override public void apply(GridDhtPartitionsReservation r) {
                                    reservations.remove(grpKey, r);
                                }
                            });
                        }
                    }
                }
            }
        }

        return true;
    }

    /**
     * @param ints Integers.
     * @return Collection wrapper.
     */
    private static Collection<Integer> wrap(final int[] ints) {
        if (ints == null)
            return null;

        if (ints.length == 0)
            return Collections.emptySet();

        return new AbstractCollection<Integer>() {
            @SuppressWarnings("NullableProblems")
            @Override public Iterator<Integer> iterator() {
                return new Iterator<Integer>() {
                    /** */
                    private int i = 0;

                    @Override public boolean hasNext() {
                        return i < ints.length;
                    }

                    @Override public Integer next() {
                        return ints[i++];
                    }

                    @Override public void remove() {
                        throw new UnsupportedOperationException();
                    }
                };
            }

            @Override public int size() {
                return ints.length;
            }
        };
    }

    /**
     * @param caches Cache IDs.
     * @return The first found partitioned cache.
     */
    private GridCacheContext<?,?> findFirstPartitioned(List<Integer> caches) {
        GridCacheSharedContext<?,?> sctx = ctx.cache().context();

        for (int i = 0; i < caches.size(); i++) {
            GridCacheContext<?,?> mainCctx = sctx.cacheContext(caches.get(i));

            if (mainCctx == null)
                throw new CacheException("Failed to find cache.");

            if (!mainCctx.isLocal() && !mainCctx.isReplicated())
                return mainCctx;
        }

        throw new IllegalStateException("Failed to find a partitioned cache.");
    }

    /**
     * @param node Node.
     * @param req Query request.
     */
    private void onQueryRequest(final ClusterNode node, final GridH2QueryRequest req) throws IgniteCheckedException {
        int[] qryParts = req.queryPartitions();

        final Map<UUID,int[]> partsMap = req.partitions();

        final int[] parts = qryParts == null ? partsMap == null ? null : partsMap.get(ctx.localNodeId()) : qryParts;

        final DistributedJoinMode joinMode = distributedJoinMode(
            req.isFlagSet(GridH2QueryRequest.FLAG_IS_LOCAL),
            req.isFlagSet(GridH2QueryRequest.FLAG_DISTRIBUTED_JOINS));

        final boolean enforceJoinOrder = req.isFlagSet(GridH2QueryRequest.FLAG_ENFORCE_JOIN_ORDER);
        final boolean explain = req.isFlagSet(GridH2QueryRequest.FLAG_EXPLAIN);
        final boolean replicated = req.isFlagSet(GridH2QueryRequest.FLAG_REPLICATED);

        final List<Integer> cacheIds = req.caches();

        int segments = explain || replicated || F.isEmpty(cacheIds) ? 1 :
            findFirstPartitioned(cacheIds).config().getQueryParallelism();

        final Object[] params = req.parameters();

        for (int i = 1; i < segments; i++) {
            assert !F.isEmpty(cacheIds);

            final int segment = i;

            ctx.closure().callLocal(
                new Callable<Void>() {
                    @Override public Void call() throws Exception {
                        onQueryRequest0(node,
                            req.requestId(),
                            segment,
                            req.schemaName(),
                            req.queries(),
                            cacheIds,
                            req.topologyVersion(),
                            partsMap,
                            parts,
                            req.tables(),
                            req.pageSize(),
                            joinMode,
                            enforceJoinOrder,
                            false,
                            req.timeout(),
                            params);

                        return null;
                    }
                }
                , QUERY_POOL);
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
            req.tables(),
            req.pageSize(),
            joinMode,
            enforceJoinOrder,
            replicated,
            req.timeout(),
            params);
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
     * @param tbls Tables.
     * @param pageSize Page size.
     * @param distributedJoinMode Query distributed join mode.
     */
    private void onQueryRequest0(
        ClusterNode node,
        long reqId,
        int segmentId,
        String schemaName,
        Collection<GridCacheSqlQuery> qrys,
        List<Integer> cacheIds,
        AffinityTopologyVersion topVer,
        Map<UUID, int[]> partsMap,
        int[] parts,
        Collection<QueryTable> tbls,
        int pageSize,
        DistributedJoinMode distributedJoinMode,
        boolean enforceJoinOrder,
        boolean replicated,
        int timeout,
        Object[] params
    ) {
        // Prepare to run queries.
        GridCacheContext<?, ?> mainCctx =
            !F.isEmpty(cacheIds) ? ctx.cache().context().cacheContext(cacheIds.get(0)) : null;

        MapNodeResults nodeRess = resultsForNode(node.id());

        MapQueryResults qr = null;

        List<GridReservable> reserved = new ArrayList<>();

        try {
            if (topVer != null) {
                // Reserve primary for topology version or explicit partitions.
                if (!reservePartitions(cacheIds, topVer, parts, reserved)) {
                    sendRetry(node, reqId, segmentId);

                    return;
                }
            }

            qr = new MapQueryResults(h2, reqId, qrys.size(), mainCctx != null ? mainCctx.name() : null);

            if (nodeRess.put(reqId, segmentId, qr) != null)
                throw new IllegalStateException();

            // Prepare query context.
            GridH2QueryContext qctx = new GridH2QueryContext(ctx.localNodeId(),
                node.id(),
                reqId,
                segmentId,
                replicated ? REPLICATED : MAP)
                .filter(h2.backupFilter(topVer, parts))
                .partitionsMap(partsMap)
                .distributedJoinMode(distributedJoinMode)
                .pageSize(pageSize)
                .topologyVersion(topVer)
                .reservations(reserved);

            List<GridH2Table> snapshotedTbls = null;

            if (!F.isEmpty(tbls)) {
                snapshotedTbls = new ArrayList<>(tbls.size());

                for (QueryTable tbl : tbls) {
                    GridH2Table h2Tbl = h2.dataTable(tbl);

                    Objects.requireNonNull(h2Tbl, tbl.toString());

                    h2Tbl.snapshotIndexes(qctx, segmentId);

                    snapshotedTbls.add(h2Tbl);
                }
            }

            Connection conn = h2.connectionForSchema(schemaName);

            H2Utils.setupConnection(conn, distributedJoinMode != OFF, enforceJoinOrder);

            GridH2QueryContext.set(qctx);

            // qctx is set, we have to release reservations inside of it.
            reserved = null;

            try {
                if (nodeRess.cancelled(reqId)) {
                    GridH2QueryContext.clear(ctx.localNodeId(), node.id(), reqId, qctx.type());

                    nodeRess.cancelRequest(reqId);

                    throw new QueryCancelledException();
                }

                // Run queries.
                int qryIdx = 0;

                boolean evt = mainCctx != null && ctx.event().isRecordable(EVT_CACHE_QUERY_EXECUTED);

                for (GridCacheSqlQuery qry : qrys) {
                    ResultSet rs = null;

                    // If we are not the target node for this replicated query, just ignore it.
                    if (qry.node() == null ||
                        (segmentId == 0 && qry.node().equals(ctx.localNodeId()))) {
                        rs = h2.executeSqlQueryWithTimer(conn, qry.query(),
                            F.asList(qry.parameters(params)), true,
                            timeout,
                            qr.queryCancel(qryIdx));

                        if (evt) {
                            assert mainCctx != null;

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

                    qr.addResult(qryIdx, qry, node.id(), rs, params);

                    if (qr.cancelled()) {
                        qr.result(qryIdx).close();

                        throw new QueryCancelledException();
                    }

                    // Send the first page.
                    sendNextPage(nodeRess, node, qr, qryIdx, segmentId, pageSize);

                    qryIdx++;
                }
            }
            finally {
                GridH2QueryContext.clearThreadLocal();

                if (distributedJoinMode == OFF)
                    qctx.clearContext(false);

                if (!F.isEmpty(snapshotedTbls)) {
                    for (GridH2Table dataTbl : snapshotedTbls)
                        dataTbl.releaseSnapshots();
                }
            }
        }
        catch (Throwable e) {
            if (qr != null) {
                nodeRess.remove(reqId, segmentId, qr);

                qr.cancel(false);
            }

            if (X.hasCause(e, GridH2RetryException.class))
                sendRetry(node, reqId, segmentId);
            else {
                U.error(log, "Failed to execute local query.", e);

                sendError(node, reqId, e);

                if (e instanceof Error)
                    throw (Error)e;
            }
        }
        finally {
            if (reserved != null) {
                // Release reserved partitions.
                for (int i = 0; i < reserved.size(); i++)
                    reserved.get(i).release();
            }
        }
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
     * @param node Node.
     * @param req Request.
     */
    private void onNextPageRequest(ClusterNode node, GridQueryNextPageRequest req) {
        MapNodeResults nodeRess = qryRess.get(node.id());

        if (nodeRess == null) {
            sendError(node, req.queryRequestId(), new CacheException("No node result found for request: " + req));

            return;
        } else if (nodeRess.cancelled(req.queryRequestId())) {
            sendError(node, req.queryRequestId(), new QueryCancelledException());

            return;
        }

        MapQueryResults qr = nodeRess.get(req.queryRequestId(), req.segmentId());

        if (qr == null)
            sendError(node, req.queryRequestId(), new CacheException("No query result found for request: " + req));
        else if (qr.cancelled())
            sendError(node, req.queryRequestId(), new QueryCancelledException());
        else
            sendNextPage(nodeRess, node, qr, req.query(), req.segmentId(), req.pageSize());
    }

    /**
     * @param nodeRess Results.
     * @param node Node.
     * @param qr Query results.
     * @param qry Query.
     * @param segmentId Index segment ID.
     * @param pageSize Page size.
     */
    private void sendNextPage(MapNodeResults nodeRess, ClusterNode node, MapQueryResults qr, int qry, int segmentId,
        int pageSize) {
        MapQueryResult res = qr.result(qry);

        assert res != null;

        if (res.closed())
            return;

        int page = res.page();

        List<Value[]> rows = new ArrayList<>(Math.min(64, pageSize));

        boolean last = res.fetchNextPage(rows, pageSize);

        if (last) {
            res.close();

            if (qr.isAllClosed())
                nodeRess.remove(qr.queryRequestId(), segmentId, qr);
        }

        try {
            boolean loc = node.isLocal();

            GridQueryNextPageResponse msg = new GridQueryNextPageResponse(qr.queryRequestId(), segmentId, qry, page,
                page == 0 ? res.rowCount() : -1,
                res.columnCount(),
                loc ? null : toMessages(rows, new ArrayList<Message>(res.columnCount())),
                loc ? rows : null);

            if (loc)
                h2.reduceQueryExecutor().onMessage(ctx.localNodeId(), msg);
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
     */
    private void sendRetry(ClusterNode node, long reqId, int segmentId) {
        try {
            boolean loc = node.isLocal();

            GridQueryNextPageResponse msg = new GridQueryNextPageResponse(reqId, segmentId,
            /*qry*/0, /*page*/0, /*allRows*/0, /*cols*/1,
                loc ? null : Collections.<Message>emptyList(),
                loc ? Collections.<Value[]>emptyList() : null);

            msg.retry(h2.readyTopologyVersion());

            if (loc)
                h2.reduceQueryExecutor().onMessage(ctx.localNodeId(), msg);
            else
                ctx.io().sendToGridTopic(node, GridTopic.TOPIC_QUERY, msg, QUERY_POOL);
        }
        catch (Exception e) {
            U.warn(log, "Failed to send retry message: " + e.getMessage());
        }
    }

    /**
     * @param cacheName Cache name.
     */
    public void onCacheStop(String cacheName) {
        // Drop group reservations.
        for (MapReservationKey grpKey : reservations.keySet()) {
            if (F.eq(grpKey.cacheName(), cacheName))
                reservations.remove(grpKey);
        }
    }
}