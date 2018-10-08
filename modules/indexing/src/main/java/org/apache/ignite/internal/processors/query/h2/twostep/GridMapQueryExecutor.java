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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.AbstractCollection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.CacheException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.PartitionLossPolicy;
import org.apache.ignite.cache.query.QueryCancelledException;
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
import org.apache.ignite.internal.processors.cache.CacheInvalidStateException;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.CompoundLockFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionsReservation;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTransactionalCacheAdapter;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxLocalAdapter;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridReservable;
import org.apache.ignite.internal.processors.cache.mvcc.MvccSnapshot;
import org.apache.ignite.internal.processors.cache.mvcc.MvccUtils;
import org.apache.ignite.internal.processors.cache.query.CacheQueryType;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryMarshallable;
import org.apache.ignite.internal.processors.cache.query.GridCacheSqlQuery;
import org.apache.ignite.internal.processors.query.GridQueryCancel;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.h2.H2Utils;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.processors.query.h2.ResultSetEnlistFuture;
import org.apache.ignite.internal.processors.query.h2.UpdateResult;
import org.apache.ignite.internal.processors.query.h2.opt.DistributedJoinMode;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2QueryContext;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2RetryException;
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
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.indexing.IndexingQueryFilter;
import org.apache.ignite.thread.IgniteThread;
import org.h2.command.Prepared;
import org.h2.jdbc.JdbcResultSet;
import org.h2.value.Value;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_SQL_FORCE_LAZY_RESULT_SET;
import static org.apache.ignite.cache.PartitionLossPolicy.READ_ONLY_SAFE;
import static org.apache.ignite.cache.PartitionLossPolicy.READ_WRITE_SAFE;
import static org.apache.ignite.events.EventType.EVT_CACHE_QUERY_EXECUTED;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.QUERY_POOL;
import static org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion.NONE;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.LOST;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.OWNING;
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
    public static final boolean FORCE_LAZY = IgniteSystemProperties.getBoolean(IGNITE_SQL_FORCE_LAZY_RESULT_SET);

    /** */
    private IgniteLogger log;

    /** */
    private GridKernalContext ctx;

    /** */
    private IgniteH2Indexing h2;

    /** */
    private ConcurrentMap<UUID, MapNodeResults> qryRess = new ConcurrentHashMap<>();

    /** */
    private final GridSpinBusyLock busyLock;

    /** */
    private final ConcurrentMap<MapReservationKey, GridReservable> reservations = new ConcurrentHashMap<>();

    /** Lazy workers. */
    private final ConcurrentHashMap<MapQueryLazyWorkerKey, MapQueryLazyWorker> lazyWorkers = new ConcurrentHashMap<>();

    /** Busy lock for lazy workers. */
    private final GridSpinBusyLock lazyWorkerBusyLock = new GridSpinBusyLock();

    /** Lazy worker stop guard. */
    private final AtomicBoolean lazyWorkerStopGuard = new AtomicBoolean();

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
     * Cancel active lazy queries and prevent submit of new queries.
     */
    public void cancelLazyWorkers() {
        if (!lazyWorkerStopGuard.compareAndSet(false, true))
            return;

        lazyWorkerBusyLock.block();

        for (MapQueryLazyWorker worker : lazyWorkers.values())
            worker.stop(false);

        lazyWorkers.clear();
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
     * @return Busy lock for lazy workers to guard their operations with.
     */
    GridSpinBusyLock busyLock() {
        return busyLock;
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
            nodeRess = new MapNodeResults(nodeId);

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
     * @param nodeId Node ID.
     * @param reqId Request ID.
     * @return String which is null in case of success or with causeMessage if failed
     * @throws IgniteCheckedException If failed.
     */
    private String reservePartitions(
        @Nullable List<Integer> cacheIds,
        AffinityTopologyVersion topVer,
        final int[] explicitParts,
        List<GridReservable> reserved,
        UUID nodeId,
        long reqId
    ) throws IgniteCheckedException {
        assert topVer != null;

        if (F.isEmpty(cacheIds))
            return null;

        Collection<Integer> partIds = wrap(explicitParts);

        for (int i = 0; i < cacheIds.size(); i++) {
            GridCacheContext<?, ?> cctx = ctx.cache().context().cacheContext(cacheIds.get(i));

            // Cache was not found, probably was not deployed yet.
            if (cctx == null) {
                return String.format("Failed to reserve partitions for query (cache is not found on " +
                    "local node) [localNodeId=%s, rmtNodeId=%s, reqId=%s, affTopVer=%s, cacheId=%s]",
                    ctx.localNodeId(), nodeId, reqId, topVer, cacheIds.get(i));
            }

            if (cctx.isLocal() || !cctx.rebalanceEnabled())
                continue;

            // For replicated cache topology version does not make sense.
            final MapReservationKey grpKey = new MapReservationKey(cctx.name(), cctx.isReplicated() ? null : topVer);

            GridReservable r = reservations.get(grpKey);

            if (explicitParts == null && r != null) { // Try to reserve group partition if any and no explicits.
                if (r != MapReplicatedReservation.INSTANCE) {
                    if (!r.reserve())
                        return String.format("Failed to reserve partitions for query (group " +
                            "reservation failed) [localNodeId=%s, rmtNodeId=%s, reqId=%s, affTopVer=%s, cacheId=%s, " +
                            "cacheName=%s]",ctx.localNodeId(), nodeId, reqId, topVer, cacheIds.get(i), cctx.name());

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
                            GridDhtPartitionState partState = part != null ? part.state() : null;

                            if (partState != OWNING)
                                return String.format("Failed to reserve partitions for query " +
                                    "(partition of REPLICATED cache is not in OWNING state) [" +
                                    "localNodeId=%s, rmtNodeId=%s, reqId=%s, affTopVer=%s, cacheId=%s, cacheName=%s, " +
                                    "part=%s, partFound=%s, partState=%s]",
                                    ctx.localNodeId(),
                                    nodeId,
                                    reqId,
                                    topVer,
                                    cacheIds.get(i),
                                    cctx.name(),
                                    p,
                                    (part != null),
                                    partState
                                );
                        }

                        // Mark that we checked this replicated cache.
                        reservations.putIfAbsent(grpKey, MapReplicatedReservation.INSTANCE);
                    }
                }
                else { // Reserve primary partitions for partitioned cache (if no explicit given).
                    if (explicitParts == null)
                        partIds = cctx.affinity().primaryPartitions(ctx.localNodeId(), topVer);

                    int reservedCnt = 0;

                    for (int partId : partIds) {
                        GridDhtLocalPartition part = partition(cctx, partId);

                        GridDhtPartitionState partState = part != null ? part.state() : null;

                        if (partState != OWNING) {
                            if (partState == LOST)
                                ignoreLostPartitionIfPossible(cctx, part);
                            else {
                                return String.format("Failed to reserve partitions for query " +
                                        "(partition of PARTITIONED cache is not found or not in OWNING state) [" +
                                        "localNodeId=%s, rmtNodeId=%s, reqId=%s, affTopVer=%s, cacheId=%s, " +
                                        "cacheName=%s, part=%s, partFound=%s, partState=%s]",
                                    ctx.localNodeId(),
                                    nodeId,
                                    reqId,
                                    topVer,
                                    cacheIds.get(i),
                                    cctx.name(),
                                    partId,
                                    (part != null),
                                    partState
                                );
                            }
                        }

                        if (!part.reserve()) {
                            return String.format("Failed to reserve partitions for query " +
                                    "(partition of PARTITIONED cache cannot be reserved) [" +
                                    "localNodeId=%s, rmtNodeId=%s, reqId=%s, affTopVer=%s, cacheId=%s, " +
                                    "cacheName=%s, part=%s, partFound=%s, partState=%s]",
                                ctx.localNodeId(),
                                nodeId,
                                reqId,
                                topVer,
                                cacheIds.get(i),
                                cctx.name(),
                                partId,
                                true,
                                partState
                            );
                        }

                        reserved.add(part);

                        reservedCnt++;

                        // Double check that we are still in owning state and partition contents are not cleared.
                        partState = part.state();

                        if (partState != OWNING) {
                            if (partState == LOST)
                                ignoreLostPartitionIfPossible(cctx, part);
                            else {
                                return String.format("Failed to reserve partitions for query " +
                                    "(partition of PARTITIONED cache is not in OWNING state after reservation) [" +
                                    "localNodeId=%s, rmtNodeId=%s, reqId=%s, affTopVer=%s, cacheId=%s, " +
                                    "cacheName=%s, part=%s, partState=%s]",
                                    ctx.localNodeId(),
                                    nodeId,
                                    reqId,
                                    topVer,
                                    cacheIds.get(i),
                                    cctx.name(),
                                    partId,
                                    partState
                                );
                            }
                        }
                    }

                    if (explicitParts == null && reservedCnt > 0) {
                        // We reserved all the primary partitions for cache, attempt to add group reservation.
                        GridDhtPartitionsReservation grp = new GridDhtPartitionsReservation(topVer, cctx, "SQL");

                        if (grp.register(reserved.subList(reserved.size() - reservedCnt, reserved.size()))) {
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

        return null;
    }

    /**
     * Decide whether to ignore or proceed with lost partition.
     *
     * @param cctx Cache context.
     * @param part Partition.
     * @throws IgniteCheckedException If failed.
     */
    private static void ignoreLostPartitionIfPossible(GridCacheContext cctx, GridDhtLocalPartition part)
        throws IgniteCheckedException {
        PartitionLossPolicy plc = cctx.config().getPartitionLossPolicy();

        if (plc != null) {
            if (plc == READ_ONLY_SAFE || plc == READ_WRITE_SAFE) {
                throw new CacheInvalidStateException("Failed to execute query because cache partition has been " +
                    "lost [cacheName=" + cctx.name() + ", part=" + part + ']');
            }
        }
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
        final boolean lazy = (FORCE_LAZY && req.queries().size() == 1) || req.isFlagSet(GridH2QueryRequest.FLAG_LAZY);

        final List<Integer> cacheIds = req.caches();

        int segments = explain || replicated || F.isEmpty(cacheIds) ? 1 :
            CU.firstPartitioned(ctx.cache().context(), cacheIds).config().getQueryParallelism();

        final Object[] params = req.parameters();

        final GridDhtTxLocalAdapter tx;

        GridH2SelectForUpdateTxDetails txReq = req.txDetails();

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
                        txReq.taskNameHash());
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

            if (lazy) {
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
                    joinMode,
                    enforceJoinOrder,
                    false, // Replicated is always false here (see condition above).
                    req.timeout(),
                    params,
                    true,
                    req.mvccSnapshot(),
                    tx,
                    txReq,
                    lockFut,
                    runCntr);
            }
            else {
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
                                joinMode,
                                enforceJoinOrder,
                                false,
                                req.timeout(),
                                params,
                                false,
                                req.mvccSnapshot(),
                                tx,
                                txReq,
                                lockFut,
                                runCntr);

                            return null;
                        }
                    }
                    , QUERY_POOL);
            }
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
            joinMode,
            enforceJoinOrder,
            replicated,
            req.timeout(),
            params,
            lazy,
            req.mvccSnapshot(),
            tx,
            txReq,
            lockFut, runCntr);
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
     * @param distributedJoinMode Query distributed join mode.
     * @param lazy Streaming flag.
     * @param mvccSnapshot MVCC snapshot.
     * @param tx Transaction.
     * @param txDetails TX details, if it's a {@code FOR UPDATE} request, or {@code null}.
     * @param lockFut Lock future.
     * @param runCntr Counter which counts remaining queries in case segmented index is used.
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
        final DistributedJoinMode distributedJoinMode,
        final boolean enforceJoinOrder,
        final boolean replicated,
        final int timeout,
        final Object[] params,
        boolean lazy,
        @Nullable final MvccSnapshot mvccSnapshot,
        @Nullable final GridDhtTxLocalAdapter tx,
        @Nullable final GridH2SelectForUpdateTxDetails txDetails,
        @Nullable final CompoundLockFuture lockFut,
        @Nullable final AtomicInteger runCntr) {
        MapQueryLazyWorker worker = MapQueryLazyWorker.currentWorker();

        // In presence of TX, we also must always have matching details.
        assert tx == null || txDetails != null;

        boolean inTx = (tx != null);

        if (lazy && worker == null) {
            // Lazy queries must be re-submitted to dedicated workers.
            MapQueryLazyWorkerKey key = new MapQueryLazyWorkerKey(node.id(), reqId, segmentId);
            worker = new MapQueryLazyWorker(ctx.igniteInstanceName(), key, log, this);

            worker.submit(new Runnable() {
                @Override public void run() {
                    onQueryRequest0(
                        node,
                        reqId,
                        segmentId,
                        schemaName,
                        qrys,
                        cacheIds,
                        topVer,
                        partsMap,
                        parts,
                        pageSize,
                        distributedJoinMode,
                        enforceJoinOrder,
                        replicated,
                        timeout,
                        params,
                        true,
                        mvccSnapshot,
                        tx,
                        txDetails,
                        lockFut,
                        runCntr);
                }
            });

            if (lazyWorkerBusyLock.enterBusy()) {
                try {
                    MapQueryLazyWorker oldWorker = lazyWorkers.put(key, worker);

                    if (oldWorker != null)
                        oldWorker.stop(false);

                    IgniteThread thread = new IgniteThread(worker);

                    thread.start();
                }
                finally {
                    lazyWorkerBusyLock.leaveBusy();
                }
            }
            else
                log.info("Ignored query request (node is stopping) [nodeId=" + node.id() + ", reqId=" + reqId + ']');

            return;
        }

        if (lazy && txDetails != null)
            throw new IgniteSQLException("Lazy execution of SELECT FOR UPDATE queries is not supported.");

        // Prepare to run queries.
        GridCacheContext<?, ?> mainCctx = mainCacheContext(cacheIds);

        MapNodeResults nodeRess = resultsForNode(node.id());

        MapQueryResults qr = null;

        List<GridReservable> reserved = new ArrayList<>();

        try {
            // We want to reserve only in not SELECT FOR UPDATE case -
            // otherwise, their state is protected by locked topology.
            if (topVer != null && txDetails == null) {
                // Reserve primary for topology version or explicit partitions.
                String err = reservePartitions(cacheIds, topVer, parts, reserved, node.id(), reqId);

                if (!F.isEmpty(err)) {
                    // Unregister lazy worker because re-try may never reach this node again.
                    if (lazy)
                        stopAndUnregisterCurrentLazyWorker();

                    sendRetry(node, reqId, segmentId, err);

                    return;
                }
            }

            qr = new MapQueryResults(h2, reqId, qrys.size(), mainCctx, MapQueryLazyWorker.currentWorker(), inTx);

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
                .reservations(reserved)
                .mvccSnapshot(mvccSnapshot)
                .lazyWorker(worker);

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

                boolean evt = mainCctx != null && mainCctx.events().isRecordable(EVT_CACHE_QUERY_EXECUTED);

                for (GridCacheSqlQuery qry : qrys) {
                    ResultSet rs = null;

                    boolean removeMapping = false;

                    // If we are not the target node for this replicated query, just ignore it.
                    if (qry.node() == null || (segmentId == 0 && qry.node().equals(ctx.localNodeId()))) {
                        String sql = qry.query(); Collection<Object> params0 = F.asList(qry.parameters(params));

                        PreparedStatement stmt;

                        try {
                            stmt = h2.prepareStatement(conn, sql, true);
                        }
                        catch (SQLException e) {
                            throw new IgniteCheckedException("Failed to parse SQL query: " + sql, e);
                        }

                        Prepared p = GridSqlQueryParser.prepared(stmt);

                        if (GridSqlQueryParser.isForUpdateQuery(p)) {
                            sql = GridSqlQueryParser.rewriteQueryForUpdateIfNeeded(p, inTx);
                            stmt = h2.prepareStatement(conn, sql, true);
                        }

                        h2.bindParameters(stmt, params0);

                        int opTimeout = IgniteH2Indexing.operationTimeout(timeout, tx);

                        rs = h2.executeSqlQueryWithTimer(stmt, conn, sql, params0, opTimeout, qr.queryCancel(qryIdx));

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

                    qr.addResult(qryIdx, qry, node.id(), rs, params);

                    if (qr.cancelled()) {
                        qr.result(qryIdx).close();

                        throw new QueryCancelledException();
                    }

                    if (inTx) {
                        if (tx.dht() && (runCntr == null || runCntr.decrementAndGet() == 0)) {
                            if (removeMapping = tx.empty() && !tx.queryEnlisted())
                                tx.rollbackAsync().get();
                        }
                    }

                    // Send the first page.
                    if (lockFut == null)
                        sendNextPage(nodeRess, node, qr, qryIdx, segmentId, pageSize, removeMapping);
                    else {
                        GridQueryNextPageResponse msg = prepareNextPage(nodeRess, node, qr, qryIdx, segmentId, pageSize, removeMapping);

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

                // All request results are in the memory in result set already, so it's ok to release partitions.
                if (!lazy)
                    releaseReservations();
            }
            catch (Throwable e){
                releaseReservations();

                throw e;
            }
        }
        catch (Throwable e) {
            if (qr != null) {
                nodeRess.remove(reqId, segmentId, qr);

                qr.cancel(false);
            }

            // Unregister worker after possible cancellation.
            if (lazy)
                stopAndUnregisterCurrentLazyWorker();

            GridH2RetryException retryErr = X.cause(e, GridH2RetryException.class);

            if (retryErr != null) {
                final String retryCause = String.format(
                    "Failed to execute non-collocated query (will retry) [localNodeId=%s, rmtNodeId=%s, reqId=%s, " +
                    "errMsg=%s]", ctx.localNodeId(), node.id(), reqId, retryErr.getMessage()
                );

                sendRetry(node, reqId, segmentId, retryCause);
            }
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
        GridH2QueryContext qctx = GridH2QueryContext.get();

        if (qctx != null) { // No-op if already released.
            GridH2QueryContext.clearThreadLocal();

            if (qctx.distributedJoinMode() == OFF)
                qctx.clearContext(false);
        }
    }

    /**
     * @param node Node.
     * @param req DML request.
     */
    private void onDmlRequest(final ClusterNode node, final GridH2DmlRequest req) throws IgniteCheckedException {
        int[] parts = req.queryPartitions();

        List<Integer> cacheIds = req.caches();

        long reqId = req.requestId();

        AffinityTopologyVersion topVer = req.topologyVersion();

        List<GridReservable> reserved = new ArrayList<>();

        MapNodeResults nodeResults = resultsForNode(node.id());

        try {
            String err = reservePartitions(cacheIds, topVer, parts, reserved, node.id(), reqId);

            if (!F.isEmpty(err)) {
                U.error(log, "Failed to reserve partitions for DML request. [localNodeId=" + ctx.localNodeId() +
                    ", nodeId=" + node.id() + ", reqId=" + req.requestId() + ", cacheIds=" + cacheIds +
                    ", topVer=" + topVer + ", parts=" + Arrays.toString(parts) + ']');

                sendUpdateResponse(node, reqId, null,
                    "Failed to reserve partitions for DML request. " + err);

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

            boolean local = true;

            final boolean replicated = req.isFlagSet(GridH2QueryRequest.FLAG_REPLICATED);

            if (!replicated && !F.isEmpty(cacheIds) &&
                CU.firstPartitioned(ctx.cache().context(), cacheIds).config().getQueryParallelism() > 1) {
                fldsQry.setDistributedJoins(true);

                local = false;
            }

            UpdateResult updRes = h2.mapDistributedUpdate(req.schemaName(), fldsQry, filter, cancel, local);

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
            if (!F.isEmpty(reserved)) {
                // Release reserved partitions.
                for (int i = 0; i < reserved.size(); i++)
                    reserved.get(i).release();
            }

            nodeResults.removeUpdate(reqId);
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
        final MapNodeResults nodeRess = qryRess.get(node.id());

        if (nodeRess == null) {
            sendError(node, req.queryRequestId(), new CacheException("No node result found for request: " + req));

            return;
        }
        else if (nodeRess.cancelled(req.queryRequestId())) {
            sendError(node, req.queryRequestId(), new QueryCancelledException());

            return;
        }

        final MapQueryResults qr = nodeRess.get(req.queryRequestId(), req.segmentId());

        if (qr == null)
            sendError(node, req.queryRequestId(), new CacheException("No query result found for request: " + req));
        else if (qr.cancelled())
            sendError(node, req.queryRequestId(), new QueryCancelledException());
        else {
            MapQueryLazyWorker lazyWorker = qr.lazyWorker();

            if (lazyWorker != null) {
                lazyWorker.submit(new Runnable() {
                    @Override public void run() {
                        sendNextPage(nodeRess, node, qr, req.query(), req.segmentId(), req.pageSize(), false);
                    }
                });
            }
            else
                sendNextPage(nodeRess, node, qr, req.query(), req.segmentId(), req.pageSize(), false);
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
     * @return Next page.
     * @throws IgniteCheckedException If failed.
     */
    private GridQueryNextPageResponse prepareNextPage(MapNodeResults nodeRess, ClusterNode node, MapQueryResults qr, int qry, int segmentId,
        int pageSize, boolean removeMapping) throws IgniteCheckedException {
        MapQueryResult res = qr.result(qry);

        assert res != null;

        if (res.closed())
            return null;

        int page = res.page();

        List<Value[]> rows = new ArrayList<>(Math.min(64, pageSize));

        boolean last = res.fetchNextPage(rows, pageSize);

        if (last) {
            res.close();

            if (qr.isAllClosed()) {
                nodeRess.remove(qr.queryRequestId(), segmentId, qr);

                // Release reservations if the last page fetched, all requests are closed and this is a lazy worker.
                if (MapQueryLazyWorker.currentWorker() != null)
                    releaseReservations();
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
     * @param nodeRess Results.
     * @param node Node.
     * @param qr Query results.
     * @param qry Query.
     * @param segmentId Index segment ID.
     * @param pageSize Page size.
     * @param removeMapping Remove mapping flag.
     */
    @SuppressWarnings("unchecked")
    private void sendNextPage(MapNodeResults nodeRess, ClusterNode node, MapQueryResults qr, int qry, int segmentId,
        int pageSize, boolean removeMapping) {
        try {
            GridQueryNextPageResponse msg = prepareNextPage(nodeRess, node, qr, qry, segmentId, pageSize, removeMapping);

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
     */
    private void sendRetry(ClusterNode node, long reqId, int segmentId, String retryCause) {
        try {
            boolean loc = node.isLocal();

            GridQueryNextPageResponse msg = new GridQueryNextPageResponse(reqId, segmentId,
            /*qry*/0, /*page*/0, /*allRows*/0, /*cols*/1,
                loc ? null : Collections.<Message>emptyList(),
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

    /**
     * Unregister lazy worker if needed (i.e. if we are currently in lazy worker thread).
     */
    public void stopAndUnregisterCurrentLazyWorker() {
        MapQueryLazyWorker worker = MapQueryLazyWorker.currentWorker();

        if (worker != null) {
            worker.stop(false);

            // Just stop is not enough as worker may be registered, but not started due to exception.
            unregisterLazyWorker(worker);
        }
    }

    /**
     * Unregister lazy worker.
     *
     * @param worker Worker.
     */
    public void unregisterLazyWorker(MapQueryLazyWorker worker) {
        lazyWorkers.remove(worker.key(), worker);
    }

    /**
     * @return Number of registered lazy workers.
     */
    public int registeredLazyWorkers() {
        return lazyWorkers.size();
    }
}
