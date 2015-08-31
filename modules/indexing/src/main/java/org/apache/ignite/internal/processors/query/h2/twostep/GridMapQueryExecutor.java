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

import java.lang.reflect.Field;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.AbstractCollection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReferenceArray;
import javax.cache.CacheException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.CacheQueryExecutedEvent;
import org.apache.ignite.events.CacheQueryReadEvent;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.GridTopic;
import org.apache.ignite.internal.managers.communication.GridMessageListener;
import org.apache.ignite.internal.managers.eventstorage.GridLocalEventListener;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionsReservation;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridReservable;
import org.apache.ignite.internal.processors.cache.query.CacheQueryType;
import org.apache.ignite.internal.processors.cache.query.GridCacheSqlQuery;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.processors.query.h2.twostep.messages.GridQueryCancelRequest;
import org.apache.ignite.internal.processors.query.h2.twostep.messages.GridQueryFailResponse;
import org.apache.ignite.internal.processors.query.h2.twostep.messages.GridQueryNextPageRequest;
import org.apache.ignite.internal.processors.query.h2.twostep.messages.GridQueryNextPageResponse;
import org.apache.ignite.internal.processors.query.h2.twostep.messages.GridQueryRequest;
import org.apache.ignite.internal.util.GridSpinBusyLock;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.h2.jdbc.JdbcResultSet;
import org.h2.result.ResultInterface;
import org.h2.value.Value;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentHashMap8;

import static org.apache.ignite.events.EventType.EVT_CACHE_QUERY_EXECUTED;
import static org.apache.ignite.events.EventType.EVT_CACHE_QUERY_OBJECT_READ;
import static org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion.NONE;
import static org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionState.OWNING;
import static org.apache.ignite.internal.processors.query.h2.twostep.GridReduceQueryExecutor.QUERY_POOL;
import static org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2ValueMessageFactory.toMessages;

/**
 * Map query executor.
 */
public class GridMapQueryExecutor {
    /** */
    private static final Field RESULT_FIELD;

    /**
     * Initialize.
     */
    static {
        try {
            RESULT_FIELD = JdbcResultSet.class.getDeclaredField("result");

            RESULT_FIELD.setAccessible(true);
        }
        catch (NoSuchFieldException e) {
            throw new IllegalStateException("Check H2 version in classpath.", e);
        }
    }

    /** */
    private IgniteLogger log;

    /** */
    private GridKernalContext ctx;

    /** */
    private IgniteH2Indexing h2;

    /** */
    private ConcurrentMap<UUID, ConcurrentMap<Long, QueryResults>> qryRess = new ConcurrentHashMap8<>();

    /** */
    private final GridSpinBusyLock busyLock;

    /** */
    private final ConcurrentMap<T2<String, AffinityTopologyVersion>, GridReservable> reservations =
        new ConcurrentHashMap8<>();

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

        ctx.event().addLocalEventListener(new GridLocalEventListener() {
            @Override public void onEvent(final Event evt) {
                UUID nodeId = ((DiscoveryEvent)evt).eventNode().id();

                ConcurrentMap<Long,QueryResults> nodeRess = qryRess.remove(nodeId);

                if (nodeRess == null)
                    return;

                for (QueryResults ress : nodeRess.values())
                    ress.cancel();
            }
        }, EventType.EVT_NODE_FAILED, EventType.EVT_NODE_LEFT);

        ctx.io().addMessageListener(GridTopic.TOPIC_QUERY, new GridMessageListener() {
            @Override public void onMessage(UUID nodeId, Object msg) {
                if (!busyLock.enterBusy())
                    return;

                try {
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

            if (msg instanceof GridQueryRequest)
                onQueryRequest(node, (GridQueryRequest)msg);
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
        ConcurrentMap<Long,QueryResults> nodeRess = resultsForNode(node.id());

        QueryResults results = nodeRess.remove(msg.queryRequestId());

        if (results == null)
            return;

        results.cancel();
    }

    /**
     * @param nodeId Node ID.
     * @return Results for node.
     */
    private ConcurrentMap<Long, QueryResults> resultsForNode(UUID nodeId) {
        ConcurrentMap<Long, QueryResults> nodeRess = qryRess.get(nodeId);

        if (nodeRess == null) {
            nodeRess = new ConcurrentHashMap8<>();

            ConcurrentMap<Long, QueryResults> old = qryRess.putIfAbsent(nodeId, nodeRess);

            if (old != null)
                nodeRess = old;
        }

        return nodeRess;
    }

    /**
     * @param cacheName Cache name.
     * @return Cache context or {@code null} if none.
     */
    @Nullable private GridCacheContext<?,?> cacheContext(String cacheName) {
        GridCacheAdapter<?,?> cache = ctx.cache().internalCache(cacheName);

        if (cache == null)
            return null;

        return cache.context();
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
     * @param cacheNames Cache names.
     * @param topVer Topology version.
     * @param explicitParts Explicit partitions list.
     * @param reserved Reserved list.
     * @return {@code true} If all the needed partitions successfully reserved.
     * @throws IgniteCheckedException If failed.
     */
    private boolean reservePartitions(
        Collection<String> cacheNames,
        AffinityTopologyVersion topVer,
        final int[] explicitParts,
        List<GridReservable> reserved
    ) throws IgniteCheckedException {
        assert topVer != null;

        Collection<Integer> partIds = wrap(explicitParts);

        for (String cacheName : cacheNames) {
            GridCacheContext<?, ?> cctx = cacheContext(cacheName);

            if (cctx == null) // Cache was not found, probably was not deployed yet.
                return false;

            if (cctx.isLocal())
                continue;

            // For replicated cache topology version does not make sense.
            final T2<String,AffinityTopologyVersion> grpKey =
                new T2<>(cctx.name(), cctx.isReplicated() ? null : topVer);

            GridReservable r = reservations.get(grpKey);

            if (explicitParts == null && r != null) { // Try to reserve group partition if any and no explicits.
                if (r != ReplicatedReservation.INSTANCE) {
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
                        reservations.putIfAbsent(grpKey, ReplicatedReservation.INSTANCE);
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
     * Executing queries locally.
     *
     * @param node Node.
     * @param req Query request.
     */
    private void onQueryRequest(ClusterNode node, GridQueryRequest req) {
        ConcurrentMap<Long,QueryResults> nodeRess = resultsForNode(node.id());

        QueryResults qr = null;

        List<GridReservable> reserved = new ArrayList<>();

        try {
            // Unmarshall query params.
            Collection<GridCacheSqlQuery> qrys;

            try {
                qrys = req.queries();

                if (!node.isLocal()) {
                    Marshaller m = ctx.config().getMarshaller();

                    for (GridCacheSqlQuery qry : qrys)
                        qry.unmarshallParams(m);
                }
            }
            catch (IgniteCheckedException e) {
                throw new CacheException("Failed to unmarshall parameters.", e);
            }

            List<String> caches = (List<String>)F.concat(true, req.space(), req.extraSpaces());

            // Topology version can be null in rolling restart with previous version!
            final AffinityTopologyVersion topVer = req.topologyVersion();

            if (topVer != null) {
                // Reserve primary for topology version or explicit partitions.
                if (!reservePartitions(caches, topVer, req.partitions(), reserved)) {
                    sendRetry(node, req.requestId());

                    return;
                }
            }

            // Prepare to run queries.
            GridCacheContext<?,?> mainCctx = cacheContext(req.space());

            if (mainCctx == null)
                throw new CacheException("Failed to find cache: " + req.space());

            qr = new QueryResults(req.requestId(), qrys.size(), mainCctx);

            if (nodeRess.put(req.requestId(), qr) != null)
                throw new IllegalStateException();

            h2.setFilters(h2.backupFilter(caches, topVer, req.partitions()));

            // TODO Prepare snapshots for all the needed tables before the run.

            // Run queries.
            int i = 0;

            for (GridCacheSqlQuery qry : qrys) {
                ResultSet rs = h2.executeSqlQueryWithTimer(req.space(), h2.connectionForSpace(req.space()), qry.query(),
                    F.asList(qry.parameters()));

                if (ctx.event().isRecordable(EVT_CACHE_QUERY_EXECUTED)) {
                    ctx.event().record(new CacheQueryExecutedEvent<>(
                        node,
                        "SQL query executed.",
                        EVT_CACHE_QUERY_EXECUTED,
                        CacheQueryType.SQL.name(),
                        mainCctx.namex(),
                        null,
                        qry.query(),
                        null,
                        null,
                        qry.parameters(),
                        node.id(),
                        null));
                }

                assert rs instanceof JdbcResultSet : rs.getClass();

                qr.addResult(i, qry, node.id(), rs);

                if (qr.canceled) {
                    qr.result(i).close();

                    return;
                }

                // Send the first page.
                sendNextPage(nodeRess, node, qr, i, req.pageSize());

                i++;
            }
        }
        catch (Throwable e) {
            if (qr != null) {
                nodeRess.remove(req.requestId(), qr);

                qr.cancel();
            }

            U.error(log, "Failed to execute local query: " + req, e);

            sendError(node, req.requestId(), e);

            if (e instanceof Error)
                throw (Error)e;
        }
        finally {
            h2.setFilters(null);

            // Release reserved partitions.
            for (GridReservable r : reserved)
                r.release();
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

            if (node.isLocal())
                h2.reduceQueryExecutor().onMessage(ctx.localNodeId(), msg);
            else
                ctx.io().send(node, GridTopic.TOPIC_QUERY, msg, QUERY_POOL);
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
        ConcurrentMap<Long, QueryResults> nodeRess = qryRess.get(node.id());

        QueryResults qr = nodeRess == null ? null : nodeRess.get(req.queryRequestId());

        if (qr == null || qr.canceled)
            sendError(node, req.queryRequestId(), new CacheException("No query result found for request: " + req));
        else
            sendNextPage(nodeRess, node, qr, req.query(), req.pageSize());
    }

    /**
     * @param node Node.
     * @param qr Query results.
     * @param qry Query.
     * @param pageSize Page size.
     */
    private void sendNextPage(ConcurrentMap<Long, QueryResults> nodeRess, ClusterNode node, QueryResults qr, int qry,
        int pageSize) {
        QueryResult res = qr.result(qry);

        assert res != null;

        int page = res.page;

        List<Value[]> rows = new ArrayList<>(Math.min(64, pageSize));

        boolean last = res.fetchNextPage(rows, pageSize);

        if (last) {
            res.close();

            if (qr.isAllClosed())
                nodeRess.remove(qr.qryReqId, qr);
        }

        try {
            boolean loc = node.isLocal();

            GridQueryNextPageResponse msg = new GridQueryNextPageResponse(qr.qryReqId, qry, page,
                page == 0 ? res.rowCount : -1 ,
                res.cols,
                loc ? null : toMessages(rows, new ArrayList<Message>(res.cols)),
                loc ? rows : null);

            if (loc)
                h2.reduceQueryExecutor().onMessage(ctx.localNodeId(), msg);
            else
                ctx.io().send(node, GridTopic.TOPIC_QUERY, msg, QUERY_POOL);
        }
        catch (IgniteCheckedException e) {
            log.error("Failed to send message.", e);

            throw new IgniteException(e);
        }
    }

    /**
     * @param node Node.
     * @param reqId Request ID.
     * @throws IgniteCheckedException If failed.
     */
    private void sendRetry(ClusterNode node, long reqId) throws IgniteCheckedException {
        boolean loc = node.isLocal();

        GridQueryNextPageResponse msg = new GridQueryNextPageResponse(reqId,
            /*qry*/0, /*page*/0, /*allRows*/0, /*cols*/1,
            loc ? null : Collections.<Message>emptyList(),
            loc ? Collections.<Value[]>emptyList() : null);

        msg.retry(h2.readyTopologyVersion());

        if (loc)
            h2.reduceQueryExecutor().onMessage(ctx.localNodeId(), msg);
        else
            ctx.io().send(node, GridTopic.TOPIC_QUERY, msg, QUERY_POOL);
    }

    /**
     * @param cacheName Cache name.
     */
    public void onCacheStop(String cacheName) {
        // Drop group reservations.
        for (T2<String,AffinityTopologyVersion> grpKey : reservations.keySet()) {
            if (F.eq(grpKey.get1(), cacheName))
                reservations.remove(grpKey);
        }
    }

    /**
     *
     */
    private class QueryResults {
        /** */
        private final long qryReqId;

        /** */
        private final AtomicReferenceArray<QueryResult> results;

        /** */
        private final GridCacheContext<?,?> cctx;

        /** */
        private volatile boolean canceled;

        /**
         * @param qryReqId Query request ID.
         * @param qrys Number of queries.
         * @param cctx Cache context.
         */
        private QueryResults(long qryReqId, int qrys, GridCacheContext<?,?> cctx) {
            this.qryReqId = qryReqId;
            this.cctx = cctx;

            results = new AtomicReferenceArray<>(qrys);
        }

        /**
         * @param qry Query result index.
         * @return Query result.
         */
        QueryResult result(int qry) {
            return results.get(qry);
        }

        /**
         * @param qry Query result index.
         * @param q Query object.
         * @param qrySrcNodeId Query source node.
         * @param rs Result set.
         */
        void addResult(int qry, GridCacheSqlQuery q, UUID qrySrcNodeId, ResultSet rs) {
            if (!results.compareAndSet(qry, null, new QueryResult(rs, cctx, qrySrcNodeId, q)))
                throw new IllegalStateException();
        }

        /**
         * @return {@code true} If all results are closed.
         */
        boolean isAllClosed() {
            for (int i = 0; i < results.length(); i++) {
                QueryResult res = results.get(i);

                if (res == null || !res.closed)
                    return false;
            }

            return true;
        }

        void cancel() {
            if (canceled)
                return;

            canceled = true;

            for (int i = 0; i < results.length(); i++) {
                QueryResult res = results.get(i);

                if (res != null)
                    res.close();
            }
        }
    }

    /**
     * Result for a single part of the query.
     */
    private class QueryResult implements AutoCloseable {
        /** */
        private final ResultInterface res;

        /** */
        private final ResultSet rs;

        /** */
        private final GridCacheContext<?,?> cctx;

        /** */
        private final GridCacheSqlQuery qry;

        /** */
        private final UUID qrySrcNodeId;

        /** */
        private final int cols;

        /** */
        private int page;

        /** */
        private final int rowCount;

        /** */
        private volatile boolean closed;

        /**
         * @param rs Result set.
         * @param cctx Cache context.
         * @param qrySrcNodeId Query source node.
         * @param qry Query.
         */
        private QueryResult(ResultSet rs, GridCacheContext<?,?> cctx, UUID qrySrcNodeId, GridCacheSqlQuery qry) {
            this.rs = rs;
            this.cctx = cctx;
            this.qry = qry;
            this.qrySrcNodeId = qrySrcNodeId;

            try {
                res = (ResultInterface)RESULT_FIELD.get(rs);
            }
            catch (IllegalAccessException e) {
                throw new IllegalStateException(e); // Must not happen.
            }

            rowCount = res.getRowCount();
            cols = res.getVisibleColumnCount();
        }

        /**
         * @param rows Collection to fetch into.
         * @param pageSize Page size.
         * @return {@code true} If there are no more rows available.
         */
        synchronized boolean fetchNextPage(List<Value[]> rows, int pageSize) {
            if (closed)
                return true;

            boolean readEvt = cctx.gridEvents().isRecordable(EVT_CACHE_QUERY_OBJECT_READ);

            page++;

            for (int i = 0 ; i < pageSize; i++) {
                if (!res.next())
                    return true;

                Value[] row = res.currentRow();

                assert row != null;

                if (readEvt) {
                    cctx.gridEvents().record(new CacheQueryReadEvent<>(
                        cctx.localNode(),
                        "SQL fields query result set row read.",
                        EVT_CACHE_QUERY_OBJECT_READ,
                        CacheQueryType.SQL.name(),
                        cctx.namex(),
                        null,
                        qry.query(),
                        null,
                        null,
                        qry.parameters(),
                        qrySrcNodeId,
                        null,
                        null,
                        null,
                        null,
                        row(row)));
                }

                rows.add(res.currentRow());
            }

            return false;
        }

        /**
         * @param row Values array row.
         * @return Objects list row.
         */
        private List<?> row(Value[] row) {
            List<Object> res = new ArrayList<>(row.length);

            for (Value v : row)
                res.add(v.getObject());

            return res;
        }

        /** {@inheritDoc} */
        @Override public synchronized void close() {
            if (closed)
                return;

            closed = true;

            Statement stmt;

            try {
                stmt = rs.getStatement();
            }
            catch (SQLException e) {
                throw new IllegalStateException(e); // Must not happen.
            }

            U.close(rs, log);
            U.close(stmt, log);
        }
    }

    /**
     * Fake reservation object for replicated caches.
     */
    private static class ReplicatedReservation implements GridReservable {
        /** */
        static final ReplicatedReservation INSTANCE = new ReplicatedReservation();

        /** {@inheritDoc} */
        @Override public boolean reserve() {
            throw new IllegalStateException();
        }

        /** {@inheritDoc} */
        @Override public void release() {
            throw new IllegalStateException();
        }
    }
}