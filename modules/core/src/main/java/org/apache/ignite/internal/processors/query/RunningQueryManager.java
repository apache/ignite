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

package org.apache.ignite.internal.processors.query;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.SqlConfiguration;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.internal.GridTopic;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.internal.managers.eventstorage.GridLocalEventListener;
import org.apache.ignite.internal.managers.systemview.walker.SqlQueryHistoryViewWalker;
import org.apache.ignite.internal.managers.systemview.walker.SqlQueryViewWalker;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryType;
import org.apache.ignite.internal.processors.closure.GridClosureProcessor;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.processors.metric.impl.AtomicLongMetric;
import org.apache.ignite.internal.processors.metric.impl.LongAdderMetric;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.processors.query.messages.GridQueryKillRequest;
import org.apache.ignite.internal.processors.query.messages.GridQueryKillResponse;
import org.apache.ignite.internal.processors.tracing.Span;
import org.apache.ignite.internal.util.GridSpinBusyLock;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.lang.GridPlainRunnable;
import org.apache.ignite.internal.util.typedef.CIX2;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.systemview.view.SqlQueryHistoryView;
import org.apache.ignite.spi.systemview.view.SqlQueryView;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.query.GridCacheQueryType.SQL;
import static org.apache.ignite.internal.processors.cache.query.GridCacheQueryType.SQL_FIELDS;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;
import static org.apache.ignite.internal.processors.security.SecurityUtils.securitySubjectId;
import static org.apache.ignite.internal.processors.tracing.SpanTags.ERROR;
import static org.apache.ignite.internal.processors.tracing.SpanTags.SQL_QRY_ID;

/**
 * Keep information about all running queries.
 */
public class RunningQueryManager {
    /** Name of the MetricRegistry which metrics measure stats of queries initiated by user. */
    public static final String SQL_USER_QUERIES_REG_NAME = "sql.queries.user";

    /** */
    public static final String SQL_QRY_VIEW = metricName("sql", "queries");

    /** */
    public static final String SQL_QRY_VIEW_DESC = "Running SQL queries.";

    /** */
    public static final String SQL_QRY_HIST_VIEW = metricName("sql", "queries", "history");

    /** */
    public static final String SQL_QRY_HIST_VIEW_DESC = "SQL queries history.";

    /** Undefined query ID value. */
    public static final long UNDEFINED_QUERY_ID = 0L;

    /** */
    private final GridClosureProcessor closure;

    /** Keep registered user queries. */
    private final ConcurrentMap<Long, GridRunningQueryInfo> runs = new ConcurrentHashMap<>();

    /** Unique id for queries on single node. */
    private final AtomicLong qryIdGen = new AtomicLong();

    /** Local node ID. */
    private final UUID localNodeId;

    /** History size. */
    private final int histSz;

    /** Query history tracker. */
    private volatile QueryHistoryTracker qryHistTracker;

    /** Number of successfully executed queries. */
    private final LongAdderMetric successQrsCnt;

    /** Number of failed queries in total by any reason. */
    private final AtomicLongMetric failedQrsCnt;

    /**
     * Number of canceled queries. Canceled queries a treated as failed and counting twice: here and in {@link
     * #failedQrsCnt}.
     */
    private final AtomicLongMetric canceledQrsCnt;

    /** Kernal context. */
    private final GridKernalContext ctx;

    /** Logger. */
    private final IgniteLogger log;

    /** Current running query info. */
    private final ThreadLocal<GridRunningQueryInfo> currQryInfo = new ThreadLocal<>();

    /** */
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    /** */
    private GridSpinBusyLock busyLock;

    /** Cancellation runs. */
    private final ConcurrentMap<Long, CancelQueryFuture> cancellationRuns = new ConcurrentHashMap<>();

    /** Query cancel request counter. */
    private final AtomicLong qryCancelReqCntr = new AtomicLong();

    /** Flag indicate that node is stopped or not. */
    private volatile boolean stopped;

    /** Local node message handler */
    private final CIX2<ClusterNode, Message> locNodeMsgHnd = new CIX2<ClusterNode, Message>() {
        @Override public void applyx(ClusterNode locNode, Message msg) {
            onMessage(locNode.id(), msg);
        }
    };

    /** */
    private final List<Consumer<GridQueryStartedInfo>> qryStartedListeners = new CopyOnWriteArrayList<>();

    /** */
    private final List<Consumer<GridQueryFinishedInfo>> qryFinishedListeners = new CopyOnWriteArrayList<>();

    /**
     * Constructor.
     *
     * @param ctx Context.
     */
    public RunningQueryManager(GridKernalContext ctx) {
        this.ctx = ctx;

        log = ctx.log(getClass());

        localNodeId = ctx.localNodeId();

        histSz = ctx.config().getSqlConfiguration().getSqlQueryHistorySize();
        closure = ctx.closure();

        qryHistTracker = new QueryHistoryTracker(histSz);

        ctx.systemView().registerView(SQL_QRY_VIEW, SQL_QRY_VIEW_DESC,
            new SqlQueryViewWalker(),
            runs.values(),
            SqlQueryView::new);

        ctx.systemView().registerView(SQL_QRY_HIST_VIEW, SQL_QRY_HIST_VIEW_DESC,
            new SqlQueryHistoryViewWalker(),
            qryHistTracker.queryHistory().values(),
            SqlQueryHistoryView::new);

        MetricRegistry userMetrics = ctx.metric().registry(SQL_USER_QUERIES_REG_NAME);

        successQrsCnt = userMetrics.longAdderMetric("success",
            "Number of successfully executed user queries that have been started on this node.");

        failedQrsCnt = userMetrics.longMetric("failed", "Total number of failed by any reason (cancel, etc)" +
            " queries that have been started on this node.");

        canceledQrsCnt = userMetrics.longMetric("canceled", "Number of canceled queries that have been started " +
            "on this node. This metric number included in the general 'failed' metric.");
    }

    /** */
    public void start(GridSpinBusyLock busyLock) {
        this.busyLock = busyLock;

        ctx.io().addMessageListener(GridTopic.TOPIC_QUERY, (nodeId, msg, plc) -> onMessage(nodeId, msg));

        ctx.event().addLocalEventListener(new GridLocalEventListener() {
            @Override public void onEvent(final Event evt) {
                UUID nodeId = ((DiscoveryEvent)evt).eventNode().id();

                List<GridFutureAdapter<String>> futs = new ArrayList<>();

                lock.writeLock().lock();

                try {
                    Iterator<CancelQueryFuture> it = cancellationRuns.values().iterator();

                    while (it.hasNext()) {
                        CancelQueryFuture fut = it.next();

                        if (fut.nodeId().equals(nodeId)) {
                            futs.add(fut);

                            it.remove();
                        }
                    }
                }
                finally {
                    lock.writeLock().unlock();
                }

                futs.forEach(f -> f.onDone("Query node has left the grid: [nodeId=" + nodeId + "]"));
            }
        }, EventType.EVT_NODE_FAILED, EventType.EVT_NODE_LEFT);
    }

    /**
     * Registers running query and returns an id associated with the query.
     *
     * @param qry Query text.
     * @param qryType Query type.
     * @param schemaName Schema name.
     * @param loc Local query flag.
     * @param cancel Query cancel. Should be passed in case query is cancelable, or {@code null} otherwise.
     * @param enforceJoinOrder Enforce join order flag.
     * @param lazy Lazy flag.
     * @param distributedJoins Distributed joins flag.
     * @return Id of registered query. Id is a positive number.
     */
    public long register(String qry, GridCacheQueryType qryType, String schemaName, boolean loc,
        @Nullable GridQueryCancel cancel,
        String qryInitiatorId, boolean enforceJoinOrder, boolean lazy, boolean distributedJoins) {
        long qryId = qryIdGen.incrementAndGet();

        if (qryInitiatorId == null)
            qryInitiatorId = SqlFieldsQuery.threadedQueryInitiatorId();

        final GridRunningQueryInfo run = new GridRunningQueryInfo(
            qryId,
            localNodeId,
            qry,
            qryType,
            schemaName,
            System.currentTimeMillis(),
            ctx.performanceStatistics().enabled() ? System.nanoTime() : 0,
            cancel,
            loc,
            qryInitiatorId,
            enforceJoinOrder,
            lazy,
            distributedJoins,
            securitySubjectId(ctx)
        );

        GridRunningQueryInfo preRun = runs.putIfAbsent(qryId, run);

        if (ctx.performanceStatistics().enabled())
            currQryInfo.set(run);

        assert preRun == null : "Running query already registered [prev_qry=" + preRun + ", newQry=" + run + ']';

        run.span().addTag(SQL_QRY_ID, run::globalQueryId);

        if (!qryStartedListeners.isEmpty()) {
            GridQueryStartedInfo info = new GridQueryStartedInfo(
                run.id(),
                localNodeId,
                run.query(),
                run.queryType(),
                run.schemaName(),
                run.startTime(),
                run.cancelable(),
                run.local(),
                run.enforceJoinOrder(),
                run.lazy(),
                run.distributedJoins(),
                run.queryInitiatorId()
            );

            try {
                closure.runLocal(
                    () -> qryStartedListeners.forEach(lsnr -> {
                        try {
                            lsnr.accept(info);
                        }
                        catch (Exception ex) {
                            log.error("Listener fails during handling query started" +
                                " event [qryId=" + qryId + "]", ex);
                        }
                    }),
                    GridIoPolicy.PUBLIC_POOL
                );
            }
            catch (IgniteCheckedException ex) {
                throw new IgniteException(ex.getMessage(), ex);
            }
        }

        return qryId;
    }

    /**
     * Unregister running query.
     *
     * @param qryId id of the query, which is given by {@link #register register} method.
     * @param failReason exception that caused query execution fail, or {@code null} if query succeded.
     */
    public void unregister(long qryId, @Nullable Throwable failReason) {
        if (qryId <= 0)
            return;

        boolean failed = failReason != null;

        GridRunningQueryInfo qry = runs.remove(qryId);

        // Attempt to unregister query twice.
        if (qry == null)
            return;

        Span qrySpan = qry.span();

        try {
            if (failed)
                qrySpan.addTag(ERROR, failReason::getMessage);

            if (!qryFinishedListeners.isEmpty()) {
                GridQueryFinishedInfo info = new GridQueryFinishedInfo(
                    qry.id(),
                    localNodeId,
                    qry.query(),
                    qry.queryType(),
                    qry.schemaName(),
                    qry.startTime(),
                    System.currentTimeMillis(),
                    qry.local(),
                    qry.enforceJoinOrder(),
                    qry.lazy(),
                    qry.distributedJoins(),
                    failed,
                    failReason,
                    qry.queryInitiatorId()
                );

                try {
                    closure.runLocal(
                        () -> qryFinishedListeners.forEach(lsnr -> {
                            try {
                                lsnr.accept(info);
                            }
                            catch (Exception ex) {
                                log.error("Listener fails during handling query finished" +
                                    " event [qryId=" + qryId + "]", ex);
                            }
                        }),
                        GridIoPolicy.PUBLIC_POOL
                    );
                }
                catch (IgniteCheckedException ex) {
                    throw new IgniteException(ex.getMessage(), ex);
                }
            }

            //We need to collect query history and metrics only for SQL queries.
            if (isSqlQuery(qry)) {
                qry.runningFuture().onDone();

                qryHistTracker.collectHistory(qry, failed);

                if (!failed)
                    successQrsCnt.increment();
                else {
                    failedQrsCnt.increment();

                    // We measure cancel metric as "number of times user's queries ended up with query cancelled exception",
                    // not "how many user's KILL QUERY command succeeded". These may be not the same if cancel was issued
                    // right when query failed due to some other reason.
                    if (QueryUtils.wasCancelled(failReason))
                        canceledQrsCnt.increment();
                }
            }

            if (ctx.performanceStatistics().enabled() && qry.startTimeNanos() > 0) {
                ctx.performanceStatistics().query(
                    qry.queryType(),
                    qry.query(),
                    qry.requestId(),
                    qry.startTime(),
                    System.nanoTime() - qry.startTimeNanos(),
                    !failed);
            }
        }
        finally {
            qrySpan.end();
        }
    }

    /** @param reqId Request ID of query to track. */
    public void trackRequestId(long reqId) {
        if (ctx.performanceStatistics().enabled()) {
            GridRunningQueryInfo info = currQryInfo.get();

            if (info != null)
                info.requestId(reqId);
        }
    }

    /**
     * Return SQL queries which executing right now.
     *
     * @return List of SQL running queries.
     */
    public List<GridRunningQueryInfo> runningSqlQueries() {
        List<GridRunningQueryInfo> res = new ArrayList<>();

        for (GridRunningQueryInfo run : runs.values()) {
            if (isSqlQuery(run))
                res.add(run);
        }

        return res;
    }

    /**
     * @param lsnr Listener.
     */
    public void registerQueryStartedListener(Consumer<GridQueryStartedInfo> lsnr) {
        A.notNull(lsnr, "lsnr");

        qryStartedListeners.add(lsnr);
    }

    /**
     * @param lsnr Listener.
     */
    @SuppressWarnings("SuspiciousMethodCalls")
    public boolean unregisterQueryStartedListener(Object lsnr) {
        A.notNull(lsnr, "lsnr");

        return qryStartedListeners.remove(lsnr);
    }

    /**
     * @param lsnr Listener.
     */
    public void registerQueryFinishedListener(Consumer<GridQueryFinishedInfo> lsnr) {
        A.notNull(lsnr, "lsnr");

        qryFinishedListeners.add(lsnr);
    }

    /**
     * @param lsnr Listener.
     */
    @SuppressWarnings("SuspiciousMethodCalls")
    public boolean unregisterQueryFinishedListener(Object lsnr) {
        A.notNull(lsnr, "lsnr");

        return qryFinishedListeners.remove(lsnr);
    }

    /**
     * Check belongs running query to an SQL type.
     *
     * @param runningQryInfo Running query info object.
     * @return {@code true} For SQL or SQL_FIELDS query type.
     */
    private boolean isSqlQuery(GridRunningQueryInfo runningQryInfo) {
        return runningQryInfo.queryType() == SQL_FIELDS || runningQryInfo.queryType() == SQL;
    }

    /**
     * Return long running user queries.
     *
     * @param duration Duration of long query.
     * @return Collection of queries which running longer than given duration.
     */
    public Collection<GridRunningQueryInfo> longRunningQueries(long duration) {
        Collection<GridRunningQueryInfo> res = new ArrayList<>();

        long curTime = System.currentTimeMillis();

        for (GridRunningQueryInfo runningQryInfo : runs.values()) {
            if (runningQryInfo.longQuery(curTime, duration))
                res.add(runningQryInfo);
        }

        return res;
    }

    /**
     * Cancel query.
     *
     * @param qryId Query id.
     */
    public void cancelLocalQuery(long qryId) {
        GridRunningQueryInfo run = runs.get(qryId);

        if (run != null)
            run.cancel();
    }

    /**
     * Cancel all executing queries and deregistering all of them.
     */
    public void stop() {
        stopped = true;

        completeCancellationFutures("Local node is stopping: [nodeId=" + ctx.localNodeId() + "]");

        Iterator<GridRunningQueryInfo> iter = runs.values().iterator();

        while (iter.hasNext()) {
            try {
                GridRunningQueryInfo r = iter.next();

                iter.remove();

                r.cancel();
            }
            catch (Exception ignore) {
                // No-op.
            }
        }
    }

    /**
     * Cancel query running on remote or local Node.
     *
     * @param queryId Query id.
     * @param nodeId Node id, if {@code null}, cancel local query.
     * @param async If {@code true}, execute asynchronously.
     */
    public void cancelQuery(long queryId, @Nullable UUID nodeId, boolean async) {
        CancelQueryFuture fut;

        lock.readLock().lock();

        try {
            if (stopped)
                throw new IgniteSQLException("Failed to cancel query due to node is stopped [nodeId=" + nodeId +
                    ", qryId=" + queryId + "]");

            final ClusterNode node = nodeId != null ? ctx.discovery().node(nodeId) : ctx.discovery().localNode();

            if (node != null) {
                fut = new CancelQueryFuture(nodeId, queryId);

                long reqId = qryCancelReqCntr.incrementAndGet();

                cancellationRuns.put(reqId, fut);

                final GridQueryKillRequest request = new GridQueryKillRequest(reqId, queryId, async);

                if (node.isLocal() && !async)
                    locNodeMsgHnd.apply(node, request);
                else {
                    try {
                        if (node.isLocal()) {
                            ctx.closure().runLocal(new GridPlainRunnable() {
                                @Override public void run() {
                                    if (!busyLock.enterBusy())
                                        return;

                                    try {
                                        locNodeMsgHnd.apply(node, request);
                                    }
                                    finally {
                                        busyLock.leaveBusy();
                                    }
                                }
                            }, GridIoPolicy.MANAGEMENT_POOL);
                        }
                        else {
                            ctx.io().sendGeneric(node, GridTopic.TOPIC_QUERY, GridTopic.TOPIC_QUERY.ordinal(), request,
                                GridIoPolicy.MANAGEMENT_POOL);
                        }
                    }
                    catch (IgniteCheckedException e) {
                        cancellationRuns.remove(reqId);

                        throw new IgniteSQLException("Failed to cancel query due communication problem " +
                            "[nodeId=" + node.id() + ",qryId=" + queryId + ", errMsg=" + e.getMessage() + "]");
                    }
                }
            }
            else
                throw new IgniteSQLException("Failed to cancel query, node is not alive [nodeId=" + nodeId + ", qryId="
                    + queryId + "]");
        }
        finally {
            lock.readLock().unlock();
        }

        try {
            String err = fut != null ? fut.get() : null;

            if (err != null) {
                throw new IgniteSQLException("Failed to cancel query [nodeId=" + nodeId + ", qryId="
                    + queryId + ", err=" + err + "]");
            }
        }
        catch (IgniteCheckedException e) {
            throw new IgniteSQLException("Failed to cancel query [nodeId=" + nodeId + ", qryId="
                + queryId + ", err=" + e + "]", e);
        }
    }

    /**
     * Client disconnected callback.
     */
    public void onDisconnected() {
        completeCancellationFutures("Failed to cancel query because local client node has been disconnected from the cluster");
    }

    /**
     * @param err Text of error to complete futures.
     */
    private void completeCancellationFutures(@Nullable String err) {
        lock.writeLock().lock();

        try {
            Iterator<CancelQueryFuture> it = cancellationRuns.values().iterator();

            while (it.hasNext()) {
                CancelQueryFuture fut = it.next();

                fut.onDone(err);

                it.remove();
            }
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * @param nodeId Node ID.
     * @param msg Message.
     */
    private void onMessage(UUID nodeId, Object msg) {
        assert msg != null;

        ClusterNode node = ctx.discovery().node(nodeId);

        if (node == null)
            return; // Node left, ignore.

        boolean processed = true;

        if (msg instanceof GridQueryKillRequest)
            onQueryKillRequest((GridQueryKillRequest)msg, node);
        if (msg instanceof GridQueryKillResponse)
            onQueryKillResponse((GridQueryKillResponse)msg);
        else
            processed = false;

        if (processed && log.isDebugEnabled())
            log.debug("Processed response: " + nodeId + "->" + ctx.localNodeId() + " " + msg);
    }

    /**
     * Process request to kill query.
     *
     * @param msg Message.
     * @param node Cluster node.
     */
    private void onQueryKillRequest(GridQueryKillRequest msg, ClusterNode node) {
        final long qryId = msg.nodeQryId();

        String err = null;

        GridRunningQueryInfo runningQryInfo = runs.get(qryId);

        if (runningQryInfo == null)
            err = "Query with provided ID doesn't exist " +
                "[nodeId=" + ctx.localNodeId() + ", qryId=" + qryId + "]";
        else if (!runningQryInfo.cancelable())
            err = "Query doesn't support cancellation " +
                "[nodeId=" + ctx.localNodeId() + ", qryId=" + qryId + "]";

        if (msg.asyncResponse() || err != null)
            sendKillResponse(msg, node, err);

        if (err == null) {
            try {
                runningQryInfo.cancel();
            }
            catch (Exception e) {
                U.warn(log, "Cancellation of query failed: [qryId=" + qryId + "]", e);

                if (!msg.asyncResponse())
                    sendKillResponse(msg, node, e.getMessage());

                return;
            }

            if (!msg.asyncResponse())
                runningQryInfo.runningFuture().listen((f) -> sendKillResponse(msg, node, f.result()));
        }
    }

    /**
     * @param request Kill request message.
     * @param node Initial kill request node.
     * @param err Error message
     */
    private void sendKillResponse(GridQueryKillRequest request, ClusterNode node, @Nullable String err) {
        GridQueryKillResponse response = new GridQueryKillResponse(request.requestId(), err);

        if (node.isLocal()) {
            locNodeMsgHnd.apply(node, response);

            return;
        }

        try {
            ctx.io().sendGeneric(node, GridTopic.TOPIC_QUERY, GridTopic.TOPIC_QUERY.ordinal(), response,
                GridIoPolicy.MANAGEMENT_POOL);
        }
        catch (IgniteCheckedException e) {
            U.warn(log, "Failed to send message [node=" + node + ", msg=" + response +
                ", errMsg=" + e.getMessage() + "]");

            U.warn(log, "Response on query cancellation wasn't send back: [qryId=" + request.nodeQryId() + "]");
        }
    }

    /**
     * Process response to kill query request.
     *
     * @param msg Message.
     */
    private void onQueryKillResponse(GridQueryKillResponse msg) {
        CancelQueryFuture fut;

        lock.readLock().lock();

        try {
            fut = cancellationRuns.remove(msg.requestId());
        }
        finally {
            lock.readLock().unlock();
        }

        if (fut != null)
            fut.onDone(msg.error());
    }

    /**
     * Gets query history statistics. Size of history could be configured via {@link
     * SqlConfiguration#setSqlQueryHistorySize(int)}
     *
     * @return Queries history statistics aggregated by query text, schema and local flag.
     */
    public Map<QueryHistoryKey, QueryHistory> queryHistoryMetrics() {
        return qryHistTracker.queryHistory();
    }

    /**
     * Gets info about running query by their id.
     *
     * @param qryId Query Id.
     * @return Running query info or {@code null} in case no running query for given id.
     */
    public @Nullable GridRunningQueryInfo runningQueryInfo(long qryId) {
        return runs.get(qryId);
    }

    /**
     * Reset query history.
     */
    public void resetQueryHistoryMetrics() {
        qryHistTracker = new QueryHistoryTracker(histSz);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(RunningQueryManager.class, this);
    }

    /**
     * Query cancel future.
     */
    private static class CancelQueryFuture extends GridFutureAdapter<String> {
        /** Node id. */
        private final UUID nodeId;

        /** Node query id. */
        private final long nodeQryId;

        /**
         * Constructor.
         *
         * @param nodeId Node id.
         * @param nodeQryId Query id.
         */
        public CancelQueryFuture(UUID nodeId, long nodeQryId) {
            assert nodeId != null;

            this.nodeId = nodeId;
            this.nodeQryId = nodeQryId;
        }

        /**
         * @return Node id.
         */
        public UUID nodeId() {
            return nodeId;
        }

        /**
         * @return Node query id.
         */
        public long nodeQryId() {
            return nodeQryId;
        }
    }
}
