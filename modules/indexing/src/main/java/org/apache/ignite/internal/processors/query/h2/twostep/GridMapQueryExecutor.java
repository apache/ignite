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

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.events.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.managers.communication.*;
import org.apache.ignite.internal.managers.eventstorage.*;
import org.apache.ignite.internal.processors.cache.query.*;
import org.apache.ignite.internal.processors.query.h2.*;
import org.apache.ignite.internal.processors.query.h2.twostep.messages.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.h2.jdbc.*;
import org.h2.result.*;
import org.h2.store.*;
import org.h2.value.*;
import org.jsr166.*;

import javax.cache.*;
import java.lang.reflect.*;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.apache.ignite.events.EventType.*;

/**
 * Map query executor.
 */
public class GridMapQueryExecutor implements GridMessageListener {
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

        ctx.io().addMessageListener(GridTopic.TOPIC_QUERY, this);
    }

    /** {@inheritDoc} */
    @Override public void onMessage(UUID nodeId, Object msg) {
        try {
            assert msg != null;

            ClusterNode node = ctx.discovery().node(nodeId);

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
     * Executing queries locally.
     *
     * @param node Node.
     * @param req Query request.
     */
    private void onQueryRequest(ClusterNode node, GridQueryRequest req) {
        ConcurrentMap<Long,QueryResults> nodeRess = resultsForNode(node.id());

        Collection<GridCacheSqlQuery> qrys;

        try {
            qrys = req.queries(ctx.config().getMarshaller());
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }

        QueryResults qr = new QueryResults(req.requestId(), qrys.size());

        if (nodeRess.put(req.requestId(), qr) != null)
            throw new IllegalStateException();

        h2.setFilters(h2.backupFilter());

        try {
            // TODO Prepare snapshots for all the needed tables before the run.

            // Run queries.
            int i = 0;

            String space = req.space();

            for (GridCacheSqlQuery qry : qrys) {
                ResultSet rs = h2.executeSqlQueryWithTimer(space, h2.connectionForSpace(space), qry.query(),
                    F.asList(qry.parameters()));

                if (ctx.event().isRecordable(EVT_CACHE_QUERY_EXECUTED)) {
                    ctx.event().record(new CacheQueryExecutedEvent<>(
                        node,
                        "SQL query executed.",
                        EVT_CACHE_QUERY_EXECUTED,
                        CacheQueryType.SQL,
                        null,
                        null,
                        qry.query(),
                        null,
                        null,
                        qry.parameters(),
                        null,
                        null));
                }

                assert rs instanceof JdbcResultSet : rs.getClass();

                qr.addResult(i, rs);

                if (qr.canceled) {
                    qr.result(i).close();

                    throw new IgniteException("Query was canceled.");
                }

                // Send the first page.
                sendNextPage(nodeRess, node, qr, i, req.pageSize());

                i++;
            }
        }
        catch (Throwable e) {
            nodeRess.remove(req.requestId(), qr);

            qr.cancel();

            U.error(log, "Failed to execute local query: " + req, e);

            sendError(node, req.requestId(), e);
        }
        finally {
            h2.setFilters(null);
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
                ctx.io().send(node, GridTopic.TOPIC_QUERY, msg, GridIoPolicy.PUBLIC_POOL);
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
            GridQueryNextPageResponse msg = new GridQueryNextPageResponse(qr.qryReqId, qry, page,
                page == 0 ? res.rowCount : -1, marshallRows(rows));

            if (node.isLocal())
                h2.reduceQueryExecutor().onMessage(ctx.localNodeId(), msg);
            else
                ctx.io().send(node, GridTopic.TOPIC_QUERY, msg, GridIoPolicy.PUBLIC_POOL);
        }
        catch (IgniteCheckedException e) {
            log.error("Failed to send message.", e);

            throw new IgniteException(e);
        }
    }

    /**
     * @param bytes Bytes.
     * @return Rows.
     */
    public static List<Value[]> unmarshallRows(byte[] bytes) {
        Data data = Data.create(null, bytes);

        int rowCnt = data.readVarInt();

        if (rowCnt == 0)
            return Collections.emptyList();

        ArrayList<Value[]> rows = new ArrayList<>(rowCnt);

        int cols = data.readVarInt();

        for (int r = 0; r < rowCnt; r++) {
            Value[] row = new Value[cols];

            for (int c = 0; c < cols; c++)
                row[c] = data.readValue();

            rows.add(row);
        }

        return rows;
    }

    /**
     * @param rows Rows.
     * @return Bytes.
     */
    public static byte[] marshallRows(Collection<Value[]> rows) {
        Data data = Data.create(null, 256);

        data.writeVarInt(rows.size());

        boolean first = true;

        for (Value[] row : rows) {
            if (first) {
                data.writeVarInt(row.length);

                first = false;
            }

            for (Value val : row) {
                data.checkCapacity(data.getValueLen(val));

                data.writeValue(val);
            }
        }

        return Arrays.copyOf(data.getBytes(), data.length());
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
        private volatile boolean canceled;

        /**
         * @param qryReqId Query request ID.
         * @param qrys Number of queries.
         */
        private QueryResults(long qryReqId, int qrys) {
            this.qryReqId = qryReqId;

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
         * @param rs Result set.
         */
        void addResult(int qry, ResultSet rs) {
            if (!results.compareAndSet(qry, null, new QueryResult(rs)))
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
        private int page;

        /** */
        private final int rowCount;

        /** */
        private volatile boolean closed;

        /**
         * @param rs Result set.
         */
        private QueryResult(ResultSet rs) {
            this.rs = rs;

            try {
                res = (ResultInterface)RESULT_FIELD.get(rs);
            }
            catch (IllegalAccessException e) {
                throw new IllegalStateException(e); // Must not happen.
            }

            rowCount = res.getRowCount();
        }

        /**
         * @param rows Collection to fetch into.
         * @param pageSize Page size.
         * @return {@code true} If there are no more rows available.
         */
        synchronized boolean fetchNextPage(List<Value[]> rows, int pageSize) {
            if (closed)
                return true;

            page++;

            for (int i = 0 ; i < pageSize; i++) {
                if (!res.next())
                    return true;

                rows.add(res.currentRow());
            }

            return false;
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
}
