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

package org.apache.ignite.internal.processors.odbc.jdbc;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.QueryCancelledException;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.binary.BinaryWriterExImpl;
import org.apache.ignite.internal.processors.cache.QueryCursorImpl;
import org.apache.ignite.internal.processors.odbc.SqlListenerRequest;
import org.apache.ignite.internal.processors.odbc.SqlListenerRequestHandler;
import org.apache.ignite.internal.processors.odbc.SqlListenerResponse;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.util.GridSpinBusyLock;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.processors.odbc.jdbc.JdbcRequest.BATCH_EXEC;
import static org.apache.ignite.internal.processors.odbc.jdbc.JdbcRequest.QRY_CANCEL;
import static org.apache.ignite.internal.processors.odbc.jdbc.JdbcRequest.QRY_CLOSE;
import static org.apache.ignite.internal.processors.odbc.jdbc.JdbcRequest.QRY_EXEC;
import static org.apache.ignite.internal.processors.odbc.jdbc.JdbcRequest.QRY_FETCH;
import static org.apache.ignite.internal.processors.odbc.jdbc.JdbcRequest.QRY_META;

/**
 * SQL query handler.
 */
public class JdbcRequestHandler implements SqlListenerRequestHandler {
    /** Query ID sequence. */
    private static final AtomicLong QRY_ID_GEN = new AtomicLong();

    /** Kernel context. */
    private final GridKernalContext ctx;

    /** Logger. */
    private final IgniteLogger log;

    /** Busy lock. */
    private final GridSpinBusyLock busyLock;

    /** Maximum allowed cursors. */
    private final int maxCursors;

    /** Current queries cursors. */
    private final ConcurrentHashMap<Long, JdbcQueryCursor> qryCursors = new ConcurrentHashMap<>();

    /** Distributed joins flag. */
    private final boolean distributedJoins;

    /** Enforce join order flag. */
    private final boolean enforceJoinOrder;

    /** Collocated flag. */
    private final boolean collocated;

    /** Replicated only flag. */
    private final boolean replicatedOnly;

    /** Automatic close of cursors. */
    private final boolean autoCloseCursors;

    /** Connection ID. */
    private final Long connId;

    private final ConcurrentHashMap<Long, JdbcRequestHandler> handlers;

    /**
     * Constructor.
     *
     * @param ctx Context.
     * @param busyLock Shutdown latch.
     * @param maxCursors Maximum allowed cursors.
     * @param distributedJoins Distributed joins flag.
     * @param enforceJoinOrder Enforce join order flag.
     * @param collocated Collocated flag.
     * @param replicatedOnly Replicated only flag.
     * @param autoCloseCursors Flag to automatically close server cursors.
     * @param connId Connection ID.
     * @param handlers Connection Id to handler map.
     */
    public JdbcRequestHandler(GridKernalContext ctx, GridSpinBusyLock busyLock, int maxCursors,
        boolean distributedJoins, boolean enforceJoinOrder, boolean collocated, boolean replicatedOnly, 
        boolean autoCloseCursors, long connId, ConcurrentHashMap<Long, JdbcRequestHandler> handlers) {
        this.ctx = ctx;
        this.busyLock = busyLock;
        this.maxCursors = maxCursors;
        this.distributedJoins = distributedJoins;
        this.enforceJoinOrder = enforceJoinOrder;
        this.collocated = collocated;
        this.replicatedOnly = replicatedOnly;
        this.autoCloseCursors = autoCloseCursors;
        this.connId = connId;
        this.handlers = handlers;

        log = ctx.log(getClass());
    }

    /** {@inheritDoc} */
    @Override public SqlListenerResponse handle(SqlListenerRequest req0) {
        assert req0 != null;

        assert req0 instanceof JdbcRequest;

        JdbcRequest req = (JdbcRequest)req0;

        if (!busyLock.enterBusy())
            return new JdbcResponse(SqlListenerResponse.STATUS_FAILED, 
                "Failed to handle JDBC request because node is stopping.");

        try {
            switch (req.type()) {
                case QRY_EXEC:
                    return executeQuery((JdbcQueryExecuteRequest)req);

                case QRY_FETCH:
                    return fetchQuery((JdbcQueryFetchRequest)req);

                case QRY_CLOSE:
                    return closeQuery((JdbcQueryCloseRequest)req);

                case QRY_META:
                    return getQueryMeta((JdbcQueryMetadataRequest)req);

                case BATCH_EXEC:
                    return executeBatch((JdbcBatchExecuteRequest)req);

                case QRY_CANCEL:
                    return cancelQuery((JdbcQueryCancelRequest)req);
            }

            return new JdbcResponse(SqlListenerResponse.STATUS_FAILED, "Unsupported JDBC request [req=" + req + ']');
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /** {@inheritDoc} */
    @Override public SqlListenerResponse handleException(Exception e) {
        return new JdbcResponse(SqlListenerResponse.STATUS_FAILED, e.toString());
    }

    /** {@inheritDoc} */
    @Override public void handshakeAdditionalResponse(BinaryWriterExImpl writer) {
        writer.writeLong(connId);
    }

    /**
     * @return Connection ID.
     */
    public Long connectionId() {
        return connId;
    }

    /**
     */
    public void cancelCurrentQuery() {
        assert qryCursors.size() == 1 : "qryCursors.size()=" + qryCursors.size();

        JdbcQueryCursor cur = qryCursors.elements().nextElement();

        ctx.query().cancelQueries(Collections.singleton(cur.queryId()));
    }

    /**
     * {@link JdbcQueryExecuteRequest} command handler.
     *
     * @param req Execute query request.
     * @return Response.
     */
    @SuppressWarnings("unchecked")
    private JdbcResponse executeQuery(JdbcQueryExecuteRequest req) {
        int cursorCnt = qryCursors.size();

        if (maxCursors > 0 && cursorCnt >= maxCursors)
            return new JdbcResponse(SqlListenerResponse.STATUS_FAILED, "Too many opened cursors (either close other " +
                "opened cursors or increase the limit through OdbcConfiguration.setMaxOpenCursors()) " +
                "[maximum=" + maxCursors + ", current=" + cursorCnt + ']');

        long qryId = QRY_ID_GEN.getAndIncrement();

        try {
            String sql = req.sqlQuery();

            SqlFieldsQuery qry = new SqlFieldsQuery(sql);

            qry.setArgs(req.arguments());

            qry.setDistributedJoins(distributedJoins);
            qry.setEnforceJoinOrder(enforceJoinOrder);
            qry.setCollocated(collocated);
            qry.setReplicatedOnly(replicatedOnly);

            if (req.pageSize() <= 0)
                return new JdbcResponse(SqlListenerResponse.STATUS_FAILED,
                    "Invalid fetch size : [fetchSize=" + req.pageSize() + ']');

            qry.setPageSize(req.pageSize());

            String schemaName = req.schemaName();

            if (F.isEmpty(schemaName))
                schemaName = QueryUtils.DFLT_SCHEMA;

            qry.setSchema(schemaName);

            FieldsQueryCursor<List<?>> qryCur = ctx.query().querySqlFieldsNoCache(qry, true);

            JdbcQueryCursor cur = new JdbcQueryCursor(qryId, req.pageSize(), req.maxRows(), (QueryCursorImpl)qryCur);

            qryCursors.put(qryId, cur);

            cur.open();

            JdbcQueryExecuteResult res;

            if (cur.isQuery())
                res = new JdbcQueryExecuteResult(qryId, cur.fetchRows(), !cur.hasNext());
            else {
                List<List<Object>> items = cur.fetchRows();

                assert items != null && items.size() == 1 && items.get(0).size() == 1
                    && items.get(0).get(0) instanceof Long :
                    "Invalid result set for not-SELECT query. [qry=" + sql +
                        ", res=" + S.toString(List.class, items) + ']';

                res = new JdbcQueryExecuteResult(qryId, (Long)items.get(0).get(0));
            }

            if (res.last() && (!res.isQuery() || autoCloseCursors)) {
                cur.close();

                qryCursors.remove(qryId);
            }

            return new JdbcResponse(res);
        }
        catch (Exception e) {
            qryCursors.remove(qryId);

            U.error(log, "Failed to execute SQL query [reqId=" + req.requestId() + ", req=" + req + ']', e);

            if (e.getCause() != null && e.getCause() instanceof QueryCancelledException)
                return new JdbcResponse(SqlListenerResponse.STATUS_FAILED, "The query was cancelled while executing.");
            else
                return new JdbcResponse(SqlListenerResponse.STATUS_FAILED, e.toString());
        }
    }

    /**
     * {@link JdbcQueryCloseRequest} command handler.
     *
     * @param req Execute query request.
     * @return Response.
     */
    private JdbcResponse closeQuery(JdbcQueryCloseRequest req) {
        try {
            JdbcQueryCursor cur = qryCursors.remove(req.queryId());

            if (cur == null)
                return new JdbcResponse(SqlListenerResponse.STATUS_FAILED,
                    "Failed to find query cursor with ID: " + req.queryId());

            cur.close();

            return new JdbcResponse(null);
        }
        catch (Exception e) {
            qryCursors.remove(req.queryId());

            U.error(log, "Failed to close SQL query [reqId=" + req.requestId() + ", req=" + req.queryId() + ']', e);

            return new JdbcResponse(SqlListenerResponse.STATUS_FAILED, e.toString());
        }
    }

    /**
     * {@link JdbcQueryFetchRequest} command handler.
     *
     * @param req Execute query request.
     * @return Response.
     */
    private JdbcResponse fetchQuery(JdbcQueryFetchRequest req) {
        try {
            JdbcQueryCursor cur = qryCursors.get(req.queryId());

            if (cur == null)
                return new JdbcResponse(SqlListenerResponse.STATUS_FAILED,
                    "Failed to find query cursor with ID: " + req.queryId());

            if (req.pageSize() <= 0)
                return new JdbcResponse(SqlListenerResponse.STATUS_FAILED,
                    "Invalid fetch size : [fetchSize=" + req.pageSize() + ']');

            cur.pageSize(req.pageSize());

            JdbcQueryFetchResult res = new JdbcQueryFetchResult(cur.fetchRows(), !cur.hasNext());

            if (res.last() && (!cur.isQuery() || autoCloseCursors)) {
                qryCursors.remove(req.queryId());

                cur.close();
            }

            return new JdbcResponse(res);
        }
        catch (Exception e) {
            U.error(log, "Failed to fetch SQL query result [reqId=" + req.requestId() + ", req=" + req + ']', e);

            return new JdbcResponse(SqlListenerResponse.STATUS_FAILED, e.toString());
        }
    }

    /**
     * @param req Request.
     * @return Response.
     */
    private JdbcResponse getQueryMeta(JdbcQueryMetadataRequest req) {
        try {
            JdbcQueryCursor cur = qryCursors.get(req.queryId());

            if (cur == null)
                return new JdbcResponse(SqlListenerResponse.STATUS_FAILED,
                    "Failed to find query with ID: " + req.queryId());

            JdbcQueryMetadataResult res = new JdbcQueryMetadataResult(req.queryId(),
                cur.meta());

            return new JdbcResponse(res);
        }
        catch (Exception e) {
            U.error(log, "Failed to fetch SQL query result [reqId=" + req.requestId() + ", req=" + req + ']', e);

            return new JdbcResponse(SqlListenerResponse.STATUS_FAILED, e.toString());
        }
    }

    /**
     * @param req Request.
     * @return Response.
     */
    private SqlListenerResponse executeBatch(JdbcBatchExecuteRequest req) {
        String schemaName = req.schema();

        if (F.isEmpty(schemaName))
            schemaName = QueryUtils.DFLT_SCHEMA;

        int successQueries = 0;
        int updCnts[] = new int[req.queries().size()];

        try {
            String sql = null;

            for (JdbcQuery q : req.queries()) {
                if (q.sql() != null)
                    sql = q.sql();

                SqlFieldsQuery qry = new SqlFieldsQuery(sql);

                qry.setArgs(q.args());

                qry.setDistributedJoins(distributedJoins);
                qry.setEnforceJoinOrder(enforceJoinOrder);
                qry.setCollocated(collocated);
                qry.setReplicatedOnly(replicatedOnly);

                qry.setSchema(schemaName);

                QueryCursorImpl<List<?>> qryCur = (QueryCursorImpl<List<?>>)ctx.query()
                    .querySqlFieldsNoCache(qry, true);

                if (qryCur.isQuery())
                    throw new IgniteCheckedException("Query produced result set [qry=" + q.sql() + ", args=" +
                        Arrays.toString(q.args()) + ']');

                List<List<?>> items = qryCur.getAll();

                updCnts[successQueries++] = ((Long)items.get(0).get(0)).intValue();
            }

            return new JdbcResponse(new JdbcBatchExecuteResult(updCnts, SqlListenerResponse.STATUS_SUCCESS, null));
        }
        catch (Exception e) {
            U.error(log, "Failed to execute batch query [reqId=" + req.requestId() + ", req=" + req + ']', e);

            return new JdbcResponse(new JdbcBatchExecuteResult(Arrays.copyOf(updCnts, successQueries),
                SqlListenerResponse.STATUS_FAILED, e.toString()));
        }
    }

    /**
     * @param req Execute query request.
     * @return Response.
     */
    private JdbcResponse cancelQuery(JdbcQueryCancelRequest req) {
        try {
            JdbcRequestHandler handler = handlers.get(req.connectionId());

            handler.cancelCurrentQuery();

            return new JdbcResponse(null);
        }
        catch (Exception e) {
            U.error(log, "Failed to cancel query[reqId=" + req.requestId() + ", req=" + req + ']', e);

            return new JdbcResponse(SqlListenerResponse.STATUS_FAILED, e.toString());
        }
    }
}
