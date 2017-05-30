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

import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.binary.GridBinaryMarshaller;
import org.apache.ignite.internal.processors.cache.QueryCursorImpl;
import org.apache.ignite.internal.processors.odbc.odbc.OdbcColumnMeta;
import org.apache.ignite.internal.processors.odbc.odbc.OdbcQueryCloseRequest;
import org.apache.ignite.internal.processors.odbc.odbc.OdbcQueryExecuteRequest;
import org.apache.ignite.internal.processors.odbc.odbc.OdbcQueryFetchRequest;
import org.apache.ignite.internal.processors.odbc.SqlListenerRequest;
import org.apache.ignite.internal.processors.odbc.SqlListenerRequestHandler;
import org.apache.ignite.internal.processors.odbc.SqlListenerResponse;
import org.apache.ignite.internal.processors.odbc.odbc.escape.OdbcEscapeUtils;
import org.apache.ignite.internal.processors.query.GridQueryFieldMetadata;
import org.apache.ignite.internal.util.GridSpinBusyLock;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.processors.odbc.jdbc.JdbcRequest.QRY_EXEC;
import static org.apache.ignite.internal.processors.odbc.jdbc.JdbcRequest.QRY_FETCH;
import static org.apache.ignite.internal.processors.odbc.jdbc.JdbcRequest.QRY_CLOSE;
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

    /**
     * Constructor.
     *
     * @param ctx Context.
     * @param busyLock Shutdown latch.
     * @param maxCursors Maximum allowed cursors.
     * @param distributedJoins Distributed joins flag.
     * @param enforceJoinOrder Enforce join order flag.
     */
    public JdbcRequestHandler(GridKernalContext ctx, GridSpinBusyLock busyLock, int maxCursors,
        boolean distributedJoins, boolean enforceJoinOrder) {
        this.ctx = ctx;
        this.busyLock = busyLock;
        this.maxCursors = maxCursors;
        this.distributedJoins = distributedJoins;
        this.enforceJoinOrder = enforceJoinOrder;

        log = ctx.log(getClass());
    }

    /** {@inheritDoc} */
    @Override public SqlListenerResponse handle(SqlListenerRequest req0) {
        assert req0 != null;

        assert req0 instanceof JdbcRequest;

        JdbcRequest req = (JdbcRequest)req0;

        if (!busyLock.enterBusy())
            return new JdbcResponse(SqlListenerResponse.STATUS_FAILED,
                    "Failed to handle ODBC request because node is stopping: " + req);

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
            }

            return new JdbcResponse(SqlListenerResponse.STATUS_FAILED, "Unsupported JDBC request: [req="
                + req + ']');
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /** {@inheritDoc} */
    @Override public SqlListenerResponse handleException(Exception e) {
        return new JdbcResponse(SqlListenerResponse.STATUS_FAILED, e.toString());
    }

    /**
     * {@link OdbcQueryExecuteRequest} command handler.
     *
     * @param req Execute query request.
     * @return Response.
     */
    private JdbcResponse executeQuery(JdbcQueryExecuteRequest req) {
        int cursorCnt = qryCursors.size();

        if (maxCursors > 0 && cursorCnt >= maxCursors)
            return new JdbcResponse(SqlListenerResponse.STATUS_FAILED, "Too many opened cursors (either close other " +
                "opened cursors or increase the limit through OdbcConfiguration.setMaxOpenCursors()) " +
                "[maximum=" + maxCursors + ", current=" + cursorCnt + ']');

        long qryId = QRY_ID_GEN.getAndIncrement();

        try {
            String sql = OdbcEscapeUtils.parse(req.sqlQuery());

            if (log.isDebugEnabled())
                log.debug("ODBC query parsed [reqId=" + req.requestId() + ", original=" + req.sqlQuery() +
                    ", parsed=" + sql + ']');

            SqlFieldsQuery qry = new SqlFieldsQuery(sql);

            qry.setArgs(req.arguments());

            qry.setDistributedJoins(distributedJoins);
            qry.setEnforceJoinOrder(enforceJoinOrder);
            qry.setPageSize(req.fetchSize());

            IgniteCache<Object, Object> cache0 = ctx.grid().cache(req.cacheName());

            if (cache0 == null)
                return new JdbcResponse(SqlListenerResponse.STATUS_FAILED,
                    "Cache doesn't exist (did you configure it?): " + req.cacheName());

            IgniteCache<Object, Object> cache = cache0.withKeepBinary();

            if (cache == null)
                return new JdbcResponse(SqlListenerResponse.STATUS_FAILED,
                    "Can not get cache with keep binary: " + req.cacheName());

            JdbcQueryCursor cur = new JdbcQueryCursor(
                qryId, req.fetchSize(), req.maxRows(), (QueryCursorImpl)cache.query(qry));

            qryCursors.put(qryId, cur);

            JdbcQueryExecuteResult res = new JdbcQueryExecuteResult(
                qryId,  cur.fetchRows(), !cur.hasNext(), cur.isQuery());

            return new JdbcResponse(res);
        }
        catch (Exception e) {
            qryCursors.remove(qryId);

            U.error(log, "Failed to execute SQL query [reqId=" + req.requestId() + ", req=" + req + ']', e);

            return new JdbcResponse(SqlListenerResponse.STATUS_FAILED, e.toString());
        }
    }

    /**
     * {@link OdbcQueryCloseRequest} command handler.
     *
     * @param req Execute query request.
     * @return Response.
     */
    private JdbcResponse closeQuery(JdbcQueryCloseRequest req) {
        try {
            JdbcQueryCursor cur = qryCursors.get(req.queryId());

            if (cur == null)
                return new JdbcResponse(SqlListenerResponse.STATUS_FAILED,
                    "Failed to find query with ID: " + req.queryId());

            cur.close();

            qryCursors.remove(req.queryId());

            JdbcQueryCloseResult res = new JdbcQueryCloseResult(req.queryId());

            return new JdbcResponse(res);
        }
        catch (Exception e) {
            qryCursors.remove(req.queryId());

            U.error(log, "Failed to close SQL query [reqId=" + req.requestId() + ", req=" + req.queryId() + ']', e);

            return new JdbcResponse(SqlListenerResponse.STATUS_FAILED, e.toString());
        }
    }

    /**
     * {@link OdbcQueryFetchRequest} command handler.
     *
     * @param req Execute query request.
     * @return Response.
     */
    private JdbcResponse fetchQuery(JdbcQueryFetchRequest req) {
        try {
            JdbcQueryCursor cur = qryCursors.get(req.queryId());

            if (cur == null)
                return new JdbcResponse(SqlListenerResponse.STATUS_FAILED,
                    "Failed to find query with ID: " + req.queryId());

            JdbcQueryFetchResult res = new JdbcQueryFetchResult(req.queryId(), cur.fetchRows(), !cur.hasNext());

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
     * Convert {@link Types} to binary type constant (See {@link GridBinaryMarshaller} constants).
     *
     * @param sqlType SQL type.
     * @return Binary type.
     */
    private static byte sqlTypeToBinary(int sqlType) {
        switch (sqlType) {
            case Types.BIGINT:
                return GridBinaryMarshaller.LONG;

            case Types.BOOLEAN:
                return GridBinaryMarshaller.BOOLEAN;

            case Types.DATE:
                return GridBinaryMarshaller.DATE;

            case Types.DOUBLE:
                return GridBinaryMarshaller.DOUBLE;

            case Types.FLOAT:
            case Types.REAL:
                return GridBinaryMarshaller.FLOAT;

            case Types.NUMERIC:
            case Types.DECIMAL:
                return GridBinaryMarshaller.DECIMAL;

            case Types.INTEGER:
                return GridBinaryMarshaller.INT;

            case Types.SMALLINT:
                return GridBinaryMarshaller.SHORT;

            case Types.TIME:
                return GridBinaryMarshaller.TIME;

            case Types.TIMESTAMP:
                return GridBinaryMarshaller.TIMESTAMP;

            case Types.TINYINT:
                return GridBinaryMarshaller.BYTE;

            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGNVARCHAR:
                return GridBinaryMarshaller.STRING;

            case Types.NULL:
                return GridBinaryMarshaller.NULL;

            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
            default:
                return GridBinaryMarshaller.BYTE_ARR;
        }
    }

    /**
     * Convert metadata in collection from {@link GridQueryFieldMetadata} to
     * {@link OdbcColumnMeta}.
     *
     * @param meta Internal query field metadata.
     * @return Odbc query field metadata.
     */
    private static Collection<OdbcColumnMeta> convertMetadata(Collection<?> meta) {
        List<OdbcColumnMeta> res = new ArrayList<>();

        if (meta != null) {
            for (Object info : meta) {
                assert info instanceof GridQueryFieldMetadata;

                res.add(new OdbcColumnMeta((GridQueryFieldMetadata)info));
            }
        }

        return res;
    }

    /**
     * Checks whether string matches SQL pattern.
     *
     * @param str String.
     * @param ptrn Pattern.
     * @return Whether string matches pattern.
     */
    private static boolean matches(String str, String ptrn) {
        return str != null && (F.isEmpty(ptrn) ||
            str.toUpperCase().matches(ptrn.toUpperCase().replace("%", ".*").replace("_", ".")));
    }
}
