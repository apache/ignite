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

package org.apache.ignite.internal.processors.odbc;

import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.binary.GridBinaryMarshaller;
import org.apache.ignite.internal.processors.cache.QueryCursorImpl;
import org.apache.ignite.internal.processors.odbc.odbc.escape.OdbcEscapeUtils;
import org.apache.ignite.internal.processors.query.GridQueryFieldMetadata;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.util.GridSpinBusyLock;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;

import static org.apache.ignite.internal.processors.odbc.SqlListenerRequest.META_COLS;
import static org.apache.ignite.internal.processors.odbc.SqlListenerRequest.META_PARAMS;
import static org.apache.ignite.internal.processors.odbc.SqlListenerRequest.META_TBLS;
import static org.apache.ignite.internal.processors.odbc.SqlListenerRequest.QRY_CLOSE;
import static org.apache.ignite.internal.processors.odbc.SqlListenerRequest.QRY_EXEC;
import static org.apache.ignite.internal.processors.odbc.SqlListenerRequest.QRY_FETCH;

/**
 * SQL query handler.
 */
public class SqlListenerRequestHandlerImpl implements SqlListenerRequestHandler {
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
    private final ConcurrentHashMap<Long, IgniteBiTuple<QueryCursor, Iterator>> qryCursors = new ConcurrentHashMap<>();

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
    public SqlListenerRequestHandlerImpl(GridKernalContext ctx, GridSpinBusyLock busyLock, int maxCursors,
        boolean distributedJoins, boolean enforceJoinOrder) {
        this.ctx = ctx;
        this.busyLock = busyLock;
        this.maxCursors = maxCursors;
        this.distributedJoins = distributedJoins;
        this.enforceJoinOrder = enforceJoinOrder;

        log = ctx.log(getClass());
    }

    /** {@inheritDoc} */
    @Override public SqlListenerResponse handle(SqlListenerRequest req) {
        assert req != null;

        if (!busyLock.enterBusy())
            return new SqlListenerResponse(SqlListenerResponse.STATUS_FAILED,
                    "Failed to handle ODBC request because node is stopping: " + req);

        try {
            switch (req.command()) {
                case QRY_EXEC:
                    return executeQuery((SqlListenerQueryExecuteRequest)req);

                case QRY_FETCH:
                    return fetchQuery((SqlListenerQueryFetchRequest)req);

                case QRY_CLOSE:
                    return closeQuery((SqlListenerQueryCloseRequest)req);

                case META_COLS:
                    return getColumnsMeta((OdbcQueryGetColumnsMetaRequest)req);

                case META_TBLS:
                    return getTablesMeta((OdbcQueryGetTablesMetaRequest)req);

                case META_PARAMS:
                    return getParamsMeta((OdbcQueryGetParamsMetaRequest)req);
            }

            return new SqlListenerResponse(SqlListenerResponse.STATUS_FAILED, "Unsupported ODBC request: " + req);
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * {@link SqlListenerQueryExecuteRequest} command handler.
     *
     * @param req Execute query request.
     * @return Response.
     */
    private SqlListenerResponse executeQuery(SqlListenerQueryExecuteRequest req) {
        int cursorCnt = qryCursors.size();

        if (maxCursors > 0 && cursorCnt >= maxCursors)
            return new SqlListenerResponse(SqlListenerResponse.STATUS_FAILED, "Too many opened cursors (either close other " +
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

            IgniteCache<Object, Object> cache0 = ctx.grid().cache(req.cacheName());

            if (cache0 == null)
                return new SqlListenerResponse(SqlListenerResponse.STATUS_FAILED,
                    "Cache doesn't exist (did you configure it?): " + req.cacheName());

            IgniteCache<Object, Object> cache = cache0.withKeepBinary();

            if (cache == null)
                return new SqlListenerResponse(SqlListenerResponse.STATUS_FAILED,
                    "Can not get cache with keep binary: " + req.cacheName());

            QueryCursor qryCur = cache.query(qry);

            qryCursors.put(qryId, new IgniteBiTuple<QueryCursor, Iterator>(qryCur, null));

            List<?> fieldsMeta = ((QueryCursorImpl) qryCur).fieldsMeta();

            SqlListenerQueryExecuteResult res = new SqlListenerQueryExecuteResult(qryId, convertMetadata(fieldsMeta));

            return new SqlListenerResponse(res);
        }
        catch (Exception e) {
            qryCursors.remove(qryId);

            U.error(log, "Failed to execute SQL query [reqId=" + req.requestId() + ", req=" + req + ']', e);

            return new SqlListenerResponse(SqlListenerResponse.STATUS_FAILED, e.toString());
        }
    }

    /**
     * {@link SqlListenerQueryCloseRequest} command handler.
     *
     * @param req Execute query request.
     * @return Response.
     */
    private SqlListenerResponse closeQuery(SqlListenerQueryCloseRequest req) {
        try {
            IgniteBiTuple<QueryCursor, Iterator> tuple = qryCursors.get(req.queryId());

            if (tuple == null)
                return new SqlListenerResponse(SqlListenerResponse.STATUS_FAILED,
                    "Failed to find query with ID: " + req.queryId());

            QueryCursor cur = tuple.get1();

            assert(cur != null);

            cur.close();

            qryCursors.remove(req.queryId());

            SqlListenerQueryCloseResult res = new SqlListenerQueryCloseResult(req.queryId());

            return new SqlListenerResponse(res);
        }
        catch (Exception e) {
            qryCursors.remove(req.queryId());

            U.error(log, "Failed to close SQL query [reqId=" + req.requestId() + ", req=" + req.queryId() + ']', e);

            return new SqlListenerResponse(SqlListenerResponse.STATUS_FAILED, e.toString());
        }
    }

    /**
     * {@link SqlListenerQueryFetchRequest} command handler.
     *
     * @param req Execute query request.
     * @return Response.
     */
    private SqlListenerResponse fetchQuery(SqlListenerQueryFetchRequest req) {
        try {
            IgniteBiTuple<QueryCursor, Iterator> tuple = qryCursors.get(req.queryId());

            if (tuple == null)
                return new SqlListenerResponse(SqlListenerResponse.STATUS_FAILED,
                    "Failed to find query with ID: " + req.queryId());

            Iterator iter = tuple.get2();

            if (iter == null) {
                QueryCursor cur = tuple.get1();

                iter = cur.iterator();

                tuple.put(cur, iter);
            }

            List<Object> items = new ArrayList<>();

            for (int i = 0; i < req.pageSize() && iter.hasNext(); ++i)
                items.add(iter.next());

            SqlListenerQueryFetchResult res = new SqlListenerQueryFetchResult(req.queryId(), items, !iter.hasNext());

            return new SqlListenerResponse(res);
        }
        catch (Exception e) {
            U.error(log, "Failed to fetch SQL query result [reqId=" + req.requestId() + ", req=" + req + ']', e);

            return new SqlListenerResponse(SqlListenerResponse.STATUS_FAILED, e.toString());
        }
    }

    /**
     * {@link OdbcQueryGetColumnsMetaRequest} command handler.
     *
     * @param req Get columns metadata request.
     * @return Response.
     */
    private SqlListenerResponse getColumnsMeta(OdbcQueryGetColumnsMetaRequest req) {
        try {
            List<SqlListenerColumnMeta> meta = new ArrayList<>();

            String cacheName;
            String tableName;

            if (req.tableName().contains(".")) {
                // Parsing two-part table name.
                String[] parts = req.tableName().split("\\.");

                cacheName = OdbcUtils.removeQuotationMarksIfNeeded(parts[0]);

                tableName = parts[1];
            }
            else {
                cacheName = OdbcUtils.removeQuotationMarksIfNeeded(req.cacheName());

                tableName = req.tableName();
            }

            Collection<GridQueryTypeDescriptor> tablesMeta = ctx.query().types(cacheName);

            for (GridQueryTypeDescriptor table : tablesMeta) {
                if (!matches(table.name(), tableName))
                    continue;

                for (Map.Entry<String, Class<?>> field : table.fields().entrySet()) {
                    if (!matches(field.getKey(), req.columnName()))
                        continue;

                    SqlListenerColumnMeta columnMeta = new SqlListenerColumnMeta(req.cacheName(), table.name(),
                        field.getKey(), field.getValue());

                    if (!meta.contains(columnMeta))
                        meta.add(columnMeta);
                }
            }

            OdbcQueryGetColumnsMetaResult res = new OdbcQueryGetColumnsMetaResult(meta);

            return new SqlListenerResponse(res);
        }
        catch (Exception e) {
            U.error(log, "Failed to get columns metadata [reqId=" + req.requestId() + ", req=" + req + ']', e);

            return new SqlListenerResponse(SqlListenerResponse.STATUS_FAILED, e.toString());
        }
    }

    /**
     * {@link OdbcQueryGetTablesMetaRequest} command handler.
     *
     * @param req Get tables metadata request.
     * @return Response.
     */
    private SqlListenerResponse getTablesMeta(OdbcQueryGetTablesMetaRequest req) {
        try {
            List<OdbcTableMeta> meta = new ArrayList<>();

            String realSchema = OdbcUtils.removeQuotationMarksIfNeeded(req.schema());

            for (String cacheName : ctx.cache().cacheNames())
            {
                if (!matches(cacheName, realSchema))
                    continue;

                Collection<GridQueryTypeDescriptor> tablesMeta = ctx.query().types(cacheName);

                for (GridQueryTypeDescriptor table : tablesMeta) {
                    if (!matches(table.name(), req.table()))
                        continue;

                    if (!matches("TABLE", req.tableType()))
                        continue;

                    OdbcTableMeta tableMeta = new OdbcTableMeta(null, cacheName, table.name(), "TABLE");

                    if (!meta.contains(tableMeta))
                        meta.add(tableMeta);
                }
            }

            OdbcQueryGetTablesMetaResult res = new OdbcQueryGetTablesMetaResult(meta);

            return new SqlListenerResponse(res);
        }
        catch (Exception e) {
            U.error(log, "Failed to get tables metadata [reqId=" + req.requestId() + ", req=" + req + ']', e);

            return new SqlListenerResponse(SqlListenerResponse.STATUS_FAILED, e.toString());
        }
    }

    /**
     * {@link OdbcQueryGetParamsMetaRequest} command handler.
     *
     * @param req Get params metadata request.
     * @return Response.
     */
    private SqlListenerResponse getParamsMeta(OdbcQueryGetParamsMetaRequest req) {
        try {
            PreparedStatement stmt = ctx.query().prepareNativeStatement(req.cacheName(), req.query());

            ParameterMetaData pmd = stmt.getParameterMetaData();

            byte[] typeIds = new byte[pmd.getParameterCount()];

            for (int i = 1; i <= pmd.getParameterCount(); ++i) {
                int sqlType = pmd.getParameterType(i);

                typeIds[i - 1] = sqlTypeToBinary(sqlType);
            }

            OdbcQueryGetParamsMetaResult res = new OdbcQueryGetParamsMetaResult(typeIds);

            return new SqlListenerResponse(res);
        }
        catch (Exception e) {
            U.error(log, "Failed to get params metadata [reqId=" + req.requestId() + ", req=" + req + ']', e);

            return new SqlListenerResponse(SqlListenerResponse.STATUS_FAILED, e.toString());
        }
    }

    /**
     * Convert {@link java.sql.Types} to binary type constant (See {@link GridBinaryMarshaller} constants).
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
     * {@link SqlListenerColumnMeta}.
     *
     * @param meta Internal query field metadata.
     * @return Odbc query field metadata.
     */
    private static Collection<SqlListenerColumnMeta> convertMetadata(Collection<?> meta) {
        List<SqlListenerColumnMeta> res = new ArrayList<>();

        if (meta != null) {
            for (Object info : meta) {
                assert info instanceof GridQueryFieldMetadata;

                res.add(new SqlListenerColumnMeta((GridQueryFieldMetadata)info));
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
