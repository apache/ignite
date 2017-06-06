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

package org.apache.ignite.internal.processors.odbc.odbc;

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
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.binary.GridBinaryMarshaller;
import org.apache.ignite.internal.processors.cache.QueryCursorImpl;
import org.apache.ignite.internal.processors.odbc.SqlListenerRequest;
import org.apache.ignite.internal.processors.odbc.SqlListenerRequestHandler;
import org.apache.ignite.internal.processors.odbc.SqlListenerResponse;
import org.apache.ignite.internal.processors.odbc.odbc.escape.OdbcEscapeUtils;
import org.apache.ignite.internal.processors.query.GridQueryFieldMetadata;
import org.apache.ignite.internal.processors.query.GridQueryIndexing;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.util.GridSpinBusyLock;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;

import static org.apache.ignite.internal.processors.odbc.odbc.OdbcRequest.META_COLS;
import static org.apache.ignite.internal.processors.odbc.odbc.OdbcRequest.META_PARAMS;
import static org.apache.ignite.internal.processors.odbc.odbc.OdbcRequest.META_TBLS;
import static org.apache.ignite.internal.processors.odbc.odbc.OdbcRequest.QRY_CLOSE;
import static org.apache.ignite.internal.processors.odbc.odbc.OdbcRequest.QRY_EXEC;
import static org.apache.ignite.internal.processors.odbc.odbc.OdbcRequest.QRY_FETCH;

/**
 * SQL query handler.
 */
public class OdbcRequestHandler implements SqlListenerRequestHandler {
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
    public OdbcRequestHandler(GridKernalContext ctx, GridSpinBusyLock busyLock, int maxCursors,
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

        OdbcRequest req = (OdbcRequest)req0;

        if (!busyLock.enterBusy())
            return new OdbcResponse(OdbcResponse.STATUS_FAILED,
                    "Failed to handle ODBC request because node is stopping: " + req);

        try {
            switch (req.command()) {
                case QRY_EXEC:
                    return executeQuery((OdbcQueryExecuteRequest)req);

                case QRY_FETCH:
                    return fetchQuery((OdbcQueryFetchRequest)req);

                case QRY_CLOSE:
                    return closeQuery((OdbcQueryCloseRequest)req);

                case META_COLS:
                    return getColumnsMeta((OdbcQueryGetColumnsMetaRequest)req);

                case META_TBLS:
                    return getTablesMeta((OdbcQueryGetTablesMetaRequest)req);

                case META_PARAMS:
                    return getParamsMeta((OdbcQueryGetParamsMetaRequest)req);
            }

            return new OdbcResponse(OdbcResponse.STATUS_FAILED, "Unsupported ODBC request: " + req);
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /** {@inheritDoc} */
    @Override public SqlListenerResponse handleException(Exception e) {
        return new OdbcResponse(SqlListenerResponse.STATUS_FAILED, e.toString());
    }

    /**
     * {@link OdbcQueryExecuteRequest} command handler.
     *
     * @param req Execute query request.
     * @return Response.
     */
    private SqlListenerResponse executeQuery(OdbcQueryExecuteRequest req) {
        int cursorCnt = qryCursors.size();

        if (maxCursors > 0 && cursorCnt >= maxCursors)
            return new OdbcResponse(SqlListenerResponse.STATUS_FAILED, "Too many opened cursors (either close other " +
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
            qry.setSchema(req.schema());

            QueryCursor qryCur = ctx.query().querySqlFieldsNoCache(qry, true);

            qryCursors.put(qryId, new IgniteBiTuple<QueryCursor, Iterator>(qryCur, null));

            List<?> fieldsMeta = ((QueryCursorImpl) qryCur).fieldsMeta();

            OdbcQueryExecuteResult res = new OdbcQueryExecuteResult(qryId, convertMetadata(fieldsMeta));

            return new OdbcResponse(res);
        }
        catch (Exception e) {
            qryCursors.remove(qryId);

            U.error(log, "Failed to execute SQL query [reqId=" + req.requestId() + ", req=" + req + ']', e);

            return new OdbcResponse(SqlListenerResponse.STATUS_FAILED, e.toString());
        }
    }

    /**
     * {@link OdbcQueryCloseRequest} command handler.
     *
     * @param req Execute query request.
     * @return Response.
     */
    private SqlListenerResponse closeQuery(OdbcQueryCloseRequest req) {
        try {
            IgniteBiTuple<QueryCursor, Iterator> tuple = qryCursors.get(req.queryId());

            if (tuple == null)
                return new OdbcResponse(SqlListenerResponse.STATUS_FAILED,
                    "Failed to find query with ID: " + req.queryId());

            QueryCursor cur = tuple.get1();

            assert(cur != null);

            cur.close();

            qryCursors.remove(req.queryId());

            OdbcQueryCloseResult res = new OdbcQueryCloseResult(req.queryId());

            return new OdbcResponse(res);
        }
        catch (Exception e) {
            qryCursors.remove(req.queryId());

            U.error(log, "Failed to close SQL query [reqId=" + req.requestId() + ", req=" + req.queryId() + ']', e);

            return new OdbcResponse(SqlListenerResponse.STATUS_FAILED, e.toString());
        }
    }

    /**
     * {@link OdbcQueryFetchRequest} command handler.
     *
     * @param req Execute query request.
     * @return Response.
     */
    private SqlListenerResponse fetchQuery(OdbcQueryFetchRequest req) {
        try {
            IgniteBiTuple<QueryCursor, Iterator> tuple = qryCursors.get(req.queryId());

            if (tuple == null)
                return new OdbcResponse(SqlListenerResponse.STATUS_FAILED,
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

            OdbcQueryFetchResult res = new OdbcQueryFetchResult(req.queryId(), items, !iter.hasNext());

            return new OdbcResponse(res);
        }
        catch (Exception e) {
            U.error(log, "Failed to fetch SQL query result [reqId=" + req.requestId() + ", req=" + req + ']', e);

            return new OdbcResponse(SqlListenerResponse.STATUS_FAILED, e.toString());
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
            List<OdbcColumnMeta> meta = new ArrayList<>();

            String schemaPattern;
            String tablePattern;

            if (req.tablePattern().contains(".")) {
                // Parsing two-part table name.
                String[] parts = req.tablePattern().split("\\.");

                schemaPattern = OdbcUtils.removeQuotationMarksIfNeeded(parts[0]);

                tablePattern = parts[1];
            }
            else {
                schemaPattern = OdbcUtils.removeQuotationMarksIfNeeded(req.schemaPattern());

                tablePattern = req.tablePattern();
            }

            GridQueryIndexing indexing = ctx.query().getIndexing();

            for (String cacheName : ctx.cache().cacheNames()) {
                String cacheSchema = indexing.schema(cacheName);

                if (!matches(cacheSchema, schemaPattern))
                    continue;

                Collection<GridQueryTypeDescriptor> tablesMeta = ctx.query().types(cacheName);

                for (GridQueryTypeDescriptor table : tablesMeta) {
                    if (!matches(table.name(), tablePattern))
                        continue;

                    for (Map.Entry<String, Class<?>> field : table.fields().entrySet()) {
                        if (!matches(field.getKey(), req.columnPattern()))
                            continue;

                    OdbcColumnMeta columnMeta = new OdbcColumnMeta(cacheSchema, table.name(),
                        field.getKey(), field.getValue());

                        if (!meta.contains(columnMeta))
                            meta.add(columnMeta);
                    }
                }
            }

            OdbcQueryGetColumnsMetaResult res = new OdbcQueryGetColumnsMetaResult(meta);

            return new OdbcResponse(res);
        }
        catch (Exception e) {
            U.error(log, "Failed to get columns metadata [reqId=" + req.requestId() + ", req=" + req + ']', e);

            return new OdbcResponse(SqlListenerResponse.STATUS_FAILED, e.toString());
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

            String schemaPattern = OdbcUtils.removeQuotationMarksIfNeeded(req.schema());

            GridQueryIndexing indexing = ctx.query().getIndexing();

            for (String cacheName : ctx.cache().cacheNames())
            {
                String cacheSchema = indexing.schema(cacheName);

                if (!matches(cacheSchema, schemaPattern))
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

            return new OdbcResponse(res);
        }
        catch (Exception e) {
            U.error(log, "Failed to get tables metadata [reqId=" + req.requestId() + ", req=" + req + ']', e);

            return new OdbcResponse(SqlListenerResponse.STATUS_FAILED, e.toString());
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
            PreparedStatement stmt = ctx.query().getIndexing().prepareNativeStatement(req.schema(), req.query());

            ParameterMetaData pmd = stmt.getParameterMetaData();

            byte[] typeIds = new byte[pmd.getParameterCount()];

            for (int i = 1; i <= pmd.getParameterCount(); ++i) {
                int sqlType = pmd.getParameterType(i);

                typeIds[i - 1] = sqlTypeToBinary(sqlType);
            }

            OdbcQueryGetParamsMetaResult res = new OdbcQueryGetParamsMetaResult(typeIds);

            return new OdbcResponse(res);
        }
        catch (Exception e) {
            U.error(log, "Failed to get params metadata [reqId=" + req.requestId() + ", req=" + req + ']', e);

            return new OdbcResponse(SqlListenerResponse.STATUS_FAILED, e.toString());
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
