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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.binary.BinaryWriterExImpl;
import org.apache.ignite.internal.binary.GridBinaryMarshaller;
import org.apache.ignite.internal.processors.cache.QueryCursorImpl;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.odbc.ClientListenerRequest;
import org.apache.ignite.internal.processors.odbc.ClientListenerRequestHandler;
import org.apache.ignite.internal.processors.odbc.ClientListenerResponse;
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
import static org.apache.ignite.internal.processors.odbc.odbc.OdbcRequest.QRY_EXEC_BATCH;
import static org.apache.ignite.internal.processors.odbc.odbc.OdbcRequest.QRY_FETCH;

/**
 * SQL query handler.
 */
public class OdbcRequestHandler implements ClientListenerRequestHandler {
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

    /** Replicated only flag. */
    private final boolean replicatedOnly;

    /** Collocated flag. */
    private final boolean collocated;

    /** Lazy flag. */
    private final boolean lazy;

    /**
     * Constructor.
     * @param ctx Context.
     * @param busyLock Shutdown latch.
     * @param maxCursors Maximum allowed cursors.
     * @param distributedJoins Distributed joins flag.
     * @param enforceJoinOrder Enforce join order flag.
     * @param replicatedOnly Replicated only flag.
     * @param collocated Collocated flag.
     * @param lazy Lazy flag.
     */
    public OdbcRequestHandler(GridKernalContext ctx, GridSpinBusyLock busyLock, int maxCursors,
        boolean distributedJoins, boolean enforceJoinOrder, boolean replicatedOnly,
        boolean collocated, boolean lazy) {
        this.ctx = ctx;
        this.busyLock = busyLock;
        this.maxCursors = maxCursors;
        this.distributedJoins = distributedJoins;
        this.enforceJoinOrder = enforceJoinOrder;
        this.replicatedOnly = replicatedOnly;
        this.collocated = collocated;
        this.lazy = lazy;

        log = ctx.log(getClass());
    }

    /** {@inheritDoc} */
    @Override public ClientListenerResponse handle(ClientListenerRequest req0) {
        assert req0 != null;

        OdbcRequest req = (OdbcRequest)req0;

        if (!busyLock.enterBusy())
            return new OdbcResponse(IgniteQueryErrorCode.UNKNOWN,
                    "Failed to handle ODBC request because node is stopping: " + req);

        try {
            switch (req.command()) {
                case QRY_EXEC:
                    return executeQuery((OdbcQueryExecuteRequest)req);

                case QRY_EXEC_BATCH:
                    return executeBatchQuery((OdbcQueryExecuteBatchRequest)req);

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

            return new OdbcResponse(IgniteQueryErrorCode.UNKNOWN, "Unsupported ODBC request: " + req);
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /** {@inheritDoc} */
    @Override public ClientListenerResponse handleException(Exception e, ClientListenerRequest req) {
        return exceptionToResult(e);
    }

    /** {@inheritDoc} */
    @Override public void writeHandshake(BinaryWriterExImpl writer) {
        writer.writeBoolean(true);
    }

    /**
     * Called whenever client is disconnected due to correct connection close
     * or due to {@code IOException} during network operations.
     */
    public void onDisconnect() {
        if (busyLock.enterBusy())
        {
            try
            {
                for (IgniteBiTuple<QueryCursor, Iterator> tuple : qryCursors.values())
                    tuple.get1().close();
            }
            finally {
                busyLock.leaveBusy();
            }
        }
    }

    /**
     * Make query considering handler configuration.
     * @param schema Schema.
     * @param sql SQL request.
     * @param args Arguments.
     * @return Query instance.
     */
    private SqlFieldsQuery makeQuery(String schema, String sql, Object[] args, int timeout) {
        SqlFieldsQuery qry = new SqlFieldsQuery(sql);

        qry.setArgs(args);

        qry.setDistributedJoins(distributedJoins);
        qry.setEnforceJoinOrder(enforceJoinOrder);
        qry.setReplicatedOnly(replicatedOnly);
        qry.setCollocated(collocated);
        qry.setLazy(lazy);
        qry.setSchema(schema);

        qry.setTimeout(timeout, TimeUnit.SECONDS);

        return qry;
    }

    /**
     * {@link OdbcQueryExecuteRequest} command handler.
     *
     * @param req Execute query request.
     * @return Response.
     */
    private ClientListenerResponse executeQuery(OdbcQueryExecuteRequest req) {
        int cursorCnt = qryCursors.size();

        if (maxCursors > 0 && cursorCnt >= maxCursors)
            return new OdbcResponse(IgniteQueryErrorCode.UNKNOWN, "Too many open cursors (either close " +
                "other open cursors or increase the limit through " +
                "ClientConnectorConfiguration.maxOpenCursorsPerConnection) [maximum=" + maxCursors +
                ", current=" + cursorCnt + ']');

        long qryId = QRY_ID_GEN.getAndIncrement();

        try {
            String sql = OdbcEscapeUtils.parse(req.sqlQuery());

            if (log.isDebugEnabled())
                log.debug("ODBC query parsed [reqId=" + req.requestId() + ", original=" + req.sqlQuery() +
                    ", parsed=" + sql + ']');

            SqlFieldsQuery qry = makeQuery(req.schema(), sql, req.arguments(), req.timeout());

            QueryCursorImpl<List<?>> qryCur = (QueryCursorImpl<List<?>>)ctx.query().querySqlFieldsNoCache(qry, true);

            long rowsAffected = getRowsAffected(qryCur);

            if (!qryCur.isQuery())
                qryCur.close();
            else
                qryCursors.put(qryId, new IgniteBiTuple<QueryCursor, Iterator>(qryCur, null));

            List<?> fieldsMeta = ((QueryCursorImpl) qryCur).fieldsMeta();

            OdbcQueryExecuteResult res = new OdbcQueryExecuteResult(qryId, convertMetadata(fieldsMeta), rowsAffected);

            return new OdbcResponse(res);
        }
        catch (Exception e) {
            qryCursors.remove(qryId);

            U.error(log, "Failed to execute SQL query [reqId=" + req.requestId() + ", req=" + req + ']', e);

            return exceptionToResult(e);
        }
    }

    /**
     * {@link OdbcQueryExecuteBatchRequest} command handler.
     *
     * @param req Execute query request.
     * @return Response.
     */
    private ClientListenerResponse executeBatchQuery(OdbcQueryExecuteBatchRequest req) {
        long rowsAffected = 0;
        int currentSet = 0;

        try {
            String sql = OdbcEscapeUtils.parse(req.sqlQuery());

            if (log.isDebugEnabled())
                log.debug("ODBC query parsed [reqId=" + req.requestId() + ", original=" + req.sqlQuery() +
                        ", parsed=" + sql + ']');

            SqlFieldsQuery qry = makeQuery(req.schema(), sql, req.arguments(), req.timeout());

            Object[][] paramSet = req.arguments();

            if (paramSet.length <= 0)
                throw new IgniteException("Batch execute request with non-positive batch length. [len="
                        + paramSet.length + ']');

            // Getting meta and do the checks for the first execution.
            qry.setArgs(paramSet[0]);

            QueryCursorImpl<List<?>> qryCur = (QueryCursorImpl<List<?>>)ctx.query().querySqlFieldsNoCache(qry, true);

            if (qryCur.isQuery())
                throw new IgniteException("Batching of parameters only supported for DML statements. [query=" +
                        req.sqlQuery() + ']');

            rowsAffected += getRowsAffected(qryCur);

            for (currentSet = 1; currentSet < paramSet.length; ++currentSet)
                rowsAffected += executeQuery(qry, paramSet[currentSet]);

            OdbcQueryExecuteBatchResult res = new OdbcQueryExecuteBatchResult(rowsAffected);

            return new OdbcResponse(res);
        }
        catch (Exception e) {
            U.error(log, "Failed to execute SQL query [reqId=" + req.requestId() + ", req=" + req + ']', e);

            return exceptionToBatchResult(e, rowsAffected, currentSet);
        }
    }

    /**
     * Execute query.
     * @param qry Query
     * @param row Row
     * @return Affected rows.
     */
    private long executeQuery(SqlFieldsQuery qry, Object[] row) {
        qry.setArgs(row);

        QueryCursor<List<?>> cur = ctx.query().querySqlFieldsNoCache(qry, true);

        return getRowsAffected(cur);
    }

    /**
     * Get affected rows for DML statement.
     * @param qryCur Cursor.
     * @return Number of table rows affected.
     */
    private static long getRowsAffected(QueryCursor<List<?>> qryCur) {
        QueryCursorImpl<List<?>> qryCur0 = (QueryCursorImpl<List<?>>)qryCur;

        if (qryCur0.isQuery())
            return -1;

        Iterator<List<?>> iter = qryCur.iterator();

        if (iter.hasNext()) {
            List<?> res = iter.next();

            if (res.size() > 0) {
                Long affected = (Long) res.get(0);

                if (affected != null)
                    return affected;
            }
        }

        return 0;
    }

    /**
     * {@link OdbcQueryCloseRequest} command handler.
     *
     * @param req Execute query request.
     * @return Response.
     */
    private ClientListenerResponse closeQuery(OdbcQueryCloseRequest req) {
        long queryId = req.queryId();

        try {
            IgniteBiTuple<QueryCursor, Iterator> tuple = qryCursors.get(queryId);

            if (tuple == null)
                return new OdbcResponse(IgniteQueryErrorCode.UNKNOWN,
                    "Failed to find query with ID: " + queryId);

            CloseCursor(tuple, queryId);

            OdbcQueryCloseResult res = new OdbcQueryCloseResult(queryId);

            return new OdbcResponse(res);
        }
        catch (Exception e) {
            qryCursors.remove(queryId);

            U.error(log, "Failed to close SQL query [reqId=" + req.requestId() + ", req=" + queryId + ']', e);

            return exceptionToResult(e);
        }
    }

    /**
     * {@link OdbcQueryFetchRequest} command handler.
     *
     * @param req Execute query request.
     * @return Response.
     */
    private ClientListenerResponse fetchQuery(OdbcQueryFetchRequest req) {
        try {
            long queryId = req.queryId();
            IgniteBiTuple<QueryCursor, Iterator> tuple = qryCursors.get(queryId);

            if (tuple == null)
                return new OdbcResponse(ClientListenerResponse.STATUS_FAILED,
                    "Failed to find query with ID: " + queryId);

            Iterator iter = tuple.get2();

            if (iter == null) {
                QueryCursor cur = tuple.get1();

                assert(cur != null);

                iter = cur.iterator();

                tuple.put(cur, iter);
            }

            List<Object> items = new ArrayList<>();

            for (int i = 0; i < req.pageSize() && iter.hasNext(); ++i)
                items.add(iter.next());

            boolean lastPage = !iter.hasNext();

            // Automatically closing cursor if no more data is available.
            if (lastPage)
                CloseCursor(tuple, queryId);

            OdbcQueryFetchResult res = new OdbcQueryFetchResult(queryId, items, lastPage);

            return new OdbcResponse(res);
        }
        catch (Exception e) {
            U.error(log, "Failed to fetch SQL query result [reqId=" + req.requestId() + ", req=" + req + ']', e);

            return exceptionToResult(e);
        }
    }

    /**
     * {@link OdbcQueryGetColumnsMetaRequest} command handler.
     *
     * @param req Get columns metadata request.
     * @return Response.
     */
    private ClientListenerResponse getColumnsMeta(OdbcQueryGetColumnsMetaRequest req) {
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

            return exceptionToResult(e);
        }
    }

    /**
     * {@link OdbcQueryGetTablesMetaRequest} command handler.
     *
     * @param req Get tables metadata request.
     * @return Response.
     */
    private ClientListenerResponse getTablesMeta(OdbcQueryGetTablesMetaRequest req) {
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

            return exceptionToResult(e);
        }
    }

    /**
     * {@link OdbcQueryGetParamsMetaRequest} command handler.
     *
     * @param req Get params metadata request.
     * @return Response.
     */
    private ClientListenerResponse getParamsMeta(OdbcQueryGetParamsMetaRequest req) {
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

            return exceptionToResult(e);
        }
    }

    /**
     * Close cursor.
     * @param tuple Query map element.
     * @param queryId Query ID.
     */
    private void CloseCursor(IgniteBiTuple<QueryCursor, Iterator> tuple, long queryId) {
        QueryCursor cur = tuple.get1();

        assert(cur != null);

        cur.close();

        qryCursors.remove(queryId);
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

    /**
     * Create {@link OdbcResponse} bearing appropriate Ignite specific result code if possible
     *     from given {@link Exception}.
     *
     * @param e Exception to convert.
     * @return resulting {@link OdbcResponse}.
     */
    private OdbcResponse exceptionToBatchResult(Exception e, long rowsAffected, long currentSet) {
        OdbcQueryExecuteBatchResult res = new OdbcQueryExecuteBatchResult(rowsAffected, currentSet,
            OdbcUtils.tryRetrieveSqlErrorCode(e), OdbcUtils.tryRetrieveH2ErrorMessage(e));

        return new OdbcResponse(res);
    }

    /**
     * Create {@link OdbcResponse} bearing appropriate Ignite specific result code if possible
     *     from given {@link Exception}.
     *
     * @param e Exception to convert.
     * @return resulting {@link OdbcResponse}.
     */
    private OdbcResponse exceptionToResult(Exception e) {
        return new OdbcResponse(OdbcUtils.tryRetrieveSqlErrorCode(e), OdbcUtils.tryRetrieveH2ErrorMessage(e));
    }
}
