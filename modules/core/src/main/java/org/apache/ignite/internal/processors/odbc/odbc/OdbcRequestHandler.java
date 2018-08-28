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

import java.sql.BatchUpdateException;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.binary.BinaryWriterExImpl;
import org.apache.ignite.internal.binary.GridBinaryMarshaller;
import org.apache.ignite.internal.processors.authentication.AuthorizationContext;
import org.apache.ignite.internal.processors.cache.query.SqlFieldsQueryEx;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.odbc.ClientListenerProtocolVersion;
import org.apache.ignite.internal.processors.odbc.ClientListenerRequest;
import org.apache.ignite.internal.processors.odbc.ClientListenerRequestHandler;
import org.apache.ignite.internal.processors.odbc.ClientListenerResponse;
import org.apache.ignite.internal.processors.odbc.odbc.escape.OdbcEscapeUtils;
import org.apache.ignite.internal.processors.query.GridQueryProperty;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.NestedTxMode;
import org.apache.ignite.internal.util.GridSpinBusyLock;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.processors.odbc.odbc.OdbcRequest.META_COLS;
import static org.apache.ignite.internal.processors.odbc.odbc.OdbcRequest.META_PARAMS;
import static org.apache.ignite.internal.processors.odbc.odbc.OdbcRequest.META_TBLS;
import static org.apache.ignite.internal.processors.odbc.odbc.OdbcRequest.MORE_RESULTS;
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

    /** Worker. */
    private final OdbcRequestHandlerWorker worker;

    /** Maximum allowed cursors. */
    private final int maxCursors;

    /** Current queries cursors. */
    private final ConcurrentHashMap<Long, OdbcQueryResults> qryResults = new ConcurrentHashMap<>();

    /** Distributed joins flag. */
    private final boolean distributedJoins;

    /** Enforce join order flag. */
    private final boolean enforceJoinOrder;

    /** Replicated only flag. */
    private final boolean replicatedOnly;

    /** Nested transaction behaviour. */
    private final NestedTxMode nestedTxMode;

    /** Collocated flag. */
    private final boolean collocated;

    /** Lazy flag. */
    private final boolean lazy;

    /** Update on server flag. */
    private final boolean skipReducerOnUpdate;

    /** Authentication context */
    private final AuthorizationContext actx;

    /** Client version. */
    private ClientListenerProtocolVersion ver;

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
     * @param skipReducerOnUpdate Skip reducer on update flag.
     * @param nestedTxMode Nested transaction mode.
     * @param actx Authentication context.
     * @param ver Client protocol version.
     */
    public OdbcRequestHandler(GridKernalContext ctx, GridSpinBusyLock busyLock, int maxCursors,
        boolean distributedJoins, boolean enforceJoinOrder, boolean replicatedOnly, boolean collocated, boolean lazy,
        boolean skipReducerOnUpdate, AuthorizationContext actx, NestedTxMode nestedTxMode, ClientListenerProtocolVersion ver) {
        this.ctx = ctx;
        this.busyLock = busyLock;
        this.maxCursors = maxCursors;
        this.distributedJoins = distributedJoins;
        this.enforceJoinOrder = enforceJoinOrder;
        this.replicatedOnly = replicatedOnly;
        this.collocated = collocated;
        this.lazy = lazy;
        this.skipReducerOnUpdate = skipReducerOnUpdate;
        this.actx = actx;
        this.nestedTxMode = nestedTxMode;
        this.ver = ver;

        log = ctx.log(getClass());

        if (ctx.grid().configuration().isMvccEnabled())
            worker = new OdbcRequestHandlerWorker(ctx.igniteInstanceName(), log, this, ctx);
        else
            worker = null;
    }

    /** {@inheritDoc} */
    @Override public ClientListenerResponse handle(ClientListenerRequest req0) {
        assert req0 != null;

        assert req0 instanceof OdbcRequest;

        OdbcRequest req = (OdbcRequest)req0;

        if (worker == null)
            return doHandle(req);
        else {
            GridFutureAdapter<ClientListenerResponse> fut = worker.process(req);

            try {
                return fut.get();
            }
            catch (IgniteCheckedException e) {
                return exceptionToResult(e);
            }
        }
    }

    /**
     * Start worker, if it's present.
     */
    void start() {
        if (worker != null)
            worker.start();
    }

    /**
     * Handle ODBC request.
     * @param req ODBC request.
     * @return Response.
     */
    public ClientListenerResponse doHandle(OdbcRequest req) {
        if (!busyLock.enterBusy())
            return new OdbcResponse(IgniteQueryErrorCode.UNKNOWN,
                "Failed to handle ODBC request because node is stopping: " + req);

        if (actx != null)
            AuthorizationContext.context(actx);

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

                case MORE_RESULTS:
                    return moreResults((OdbcQueryMoreResultsRequest)req);
            }

            return new OdbcResponse(IgniteQueryErrorCode.UNKNOWN, "Unsupported ODBC request: " + req);
        }
        finally {
            AuthorizationContext.clear();

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
            if (worker != null) {
                worker.cancel();

                try {
                    worker.join();
                }
                catch (InterruptedException e) {
                    // No-op.
                }
            }

            try
            {
                for (OdbcQueryResults res : qryResults.values())
                    res.closeAll();
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
     * @param autoCommit Autocommit transaction.
     * @param timeout Query timeout.
     * @return Query instance.
     */
    private SqlFieldsQueryEx makeQuery(String schema, String sql, Object[] args, int timeout, boolean autoCommit) {
        SqlFieldsQueryEx qry = new SqlFieldsQueryEx(sql, null);

        qry.setArgs(args);

        qry.setDistributedJoins(distributedJoins);
        qry.setEnforceJoinOrder(enforceJoinOrder);
        qry.setReplicatedOnly(replicatedOnly);
        qry.setCollocated(collocated);
        qry.setLazy(lazy);
        qry.setSchema(schema);
        qry.setSkipReducerOnUpdate(skipReducerOnUpdate);
        qry.setNestedTxMode(nestedTxMode);
        qry.setAutoCommit(autoCommit);

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
        int cursorCnt = qryResults.size();

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

            SqlFieldsQuery qry = makeQuery(req.schema(), sql, req.arguments(), req.timeout(), req.autoCommit());

            List<FieldsQueryCursor<List<?>>> cursors = ctx.query().querySqlFields(qry, true, false);

            OdbcQueryResults results = new OdbcQueryResults(cursors, ver);

            Collection<OdbcColumnMeta> fieldsMeta;

            if (!results.hasUnfetchedRows()) {
                results.closeAll();

                fieldsMeta = new ArrayList<>();
            } else {
                qryResults.put(qryId, results);

                fieldsMeta = results.currentResultSet().fieldsMeta();

                for (OdbcColumnMeta meta : fieldsMeta) {
                    log.warning("Meta - " + meta.columnName + ", " + meta.precision + ", " + meta.scale);
                }
            }

            OdbcQueryExecuteResult res = new OdbcQueryExecuteResult(qryId, fieldsMeta, results.rowsAffected());

            return new OdbcResponse(res);
        }
        catch (Exception e) {
            qryResults.remove(qryId);

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
        try {
            String sql = OdbcEscapeUtils.parse(req.sqlQuery());

            if (log.isDebugEnabled())
                log.debug("ODBC query parsed [reqId=" + req.requestId() + ", original=" + req.sqlQuery() +
                        ", parsed=" + sql + ']');

            SqlFieldsQueryEx qry = makeQuery(req.schema(), sql, null, req.timeout(), req.autoCommit());

            Object[][] paramSet = req.arguments();

            if (paramSet.length <= 0)
                throw new IgniteException("Batch execute request with non-positive batch length. [len="
                        + paramSet.length + ']');

            // Getting meta and do the checks for the first execution.
            for (Object[] set  : paramSet)
                qry.addBatchedArgs(set);

            List<FieldsQueryCursor<List<?>>> qryCurs =
                ctx.query().querySqlFields(qry, true, true);

            List<Long> rowsAffected = new ArrayList<>(req.arguments().length);

            for (FieldsQueryCursor<List<?>> qryCur : qryCurs)
                rowsAffected.add(OdbcUtils.rowsAffected(qryCur));

            OdbcQueryExecuteBatchResult res = new OdbcQueryExecuteBatchResult(rowsAffected);

            return new OdbcResponse(res);
        }
        catch (Exception e) {
            U.error(log, "Failed to execute SQL query [reqId=" + req.requestId() + ", req=" + req + ']', e);

            return exceptionToBatchResult(e);
        }
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
            OdbcQueryResults results = qryResults.get(queryId);

            if (results == null)
                return new OdbcResponse(IgniteQueryErrorCode.UNKNOWN,
                    "Failed to find query with ID: " + queryId);

            CloseCursor(results, queryId);

            OdbcQueryCloseResult res = new OdbcQueryCloseResult(queryId);

            return new OdbcResponse(res);
        }
        catch (Exception e) {
            qryResults.remove(queryId);

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
            OdbcQueryResults results = qryResults.get(queryId);

            if (results == null)
                return new OdbcResponse(ClientListenerResponse.STATUS_FAILED,
                    "Failed to find query with ID: " + queryId);

            OdbcResultSet set = results.currentResultSet();

            List<Object> items = set.fetch(req.pageSize());

            boolean lastPage = !set.hasUnfetchedRows();

            // Automatically closing cursor if no more data is available.
            if (!results.hasUnfetchedRows())
                CloseCursor(results, queryId);

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

            for (String cacheName : ctx.cache().publicCacheNames()) {
                for (GridQueryTypeDescriptor table : ctx.query().types(cacheName)) {
                    if (!matches(table.schemaName(), schemaPattern) ||
                        !matches(table.tableName(), tablePattern))
                        continue;

                    for (Map.Entry<String, Class<?>> field : table.fields().entrySet()) {
                        if (!matches(field.getKey(), req.columnPattern()))
                            continue;

                        GridQueryProperty prop = table.property(field.getKey());

                        OdbcColumnMeta columnMeta = new OdbcColumnMeta(table.schemaName(), table.tableName(),
                            field.getKey(), field.getValue(), prop.precision(), prop.scale(), ver);

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

            for (String cacheName : ctx.cache().publicCacheNames()) {
                for (GridQueryTypeDescriptor table : ctx.query().types(cacheName)) {
                    if (!matches(table.schemaName(), schemaPattern) ||
                        !matches(table.tableName(), req.table()) ||
                        !matches("TABLE", req.tableType()))
                        continue;

                    OdbcTableMeta tableMeta = new OdbcTableMeta(null, table.schemaName(), table.tableName(), "TABLE");

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
     * {@link OdbcQueryMoreResultsRequest} command handler.
     *
     * @param req Execute query request.
     * @return Response.
     */
    private ClientListenerResponse moreResults(OdbcQueryMoreResultsRequest req) {
        try {
            long queryId = req.queryId();
            OdbcQueryResults results = qryResults.get(queryId);

            if (results == null)
                return new OdbcResponse(ClientListenerResponse.STATUS_FAILED,
                    "Failed to find query with ID: " + queryId);

            results.nextResultSet();

            OdbcResultSet set = results.currentResultSet();

            List<Object> items = set.fetch(req.pageSize());

            boolean lastPage = !set.hasUnfetchedRows();

            // Automatically closing cursor if no more data is available.
            if (!results.hasUnfetchedRows())
                CloseCursor(results, queryId);

            OdbcQueryMoreResultsResult res = new OdbcQueryMoreResultsResult(queryId, items, lastPage);

            return new OdbcResponse(res);
        }
        catch (Exception e) {
            U.error(log, "Failed to get more SQL query results [reqId=" +
                req.requestId() + ", req=" + req + ']', e);

            return exceptionToResult(e);
        }
    }

    /**
     * Close cursor.
     * @param results Query map element.
     * @param queryId Query ID.
     */
    private void CloseCursor(OdbcQueryResults results, long queryId) {
        assert(results != null);

        results.closeAll();

        qryResults.remove(queryId);
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
     * Checks whether string matches SQL pattern.
     *
     * @param str String.
     * @param ptrn Pattern.
     * @return Whether string matches pattern.
     */
    private static boolean matches(String str, String ptrn) {
        if (F.isEmpty(ptrn))
            return true;

        if (str == null)
            return false;

        String pattern = ptrn.toUpperCase().replace("%", ".*").replace("_", ".");

        if (pattern.length() >= 2 && pattern.matches("['\"].*['\"]"))
            pattern = pattern.substring(1, pattern.length() - 1);

        return str.toUpperCase().matches(pattern);
    }

    /**
     * Create {@link OdbcResponse} bearing appropriate Ignite specific result code if possible
     *     from given {@link Exception}.
     *
     * @param e Exception to convert.
     * @return resulting {@link OdbcResponse}.
     */
    private OdbcResponse exceptionToBatchResult(Exception e) {
        int code;
        String msg;
        List<Long> rowsAffected = new ArrayList<>();

        if (e instanceof IgniteSQLException) {
            BatchUpdateException batchCause = X.cause(e, BatchUpdateException.class);

            if (batchCause != null) {
                for (long cnt : batchCause.getLargeUpdateCounts())
                    rowsAffected.add(cnt);

                msg = batchCause.getMessage();

                code = batchCause.getErrorCode();
            }
            else {
                msg = OdbcUtils.tryRetrieveH2ErrorMessage(e);

                code = ((IgniteSQLException)e).statusCode();
            }
        }
        else {
            msg = e.getMessage();

            code = IgniteQueryErrorCode.UNKNOWN;
        }

        OdbcQueryExecuteBatchResult res = new OdbcQueryExecuteBatchResult(rowsAffected, -1, code, msg);

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
