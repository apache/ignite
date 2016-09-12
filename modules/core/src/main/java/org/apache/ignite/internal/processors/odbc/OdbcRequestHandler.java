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

import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.QueryCursorImpl;
import org.apache.ignite.internal.processors.odbc.escape.OdbcEscapeUtils;
import org.apache.ignite.internal.processors.query.GridQueryFieldMetadata;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.util.GridSpinBusyLock;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteProductVersion;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.ignite.internal.processors.odbc.OdbcRequest.*;

/**
 * SQL query handler.
 */
public class OdbcRequestHandler {
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
    private boolean distributedJoins = false;

    /** Enforce join order flag. */
    private boolean enforceJoinOrder = false;

    /**
     * Constructor.
     *
     * @param ctx Context.
     * @param busyLock Shutdown latch.
     * @param maxCursors Maximum allowed cursors.
     */
    public OdbcRequestHandler(GridKernalContext ctx, GridSpinBusyLock busyLock, int maxCursors) {
        this.ctx = ctx;
        this.busyLock = busyLock;
        this.maxCursors = maxCursors;

        log = ctx.log(OdbcRequestHandler.class);
    }

    /**
     * Handle request.
     *
     * @param reqId Request ID.
     * @param req Request.
     * @return Response.
     */
    public OdbcResponse handle(long reqId, OdbcRequest req) {
        assert req != null;

        if (!busyLock.enterBusy())
            return new OdbcResponse(OdbcResponse.STATUS_FAILED,
                    "Failed to handle ODBC request because node is stopping: " + req);

        try {
            switch (req.command()) {
                case HANDSHAKE:
                    return performHandshake(reqId, (OdbcHandshakeRequest)req);

                case EXECUTE_SQL_QUERY:
                    return executeQuery(reqId, (OdbcQueryExecuteRequest)req);

                case FETCH_SQL_QUERY:
                    return fetchQuery(reqId, (OdbcQueryFetchRequest)req);

                case CLOSE_SQL_QUERY:
                    return closeQuery(reqId, (OdbcQueryCloseRequest)req);

                case GET_COLUMNS_META:
                    return getColumnsMeta(reqId, (OdbcQueryGetColumnsMetaRequest)req);

                case GET_TABLES_META:
                    return getTablesMeta(reqId, (OdbcQueryGetTablesMetaRequest)req);
            }

            return new OdbcResponse(OdbcResponse.STATUS_FAILED, "Unsupported ODBC request: " + req);
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * {@link OdbcHandshakeRequest} command handler.
     *
     * @param reqId Request ID.
     * @param req Handshake request.
     * @return Response.
     */
    private OdbcResponse performHandshake(long reqId, OdbcHandshakeRequest req) {
        OdbcProtocolVersion version = req.version();

        if (version.isUnknown()) {
            IgniteProductVersion ver = ctx.grid().version();

            String verStr = Byte.toString(ver.major()) + '.' + ver.minor() + '.' + ver.maintenance();

            OdbcHandshakeResult res = new OdbcHandshakeResult(false, OdbcProtocolVersion.current().since(), verStr);

            return new OdbcResponse(res);
        }

        OdbcHandshakeResult res = new OdbcHandshakeResult(true, null, null);

        if (version.isDistributedJoinsSupported()) {
            distributedJoins = req.distributedJoins();
            enforceJoinOrder = req.enforceJoinOrder();
        }

        return new OdbcResponse(res);
    }

    /**
     * {@link OdbcQueryExecuteRequest} command handler.
     *
     * @param reqId Request ID.
     * @param req Execute query request.
     * @return Response.
     */
    private OdbcResponse executeQuery(long reqId, OdbcQueryExecuteRequest req) {
        int cursorCnt = qryCursors.size();

        if (maxCursors > 0 && cursorCnt >= maxCursors)
            return new OdbcResponse(OdbcResponse.STATUS_FAILED, "Too many opened cursors (either close other " +
                "opened cursors or increase the limit through OdbcConfiguration.setMaxOpenCursors()) " +
                "[maximum=" + maxCursors + ", current=" + cursorCnt + ']');

        long qryId = QRY_ID_GEN.getAndIncrement();

        try {
            String sql = OdbcEscapeUtils.parse(req.sqlQuery());

            if (log.isDebugEnabled())
                log.debug("ODBC query parsed [reqId=" + reqId + ", original=" + req.sqlQuery() +
                    ", parsed=" + sql + ']');

            SqlFieldsQuery qry = new SqlFieldsQuery(sql);

            qry.setArgs(req.arguments());

            qry.setDistributedJoins(distributedJoins);
            qry.setEnforceJoinOrder(enforceJoinOrder);

            IgniteCache<Object, Object> cache = ctx.grid().cache(req.cacheName());

            if (cache == null)
                return new OdbcResponse(OdbcResponse.STATUS_FAILED,
                    "Cache doesn't exist (did you configure it?): " + req.cacheName());

            QueryCursor qryCur = cache.query(qry);

            Iterator iter = qryCur.iterator();

            qryCursors.put(qryId, new IgniteBiTuple<>(qryCur, iter));

            List<?> fieldsMeta = ((QueryCursorImpl) qryCur).fieldsMeta();

            OdbcQueryExecuteResult res = new OdbcQueryExecuteResult(qryId, convertMetadata(fieldsMeta));

            return new OdbcResponse(res);
        }
        catch (Exception e) {
            qryCursors.remove(qryId);

            U.error(log, "Failed to execute SQL query [reqId=" + reqId + ", req=" + req + ']', e);

            return new OdbcResponse(OdbcResponse.STATUS_FAILED, e.getMessage());
        }
    }

    /**
     * {@link OdbcQueryCloseRequest} command handler.
     *
     * @param reqId Request ID.
     * @param req Execute query request.
     * @return Response.
     */
    private OdbcResponse closeQuery(long reqId, OdbcQueryCloseRequest req) {
        try {
            QueryCursor cur = qryCursors.get(req.queryId()).get1();

            if (cur == null)
                return new OdbcResponse(OdbcResponse.STATUS_FAILED, "Failed to find query with ID: " + req.queryId());

            cur.close();

            qryCursors.remove(req.queryId());

            OdbcQueryCloseResult res = new OdbcQueryCloseResult(req.queryId());

            return new OdbcResponse(res);
        }
        catch (Exception e) {
            qryCursors.remove(req.queryId());

            U.error(log, "Failed to close SQL query [reqId=" + reqId + ", req=" + req.queryId() + ']', e);

            return new OdbcResponse(OdbcResponse.STATUS_FAILED, e.getMessage());
        }
    }

    /**
     * {@link OdbcQueryFetchRequest} command handler.
     *
     * @param reqId Request ID.
     * @param req Execute query request.
     * @return Response.
     */
    private OdbcResponse fetchQuery(long reqId, OdbcQueryFetchRequest req) {
        try {
            Iterator cur = qryCursors.get(req.queryId()).get2();

            if (cur == null)
                return new OdbcResponse(OdbcResponse.STATUS_FAILED, "Failed to find query with ID: " + req.queryId());

            List<Object> items = new ArrayList<>();

            for (int i = 0; i < req.pageSize() && cur.hasNext(); ++i)
                items.add(cur.next());

            OdbcQueryFetchResult res = new OdbcQueryFetchResult(req.queryId(), items, !cur.hasNext());

            return new OdbcResponse(res);
        }
        catch (Exception e) {
            U.error(log, "Failed to fetch SQL query result [reqId=" + reqId + ", req=" + req + ']', e);

            return new OdbcResponse(OdbcResponse.STATUS_FAILED, e.getMessage());
        }
    }

    /**
     * {@link OdbcQueryGetColumnsMetaRequest} command handler.
     *
     * @param reqId Request ID.
     * @param req Get columns metadata request.
     * @return Response.
     */
    private OdbcResponse getColumnsMeta(long reqId, OdbcQueryGetColumnsMetaRequest req) {
        try {
            List<OdbcColumnMeta> meta = new ArrayList<>();

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

                    OdbcColumnMeta columnMeta = new OdbcColumnMeta(req.cacheName(), table.name(),
                        field.getKey(), field.getValue());

                    if (!meta.contains(columnMeta))
                        meta.add(columnMeta);
                }
            }

            OdbcQueryGetColumnsMetaResult res = new OdbcQueryGetColumnsMetaResult(meta);

            return new OdbcResponse(res);
        }
        catch (Exception e) {
            U.error(log, "Failed to get columns metadata [reqId=" + reqId + ", req=" + req + ']', e);

            return new OdbcResponse(OdbcResponse.STATUS_FAILED, e.getMessage());
        }
    }

    /**
     * {@link OdbcQueryGetTablesMetaRequest} command handler.
     *
     * @param reqId Request ID.
     * @param req Get tables metadata request.
     * @return Response.
     */
    private OdbcResponse getTablesMeta(long reqId, OdbcQueryGetTablesMetaRequest req) {
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

                    OdbcTableMeta tableMeta = new OdbcTableMeta(req.catalog(), cacheName, table.name(), "TABLE");

                    if (!meta.contains(tableMeta))
                        meta.add(tableMeta);
                }
            }

            OdbcQueryGetTablesMetaResult res = new OdbcQueryGetTablesMetaResult(meta);

            return new OdbcResponse(res);
        }
        catch (Exception e) {
            U.error(log, "Failed to get tables metadata [reqId=" + reqId + ", req=" + req + ']', e);

            return new OdbcResponse(OdbcResponse.STATUS_FAILED, e.getMessage());
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
