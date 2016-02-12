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
import org.apache.ignite.internal.processors.query.GridQueryFieldMetadata;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.util.GridSpinBusyLock;
import org.apache.ignite.internal.util.nio.GridNioServerListenerAdapter;
import org.apache.ignite.internal.util.nio.GridNioSession;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.ignite.internal.processors.odbc.OdbcRequest.CLOSE_SQL_QUERY;
import static org.apache.ignite.internal.processors.odbc.OdbcRequest.EXECUTE_SQL_QUERY;
import static org.apache.ignite.internal.processors.odbc.OdbcRequest.FETCH_SQL_QUERY;
import static org.apache.ignite.internal.processors.odbc.OdbcRequest.GET_COLUMNS_META;
import static org.apache.ignite.internal.processors.odbc.OdbcRequest.GET_TABLES_META;

/**
 * SQL query handler.
 */
public class OdbcNioListener extends GridNioServerListenerAdapter<OdbcRequest> {
    /** Query ID sequence. */
    private static final AtomicLong QRY_ID_GEN = new AtomicLong();

    /** Request ID generator. */
    private static final AtomicLong REQ_ID_GEN = new AtomicLong();

    /** Kernel context. */
    private final GridKernalContext ctx;

    /** Busy lock. */
    private final GridSpinBusyLock busyLock;

    /** Logger. */
    private final IgniteLogger log;

    /** Current queries cursors. */
    private final ConcurrentHashMap<Long, IgniteBiTuple<QueryCursor, Iterator>> qryCurs = new ConcurrentHashMap<>();

    /**
     * Constructor.
     *
     * @param ctx Context.
     */
    public OdbcNioListener(final GridKernalContext ctx, final GridSpinBusyLock busyLock) {
        this.ctx = ctx;
        this.busyLock = busyLock;

        this.log = ctx.log(getClass());
    }

    /** {@inheritDoc} */
    @Override public void onConnected(GridNioSession ses) {
        if (log.isDebugEnabled())
            log.debug("ODBC client connected: " + ses.remoteAddress());
    }

    /** {@inheritDoc} */
    @Override public void onDisconnected(GridNioSession ses, @Nullable Exception e) {
        if (log.isDebugEnabled()) {
            if (e == null)
                log.debug("ODBC client disconnected: " + ses.remoteAddress());
            else
                log.debug("ODBC client disconnected due to an error [addr=" + ses.remoteAddress() + ", err=" + e + ']');
        }
    }

    /** {@inheritDoc} */
    @Override public void onMessage(GridNioSession ses, OdbcRequest req) {
        assert req != null;

        long reqId = REQ_ID_GEN.incrementAndGet();
        long startTime = 0;

        if (log.isDebugEnabled()) {
            startTime = System.nanoTime();

            log.debug("ODBC request received [id=" + reqId + ", addr=" + ses.remoteAddress() + ", req=" + req + ']');
        }

        OdbcResponse res = handle(req);

        if (log.isDebugEnabled()) {
            long dur = (System.nanoTime() - startTime) / 1000;

            log.debug("ODBC request processed [id=" + reqId + ", dur(mcs)=" + dur  + ", res=" + res.status() + ']');
        }

        ses.send(res);
    }

    /**
     * Handle request.
     *
     * @param req Request.
     * @return Response.
     */
    public OdbcResponse handle(OdbcRequest req) {
        assert req != null;

        if (!busyLock.enterBusy())
            return new OdbcResponse(OdbcResponse.STATUS_FAILED,
                "Failed to handle ODBC request because node is stopping: " + req);

        try {
            switch (req.command()) {
                case EXECUTE_SQL_QUERY:
                    return executeQuery((OdbcQueryExecuteRequest)req);

                case FETCH_SQL_QUERY:
                    return fetchQuery((OdbcQueryFetchRequest)req);

                case CLOSE_SQL_QUERY:
                    return closeQuery((OdbcQueryCloseRequest)req);

                case GET_COLUMNS_META:
                    return getColumnsMeta((OdbcQueryGetColumnsMetaRequest) req);

                case GET_TABLES_META:
                    return getTablesMeta((OdbcQueryGetTablesMetaRequest) req);
            }

            return new OdbcResponse(OdbcResponse.STATUS_FAILED, "Unsupported ODBC request: " + req);
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * {@link OdbcQueryExecuteRequest} command handler.
     *
     * @param req Execute query request.
     * @return Response.
     */
    private OdbcResponse executeQuery(OdbcQueryExecuteRequest req) {
        long qryId = QRY_ID_GEN.getAndIncrement();

        try {
            SqlFieldsQuery qry = new SqlFieldsQuery(req.sqlQuery());

            qry.setArgs(req.arguments());

            IgniteCache<Object, Object> cache = ctx.grid().cache(req.cacheName());

            if (cache == null)
                return new OdbcResponse(OdbcResponse.STATUS_FAILED,
                    "Cache doesn't exist (did you configure it?): " + req.cacheName());

            QueryCursor qryCur = cache.query(qry);

            Iterator iter = qryCur.iterator();

            qryCurs.put(qryId, new IgniteBiTuple<>(qryCur, iter));

            List<?> fieldsMeta = ((QueryCursorImpl) qryCur).fieldsMeta();

            OdbcQueryExecuteResult res = new OdbcQueryExecuteResult(qryId, convertMetadata(fieldsMeta));

            return new OdbcResponse(res);
        }
        catch (Exception e) {
            qryCurs.remove(qryId);

            return new OdbcResponse(OdbcResponse.STATUS_FAILED, e.getMessage());
        }
    }

    /**
     * {@link OdbcQueryCloseRequest} command handler.
     *
     * @param req Execute query request.
     * @return Response.
     */
    private OdbcResponse closeQuery(OdbcQueryCloseRequest req) {
        try {
            QueryCursor cur = qryCurs.get(req.queryId()).get1();

            if (cur == null)
                return new OdbcResponse(OdbcResponse.STATUS_FAILED, "Failed to find query with ID: " + req.queryId());

            cur.close();

            qryCurs.remove(req.queryId());

            OdbcQueryCloseResult res = new OdbcQueryCloseResult(req.queryId());

            return new OdbcResponse(res);
        }
        catch (Exception e) {
            qryCurs.remove(req.queryId());

            return new OdbcResponse(OdbcResponse.STATUS_FAILED, e.getMessage());
        }
    }

    /**
     * {@link OdbcQueryFetchRequest} command handler.
     *
     * @param req Execute query request.
     * @return Response.
     */
    private OdbcResponse fetchQuery(OdbcQueryFetchRequest req) {
        try {
            Iterator cur = qryCurs.get(req.queryId()).get2();

            if (cur == null)
                return new OdbcResponse(OdbcResponse.STATUS_FAILED, "Failed to find query with ID: " + req.queryId());

            List<Object> items = new ArrayList<>();

            for (int i = 0; i < req.pageSize() && cur.hasNext(); ++i)
                items.add(cur.next());

            OdbcQueryFetchResult res = new OdbcQueryFetchResult(req.queryId(), items, !cur.hasNext());

            return new OdbcResponse(res);
        }
        catch (Exception e) {
            qryCurs.remove(req.queryId());

            return new OdbcResponse(OdbcResponse.STATUS_FAILED, e.getMessage());
        }
    }

    /**
     * {@link OdbcQueryGetColumnsMetaRequest} command handler.
     *
     * @param req Get columns metadata request.
     * @return Response.
     */
    private OdbcResponse getColumnsMeta(OdbcQueryGetColumnsMetaRequest req) {
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
            return new OdbcResponse(OdbcResponse.STATUS_FAILED, e.getMessage());
        }
    }

    /**
     * {@link OdbcQueryGetTablesMetaRequest} command handler.
     *
     * @param req Get tables metadata request.
     * @return Response.
     */
    private OdbcResponse getTablesMeta(OdbcQueryGetTablesMetaRequest req) {
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
