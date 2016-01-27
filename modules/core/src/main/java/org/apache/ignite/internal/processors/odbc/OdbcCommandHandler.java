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
import org.apache.ignite.lang.IgniteBiTuple;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.ignite.internal.processors.odbc.OdbcRequest.*;

/**
 * SQL query handler.
 */
public class OdbcCommandHandler {
    /** Kernel context. */
    protected final GridKernalContext ctx;

    /** Logger. */
    protected final IgniteLogger log;

    /** Query ID sequence. */
    private static final AtomicLong qryIdGen = new AtomicLong();

    /** Current queries cursors. */
    private final ConcurrentHashMap<Long, IgniteBiTuple<QueryCursor, Iterator>> qryCurs = new ConcurrentHashMap<>();

    /**
     * @param ctx Context.
     */
    public OdbcCommandHandler(GridKernalContext ctx) {
        this.ctx = ctx;

        log = ctx.log(getClass());
    }

    /**
     * @param req Request.
     * @return Response.
     */
    public OdbcResponse handle(OdbcRequest req) {
        assert req != null;

        switch (req.command()) {
            case EXECUTE_SQL_QUERY: {
                return executeQuery((OdbcQueryExecuteRequest)req, qryCurs);
            }

            case FETCH_SQL_QUERY: {
                return fetchQuery((OdbcQueryFetchRequest)req, qryCurs);
            }

            case CLOSE_SQL_QUERY: {
                return closeQuery((OdbcQueryCloseRequest)req, qryCurs);
            }

            case GET_COLUMNS_META: {
                return getColumnsMeta((OdbcQueryGetColumnsMetaRequest) req);
            }

            case GET_TABLES_META: {
                return getTablesMeta((OdbcQueryGetTablesMetaRequest) req);
            }
        }

        return null;
    }

    /**
     * @param cur Current cursor.
     * @param req Sql fetch request.
     * @param qryId Query id.
     * @return Query result with items.
     */
    private static OdbcQueryFetchResult createQueryResult(Iterator cur, OdbcQueryFetchRequest req, Long qryId) {

        List<Object> items = new ArrayList<>();

        for (int i = 0; i < req.pageSize() && cur.hasNext(); ++i)
            items.add(cur.next());

        return new OdbcQueryFetchResult(qryId, items, !cur.hasNext());
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
    private boolean matches(String str, String ptrn) {
        return str != null && (ptrn == null || ptrn.isEmpty() ||
                str.toUpperCase().matches(ptrn.toUpperCase().replace("%", ".*").replace("_", ".")));
    }

    /**
     * Remove quotation marks at the beginning and end of the string if present.
     *
     * @param str Input string.
     * @return String without leading and trailing quotation marks.
     */
    private String RemoveQuotationMarksIfNeeded(String str) {
        if (str.startsWith("\"") && str.endsWith("\""))
            return str.substring(1, str.length() - 1);

        return str;
    }

    /**
     * {@link OdbcQueryExecuteRequest} command handler.
     *
     * @param req Execute query request.
     * @param qryCurs Queries cursors.
     * @return Response.
     */
    private OdbcResponse executeQuery(OdbcQueryExecuteRequest req,
                                      ConcurrentHashMap<Long, IgniteBiTuple<QueryCursor, Iterator>> qryCurs) {
        long qryId = qryIdGen.getAndIncrement();

        try {
            SqlFieldsQuery qry = new SqlFieldsQuery(req.sqlQuery());

            qry.setArgs(req.arguments());

            IgniteCache<Object, Object> cache = ctx.grid().cache(req.cacheName());

            if (cache == null)
                return new OdbcResponse(OdbcResponse.STATUS_FAILED,
                        "Failed to find cache with name: " + req.cacheName());

            QueryCursor qryCur = cache.query(qry);

            Iterator cur = qryCur.iterator();

            qryCurs.put(qryId, new IgniteBiTuple<>(qryCur, cur));

            List<?> fieldsMeta = ((QueryCursorImpl) qryCur).fieldsMeta();

            log.debug("Field meta: " + fieldsMeta);

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
     * @param qryCurs Queries cursors.
     * @return Response.
     */
    private OdbcResponse closeQuery(OdbcQueryCloseRequest req,
                                    ConcurrentHashMap<Long, IgniteBiTuple<QueryCursor, Iterator>> qryCurs) {
        try {
            QueryCursor cur = qryCurs.get(req.queryId()).get1();

            if (cur == null)
                return new OdbcResponse(OdbcResponse.STATUS_FAILED,
                        "Failed to find query with ID: " + req.queryId());

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
     * @param qryCurs Queries cursors.
     * @return Response.
     */
    private OdbcResponse fetchQuery(OdbcQueryFetchRequest req,
                                    ConcurrentHashMap<Long, IgniteBiTuple<QueryCursor, Iterator>> qryCurs) {
        try {
            Iterator cur = qryCurs.get(req.queryId()).get2();

            if (cur == null)
                return new OdbcResponse(OdbcResponse.STATUS_FAILED,
                        "Failed to find query with ID: " + req.queryId());

            OdbcQueryFetchResult res = createQueryResult(cur, req, req.queryId());

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

                cacheName = RemoveQuotationMarksIfNeeded(parts[0]);

                tableName = parts[1];
            }
            else {
                cacheName = RemoveQuotationMarksIfNeeded(req.cacheName());

                tableName = req.tableName();
            }

            Collection<GridQueryTypeDescriptor> tablesMeta = ctx.query().types(cacheName);

            for (GridQueryTypeDescriptor table : tablesMeta) {
                if (!matches(table.name(), tableName))
                    continue;

                for (Map.Entry<String, Class<?>> field : table.fields().entrySet()) {
                    if (!matches(field.getKey(), req.columnName()))
                        continue;

                    OdbcColumnMeta columnMeta = new OdbcColumnMeta(req.cacheName(),
                            table.name(), field.getKey(), field.getValue());

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

            String realSchema = RemoveQuotationMarksIfNeeded(req.schema());

            Collection<GridQueryTypeDescriptor> tablesMeta = ctx.query().types(realSchema);

            for (GridQueryTypeDescriptor table : tablesMeta) {
                if (!matches(table.name(), req.table()))
                    continue;

                if (!matches("TABLE", req.tableType()))
                    continue;

                OdbcTableMeta tableMeta = new OdbcTableMeta(req.catalog(), req.schema(),
                        table.name(), "TABLE");

                if (!meta.contains(tableMeta))
                    meta.add(tableMeta);
            }

            OdbcQueryGetTablesMetaResult res = new OdbcQueryGetTablesMetaResult(meta);

            return new OdbcResponse(res);
        }
        catch (Exception e) {
            return new OdbcResponse(OdbcResponse.STATUS_FAILED, e.getMessage());
        }
    }
}
