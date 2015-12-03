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
package org.apache.ignite.internal.processors.odbc.handlers;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.QueryCursorImpl;
import org.apache.ignite.internal.processors.odbc.GridOdbcColumnMeta;
import org.apache.ignite.internal.processors.odbc.request.*;
import org.apache.ignite.internal.processors.odbc.response.*;
import org.apache.ignite.internal.processors.query.GridQueryFieldMetadata;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.rest.handlers.query.CacheQueryFieldsMetaResult;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.ignite.internal.processors.odbc.request.GridOdbcRequest.EXECUTE_SQL_QUERY;
import static org.apache.ignite.internal.processors.odbc.request.GridOdbcRequest.FETCH_SQL_QUERY;
import static org.apache.ignite.internal.processors.odbc.request.GridOdbcRequest.CLOSE_SQL_QUERY;
import static org.apache.ignite.internal.processors.odbc.request.GridOdbcRequest.GET_COLUMNS_META;

/**
 * SQL query handler.
 */
public class GridOdbcQueryCommandHandler extends GridOdbcCommandHandlerAdapter {
    /** Supported commands. */
    private static final Collection<Integer> SUPPORTED_COMMANDS =
            U.sealList(EXECUTE_SQL_QUERY, FETCH_SQL_QUERY, CLOSE_SQL_QUERY, GET_COLUMNS_META);

    /** Query ID sequence. */
    private static final AtomicLong qryIdGen = new AtomicLong();

    /** Current queries cursors. */
    private final ConcurrentHashMap<Long, IgniteBiTuple<QueryCursor, Iterator>> qryCurs = new ConcurrentHashMap<>();

    /**
     * @param ctx Context.
     */
    public GridOdbcQueryCommandHandler(GridKernalContext ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Override public Collection<Integer> supportedCommands() {
        return SUPPORTED_COMMANDS;
    }

    /** {@inheritDoc} */
    @Override public GridOdbcResponse handle(GridOdbcRequest req) {
        assert req != null;

        assert SUPPORTED_COMMANDS.contains(req.command());

        switch (req.command()) {
            case EXECUTE_SQL_QUERY: {
                return ExecuteQuery((QueryExecuteRequest)req, qryCurs);
            }

            case FETCH_SQL_QUERY: {
                return FetchQuery((QueryFetchRequest)req, qryCurs);
            }

            case CLOSE_SQL_QUERY: {
                return CloseQuery((QueryCloseRequest)req, qryCurs);
            }

            case GET_COLUMNS_META: {
                return  GetColumnsMeta((QueryGetColumnsMetaRequest) req);
            }
        }

        throw null;
    }

    /**
     * @param qryCurs Query cursors.
     * @param cur Current cursor.
     * @param req Sql fetch request.
     * @param qryId Query id.
     * @return Query result with items.
     */
    private static QueryFetchResult createQueryResult(
            ConcurrentHashMap<Long, IgniteBiTuple<QueryCursor, Iterator>> qryCurs,
            Iterator cur, QueryFetchRequest req, Long qryId) {
        QueryFetchResult res = new QueryFetchResult(qryId);

        List<Object> items = new ArrayList<>();

        for (int i = 0; i < req.pageSize() && cur.hasNext(); ++i)
            items.add(cur.next());

        res.setItems(items);

        res.setLast(!cur.hasNext());

        return res;
    }

    /**
     * @param meta Internal query field metadata.
     * @return Rest query field metadata.
     */
    private static Collection<GridOdbcColumnMeta> convertMetadata(Collection<GridQueryFieldMetadata> meta) {
        List<GridOdbcColumnMeta> res = new ArrayList<>();

        if (meta != null) {
            for (GridQueryFieldMetadata info : meta)
                res.add(new GridOdbcColumnMeta(info));
        }

        return res;
    }

    /**
     * @param req Execute query request.
     * @param qryCurs Queries cursors.
     * @return Response.
     */
    private GridOdbcResponse ExecuteQuery(QueryExecuteRequest req,
                                          ConcurrentHashMap<Long, IgniteBiTuple<QueryCursor, Iterator>> qryCurs) {
        long qryId = qryIdGen.getAndIncrement();

        try {
            SqlFieldsQuery qry = new SqlFieldsQuery(req.sqlQuery());

            qry.setArgs(req.arguments());

            IgniteCache<Object, Object> cache = ctx.grid().cache(req.cacheName());

            if (cache == null)
                return new GridOdbcResponse(GridOdbcResponse.STATUS_FAILED,
                        "Failed to find cache with name: " + req.cacheName());

            QueryCursor qryCur = cache.query(qry);

            Iterator cur = qryCur.iterator();

            qryCurs.put(qryId, new IgniteBiTuple<>(qryCur, cur));

            List<GridQueryFieldMetadata> fieldsMeta = ((QueryCursorImpl) qryCur).fieldsMeta();

            System.out.println("Field meta: " + fieldsMeta);

            QueryExecuteResult res = new QueryExecuteResult(qryId, convertMetadata(fieldsMeta));

            return new GridOdbcResponse(res);
        }
        catch (Exception e) {
            qryCurs.remove(qryId);

            return new GridOdbcResponse(GridOdbcResponse.STATUS_FAILED, e.getMessage());
        }
    }

    /**
     * @param req Execute query request.
     * @param qryCurs Queries cursors.
     * @return Response.
     */
    private GridOdbcResponse CloseQuery(QueryCloseRequest req,
                                        ConcurrentHashMap<Long, IgniteBiTuple<QueryCursor, Iterator>> qryCurs) {
        try {
            QueryCursor cur = qryCurs.get(req.queryId()).get1();

            if (cur == null)
                return new GridOdbcResponse(GridOdbcResponse.STATUS_FAILED,
                        "Failed to find query with ID: " + req.queryId());

            cur.close();

            qryCurs.remove(req.queryId());

            QueryCloseResult res = new QueryCloseResult(req.queryId());

            return new GridOdbcResponse(res);
        }
        catch (Exception e) {
            qryCurs.remove(req.queryId());

            return new GridOdbcResponse(GridOdbcResponse.STATUS_FAILED, e.getMessage());
        }
    }

    /**
     * @param req Execute query request.
     * @param qryCurs Queries cursors.
     * @return Response.
     */
    private GridOdbcResponse FetchQuery(QueryFetchRequest req,
                                        ConcurrentHashMap<Long, IgniteBiTuple<QueryCursor, Iterator>> qryCurs) {
        try {
            Iterator cur = qryCurs.get(req.queryId()).get2();

            if (cur == null)
                return new GridOdbcResponse(GridOdbcResponse.STATUS_FAILED,
                        "Failed to find query with ID: " + req.queryId());

            QueryFetchResult res = createQueryResult(qryCurs, cur, req, req.queryId());

            return new GridOdbcResponse(res);
        }
        catch (Exception e) {
            qryCurs.remove(req.queryId());

            return new GridOdbcResponse(GridOdbcResponse.STATUS_FAILED, e.getMessage());
        }
    }

    /**
     * @param req Get columns metadata request.
     * @return Response.
     */
    private GridOdbcResponse GetColumnsMeta(QueryGetColumnsMetaRequest req) {
        try {
            List<GridOdbcColumnMeta> meta = new ArrayList<>();

            Collection<GridQueryTypeDescriptor> tablesMeta = ctx.query().types(req.cacheName());

            for (GridQueryTypeDescriptor table : tablesMeta) {
                if (!req.tableName().isEmpty() && !table.name().equals(req.tableName()))
                    continue;

                for (Map.Entry<String, Class<?>> field : table.fields().entrySet()) {
                    if (!req.columnName().isEmpty() && !field.getKey().equals(req.columnName()))
                        continue;

                    GridOdbcColumnMeta columnMeta = new GridOdbcColumnMeta(req.cacheName(),
                            table.name(), field.getKey(), field.getValue());

                    meta.add(columnMeta);
                }
            }
            QueryGetColumnsMetaResult res = new QueryGetColumnsMetaResult(meta);

            return new GridOdbcResponse(res);
        }
        catch (Exception e) {
            return new GridOdbcResponse(GridOdbcResponse.STATUS_FAILED, e.getMessage());
        }
    }
}
