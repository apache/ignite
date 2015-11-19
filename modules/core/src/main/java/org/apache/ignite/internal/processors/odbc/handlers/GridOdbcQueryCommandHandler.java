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
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.odbc.request.GridOdbcRequest;
import org.apache.ignite.internal.processors.odbc.GridOdbcResponse;
import org.apache.ignite.internal.processors.odbc.request.QueryCloseRequest;
import org.apache.ignite.internal.processors.odbc.request.QueryExecuteRequest;
import org.apache.ignite.internal.processors.odbc.request.QueryFetchRequest;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.ignite.internal.processors.odbc.request.GridOdbcRequest.EXECUTE_SQL_QUERY;
import static org.apache.ignite.internal.processors.odbc.request.GridOdbcRequest.FETCH_SQL_QUERY;
import static org.apache.ignite.internal.processors.odbc.request.GridOdbcRequest.CLOSE_SQL_QUERY;

/**
 * SQL query handler.
 */
public class GridOdbcQueryCommandHandler extends GridOdbcCommandHandlerAdapter {
    /** Supported commands. */
    private static final Collection<Integer> SUPPORTED_COMMANDS =
            U.sealList(EXECUTE_SQL_QUERY, FETCH_SQL_QUERY, CLOSE_SQL_QUERY);

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
    @Override public IgniteInternalFuture<GridOdbcResponse> handleAsync(GridOdbcRequest req) {
        assert req != null;

        assert SUPPORTED_COMMANDS.contains(req.command());

        switch (req.command()) {
            case EXECUTE_SQL_QUERY: {
                return ctx.closure().callLocalSafe(
                        new ExecuteQueryCallable(ctx, (QueryExecuteRequest)req, qryCurs), false);
            }

            case FETCH_SQL_QUERY: {
                return ctx.closure().callLocalSafe(
                        new FetchQueryCallable((QueryFetchRequest)req, qryCurs), false);
            }

            case CLOSE_SQL_QUERY: {
                return ctx.closure().callLocalSafe(
                        new CloseQueryCallable((QueryCloseRequest)req, qryCurs), false);
            }
        }

        return new GridFinishedFuture<>();
    }

    /**
     * @param qryCurs Query cursors.
     * @param cur Current cursor.
     * @param req Sql fetch request.
     * @param qryId Query id.
     * @return Query result with items.
     */
    private static GridOdbcQueryResult createQueryResult(
            ConcurrentHashMap<Long, IgniteBiTuple<QueryCursor, Iterator>> qryCurs,
            Iterator cur, QueryFetchRequest req, Long qryId) {
        GridOdbcQueryResult res = new GridOdbcQueryResult(qryId);

        List<Object> items = new ArrayList<>();

        for (int i = 0; i < req.pageSize() && cur.hasNext(); ++i)
            items.add(cur.next());

        res.setItems(items);

        res.setLast(!cur.hasNext());

        if (!cur.hasNext())
            qryCurs.remove(qryId);

        return res;
    }

    /**
     * Execute query callable.
     */
    private static class ExecuteQueryCallable implements Callable<GridOdbcResponse> {
        /** Kernal context. */
        private GridKernalContext ctx;

        /** Execute query request. */
        private QueryExecuteRequest req;

        /** Queries cursors. */
        private ConcurrentHashMap<Long, IgniteBiTuple<QueryCursor, Iterator>> qryCurs;

        /**
         * @param ctx Kernal context.
         * @param req Execute query request.
         * @param qryCurs Queries cursors.
         */
        public ExecuteQueryCallable(GridKernalContext ctx, QueryExecuteRequest req,
                                    ConcurrentHashMap<Long, IgniteBiTuple<QueryCursor, Iterator>> qryCurs) {
            this.ctx = ctx;
            this.req = req;
            this.qryCurs = qryCurs;
        }

        /** {@inheritDoc} */
        @Override public GridOdbcResponse call() throws Exception {
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

                GridOdbcQueryResult res = new GridOdbcQueryResult(qryId);

                return new GridOdbcResponse(res);
            }
            catch (Exception e) {
                qryCurs.remove(qryId);

                return new GridOdbcResponse(GridOdbcResponse.STATUS_FAILED, e.getMessage());
            }
        }
    }

    /**
     * Close query callable.
     */
    private static class CloseQueryCallable implements Callable<GridOdbcResponse> {
        /** Queries cursors. */
        private final ConcurrentHashMap<Long, IgniteBiTuple<QueryCursor, Iterator>> qryCurs;

        /** Execute query request. */
        private QueryCloseRequest req;

        /**
         * @param req Execute query request.
         * @param qryCurs Queries cursors.
         */
        public CloseQueryCallable(QueryCloseRequest req,
                                  ConcurrentHashMap<Long, IgniteBiTuple<QueryCursor, Iterator>> qryCurs) {
            this.req = req;
            this.qryCurs = qryCurs;
        }

        /** {@inheritDoc} */
        @Override public GridOdbcResponse call() throws Exception {
            try {
                QueryCursor cur = qryCurs.get(req.queryId()).get1();

                if (cur == null)
                    return new GridOdbcResponse(GridOdbcResponse.STATUS_FAILED,
                            "Failed to find query with ID: " + req.queryId());

                cur.close();

                qryCurs.remove(req.queryId());

                GridOdbcQueryResult res = new GridOdbcQueryResult(req.queryId());

                return new GridOdbcResponse(res);
            }
            catch (Exception e) {
                qryCurs.remove(req.queryId());

                return new GridOdbcResponse(GridOdbcResponse.STATUS_FAILED, e.getMessage());
            }
        }
    }

    /**
     * Fetch query callable.
     */
    private static class FetchQueryCallable implements Callable<GridOdbcResponse> {
        /** Queries cursors. */
        private final ConcurrentHashMap<Long, IgniteBiTuple<QueryCursor, Iterator>> qryCurs;

        /** Execute query request. */
        private QueryFetchRequest req;

        /**
         * @param req Execute query request.
         * @param qryCurs Queries cursors.
         */
        public FetchQueryCallable(QueryFetchRequest req,
                                  ConcurrentHashMap<Long, IgniteBiTuple<QueryCursor, Iterator>> qryCurs) {
            this.req = req;
            this.qryCurs = qryCurs;
        }

        /** {@inheritDoc} */
        @Override public GridOdbcResponse call() throws Exception {
            try {
                Iterator cur = qryCurs.get(req.queryId()).get2();

                if (cur == null)
                    return new GridOdbcResponse(GridOdbcResponse.STATUS_FAILED,
                            "Failed to find query with ID: " + req.queryId());

                GridOdbcQueryResult res = createQueryResult(qryCurs, cur, req, req.queryId());

                return new GridOdbcResponse(res);
            }
            catch (Exception e) {
                qryCurs.remove(req.queryId());

                return new GridOdbcResponse(GridOdbcResponse.STATUS_FAILED, e.getMessage());
            }
        }
    }
}
