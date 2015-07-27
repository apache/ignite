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

package org.apache.ignite.internal.processors.rest.handlers.query;

import org.apache.ignite.*;
import org.apache.ignite.cache.query.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.query.*;
import org.apache.ignite.internal.processors.rest.*;
import org.apache.ignite.internal.processors.rest.handlers.*;
import org.apache.ignite.internal.processors.rest.request.*;
import org.apache.ignite.internal.util.future.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.apache.ignite.internal.processors.rest.GridRestCommand.*;

/**
 * Query command handler.
 */
public class QueryCommandHandler extends GridRestCommandHandlerAdapter {
    /** Supported commands. */
    private static final Collection<GridRestCommand> SUPPORTED_COMMANDS = U.sealList(EXECUTE_SQL_QUERY,
        EXECUTE_SQL_FIELDS_QUERY,
        FETCH_SQL_QUERY,
        CLOSE_SQL_QUERY);

    /** Query ID sequence. */
    private static final AtomicLong qryIdGen = new AtomicLong();

    /** Current queries cursors. */
    private final ConcurrentHashMap<Long, IgniteBiTuple<QueryCursor, Iterator>> qryCurs = new ConcurrentHashMap<>();

    /**
     * @param ctx Context.
     */
    public QueryCommandHandler(GridKernalContext ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Override public Collection<GridRestCommand> supportedCommands() {
        return SUPPORTED_COMMANDS;
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<GridRestResponse> handleAsync(GridRestRequest req) {
        assert req != null;

        assert SUPPORTED_COMMANDS.contains(req.command());
        assert req instanceof RestSqlQueryRequest : "Invalid type of query request.";

        switch (req.command()) {
            case EXECUTE_SQL_QUERY:
            case EXECUTE_SQL_FIELDS_QUERY: {
                return ctx.closure().callLocalSafe(
                    new ExecuteQueryCallable(ctx, (RestSqlQueryRequest)req, qryCurs), false);
            }

            case FETCH_SQL_QUERY: {
                return ctx.closure().callLocalSafe(
                    new FetchQueryCallable((RestSqlQueryRequest)req, qryCurs), false);
            }

            case CLOSE_SQL_QUERY: {
                return ctx.closure().callLocalSafe(
                    new CloseQueryCallable((RestSqlQueryRequest)req, qryCurs), false);
            }
        }

        return new GridFinishedFuture<>();
    }

    /**
     * Execute query callable.
     */
    private static class ExecuteQueryCallable implements Callable<GridRestResponse> {
        /** Kernal context. */
        private GridKernalContext ctx;

        /** Execute query request. */
        private RestSqlQueryRequest req;

        /** Queries cursors. */
        private ConcurrentHashMap<Long, IgniteBiTuple<QueryCursor, Iterator>> qryCurs;

        /**
         * @param ctx Kernal context.
         * @param req Execute query request.
         * @param qryCurs Queries cursors.
         */
        public ExecuteQueryCallable(GridKernalContext ctx, RestSqlQueryRequest req,
            ConcurrentHashMap<Long, IgniteBiTuple<QueryCursor, Iterator>> qryCurs) {
            this.ctx = ctx;
            this.req = req;
            this.qryCurs = qryCurs;
        }

        /** {@inheritDoc} */
        @Override public GridRestResponse call() throws Exception {
            long qryId = qryIdGen.getAndIncrement();

            try {
                Query qry;

                if (req.typeName() != null) {
                    qry = new SqlQuery(req.typeName(), req.sqlQuery());

                    ((SqlQuery)qry).setArgs(req.arguments());
                }
                else {
                    qry = new SqlFieldsQuery(req.sqlQuery());

                    ((SqlFieldsQuery)qry).setArgs(req.arguments());
                }

                IgniteCache<Object, Object> cache = ctx.grid().cache(req.cacheName());

                if (cache == null)
                    return new GridRestResponse(GridRestResponse.STATUS_FAILED,
                        "No cache with name [cacheName=" + req.cacheName() + "]");

                QueryCursor qryCur = cache.query(qry);

                Iterator cur = qryCur.iterator();

                qryCurs.put(qryId, new IgniteBiTuple<>(qryCur, cur));

                CacheQueryResult res = createQueryResult(qryCurs, cur, req, qryId);

                List<GridQueryFieldMetadata> fieldsMeta = ((QueryCursorImpl<?>) qryCur).fieldsMeta();

                res.setFieldsMetadata(convertMetadata(fieldsMeta));

                return new GridRestResponse(res);
            }
            catch (Exception e) {
                qryCurs.remove(qryId);

                return new GridRestResponse(GridRestResponse.STATUS_FAILED, e.getMessage());
            }
        }

        /**
         * @param meta Internal query field metadata.
         * @return Rest query field metadata.
         */
        private Collection<CacheQueryFieldsMetaResult> convertMetadata(Collection<GridQueryFieldMetadata> meta) {
            List<CacheQueryFieldsMetaResult> res = new ArrayList<>();

            if (meta != null) {
                for (GridQueryFieldMetadata info : meta)
                    res.add(new CacheQueryFieldsMetaResult(info));
            }

            return res;
        }
    }

    /**
     * Close query callable.
     */
    private static class CloseQueryCallable implements Callable<GridRestResponse> {
        /** Execute query request. */
        private RestSqlQueryRequest req;

        /** Queries cursors. */
        private final ConcurrentHashMap<Long, IgniteBiTuple<QueryCursor, Iterator>> qryCurs;

        /**
         * @param req Execute query request.
         * @param qryCurs Queries cursors.
         */
        public CloseQueryCallable(RestSqlQueryRequest req,
            ConcurrentHashMap<Long, IgniteBiTuple<QueryCursor, Iterator>> qryCurs) {
            this.req = req;
            this.qryCurs = qryCurs;
        }

        /** {@inheritDoc} */
        @Override public GridRestResponse call() throws Exception {
            try {
                QueryCursor cur = qryCurs.get(req.queryId()).get1();

                if (cur == null)
                    return new GridRestResponse(GridRestResponse.STATUS_FAILED,
                        "Cannot find query [qryId=" + req.queryId() + "]");

                cur.close();

                qryCurs.remove(req.queryId());

                return new GridRestResponse(true);
            }
            catch (Exception e) {
                qryCurs.remove(req.queryId());

                return new GridRestResponse(GridRestResponse.STATUS_FAILED, e.getMessage());
            }
        }
    }

    /**
     * Fetch query callable.
     */
    private static class FetchQueryCallable implements Callable<GridRestResponse> {
        /** Execute query request. */
        private RestSqlQueryRequest req;

        /** Queries cursors. */
        private final ConcurrentHashMap<Long, IgniteBiTuple<QueryCursor, Iterator>> qryCurs;

        /**
         * @param req Execute query request.
         * @param qryCurs Queries cursors.
         */
        public FetchQueryCallable(RestSqlQueryRequest req,
            ConcurrentHashMap<Long, IgniteBiTuple<QueryCursor, Iterator>> qryCurs) {
            this.req = req;
            this.qryCurs = qryCurs;
        }

        /** {@inheritDoc} */
        @Override public GridRestResponse call() throws Exception {
            try {
                Iterator cur = qryCurs.get(req.queryId()).get2();

                if (cur == null)
                    return new GridRestResponse(GridRestResponse.STATUS_FAILED,
                        "Cannot find query [qryId=" + req.queryId() + "]");

                CacheQueryResult res = createQueryResult(qryCurs, cur, req, req.queryId());

                return new GridRestResponse(res);
            }
            catch (Exception e) {
                qryCurs.remove(req.queryId());

                return new GridRestResponse(GridRestResponse.STATUS_FAILED, e.getMessage());
            }
        }
    }

    /**
     * @param qryCurs Query cursors.
     * @param cur Current cursor.
     * @param req Sql request.
     * @param qryId Query id.
     * @return Query result with items.
     */
    private static CacheQueryResult createQueryResult(
        ConcurrentHashMap<Long, IgniteBiTuple<QueryCursor, Iterator>> qryCurs,
        Iterator cur, RestSqlQueryRequest req, Long qryId) {
        CacheQueryResult res = new CacheQueryResult();

        List<Object> items = new ArrayList<>();

        for (int i = 0; i < req.pageSize() && cur.hasNext(); ++i)
            items.add(cur.next());

        res.setItems(items);

        res.setLast(!cur.hasNext());

        res.setQueryId(qryId);

        if (!cur.hasNext())
            qryCurs.remove(qryId);

        return res;
    }
}
