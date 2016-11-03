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

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.query.Query;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.QueryCursorImpl;
import org.apache.ignite.internal.processors.query.GridQueryFieldMetadata;
import org.apache.ignite.internal.processors.rest.GridRestCommand;
import org.apache.ignite.internal.processors.rest.GridRestResponse;
import org.apache.ignite.internal.processors.rest.handlers.GridRestCommandHandlerAdapter;
import org.apache.ignite.internal.processors.rest.request.GridRestRequest;
import org.apache.ignite.internal.processors.rest.request.RestQueryRequest;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;

import static org.apache.ignite.internal.processors.rest.GridRestCommand.CLOSE_SQL_QUERY;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.EXECUTE_SCAN_QUERY;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.EXECUTE_SQL_FIELDS_QUERY;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.EXECUTE_SQL_QUERY;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.FETCH_SQL_QUERY;

/**
 * Query command handler.
 */
public class QueryCommandHandler extends GridRestCommandHandlerAdapter {
    /** Supported commands. */
    private static final Collection<GridRestCommand> SUPPORTED_COMMANDS = U.sealList(EXECUTE_SQL_QUERY,
        EXECUTE_SQL_FIELDS_QUERY,
        EXECUTE_SCAN_QUERY,
        FETCH_SQL_QUERY,
        CLOSE_SQL_QUERY);

    /** Query ID sequence. */
    private static final AtomicLong qryIdGen = new AtomicLong();

    /** Current queries cursors. */
    private final ConcurrentHashMap<Long, QueryCursorIterator> qryCurs = new ConcurrentHashMap<>();

    /**
     * @param ctx Context.
     */
    public QueryCommandHandler(GridKernalContext ctx) {
        super(ctx);

        final long idleQryCurTimeout = ctx.config().getConnectorConfiguration().getIdleQueryCursorTimeout();

        long idleQryCurCheckFreq = ctx.config().getConnectorConfiguration().getIdleQueryCursorCheckFrequency();

        ctx.timeout().schedule(new Runnable() {
            @Override public void run() {
                long time = U.currentTimeMillis();

                for (Map.Entry<Long, QueryCursorIterator> e : qryCurs.entrySet()) {
                    QueryCursorIterator qryCurIt = e.getValue();

                    long createTime = qryCurIt.timestamp();

                    if (time > createTime + idleQryCurTimeout && qryCurIt.tryLock()) {
                        try {
                            qryCurIt.timestamp(-1);

                            qryCurs.remove(e.getKey(), qryCurIt);

                            qryCurIt.close();
                        }
                        finally {
                            qryCurIt.unlock();
                        }
                    }
                }
            }
        }, idleQryCurCheckFreq, idleQryCurCheckFreq);
    }

    /**
     * @param cur Current cursor.
     * @param req Sql request.
     * @param qryId Query id.
     * @param qryCurs Query cursors.
     * @return Query result with items.
     */
    private static CacheQueryResult createQueryResult(
        Iterator cur, RestQueryRequest req, Long qryId, ConcurrentHashMap<Long, QueryCursorIterator> qryCurs) {
        CacheQueryResult res = new CacheQueryResult();

        List<Object> items = new ArrayList<>();

        for (int i = 0; i < req.pageSize() && cur.hasNext(); ++i)
            items.add(cur.next());

        res.setItems(items);

        res.setLast(!cur.hasNext());

        res.setQueryId(qryId);

        if (!cur.hasNext())
            removeQueryCursor(qryId, qryCurs);

        return res;
    }

    /**
     * Removes query cursor.
     *
     * @param qryId Query id.
     * @param qryCurs Query cursors.
     */
    private static void removeQueryCursor(Long qryId, ConcurrentHashMap<Long, QueryCursorIterator> qryCurs) {
        QueryCursorIterator qryCurIt = qryCurs.get(qryId);

        if (qryCurIt == null)
            return;

        qryCurIt.lock();

        try {
            if (qryCurIt.timestamp() == -1)
                return;

            qryCurIt.close();

            qryCurs.remove(qryId);
        }
        finally {
            qryCurIt.unlock();
        }
    }

    /**
     * Creates class instance.
     *
     * @param cls Target class.
     * @param clsName Implementing class name.
     * @return Class instance.
     * @throws IgniteException If failed.
     */
    private static <T> T instance(Class<? extends T> cls, String clsName) throws IgniteException {
        try {
            Class<?> implCls = Class.forName(clsName);

            if (!cls.isAssignableFrom(implCls))
                throw new IgniteException("Failed to create instance (target class does not extend or implement " +
                    "required class or interface) [cls=" + cls.getName() + ", clsName=" + clsName + ']');

            Constructor<?> ctor = implCls.getConstructor();

            return (T)ctor.newInstance();
        }
        catch (ClassNotFoundException e) {
            throw new IgniteException("Failed to find target class: " + clsName, e);
        }
        catch (NoSuchMethodException e) {
            throw new IgniteException("Failed to find constructor for provided arguments " +
                "[clsName=" + clsName + ']', e);
        }
        catch (InstantiationException e) {
            throw new IgniteException("Failed to instantiate target class " +
                "[clsName=" + clsName + ']', e);
        }
        catch (IllegalAccessException e) {
            throw new IgniteException("Failed to instantiate class (constructor is not available) " +
                "[clsName=" + clsName + ']', e);
        }
        catch (InvocationTargetException e) {
            throw new IgniteException("Failed to instantiate class (constructor threw an exception) " +
                "[clsName=" + clsName + ']', e.getCause());
        }
    }

    /** {@inheritDoc} */
    @Override public Collection<GridRestCommand> supportedCommands() {
        return SUPPORTED_COMMANDS;
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<GridRestResponse> handleAsync(GridRestRequest req) {
        assert req != null;

        assert SUPPORTED_COMMANDS.contains(req.command());
        assert req instanceof RestQueryRequest : "Invalid type of query request.";

        switch (req.command()) {
            case EXECUTE_SQL_QUERY:
            case EXECUTE_SQL_FIELDS_QUERY:
            case EXECUTE_SCAN_QUERY: {
                return ctx.closure().callLocalSafe(
                    new ExecuteQueryCallable(ctx, (RestQueryRequest)req, qryCurs), false);
            }

            case FETCH_SQL_QUERY: {
                return ctx.closure().callLocalSafe(
                    new FetchQueryCallable((RestQueryRequest)req, qryCurs), false);
            }

            case CLOSE_SQL_QUERY: {
                return ctx.closure().callLocalSafe(
                    new CloseQueryCallable((RestQueryRequest)req, qryCurs), false);
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
        private RestQueryRequest req;

        /** Current queries cursors. */
        private final ConcurrentHashMap<Long, QueryCursorIterator> qryCurs;

        /**
         * @param ctx Kernal context.
         * @param req Execute query request.
         * @param qryCurs Query cursors.
         */
        public ExecuteQueryCallable(GridKernalContext ctx, RestQueryRequest req,
            ConcurrentHashMap<Long, QueryCursorIterator> qryCurs) {
            this.ctx = ctx;
            this.req = req;
            this.qryCurs = qryCurs;
        }

        /** {@inheritDoc} */
        @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
        @Override public GridRestResponse call() throws Exception {
            final long qryId = qryIdGen.getAndIncrement();

            try {
                Query qry;

                switch (req.queryType()) {
                    case SQL:
                        qry = new SqlQuery(req.typeName(), req.sqlQuery());

                        ((SqlQuery)qry).setArgs(req.arguments());

                        ((SqlQuery)qry).setDistributedJoins(req.distributedJoins());

                        break;

                    case SQL_FIELDS:
                        qry = new SqlFieldsQuery(req.sqlQuery());

                        ((SqlFieldsQuery)qry).setArgs(req.arguments());

                        ((SqlFieldsQuery)qry).setDistributedJoins(req.distributedJoins());

                        break;

                    case SCAN:
                        IgniteBiPredicate pred = null;

                        if (req.className() != null)
                            pred = instance(IgniteBiPredicate.class, req.className());

                        qry = new ScanQuery(pred);

                        break;

                    default:
                        throw new IgniteException("Incorrect query type [type=" + req.queryType() + "]");
                }

                IgniteCache<Object, Object> cache = ctx.grid().cache(req.cacheName());

                if (cache == null)
                    return new GridRestResponse(GridRestResponse.STATUS_FAILED,
                        "Failed to find cache with name: " + req.cacheName());

                final QueryCursor qryCur = cache.query(qry);

                Iterator cur = qryCur.iterator();

                QueryCursorIterator qryCurIt = new QueryCursorIterator(qryCur, cur);

                qryCurIt.lock();

                try {
                    qryCurs.put(qryId, qryCurIt);

                    CacheQueryResult res = createQueryResult(cur, req, qryId, qryCurs);

                    switch (req.queryType()) {
                        case SQL:
                        case SQL_FIELDS:
                            List<GridQueryFieldMetadata> fieldsMeta = ((QueryCursorImpl)qryCur).fieldsMeta();

                            res.setFieldsMetadata(convertMetadata(fieldsMeta));

                            break;
                        case SCAN:
                            CacheQueryFieldsMetaResult keyField = new CacheQueryFieldsMetaResult();
                            keyField.setFieldName("key");

                            CacheQueryFieldsMetaResult valField = new CacheQueryFieldsMetaResult();
                            valField.setFieldName("value");

                            res.setFieldsMetadata(U.sealList(keyField, valField));

                            break;
                    }

                    return new GridRestResponse(res);
                }
                finally {
                    qryCurIt.unlock();
                }
            }
            catch (Exception e) {
                removeQueryCursor(qryId, qryCurs);

                SQLException sqlErr = X.cause(e, SQLException.class);

                return new GridRestResponse(GridRestResponse.STATUS_FAILED,
                    sqlErr != null ? sqlErr.getMessage() : e.getMessage());
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
        /** Current queries cursors. */
        private final ConcurrentHashMap<Long, QueryCursorIterator> qryCurs;

        /** Execute query request. */
        private RestQueryRequest req;

        /**
         * @param req Execute query request.
         * @param qryCurs Query cursors.
         */
        public CloseQueryCallable(RestQueryRequest req, ConcurrentHashMap<Long, QueryCursorIterator> qryCurs) {
            this.req = req;
            this.qryCurs = qryCurs;
        }

        /** {@inheritDoc} */
        @Override public GridRestResponse call() throws Exception {
            try {
                QueryCursorIterator qryCurIt = qryCurs.get(req.queryId());

                if (qryCurIt == null)
                    return new GridRestResponse(true);

                qryCurIt.lock();

                try {
                    if (qryCurIt.timestamp() == -1)
                        return new GridRestResponse(true);

                    qryCurIt.close();

                    qryCurs.remove(req.queryId());
                }
                finally {
                    qryCurIt.unlock();
                }

                return new GridRestResponse(true);
            }
            catch (Exception e) {
                removeQueryCursor(req.queryId(), qryCurs);

                return new GridRestResponse(GridRestResponse.STATUS_FAILED, e.getMessage());
            }
        }
    }

    /**
     * Fetch query callable.
     */
    private static class FetchQueryCallable implements Callable<GridRestResponse> {
        /** Current queries cursors. */
        private final ConcurrentHashMap<Long, QueryCursorIterator> qryCurs;

        /** Execute query request. */
        private RestQueryRequest req;

        /**
         * @param req Execute query request.
         * @param qryCurs Query cursors.
         */
        public FetchQueryCallable(RestQueryRequest req, ConcurrentHashMap<Long, QueryCursorIterator> qryCurs) {
            this.req = req;
            this.qryCurs = qryCurs;
        }

        /** {@inheritDoc} */
        @Override public GridRestResponse call() throws Exception {
            try {
                QueryCursorIterator qryCurIt = qryCurs.get(req.queryId());

                if (qryCurIt == null)
                    return new GridRestResponse(GridRestResponse.STATUS_FAILED,
                        "Failed to find query with ID: " + req.queryId() + ". " +
                        "Possible reasons: wrong query ID, no more data to fetch from query, query was closed by timeout" +
                        " or node where query was executed is not found.");

                qryCurIt.lock();

                try {
                    if (qryCurIt.timestamp() == -1)
                        return new GridRestResponse(GridRestResponse.STATUS_FAILED,
                            "Query with ID: " + req.queryId() + " was closed by timeout");

                    qryCurIt.timestamp(U.currentTimeMillis());

                    Iterator cur = qryCurIt.iterator();

                    CacheQueryResult res = createQueryResult(cur, req, req.queryId(), qryCurs);

                    return new GridRestResponse(res);
                }
                finally {
                    qryCurIt.unlock();
                }
            }
            catch (Exception e) {
                removeQueryCursor(req.queryId(), qryCurs);

                return new GridRestResponse(GridRestResponse.STATUS_FAILED, e.getMessage());
            }
        }
    }

    /**
     * Query cursor iterator.
     */
    private static class QueryCursorIterator extends ReentrantLock {
        /** */
        private static final long serialVersionUID = 0L;

        /** Query cursor. */
        private QueryCursor cur;

        /** Query iterator. */
        private Iterator it;

        /** Last timestamp. */
        private volatile long ts;

        /**
         * @param cur Query cursor.
         * @param it Query iterator.
         */
        public QueryCursorIterator(QueryCursor cur, Iterator it) {
            this.cur = cur;
            this.it = it;
            ts = U.currentTimeMillis();
        }

        /**
         * @return Query iterator.
         */
        public Iterator iterator() {
            return it;
        }

        /**
         * @return Timestamp.
         */
        public long timestamp() {
            return ts;
        }

        /**
         * @param time Current time or -1 if cursor is closed.
         */
        public void timestamp(long time) {
            ts = time;
        }

        /**
         * Close query cursor.
         */
        public void close() {
            cur.close();
        }
    }
}
