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

package org.apache.ignite.internal.processors.rest.handlers.cache;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.cache.query.*;
import org.apache.ignite.internal.processors.rest.*;
import org.apache.ignite.internal.processors.rest.client.message.*;
import org.apache.ignite.internal.processors.rest.handlers.*;
import org.apache.ignite.internal.processors.rest.request.*;
import org.apache.ignite.internal.util.future.*;
import org.apache.ignite.internal.util.lang.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.resources.*;

import java.lang.reflect.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.apache.ignite.internal.GridClosureCallMode.*;
import static org.apache.ignite.internal.processors.rest.GridRestCommand.*;
import static org.apache.ignite.internal.processors.task.GridTaskThreadContextKey.*;

/**
 * Cache query command handler.
 */
public class GridCacheQueryCommandHandler extends GridRestCommandHandlerAdapter {
    /** Supported commands. */
    private static final Collection<GridRestCommand> SUPPORTED_COMMANDS = U.sealList(
        CACHE_QUERY_EXECUTE,
        CACHE_QUERY_FETCH,
        CACHE_QUERY_REBUILD_INDEXES
    );

    /** Query ID sequence. */
    private static final AtomicLong qryIdGen = new AtomicLong();

    /**
     * @param ctx Context.
     */
    public GridCacheQueryCommandHandler(GridKernalContext ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Override public Collection<GridRestCommand> supportedCommands() {
        return SUPPORTED_COMMANDS;
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<GridRestResponse> handleAsync(GridRestRequest req) {
        assert req instanceof GridRestCacheQueryRequest;
        assert SUPPORTED_COMMANDS.contains(req.command());

        GridRestCacheQueryRequest qryReq = (GridRestCacheQueryRequest)req;

        UUID destId = qryReq.destinationId();
        String cacheName = qryReq.cacheName();

        switch (qryReq.command()) {
            case CACHE_QUERY_EXECUTE: {
                return execute(destId, cacheName, new ExecuteQuery(qryReq));
            }

            case CACHE_QUERY_FETCH: {
                return execute(destId, cacheName, new FetchQueryResults(qryReq));
            }

            case CACHE_QUERY_REBUILD_INDEXES: {
                return broadcast(qryReq.cacheName(), new RebuildIndexes(qryReq.cacheName(), qryReq.className()));
            }

            default:
                return new GridFinishedFuture<>(new IgniteCheckedException("Unsupported query command: " + req.command()));
        }
    }

    /**
     * @param cacheName Cache name.
     * @return If replicated cache with given name is locally available.
     */
    private boolean replicatedCacheAvailable(String cacheName) {
        GridCacheAdapter<Object,Object> cache = ctx.cache().internalCache(cacheName);

        return cache != null && cache.configuration().getCacheMode() == CacheMode.REPLICATED;
    }

    /**
     * Executes given closure either locally or on specified node.
     *
     * @param destId Destination node ID.
     * @param cacheName Cache name.
     * @param c Closure to execute.
     * @return Execution future.
     */
    private IgniteInternalFuture<GridRestResponse> execute(UUID destId,
        String cacheName,
        Callable<GridRestResponse> c)
    {
        boolean locExec = destId == null || destId.equals(ctx.localNodeId()) || replicatedCacheAvailable(cacheName);

        if (locExec)
            return ctx.closure().callLocalSafe(c, false);
        else {
            if (ctx.discovery().node(destId) == null)
                return new GridFinishedFuture<>(new IgniteCheckedException("Destination node ID has left the grid " +
                    "(retry the query): " + destId));

            ctx.task().setThreadContext(TC_NO_FAILOVER, true);

            return ctx.closure().callAsync(BALANCE, c, ctx.grid().cluster().forNodeId(destId).nodes());
        }
    }

    /**
     * @param cacheName Cache name.
     * @param c Closure to execute.
     * @return Execution future.
     */
    private IgniteInternalFuture<GridRestResponse> broadcast(String cacheName, Callable<Object> c) {
        ctx.task().setThreadContext(TC_NO_FAILOVER, true);

        IgniteInternalFuture<Collection<Object>> fut = ctx.closure().callAsync(BROADCAST,
            Arrays.asList(c),
            ctx.grid().cluster().forCacheNodes(cacheName).nodes());

        return fut.chain(new C1<IgniteInternalFuture<Collection<Object>>, GridRestResponse>() {
            @Override public GridRestResponse apply(IgniteInternalFuture<Collection<Object>> fut) {
                try {
                    fut.get();

                    return new GridRestResponse();
                }
                catch (IgniteCheckedException e) {
                    throw new GridClosureException(e);
                }
            }
        });
    }

    /**
     * @param qryId Query ID.
     * @param wrapper Query future wrapper.
     * @param locMap Queries map.
     * @param locNodeId Local node ID.
     * @return Rest response.
     * @throws IgniteCheckedException If failed.
     */
    private static GridRestResponse fetchQueryResults(
        long qryId,
        QueryFutureWrapper wrapper,
        ConcurrentMap<QueryExecutionKey, QueryFutureWrapper> locMap,
        UUID locNodeId
    ) throws IgniteCheckedException {
        if (wrapper == null)
            throw new IgniteCheckedException("Failed to find query future (query has been expired).");

        GridCacheQueryFutureAdapter<?, ?, ?> fut = wrapper.future();

        Collection<Object> col = (Collection<Object>)fut.nextPage();

        GridCacheRestResponse res = new GridCacheRestResponse();

        GridCacheClientQueryResult qryRes = new GridCacheClientQueryResult();

        if (col == null) {
            col = Collections.emptyList();

            qryRes.last(true);

            locMap.remove(new QueryExecutionKey(qryId), wrapper);
        }

        qryRes.items(col);
        qryRes.queryId(qryId);
        qryRes.nodeId(locNodeId);

        res.setResponse(qryRes);

        return res;
    }

    /**
     * Creates class instance.
     *
     * @param cls Target class.
     * @param clsName Implementing class name.
     * @return Class instance.
     * @throws IgniteCheckedException If failed.
     */
    private static <T> T instance(Class<? extends T> cls, String clsName) throws IgniteCheckedException {
        try {
            Class<?> implCls = Class.forName(clsName);

            if (!cls.isAssignableFrom(implCls))
                throw new IgniteCheckedException("Failed to create instance (target class does not extend or implement " +
                    "required class or interface) [cls=" + cls.getName() + ", clsName=" + clsName + ']');

            Constructor<?> ctor = implCls.getConstructor();

            return (T)ctor.newInstance();
        }
        catch (ClassNotFoundException e) {
            throw new IgniteCheckedException("Failed to find target class: " + clsName, e);
        }
        catch (NoSuchMethodException e) {
            throw new IgniteCheckedException("Failed to find constructor for provided arguments " +
                "[clsName=" + clsName + ']', e);
        }
        catch (InstantiationException e) {
            throw new IgniteCheckedException("Failed to instantiate target class " +
                "[clsName=" + clsName + ']', e);
        }
        catch (IllegalAccessException e) {
            throw new IgniteCheckedException("Failed to instantiate class (constructor is not available) " +
                "[clsName=" + clsName + ']', e);
        }
        catch (InvocationTargetException e) {
            throw new IgniteCheckedException("Failed to instantiate class (constructor threw an exception) " +
                "[clsName=" + clsName + ']', e.getCause());
        }
    }

    /**
     *
     */
    private static class ExecuteQuery implements IgniteCallable<GridRestResponse> {
        /** */
        private static final long serialVersionUID = 0L;

        /** Injected grid. */
        @IgniteInstanceResource
        private Ignite g;

        /** Query request. */
        private GridRestCacheQueryRequest req;

        /**
         * @param req Request.
         */
        private ExecuteQuery(GridRestCacheQueryRequest req) {
            this.req = req;
        }

        /** {@inheritDoc} */
        @SuppressWarnings({"unchecked", "IfMayBeConditional"})
        @Override public GridRestResponse call() throws Exception {
            long qryId = qryIdGen.getAndIncrement();

            CacheQueries<Object,Object> queries = ((IgniteKernal)g).getCache(req.cacheName()).queries();

            CacheQuery<?> qry;

            switch (req.type()) {
                case SQL:
                    qry = queries.createSqlQuery(req.className(), req.clause());

                    break;

                case SQL_FIELDS:
                    qry = queries.createSqlFieldsQuery(req.clause());

                    break;

                case FULL_TEXT:
                    qry = queries.createFullTextQuery(req.className(), req.clause());

                    break;

                case SCAN:
                    qry = queries.createScanQuery(instance(IgniteBiPredicate.class, req.className()));

                    break;

                default:
                    throw new IgniteCheckedException("Unsupported query type: " + req.type());
            }

            boolean keepPortable = req.keepPortable();

            if (!keepPortable) {
                if (req.type() != GridClientCacheQueryRequest.GridQueryType.SCAN &&
                    (req.remoteReducerClassName() == null && req.remoteTransformerClassName() == null))
                    // Do not deserialize values on server if not needed.
                    keepPortable = true;
            }

            ((GridCacheQueryAdapter)qry).keepPortable(keepPortable);
            ((GridCacheQueryAdapter)qry).subjectId(req.clientId());

            if (req.pageSize() > 0)
                qry = qry.pageSize(req.pageSize());

            if (req.timeout() > 0)
                qry = qry.timeout(req.timeout());

            qry = qry.includeBackups(req.includeBackups()).enableDedup(req.enableDedup()).keepAll(false);

            GridCacheQueryFutureAdapter<?, ?, ?> fut;

            if (req.remoteReducerClassName() != null)
                fut = (GridCacheQueryFutureAdapter<?, ?, ?>)qry.execute(
                    instance(IgniteReducer.class, req.remoteReducerClassName()),
                    req.queryArguments());
            else if (req.remoteTransformerClassName() != null)
                fut = (GridCacheQueryFutureAdapter<?, ?, ?>)qry.execute(
                    instance(IgniteClosure.class, req.remoteTransformerClassName()),
                    req.queryArguments());
            else
                fut = (GridCacheQueryFutureAdapter<?, ?, ?>)qry.execute(req.queryArguments());

            ConcurrentMap<QueryExecutionKey, QueryFutureWrapper> locMap = g.cluster().nodeLocalMap();

            QueryFutureWrapper wrapper = new QueryFutureWrapper(fut);

            QueryFutureWrapper old = locMap.putIfAbsent(new QueryExecutionKey(qryId), wrapper);

            assert old == null;

            return fetchQueryResults(qryId, wrapper, locMap, g.cluster().localNode().id());
        }
    }

    /**
     *
     */
    private static class FetchQueryResults implements IgniteCallable<GridRestResponse> {
        /** */
        private static final long serialVersionUID = 0L;

        /** Injected grid. */
        @IgniteInstanceResource
        private Ignite g;

        /** Query request. */
        private GridRestCacheQueryRequest req;

        /**
         * @param req Request.
         */
        private FetchQueryResults(GridRestCacheQueryRequest req) {
            this.req = req;
        }

        /** {@inheritDoc} */
        @Override public GridRestResponse call() throws Exception {
            ConcurrentMap<QueryExecutionKey, QueryFutureWrapper> locMap = g.cluster().nodeLocalMap();

            return fetchQueryResults(req.queryId(), locMap.get(new QueryExecutionKey(req.queryId())),
                locMap, g.cluster().localNode().id());
        }
    }

    /**
     * Rebuild indexes closure.
     */
    private static class RebuildIndexes implements IgniteCallable<Object> {
        /** */
        private static final long serialVersionUID = 0L;

        /** Injected grid. */
        @IgniteInstanceResource
        private Ignite g;

        /** Cache name. */
        private String cacheName;

        /** Class name. */
        private String clsName;

        /**
         * @param cacheName Cache name.
         * @param clsName Optional class name to rebuild indexes for.
         */
        private RebuildIndexes(String cacheName, String clsName) {
            this.cacheName = cacheName;
            this.clsName = clsName;
        }

        /** {@inheritDoc} */
        @Override public Object call() throws Exception {
            if (clsName == null)
                ((IgniteKernal)g).getCache(cacheName).queries().rebuildAllIndexes();
            else
                ((IgniteKernal)g).getCache(cacheName).queries().rebuildIndexes(clsName);

            return null;
        }
    }

    /**
     *
     */
    private static class QueryExecutionKey {
        /** Query ID. */
        private long qryId;

        /**
         * @param qryId Query ID.
         */
        private QueryExecutionKey(long qryId) {
            this.qryId = qryId;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (!(o instanceof QueryExecutionKey))
                return false;

            QueryExecutionKey that = (QueryExecutionKey)o;

            return qryId == that.qryId;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return (int)(qryId ^ (qryId >>> 32));
        }
    }

    /**
     * Query future wrapper.
     */
    private static class QueryFutureWrapper {
        /** Query future. */
        private final GridCacheQueryFutureAdapter<?, ?, ?> qryFut;

        /** Last future use timestamp. */
        private volatile long lastUseTs;

        /**
         * @param qryFut Query future.
         */
        private QueryFutureWrapper(GridCacheQueryFutureAdapter<?, ?, ?> qryFut) {
            this.qryFut = qryFut;

            lastUseTs = U.currentTimeMillis();
        }

        /**
         * @return Query future.
         */
        private GridCacheQueryFutureAdapter<?, ?, ?> future() {
            lastUseTs = U.currentTimeMillis();

            return qryFut;
        }

        /**
         * @return Last use timestamp.
         */
        private long lastUseTimestamp() {
            return lastUseTs;
        }
    }
}
