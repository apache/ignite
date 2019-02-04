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

package org.apache.ignite.internal.processors.query.h2.opt;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridReservable;
import org.apache.ignite.internal.processors.cache.mvcc.MvccSnapshot;
import org.apache.ignite.internal.processors.query.h2.opt.join.DistributedJoinContext;
import org.apache.ignite.internal.processors.query.h2.twostep.MapQueryLazyWorker;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.spi.indexing.IndexingQueryFilter;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.query.h2.opt.GridH2QueryType.MAP;

/**
 * Thread local SQL query context which is intended to be accessible from everywhere.
 */
public class GridH2QueryContext {
    /** */
    private static final ThreadLocal<GridH2QueryContext> qctx = new ThreadLocal<>();

    /** */
    private static final ConcurrentMap<QueryContextKey, GridH2QueryContext> qctxs = new ConcurrentHashMap<>();

    /** */
    private final QueryContextKey key;

    /** */
    private final IndexingQueryFilter filter;

    /** Distributed join context. */
    private final DistributedJoinContext distributedJoinCtx;

    /** */
    private final MvccSnapshot mvccSnapshot;

    /** */
    private final List<GridReservable> reservations;

    /** */
    private MapQueryLazyWorker lazyWorker;

    /**
     * @param locNodeId Local node ID.
     * @param nodeId The node who initiated the query.
     * @param qryId The query ID.
     * @param segmentId Index segment ID.
     * @param type Query type.
     * @param filter Filter.
     * @param distributedJoinCtx Distributed join context.
     * @param mvccSnapshot MVCC snapshot.
     */
    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
    public GridH2QueryContext(
        UUID locNodeId,
        UUID nodeId,
        long qryId,
        int segmentId,
        GridH2QueryType type,
        @Nullable IndexingQueryFilter filter,
        @Nullable DistributedJoinContext distributedJoinCtx,
        @Nullable MvccSnapshot mvccSnapshot,
        @Nullable List<GridReservable> reservations
    ) {
        assert segmentId == 0 || type == MAP;

        key = new QueryContextKey(locNodeId, nodeId, qryId, segmentId, type);

        this.filter = filter;
        this.distributedJoinCtx = distributedJoinCtx;
        this.mvccSnapshot = mvccSnapshot;
        this.reservations = reservations;
    }

    /**
     * @return Mvcc snapshot.
     */
    @Nullable public MvccSnapshot mvccSnapshot() {
        return mvccSnapshot;
    }

    /**
     * @return Distributed join context.
     */
    @Nullable public DistributedJoinContext distributedJoinContext() {
        return distributedJoinCtx;
    }

    /**
     * @return index segment ID.
     */
    public int segment() {
        return key.segmentId();
    }

    /**
     * Sets current thread local context. This method must be called when all the non-volatile properties are
     * already set to ensure visibility for other threads.
     *
     * @param x Query context.
     */
    public static void setThreadLocal(GridH2QueryContext x) {
        assert qctx.get() == null;

        qctx.set(x);
    }

    /**
     * Sets current thread local context. This method must be called when all the non-volatile properties are
     * already set to ensure visibility for other threads.
     *
     * @param ctx Query context.
     */
    public static void setShared(GridH2QueryContext ctx) {
        assert ctx.key.type() == MAP;
        assert ctx.distributedJoinContext() != null;

        GridH2QueryContext oldCtx = qctxs.putIfAbsent(ctx.key, ctx);

        assert oldCtx == null;
    }

    /**
     * Drops current thread local context.
     */
    public static void clearThreadLocal() {
        assert qctx.get() != null;

        qctx.remove();
    }

    /**
     * @param locNodeId Local node ID.
     * @param nodeId The node who initiated the query.
     * @param qryId The query ID.
     * @param type Query type.
     * @return {@code True} if context was found.
     */
    public static boolean clearShared(UUID locNodeId, UUID nodeId, long qryId, GridH2QueryType type) {
        boolean res = false;

        for (QueryContextKey key : qctxs.keySet()) {
            if (key.localNodeId().equals(locNodeId) &&
                key.nodeId().equals(nodeId) &&
                key.queryId() == qryId &&
                key.type() == type
            )
                res |= doClear(key, false);
        }

        return res;
    }

    /**
     * @param key Context key.
     * @param nodeStop Node is stopping.
     * @return {@code True} if context was found.
     */
    private static boolean doClear(QueryContextKey key, boolean nodeStop) {
        assert key.type() == MAP : key.type();

        GridH2QueryContext x = qctxs.remove(key);

        if (x == null)
            return false;

        assert x.key.equals(key);

        if (x.lazyWorker() != null)
            x.lazyWorker().stop(nodeStop);
        else
            x.clearContext(nodeStop);

        return true;
    }

    /**
     * @param nodeStop Node is stopping.
     */
    @SuppressWarnings("ForLoopReplaceableByForEach")
    public void clearContext(boolean nodeStop) {
        if (distributedJoinCtx != null)
            distributedJoinCtx.cancel();

        List<GridReservable> r = reservations;

        if (!nodeStop && !F.isEmpty(r)) {
            for (int i = 0; i < r.size(); i++)
                r.get(i).release();
        }
    }

    /**
     * @param locNodeId Local node ID.
     * @param nodeId Dead node ID.
     */
    public static void clearSharedOnNodeLeave(UUID locNodeId, UUID nodeId) {
        for (QueryContextKey key : qctxs.keySet()) {
            if (key.localNodeId().equals(locNodeId) && key.nodeId().equals(nodeId))
                doClear(key, false);
        }
    }

    /**
     * @param locNodeId Local node ID.
     */
    public static void clearSharedOnLocalNodeStop(UUID locNodeId) {
        for (QueryContextKey key : qctxs.keySet()) {
            if (key.localNodeId().equals(locNodeId))
                doClear(key, true);
        }
    }

    /**
     * Access current thread local query context (if it was set).
     *
     * @return Current thread local query context or {@code null} if the query runs outside of Ignite context.
     */
    @Nullable public static GridH2QueryContext getThreadLocal() {
        return qctx.get();
    }

    /**
     * Access query context from another thread.
     *
     * @param locNodeId Local node ID.
     * @param nodeId The node who initiated the query.
     * @param qryId The query ID.
     * @param segmentId Index segment ID.
     * @param type Query type.
     * @return Query context.
     */
    @Nullable public static GridH2QueryContext getShared(
        UUID locNodeId,
        UUID nodeId,
        long qryId,
        int segmentId,
        GridH2QueryType type
    ) {
        return qctxs.get(new QueryContextKey(locNodeId, nodeId, qryId, segmentId, type));
    }

    /**
     * @return Filter.
     */
    public IndexingQueryFilter filter() {
        return filter;
    }

    /**
     * @return Lazy worker, if any, or {@code null} if none.
     */
    public MapQueryLazyWorker lazyWorker() {
        return lazyWorker;
    }

    /**
     * @param lazyWorker Lazy worker, if any, or {@code null} if none.
     * @return {@code this}.
     */
    public GridH2QueryContext lazyWorker(MapQueryLazyWorker lazyWorker) {
        this.lazyWorker = lazyWorker;

        return this;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridH2QueryContext.class, this);
    }
}
