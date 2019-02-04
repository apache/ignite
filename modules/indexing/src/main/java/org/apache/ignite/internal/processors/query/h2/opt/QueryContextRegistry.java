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

import org.jetbrains.annotations.Nullable;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.apache.ignite.internal.processors.query.h2.opt.GridH2QueryType.MAP;

/**
 * Registry of all currently available query contexts.
 */
public class QueryContextRegistry {
    /** Current local context. */
    private final ThreadLocal<GridH2QueryContext> curCtx = new ThreadLocal<>();

    /** Shared contexts. */
    private final ConcurrentMap<QueryContextKey, GridH2QueryContext> sharedCtxs = new ConcurrentHashMap<>();

    /**
     * Access current thread local query context (if it was set).
     *
     * @return Current thread local query context or {@code null} if the query runs outside of Ignite context.
     */
    @Nullable public GridH2QueryContext getThreadLocal() {
        return curCtx.get();
    }

    /**
     * Sets current thread local context. This method must be called when all the non-volatile properties are
     * already set to ensure visibility for other threads.
     *
     * @param x Query context.
     */
    public void setThreadLocal(GridH2QueryContext x) {
        assert curCtx.get() == null;

        curCtx.set(x);
    }

    /**
     * Drops current thread local context.
     */
    public void clearThreadLocal() {
        assert curCtx.get() != null;

        curCtx.remove();
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
    @Nullable public GridH2QueryContext getShared(
        UUID locNodeId,
        UUID nodeId,
        long qryId,
        int segmentId,
        GridH2QueryType type
    ) {
        return sharedCtxs.get(new QueryContextKey(locNodeId, nodeId, qryId, segmentId, type));
    }

    /**
     * Sets current thread local context. This method must be called when all the non-volatile properties are
     * already set to ensure visibility for other threads.
     *
     * @param ctx Query context.
     */
    public void setShared(GridH2QueryContext ctx) {
        assert ctx.key().type() == MAP;
        assert ctx.distributedJoinContext() != null;

        GridH2QueryContext oldCtx = sharedCtxs.putIfAbsent(ctx.key(), ctx);

        assert oldCtx == null;
    }

    /**
     * @param locNodeId Local node ID.
     * @param nodeId The node who initiated the query.
     * @param qryId The query ID.
     * @param type Query type.
     * @return {@code True} if context was found.
     */
    // TODO: Remove local node, type
    public boolean clearShared(UUID locNodeId, UUID nodeId, long qryId, GridH2QueryType type) {
        boolean res = false;

        for (QueryContextKey key : sharedCtxs.keySet()) {
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
     * @param locNodeId Local node ID.
     * @param rmtNodeId Remote node ID.
     */
    // TODO: Local node should go away.
    public void clearSharedOnRemoteNodeLeave(UUID locNodeId, UUID rmtNodeId) {
        for (QueryContextKey key : sharedCtxs.keySet()) {
            if (key.localNodeId().equals(locNodeId) && key.nodeId().equals(rmtNodeId))
                doClear(key, false);
        }
    }

    /**
     * @param locNodeId Local node ID.
     */
    // TODO: Most likely we do not need it.
    public void clearSharedOnLocalNodeLeave(UUID locNodeId) {
        for (QueryContextKey key : sharedCtxs.keySet()) {
            if (key.localNodeId().equals(locNodeId))
                doClear(key, true);
        }
    }

    /**
     * @param key Context key.
     * @param nodeStop Node is stopping.
     * @return {@code True} if context was found.
     */
    private boolean doClear(QueryContextKey key, boolean nodeStop) {
        assert key.type() == MAP : key.type();

        GridH2QueryContext ctx = sharedCtxs.remove(key);

        if (ctx == null)
            return false;

        assert ctx.key().equals(key);

        if (ctx.lazyWorker() != null)
            ctx.lazyWorker().stop(nodeStop);
        else
            ctx.clearContext(nodeStop);

        return true;
    }
}
