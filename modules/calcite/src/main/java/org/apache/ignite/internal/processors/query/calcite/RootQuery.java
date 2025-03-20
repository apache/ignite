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

package org.apache.ignite.internal.processors.query.calcite;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import org.apache.calcite.plan.Context;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.util.CancelFlag;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.query.QueryCancelledException;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.query.GridQueryCancel;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.QueryContext;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.calcite.exec.ExchangeService;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.rel.Node;
import org.apache.ignite.internal.processors.query.calcite.exec.rel.RootNode;
import org.apache.ignite.internal.processors.query.calcite.prepare.BaseQueryContext;
import org.apache.ignite.internal.processors.query.calcite.prepare.ExecutionPlan;
import org.apache.ignite.internal.processors.query.calcite.prepare.FieldsMetadata;
import org.apache.ignite.internal.processors.query.calcite.prepare.Fragment;
import org.apache.ignite.internal.processors.query.calcite.prepare.PlanningContext;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.processors.query.running.TrackableQuery;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.session.SessionContext;
import org.jetbrains.annotations.Nullable;

/**
 * The RootQuery is created on the query initiator (originator) node as the first step of a query run;
 * It contains the information about query state, contexts, remote fragments;
 * It provides 'cancel' functionality for running query like a base query class.
 */
public class RootQuery<RowT> extends Query<RowT> implements TrackableQuery {
    /** SQL query. */
    private final String sql;

    /** Parameters. */
    private final Object[] params;

    /** Remote nodes unfinished fragments count. AtomicInteger used just as int holder, there is no concurrency here. */
    private final Map<UUID, AtomicInteger> remoteFragments;

    /** Node to fragment. */
    private final Set<RemoteFragmentKey> waiting;

    /** */
    private volatile RootNode<RowT> root;

    /** */
    private volatile PlanningContext pctx;

    /** */
    private final BaseQueryContext ctx;

    /** */
    private final long plannerTimeout;

    /** */
    private final long totalTimeout;

    /** */
    private volatile long locQryId;

    /** Query start timestamp (millis). */
    private final long startTs;

    /** Planning time (millys). */
    private long planningTime;

    /** */
    public RootQuery(
        String sql,
        SchemaPlus schema,
        Object[] params,
        QueryContext qryCtx,
        boolean isLocal,
        boolean forcedJoinOrder,
        int[] parts,
        ExchangeService exch,
        BiConsumer<Query<RowT>, Throwable> unregister,
        IgniteLogger log,
        long plannerTimeout,
        long totalTimeout
    ) {
        super(
            UUID.randomUUID(),
            exch.localNodeId(),
            qryCtx != null ? qryCtx.unwrap(GridQueryCancel.class) : null,
            exch,
            unregister,
            log,
            0 // Total fragments count not used for RootQuery.
        );

        this.sql = sql;
        this.params = params;

        startTs = U.currentTimeMillis();

        remoteFragments = new HashMap<>();
        waiting = new HashSet<>();

        this.plannerTimeout = totalTimeout > 0 ? Math.min(plannerTimeout, totalTimeout) : plannerTimeout;
        this.totalTimeout = totalTimeout;

        Context parent = Commons.convert(qryCtx);

        FrameworkConfig frameworkCfg = qryCtx != null ? qryCtx.unwrap(FrameworkConfig.class) : null;

        ctx = BaseQueryContext.builder()
            .parentContext(parent)
            .frameworkConfig(frameworkCfg)
            .defaultSchema(schema)
            .local(isLocal)
            .forcedJoinOrder(forcedJoinOrder)
            .partitions(parts)
            .logger(log)
            .build();
    }

    /**
     * Creates the new root that inherits the query parameters from {@code this} query.
     * Is used to execute DML query immediately after (inside) DDL.
     * e.g.:
     *      CREATE TABLE MY_TABLE AS SELECT ... FROM ...;
     *
     * @param schema new schema.
     */
    public RootQuery<RowT> childQuery(SchemaPlus schema) {
        return new RootQuery<>(
            sql,
            schema,
            params,
            QueryContext.of(cancel, ctx.unwrap(SessionContext.class)),
            ctx.isLocal(),
            ctx.isForcedJoinOrder(),
            ctx.partitions(),
            exch,
            unregister,
            log,
            plannerTimeout,
            totalTimeout);
    }

    /** */
    public BaseQueryContext context() {
        return ctx;
    }

    /** */
    public String sql() {
        return sql;
    }

    /** */
    public Object[] parameters() {
        return params;
    }

    /**
     * Starts maping phase for the query.
     */
    public void mapping() {
        synchronized (mux) {
            if (state == QueryState.CLOSED)
                throw queryCanceledException();

            state = QueryState.MAPPING;
        }
    }

    /**
     * Starts execution phase for the query and setup remote fragments.
     */
    public void run(ExecutionContext<RowT> ctx, ExecutionPlan plan, FieldsMetadata metadata, Node<RowT> root) {
        synchronized (mux) {
            if (state == QueryState.CLOSED)
                throw queryCanceledException();

            planningTime = U.currentTimeMillis() - startTs;

            RootNode<RowT> rootNode = new RootNode<>(ctx, metadata.rowType(), this::tryClose);
            rootNode.register(root);

            addFragment(new RunningFragment<>(F.first(plan.fragments()).root(), rootNode, ctx));

            this.root = rootNode;

            for (int i = 1; i < plan.fragments().size(); i++) {
                Fragment fragment = plan.fragments().get(i);
                List<UUID> nodes = plan.mapping(fragment).nodeIds();

                nodes.forEach(n -> remoteFragments.compute(n, (id, cnt) -> {
                    if (cnt == null)
                        return new AtomicInteger(1);
                    else {
                        cnt.incrementAndGet();

                        return cnt;
                    }
                }));

                for (UUID node : nodes)
                    waiting.add(new RemoteFragmentKey(node, fragment.fragmentId()));
            }

            state = QueryState.EXECUTING;
        }
    }

    /**
     * Can be called multiple times after receive each error
     * at {@link #onResponse(RemoteFragmentKey, Throwable)}.
     */
    @Override protected void tryClose(@Nullable Throwable failure) {
        QueryState state0 = null;

        synchronized (mux) {
            if (state == QueryState.CLOSED)
                return;

            if (state == QueryState.INITED || state == QueryState.PLANNING || state == QueryState.MAPPING) {
                state = QueryState.CLOSED;

                return;
            }

            if (state == QueryState.EXECUTING) {
                state0 = state = QueryState.CLOSING;

                root.closeInternal();
            }

            if (state == QueryState.CLOSING && waiting.isEmpty())
                state0 = state = QueryState.CLOSED;
        }

        if (state0 == QueryState.CLOSED) {
            try {
                IgniteException wrpEx = null;

                for (Map.Entry<UUID, AtomicInteger> entry : remoteFragments.entrySet()) {
                    try {
                        // Don't send close message if all remote fragments are finished (query is self-closed on the
                        // remote node in this case).
                        if (!entry.getKey().equals(root.context().localNodeId()) && entry.getValue().get() > 0)
                            exch.closeQuery(entry.getKey(), id());
                    }
                    catch (IgniteCheckedException e) {
                        if (wrpEx == null)
                            wrpEx = new IgniteException("Failed to send cancel message. [nodeId=" + entry.getKey() + ']', e);
                        else
                            wrpEx.addSuppressed(e);
                    }
                }

                if (wrpEx != null)
                    log.warning("An exception occurs during the query cancel", wrpEx);
            }
            finally {
                super.tryClose(failure == null && root != null ? root.failure() : failure);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void cancel() {
        cancel.cancel();

        U.closeQuiet(root);
        tryClose(queryCanceledException());
    }

    /** */
    public PlanningContext planningContext() {
        synchronized (mux) {
            if (state == QueryState.CLOSED || state == QueryState.CLOSING)
                throw queryCanceledException();

            if (state == QueryState.EXECUTING || state == QueryState.MAPPING) {
                throw new IgniteSQLException(
                    "Invalid query flow",
                    IgniteQueryErrorCode.UNKNOWN
                );
            }

            if (pctx == null) {
                state = QueryState.PLANNING;

                pctx = PlanningContext.builder()
                    .parentContext(ctx)
                    .query(sql)
                    .parameters(params)
                    .plannerTimeout(plannerTimeout)
                    .build();

                try {
                    cancel.add(() -> pctx.unwrap(CancelFlag.class).requestCancel());
                }
                catch (QueryCancelledException e) {
                    throw new IgniteSQLException(e.getMessage(), IgniteQueryErrorCode.QUERY_CANCELED, e);
                }
            }

            return pctx;
        }
    }

    /** */
    public Iterator<RowT> iterator() {
        return root;
    }

    /** */
    public long localQueryId() {
        return locQryId;
    }

    /** */
    public void localQueryId(long locQryId) {
        this.locQryId = locQryId;
    }

    /** */
    @Override public void onNodeLeft(UUID nodeId) {
        List<RemoteFragmentKey> fragments = null;

        synchronized (mux) {
            fragments = waiting.stream().filter(f -> f.nodeId().equals(nodeId)).collect(Collectors.toList());
        }

        if (!F.isEmpty(fragments)) {
            ClusterTopologyCheckedException ex = new ClusterTopologyCheckedException(
                "Failed to start query, node left. nodeId=" + nodeId);

            for (RemoteFragmentKey fragment : fragments)
                onResponse(fragment, ex);
        }
    }

    /** */
    public void onResponse(UUID nodeId, long fragmentId, Throwable error) {
        onResponse(new RemoteFragmentKey(nodeId, fragmentId), error);
    }

    /** */
    private void onResponse(RemoteFragmentKey fragment, Throwable error) {
        QueryState state;
        synchronized (mux) {
            waiting.remove(fragment);

            state = this.state;
        }

        if (error != null)
            onError(error);
        else if (state == QueryState.CLOSING)
            tryClose(null);
    }

    /** */
    @Override public void onError(Throwable error) {
        root.onError(error);

        tryClose(error);
    }

    /** {@inheritDoc} */
    @Override public void onInboundExchangeStarted(UUID nodeId, long exchangeId) {
        onResponse(nodeId, exchangeId, null);
    }

    /** {@inheritDoc} */
    @Override public void onInboundExchangeFinished(UUID nodeId, long exchangeId) {
        AtomicInteger cnt = remoteFragments.get(nodeId);

        assert cnt != null : nodeId;

        cnt.decrementAndGet();
    }

    /** {@inheritDoc} */
    @Override public void onOutboundExchangeStarted(UUID nodeId, long exchangeId) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onOutboundExchangeFinished(long exchangeId) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return id().hashCode();
    }

    /** {@inheritDoc} */
    @Override public String queryInfo(@Nullable String additionalInfo) {
        StringBuilder msgSb = new StringBuilder();

        msgSb.append(" [queryId=").append(id());
        msgSb.append(", globalQueryId=").append(QueryUtils.globalQueryId(initiatorNodeId(), localQueryId()));

        if (additionalInfo != null)
            msgSb.append(", ").append(additionalInfo);

        msgSb.append(", planningTime=").append(root == null ? U.currentTimeMillis() - startTs : planningTime).append("ms")
            .append(", execTime=").append(root == null ? 0 : root.execTime()).append("ms")
            .append(", idleTime=").append(root == null ? 0 : root.idleTime()).append("ms")
            .append(", timeout=").append(totalTimeout).append("ms")
            .append(", type=CALCITE")
            .append(", state=").append(state)
            .append(", schema=").append(ctx.schemaName())
            .append(", sql='").append(sql).append('\'')
            .append(", dump='").append(root == null ? null : root.dump("")).append('\'');

        msgSb.append(']');

        return msgSb.toString();
    }

    /** {@inheritDoc} */
    @Override public long time() {
        return root == null ? U.currentTimeMillis() - startTs : planningTime + root.execTime();
    }

    /**
     * @return Time left to execute the query, {@code -1} if timeout is not set, {@code 0} if timeout reached.
     */
    public long remainingTime() {
        if (totalTimeout <= 0)
            return -1;

        long curTimeout = totalTimeout - (U.currentTimeMillis() - startTs);

        return curTimeout <= 0 ? 0 : curTimeout;
    }

    /** */
    @Override public String toString() {
        return S.toString(RootQuery.class, this);
    }
}
