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

package org.apache.ignite.internal.processors.query.calcite.exec;

import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.ExpressionFactory;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.ExpressionFactoryImpl;
import org.apache.ignite.internal.processors.query.calcite.metadata.ColocationGroup;
import org.apache.ignite.internal.processors.query.calcite.metadata.FragmentDescription;
import org.apache.ignite.internal.processors.query.calcite.prepare.PlanningContext;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.processors.query.calcite.util.TypeUtils;
import org.apache.ignite.lang.IgniteInternalException;
import org.jetbrains.annotations.NotNull;

import static org.apache.ignite.internal.processors.query.calcite.util.Commons.checkRange;

/**
 * Runtime context allowing access to the tables in a database.
 */
public class ExecutionContext<Row> implements DataContext {
    /** */
    private static final TimeZone TIME_ZONE = TimeZone.getDefault(); // TODO DistributedSqlConfiguration#timeZone

    /** */
    private final UUID qryId;

    /** */
    private final PlanningContext ctx;

    /** */
    private final FragmentDescription fragmentDesc;

    /** */
    private final Map<String, Object> params;

    /** */
    private final QueryTaskExecutor executor;

    /** */
    private final RowHandler<Row> handler;

    /** */
    private final ExpressionFactory<Row> expressionFactory;

    /** */
    private final AtomicBoolean cancelFlag = new AtomicBoolean();

    /** */
    private Object[] correlations = new Object[16];

    /**
     * @param executor Task executor.
     * @param ctx Parent context.
     * @param qryId Query ID.
     * @param fragmentDesc Partitions information.
     * @param handler Row handler.
     * @param params Parameters.
     */
    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
    public ExecutionContext(
        QueryTaskExecutor executor,
        PlanningContext ctx,
        UUID qryId,
        FragmentDescription fragmentDesc,
        RowHandler<Row> handler,
        Map<String, Object> params
    ) {
        this.executor = executor;
        this.ctx = ctx;
        this.qryId = qryId;
        this.fragmentDesc = fragmentDesc;
        this.handler = handler;
        this.params = params;

        expressionFactory = new ExpressionFactoryImpl<>(this, ctx.typeFactory(), ctx.conformance());
    }

    /**
     * @return Parent context.
     */
    public PlanningContext planningContext() {
        return ctx;
    }

    /**
     * @return Query ID.
     */
    public UUID queryId() {
        return qryId;
    }

    /**
     * @return Fragment ID.
     */
    public long fragmentId() {
        return fragmentDesc.fragmentId();
    }

    /**
     * @return Target mapping.
     */
    public ColocationGroup target() {
        return fragmentDesc.target();
    }

    /**
     * @param exchangeId ExchangeId to find remote nodes for.
     * @return Remote nodes for given exchangeId.
     */
    public List<UUID> remotes(long exchangeId) {
        return fragmentDesc.remotes().get(exchangeId);
    }

    /**
     * @param sourceId SourceId to find colocation group for.
     * @return Colocation group for given sourceId.
     */
    public ColocationGroup group(long sourceId) {
        return fragmentDesc.mapping().findGroup(sourceId);
    }

    /**
     * @return Keep binary flag.
     */
    public boolean keepBinary() {
        return true; // TODO
    }

    /**
     * @return Handler to access row fields.
     */
    public RowHandler<Row> rowHandler() {
        return handler;
    }

    /**
     * @return Expression factory.
     */
    public ExpressionFactory<Row> expressionFactory() {
        return expressionFactory;
    }

    /**
     * @return Originating node ID.
     */
    public UUID originatingNodeId() {
        return planningContext().originatingNodeId();
    }

    /** {@inheritDoc} */
    @Override public SchemaPlus getRootSchema() {
        return ctx.schema();
    }

    /** {@inheritDoc} */
    @Override public IgniteTypeFactory getTypeFactory() {
        return ctx.typeFactory();
    }

    /** {@inheritDoc} */
    @Override public QueryProvider getQueryProvider() {
        return null; // TODO
    }

    /** {@inheritDoc} */
    @Override public Object get(String name) {
        if (Variable.CANCEL_FLAG.camelName.equals(name))
            return cancelFlag;
        if (Variable.TIME_ZONE.camelName.equals(name))
            return TIME_ZONE; // TODO DistributedSqlConfiguration#timeZone
        if (name.startsWith("?"))
            return TypeUtils.toInternal(this, params.get(name));

        return params.get(name);
    }

    /**
     * Gets correlated value.
     *
     * @param id Correlation ID.
     * @return Correlated value.
     */
    public @NotNull Object getCorrelated(int id) {
        checkRange(correlations, id);

        return correlations[id];
    }

    /**
     * Sets correlated value.
     *
     * @param id Correlation ID.
     * @param value Correlated value.
     */
    public void setCorrelated(@NotNull Object value, int id) {
        correlations = Commons.ensureCapacity(correlations, id + 1);

        correlations[id] = value;
    }

    /**
     * Executes a query task.
     *
     * @param task Query task.
     */
    public void execute(RunnableX task, Consumer<Throwable> onError) {
        if (isCancelled())
            return;

        executor.execute(qryId, fragmentId(), () -> {
            try {
                task.run();
            }
            catch (Throwable e) {
                onError.accept(e);

                throw new IgniteInternalException("Unexpected exception", e);
            }
        });
    }

    /**
     * Submits a Runnable task for execution and returns a Future
     * representing that task. The Future's {@code get} method will
     * return {@code null} upon <em>successful</em> completion.
     *
     * @param task the task to submit.
     * @return a {@link CompletableFuture} representing pending task
     */
    public CompletableFuture<?> submit(RunnableX task, Consumer<Throwable> onError) {
        assert !isCancelled() : "Call submit after execution was cancelled.";

        return executor.submit(qryId, fragmentId(), () -> {
            try {
                task.run();
            }
            catch (Throwable e) {
                onError.accept(e);

                throw new IgniteInternalException("Unexpected exception", e);
            }
        });
    }

    /** */
    @FunctionalInterface
    public interface RunnableX {
        /** */
        void run() throws Exception;
    }

    /**
     * Sets cancel flag, returns {@code true} if flag was changed by this call.
     *
     * @return {@code True} if flag was changed by this call.
     */
    public boolean cancel() {
        return !cancelFlag.get() && cancelFlag.compareAndSet(false, true);
    }

    /** */
    public boolean isCancelled() {
        return cancelFlag.get();
    }
}
