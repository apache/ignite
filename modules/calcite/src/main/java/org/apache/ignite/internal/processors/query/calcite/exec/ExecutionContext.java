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

import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.ApplicationContext;
import org.apache.ignite.cache.ApplicationContextProvider;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.ExpressionFactory;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.ExpressionFactoryImpl;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.ReflectiveCallNotNullImplementor;
import org.apache.ignite.internal.processors.query.calcite.exec.tracker.ExecutionNodeMemoryTracker;
import org.apache.ignite.internal.processors.query.calcite.exec.tracker.IoTracker;
import org.apache.ignite.internal.processors.query.calcite.exec.tracker.MemoryTracker;
import org.apache.ignite.internal.processors.query.calcite.exec.tracker.RowTracker;
import org.apache.ignite.internal.processors.query.calcite.metadata.ColocationGroup;
import org.apache.ignite.internal.processors.query.calcite.metadata.FragmentDescription;
import org.apache.ignite.internal.processors.query.calcite.prepare.AbstractQueryContext;
import org.apache.ignite.internal.processors.query.calcite.prepare.BaseDataContext;
import org.apache.ignite.internal.processors.query.calcite.prepare.BaseQueryContext;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.processors.query.calcite.util.TypeUtils;
import org.apache.ignite.internal.processors.resource.GridResourceProcessor;
import org.apache.ignite.internal.util.lang.RunnableX;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.query.calcite.util.Commons.checkRange;

/**
 * Runtime context allowing access to the tables in a database.
 */
public class ExecutionContext<Row> extends AbstractQueryContext implements DataContext {
    /** Placeholder for values, which expressions is not specified. */
    private static final Object UNSPECIFIED_VALUE = new Object();

    /** Placeholder for NULL values in search bounds. */
    private static final Object NULL_BOUND = new Object();

    /** */
    private final UUID qryId;

    /** */
    private final UUID locNodeId;

    /** */
    private final UUID originatingNodeId;

    /** */
    private final AffinityTopologyVersion topVer;

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
    private final BaseDataContext baseDataContext;

    /** */
    private final MemoryTracker qryMemoryTracker;

    /** */
    private final IoTracker ioTracker;

    /** */
    private final long timeout;

    /** */
    private final long startTs;

    /** Objects to call UDF with injected resources. */
    private final Map<String, Object> funcTargets = new ConcurrentHashMap<>();

    /** Application context provider for UDF. */
    private final ApplicationContextProvider appCtxProv = new ApplicationContextProviderImpl();

    /** */
    private Object[] correlations = new Object[16];

    /**
     * @param qctx Parent base query context.
     * @param qryId Query ID.
     * @param fragmentDesc Partitions information.
     * @param params Parameters.
     */
    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
    public ExecutionContext(
        BaseQueryContext qctx,
        QueryTaskExecutor executor,
        UUID qryId,
        UUID locNodeId,
        UUID originatingNodeId,
        AffinityTopologyVersion topVer,
        FragmentDescription fragmentDesc,
        RowHandler<Row> handler,
        MemoryTracker qryMemoryTracker,
        IoTracker ioTracker,
        long timeout,
        Map<String, Object> params
    ) {
        super(qctx);

        this.executor = executor;
        this.qryId = qryId;
        this.locNodeId = locNodeId;
        this.originatingNodeId = originatingNodeId;
        this.topVer = topVer;
        this.fragmentDesc = fragmentDesc;
        this.handler = handler;
        this.qryMemoryTracker = qryMemoryTracker;
        this.ioTracker = ioTracker;
        this.params = params;
        this.timeout = timeout;

        startTs = U.currentTimeMillis();

        baseDataContext = new BaseDataContext(qctx.typeFactory());

        expressionFactory = new ExpressionFactoryImpl<>(
            this,
            qctx.typeFactory(),
            qctx.config().getParserConfig().conformance(),
            qctx.rexBuilder()
        );
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

    /** */
    public List<UUID> remotes(long exchangeId) {
        return fragmentDesc.remotes().get(exchangeId);
    }

    /** */
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
     * @return Local node ID.
     */
    public UUID localNodeId() {
        return locNodeId;
    }

    /**
     * @return Originating node ID (the node, who started the execution).
     */
    public UUID originatingNodeId() {
        return originatingNodeId;
    }

    /**
     * @return Topology version.
     */
    public AffinityTopologyVersion topologyVersion() {
        return topVer;
    }

    /** */
    public IgniteLogger logger() {
        return unwrap(BaseQueryContext.class).logger();
    }

    /** {@inheritDoc} */
    @Override public SchemaPlus getRootSchema() {
        return baseDataContext.getRootSchema();
    }

    /** {@inheritDoc} */
    @Override public IgniteTypeFactory getTypeFactory() {
        return baseDataContext.getTypeFactory();
    }

    /** {@inheritDoc} */
    @Override public QueryProvider getQueryProvider() {
        return baseDataContext.getQueryProvider();
    }

    /** {@inheritDoc} */
    @Override public Object get(String name) {
        if (Variable.CANCEL_FLAG.camelName.equals(name))
            return cancelFlag;
        if (name.startsWith("?"))
            return TypeUtils.toInternal(this, params.get(name));

        return baseDataContext.get(name);
    }

    /** */
    public Object getParameter(String name, Type storageType) {
        assert name.startsWith("?") : name;

        return TypeUtils.toInternal(this, params.get(name), storageType);
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
                if (!isCancelled())
                    task.run();
            }
            catch (Throwable e) {
                onError.accept(e);

                throw new IgniteException("Unexpected exception", e);
            }
        });
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

    /** */
    public boolean isTimedOut() {
        return timeout > 0 && U.currentTimeMillis() - startTs >= timeout;
    }

    /** */
    public Object unspecifiedValue() {
        return UNSPECIFIED_VALUE;
    }

    /** */
    public Object nullBound() {
        return NULL_BOUND;
    }

    /** */
    public <R> RowTracker<R> createNodeMemoryTracker(long rowOverhead) {
        return ExecutionNodeMemoryTracker.create(qryMemoryTracker, rowOverhead);
    }

    /** */
    public IoTracker ioTracker() {
        return ioTracker;
    }

    /**
     * Create an object contained a user defined function and inject resources.
     * Used by {@link ReflectiveCallNotNullImplementor} while prepare user function call.
     *
     * @param targetCls Classname of the object.
     * @return Object with injected resources.
     */
    public Object target(String targetCls) {
        return funcTargets.computeIfAbsent(targetCls, ignore -> {
            try {
                Class<?> funcCls = getClass().getClassLoader().loadClass(targetCls);

                Object target = funcCls.getConstructor().newInstance();

                unwrap(GridResourceProcessor.class).injectToUserDefinedFunction(target, appCtxProv);

                return target;
            }
            catch (Exception e) {
                throw new IgniteException("Failed to construct object for UDF. " +
                    "Class " + targetCls + " must have public zero-args constructor.", e);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        ExecutionContext<?> ctx = (ExecutionContext<?>)o;

        return qryId.equals(ctx.qryId) && fragmentDesc.fragmentId() == ctx.fragmentDesc.fragmentId();
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(qryId, fragmentDesc.fragmentId());
    }

    /** */
    private class ApplicationContextProviderImpl implements ApplicationContextProvider {
        @Override public @Nullable ApplicationContext getApplicationContext() {
            return unwrap(ApplicationContext.class);
        }
    }
}
