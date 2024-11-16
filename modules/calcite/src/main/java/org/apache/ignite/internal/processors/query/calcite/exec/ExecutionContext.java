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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.SessionContext;
import org.apache.ignite.cache.SessionContextProvider;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRowAdapter;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxEntry;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.ExpressionFactory;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.ExpressionFactoryImpl;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.ReflectiveCallNotNullImplementor;
import org.apache.ignite.internal.processors.query.calcite.exec.tracker.ExecutionNodeMemoryTracker;
import org.apache.ignite.internal.processors.query.calcite.exec.tracker.IoTracker;
import org.apache.ignite.internal.processors.query.calcite.exec.tracker.MemoryTracker;
import org.apache.ignite.internal.processors.query.calcite.exec.tracker.RowTracker;
import org.apache.ignite.internal.processors.query.calcite.message.QueryTxEntry;
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
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
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
    private final Collection<QueryTxEntry> qryTxEntries;

    /** */
    private final long startTs;

    /** Map associates UDF name to instance of class that contains this UDF. */
    private final Map<String, Object> udfObjs = new ConcurrentHashMap<>();

    /** Session context provider injected into UDF targets. */
    private final SessionContextProvider sesCtxProv = new SessionContextProviderImpl();

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
        Map<String, Object> params,
        @Nullable Collection<QueryTxEntry> qryTxEntries
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
        this.qryTxEntries = qryTxEntries;

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
     * @return Transaction write map.
     */
    public Collection<QueryTxEntry> getQryTxEntries() {
        return qryTxEntries;
    }

    /** */
    public static Collection<QueryTxEntry> transactionChanges(
        Collection<IgniteTxEntry> writeEntries
    ) {
        if (F.isEmpty(writeEntries))
            return null;

        Collection<QueryTxEntry> res = new ArrayList<>();

        for (IgniteTxEntry e : writeEntries) {
            CacheObject val = e.value();

            if (!F.isEmpty(e.entryProcessors()))
                val = e.applyEntryProcessors(val);

            res.add(new QueryTxEntry(e.cacheId(), e.key(), val, e.explicitVersion()));
        }

        return res;
    }

    /**
     * @param cacheId Cache id.
     * @param parts Partitions set.
     * @param mapper Mapper to specific data type.
     * @return First, set of object changed in transaction, second, list of transaction data in required format.
     * @param <R> Required type.
     */
    public <R> IgniteBiTuple<Set<KeyCacheObject>, List<R>> transactionChanges(
        int cacheId,
        int[] parts,
        Function<CacheDataRow, R> mapper
    ) {
        if (F.isEmpty(qryTxEntries))
            return F.t(Collections.emptySet(), Collections.emptyList());

        // Expecting parts are sorted or almost sorted and amount of transaction entries are relatively small.
        if (parts != null && !F.isSorted(parts))
            Arrays.sort(parts);

        Set<KeyCacheObject> changedKeys = new HashSet<>(qryTxEntries.size());
        List<R> newAndUpdatedRows = new ArrayList<>(qryTxEntries.size());

        for (QueryTxEntry e : qryTxEntries) {
            int part = e.key().partition();

            assert part != -1;

            if (e.cacheId() != cacheId)
                continue;

            if (parts != null && Arrays.binarySearch(parts, part) < 0)
                continue;

            changedKeys.add(e.key());

            CacheObject val = e.value();

            if (val != null) { // Mix only updated or inserted entries. In case val == null entry removed.
                newAndUpdatedRows.add(mapper.apply(new CacheDataRowAdapter(
                    e.key(),
                    val,
                    e.version(),
                    CU.EXPIRE_TIME_ETERNAL // Expire time calculated on commit, can use eternal here.
                )));
            }
        }

        return F.t(changedKeys, newAndUpdatedRows);
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
     * Return an object contained a user defined function. If not exist yet, then instantiate the object and inject resources into it.
     * Used by {@link ReflectiveCallNotNullImplementor} while it is preparing user function call.
     *
     * @param udfClsName Classname of the class contained UDF.
     * @return Object with injected resources.
     */
    public Object udfObject(String udfClsName) {
        return udfObjs.computeIfAbsent(udfClsName, ignore -> {
            try {
                Class<?> funcCls = getClass().getClassLoader().loadClass(udfClsName);

                Object target = funcCls.getConstructor().newInstance();

                unwrap(GridResourceProcessor.class).injectToUserDefinedFunction(target, sesCtxProv);

                return target;
            }
            catch (Exception e) {
                throw new IgniteException("Failed to instantiate an object for UDF. " +
                    "Class " + udfClsName + " must have public zero-args constructor.", e);
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
    private class SessionContextProviderImpl implements SessionContextProvider {
        @Override public @Nullable SessionContext getSessionContext() {
            return unwrap(SessionContext.class);
        }
    }
}
