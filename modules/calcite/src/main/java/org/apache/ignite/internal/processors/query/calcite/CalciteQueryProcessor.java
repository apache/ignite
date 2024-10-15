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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.apache.calcite.DataContexts;
import org.apache.calcite.config.Lex;
import org.apache.calcite.config.NullCollation;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDdl;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.ignite.SystemProperty;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.QueryCancelledException;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.calcite.CalciteQueryEngineConfiguration;
import org.apache.ignite.configuration.QueryEngineConfiguration;
import org.apache.ignite.events.SqlQueryExecutionEvent;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxManager;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.failure.FailureProcessor;
import org.apache.ignite.internal.processors.query.GridQueryFieldMetadata;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.QueryContext;
import org.apache.ignite.internal.processors.query.QueryEngine;
import org.apache.ignite.internal.processors.query.QueryParserMetricsHolder;
import org.apache.ignite.internal.processors.query.QueryProperties;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.calcite.exec.ArrayRowHandler;
import org.apache.ignite.internal.processors.query.calcite.exec.ExchangeService;
import org.apache.ignite.internal.processors.query.calcite.exec.ExchangeServiceImpl;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionService;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionServiceImpl;
import org.apache.ignite.internal.processors.query.calcite.exec.MailboxRegistry;
import org.apache.ignite.internal.processors.query.calcite.exec.MailboxRegistryImpl;
import org.apache.ignite.internal.processors.query.calcite.exec.QueryTaskExecutor;
import org.apache.ignite.internal.processors.query.calcite.exec.QueryTaskExecutorImpl;
import org.apache.ignite.internal.processors.query.calcite.exec.TimeoutService;
import org.apache.ignite.internal.processors.query.calcite.exec.TimeoutServiceImpl;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.RexExecutorImpl;
import org.apache.ignite.internal.processors.query.calcite.hint.HintsConfig;
import org.apache.ignite.internal.processors.query.calcite.message.MessageService;
import org.apache.ignite.internal.processors.query.calcite.message.MessageServiceImpl;
import org.apache.ignite.internal.processors.query.calcite.metadata.AffinityService;
import org.apache.ignite.internal.processors.query.calcite.metadata.AffinityServiceImpl;
import org.apache.ignite.internal.processors.query.calcite.metadata.MappingService;
import org.apache.ignite.internal.processors.query.calcite.metadata.MappingServiceImpl;
import org.apache.ignite.internal.processors.query.calcite.metadata.cost.IgniteCostFactory;
import org.apache.ignite.internal.processors.query.calcite.prepare.CacheKey;
import org.apache.ignite.internal.processors.query.calcite.prepare.ExplainPlan;
import org.apache.ignite.internal.processors.query.calcite.prepare.FieldsMetadata;
import org.apache.ignite.internal.processors.query.calcite.prepare.IgniteConvertletTable;
import org.apache.ignite.internal.processors.query.calcite.prepare.IgniteTypeCoercion;
import org.apache.ignite.internal.processors.query.calcite.prepare.MultiStepPlan;
import org.apache.ignite.internal.processors.query.calcite.prepare.PrepareServiceImpl;
import org.apache.ignite.internal.processors.query.calcite.prepare.QueryPlan;
import org.apache.ignite.internal.processors.query.calcite.prepare.QueryPlanCache;
import org.apache.ignite.internal.processors.query.calcite.prepare.QueryPlanCacheImpl;
import org.apache.ignite.internal.processors.query.calcite.schema.SchemaHolder;
import org.apache.ignite.internal.processors.query.calcite.schema.SchemaHolderImpl;
import org.apache.ignite.internal.processors.query.calcite.sql.IgniteSqlAlterUser;
import org.apache.ignite.internal.processors.query.calcite.sql.IgniteSqlConformance;
import org.apache.ignite.internal.processors.query.calcite.sql.IgniteSqlCreateTable;
import org.apache.ignite.internal.processors.query.calcite.sql.IgniteSqlCreateUser;
import org.apache.ignite.internal.processors.query.calcite.sql.IgniteSqlOption;
import org.apache.ignite.internal.processors.query.calcite.sql.fun.IgniteOwnSqlOperatorTable;
import org.apache.ignite.internal.processors.query.calcite.sql.fun.IgniteStdSqlOperatorTable;
import org.apache.ignite.internal.processors.query.calcite.sql.generated.IgniteSqlParserImpl;
import org.apache.ignite.internal.processors.query.calcite.trait.CorrelationTraitDef;
import org.apache.ignite.internal.processors.query.calcite.trait.DistributionTraitDef;
import org.apache.ignite.internal.processors.query.calcite.trait.RewindabilityTraitDef;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeSystem;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.processors.query.calcite.util.LifecycleAware;
import org.apache.ignite.internal.processors.query.calcite.util.Service;
import org.apache.ignite.internal.processors.security.SecurityUtils;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.IgniteSystemProperties.getLong;
import static org.apache.ignite.events.EventType.EVT_SQL_QUERY_EXECUTION;

/** */
public class CalciteQueryProcessor extends GridProcessorAdapter implements QueryEngine {
    /**
     * Default planner timeout, in ms.
     */
    private static final long DFLT_IGNITE_CALCITE_PLANNER_TIMEOUT = 15000;

    /**
     * Planner timeout property name.
     */
    @SystemProperty(value = "Timeout of calcite based sql engine's planner, in ms", type = Long.class,
        defaults = "" + DFLT_IGNITE_CALCITE_PLANNER_TIMEOUT)
    public static final String IGNITE_CALCITE_PLANNER_TIMEOUT = "IGNITE_CALCITE_PLANNER_TIMEOUT";

    /** */
    public static final FrameworkConfig FRAMEWORK_CONFIG = Frameworks.newConfigBuilder()
        .executor(new RexExecutorImpl(DataContexts.EMPTY))
        .sqlToRelConverterConfig(SqlToRelConverter.config()
            .withRemoveSortInSubQuery(false)
            .withTrimUnusedFields(true)
            // currently SqlToRelConverter creates not optimal plan for both optimization and execution
            // so it's better to disable such rewriting right now
            // TODO: remove this after IGNITE-14277
            .withInSubQueryThreshold(Integer.MAX_VALUE)
            .withDecorrelationEnabled(true)
            .withExpand(false)
            .withHintStrategyTable(HintsConfig.buildHintTable())
        )
        .convertletTable(IgniteConvertletTable.INSTANCE)
        .parserConfig(
            SqlParser.config()
                .withParserFactory(IgniteSqlParserImpl.FACTORY)
                .withLex(Lex.ORACLE)
                .withConformance(IgniteSqlConformance.INSTANCE))
        .sqlValidatorConfig(SqlValidator.Config.DEFAULT
            .withIdentifierExpansion(true)
            .withDefaultNullCollation(NullCollation.LOW)
            .withSqlConformance(IgniteSqlConformance.INSTANCE)
            .withTypeCoercionFactory(IgniteTypeCoercion::new))
        // Dialects support.
        .operatorTable(SqlOperatorTables.chain(IgniteStdSqlOperatorTable.INSTANCE, IgniteOwnSqlOperatorTable.instance()))
        // Context provides a way to store data within the planner session that can be accessed in planner rules.
        .context(Contexts.empty())
        // Custom cost factory to use during optimization
        .costFactory(new IgniteCostFactory())
        .typeSystem(IgniteTypeSystem.INSTANCE)
        .traitDefs(new RelTraitDef<?>[] {
            ConventionTraitDef.INSTANCE,
            RelCollationTraitDef.INSTANCE,
            DistributionTraitDef.INSTANCE,
            RewindabilityTraitDef.INSTANCE,
            CorrelationTraitDef.INSTANCE,
        })
        .build();

    /** Query planner timeout. */
    private final long queryPlannerTimeout = getLong(IGNITE_CALCITE_PLANNER_TIMEOUT,
        DFLT_IGNITE_CALCITE_PLANNER_TIMEOUT);

    /** */
    private final QueryPlanCache qryPlanCache;

    /** */
    private final QueryParserMetricsHolder parserMetrics;

    /** */
    private final QueryTaskExecutor taskExecutor;

    /** */
    private final FailureProcessor failureProcessor;

    /** */
    private final AffinityService partSvc;

    /** */
    private final SchemaHolder schemaHolder;

    /** */
    private final MessageService msgSvc;

    /** */
    private final ExchangeService exchangeSvc;

    /** */
    private final MappingService mappingSvc;

    /** */
    private final MailboxRegistry mailboxRegistry;

    /** */
    private final ExecutionService<Object[]> executionSvc;

    /** */
    private final PrepareServiceImpl prepareSvc;

    /** */
    private final TimeoutService timeoutSvc;

    /** */
    private final QueryRegistry qryReg;

    /** */
    private final CalciteQueryEngineConfiguration cfg;

    /** */
    private final DistributedCalciteConfiguration distrCfg;

    /** */
    private volatile boolean started;

    /**
     * @param ctx Kernal context.
     */
    public CalciteQueryProcessor(GridKernalContext ctx) {
        super(ctx);

        failureProcessor = ctx.failure();
        schemaHolder = new SchemaHolderImpl(ctx);
        qryPlanCache = new QueryPlanCacheImpl(ctx);
        parserMetrics = new QueryParserMetricsHolder(ctx.metric());
        mailboxRegistry = new MailboxRegistryImpl(ctx);
        taskExecutor = new QueryTaskExecutorImpl(ctx);
        executionSvc = new ExecutionServiceImpl<>(ctx, ArrayRowHandler.INSTANCE);
        partSvc = new AffinityServiceImpl(ctx);
        msgSvc = new MessageServiceImpl(ctx);
        mappingSvc = new MappingServiceImpl(ctx);
        exchangeSvc = new ExchangeServiceImpl(ctx);
        prepareSvc = new PrepareServiceImpl(ctx);
        timeoutSvc = new TimeoutServiceImpl(ctx);
        qryReg = new QueryRegistryImpl(ctx);

        QueryEngineConfiguration[] qryEnginesCfg = ctx.config().getSqlConfiguration().getQueryEnginesConfiguration();

        if (F.isEmpty(qryEnginesCfg))
            cfg = new CalciteQueryEngineConfiguration();
        else {
            cfg = (CalciteQueryEngineConfiguration)Arrays.stream(qryEnginesCfg)
                .filter(c -> c instanceof CalciteQueryEngineConfiguration)
                .findAny()
                .orElse(new CalciteQueryEngineConfiguration());
        }

        distrCfg = new DistributedCalciteConfiguration(ctx, log);
    }

    /**
     * @return Affinity service.
     */
    public AffinityService affinityService() {
        return partSvc;
    }

    /**
     * @return Query cache.
     */
    public QueryPlanCache queryPlanCache() {
        return qryPlanCache;
    }

    /**
     * @return Task executor.
     */
    public QueryTaskExecutor taskExecutor() {
        return taskExecutor;
    }

    /**
     * @return Schema holder.
     */
    public SchemaHolder schemaHolder() {
        return schemaHolder;
    }

    /**
     * @return Message service.
     */
    public MessageService messageService() {
        return msgSvc;
    }

    /**
     * @return Mapping service.
     */
    public MappingService mappingService() {
        return mappingSvc;
    }

    /**
     * @return Exchange service.
     */
    public ExchangeService exchangeService() {
        return exchangeSvc;
    }

    /**
     * @return Mailbox registry.
     */
    public MailboxRegistry mailboxRegistry() {
        return mailboxRegistry;
    }

    /**
     * @return Failure processor.
     */
    public FailureProcessor failureProcessor() {
        return failureProcessor;
    }

    /** */
    public PrepareServiceImpl prepareService() {
        return prepareSvc;
    }

    /** */
    public TimeoutService timeoutService() {
        return timeoutSvc;
    }

    /** */
    public ExecutionService<Object[]> executionService() {
        return executionSvc;
    }

    /** {@inheritDoc} */
    @Override public void onKernalStart(boolean active) {
        onStart(ctx,
            executionSvc,
            mailboxRegistry,
            partSvc,
            schemaHolder,
            msgSvc,
            taskExecutor,
            mappingSvc,
            qryPlanCache,
            exchangeSvc,
            qryReg
        );

        started = true;
    }

    /** {@inheritDoc} */
    @Override public void onKernalStop(boolean cancel) {
        if (started) {
            started = false;

            onStop(
                qryReg,
                executionSvc,
                mailboxRegistry,
                partSvc,
                schemaHolder,
                msgSvc,
                taskExecutor,
                mappingSvc,
                qryPlanCache,
                exchangeSvc
            );
        }
    }

    /** {@inheritDoc} */
    @Override public List<FieldsQueryCursor<List<?>>> query(
        @Nullable QueryContext qryCtx,
        @Nullable String schemaName,
        String sql,
        Object... params
    ) throws IgniteSQLException {
        return parseAndProcessQuery(qryCtx, executionSvc::executePlan, schemaName, sql, params);
    }

    /** {@inheritDoc} */
    @Override public List<List<GridQueryFieldMetadata>> parameterMetaData(
        @Nullable QueryContext ctx,
        String schemaName,
        String sql
    ) throws IgniteSQLException {
        return parseAndProcessQuery(ctx, (qry, plan) -> fieldsMeta(plan, true), schemaName, sql);
    }

    /** {@inheritDoc} */
    @Override public List<List<GridQueryFieldMetadata>> resultSetMetaData(
        @Nullable QueryContext ctx,
        String schemaName,
        String sql
    ) throws IgniteSQLException {
        return parseAndProcessQuery(ctx, (qry, plan) -> fieldsMeta(plan, false), schemaName, sql);
    }

    /** {@inheritDoc} */
    @Override public List<FieldsQueryCursor<List<?>>> queryBatched(
        @Nullable QueryContext qryCtx,
        String schemaName,
        String sql,
        List<Object[]> batchedParams
    ) throws IgniteSQLException {
        SchemaPlus schema = schemaHolder.schema(schemaName);

        assert schema != null : "Schema not found: " + schemaName;

        SqlNodeList qryNodeList = Commons.parse(sql, FRAMEWORK_CONFIG.getParserConfig());

        if (qryNodeList.size() != 1) {
            throw new IgniteSQLException("Multiline statements are not supported in batched query",
                IgniteQueryErrorCode.PARSING);
        }

        SqlNode qryNode = qryNodeList.get(0);

        if (qryNode.getKind() != SqlKind.INSERT && qryNode.getKind() != SqlKind.UPDATE
            && qryNode.getKind() != SqlKind.MERGE && qryNode.getKind() != SqlKind.DELETE) {
            throw new IgniteSQLException("Unexpected operation kind for batched query [kind=" + qryNode.getKind() + "]",
                IgniteQueryErrorCode.UNEXPECTED_OPERATION);
        }

        List<FieldsQueryCursor<List<?>>> cursors = new ArrayList<>(batchedParams.size());

        List<RootQuery<Object[]>> qrys = new ArrayList<>(batchedParams.size());

        BiFunction<RootQuery<Object[]>, Object[], QueryPlan> planSupplier =
            new BiFunction<RootQuery<Object[]>, Object[], QueryPlan>() {
                private QueryPlan plan;

                @Override public QueryPlan apply(RootQuery<Object[]> qry, Object[] params) {
                    if (plan == null) {
                        AtomicBoolean miss = new AtomicBoolean();

                        plan = queryPlanCache().queryPlan(
                                new CacheKey(schema.getName(), sql, contextKey(qryCtx), params),
                                () -> {
                                    miss.set(true);

                                    return prepareSvc.prepareSingle(qryNode, qry.planningContext());
                                });

                        if (miss.get())
                            parserMetrics.countCacheMiss();
                        else
                            parserMetrics.countCacheHit();
                    }

                    return plan;
                }
            };

        for (final Object[] batch: batchedParams) {
            FieldsQueryCursor<List<?>> cur = processQuery(qryCtx, qry ->
                executionSvc.executePlan(qry, planSupplier.apply(qry, batch)), schema.getName(),
                    removeSensitive(qryNode), qrys, batch);

            cursors.add(cur);
        }

        return cursors;
    }

    /** */
    private <T> List<T> parseAndProcessQuery(
        @Nullable QueryContext qryCtx,
        BiFunction<RootQuery<Object[]>, QueryPlan, T> action,
        @Nullable String schemaName,
        String sql,
        Object... params
    ) throws IgniteSQLException {
        ensureTransactionModeSupported(qryCtx);

        SchemaPlus schema = schemaHolder.schema(schemaName);

        assert schema != null : "Schema not found: " + schemaName;

        QueryPlan plan = queryPlanCache().queryPlan(new CacheKey(schema.getName(), sql, contextKey(qryCtx), params));

        if (plan != null) {
            parserMetrics.countCacheHit();

            return Collections.singletonList(
                processQuery(qryCtx, qry -> action.apply(qry, plan), schema.getName(), plan.query(), null, params)
            );
        }

        parserMetrics.countCacheMiss();

        QueryProperties qryProps = qryCtx != null ? qryCtx.unwrap(QueryProperties.class) : null;

        SqlNodeList qryList = Commons.parse(sql, FRAMEWORK_CONFIG.getParserConfig());

        if (qryList.size() > 1 && qryProps != null && qryProps.isFailOnMultipleStmts()) {
            throw new IgniteSQLException("Multiple statements queries are not supported.",
                IgniteQueryErrorCode.UNSUPPORTED_OPERATION);
        }

        List<T> res = new ArrayList<>(qryList.size());
        List<RootQuery<Object[]>> qrys = new ArrayList<>(qryList.size());

        for (final SqlNode sqlNode: qryList) {
            T singleRes = processQuery(qryCtx, qry -> {
                QueryPlan plan0;
                if (qryList.size() == 1) {
                    plan0 = queryPlanCache().queryPlan(
                        // Use source SQL to avoid redundant parsing next time.
                        new CacheKey(schema.getName(), sql, contextKey(qryCtx), params),
                        () -> prepareSvc.prepareSingle(sqlNode, qry.planningContext())
                    );
                }
                else
                    plan0 = prepareSvc.prepareSingle(sqlNode, qry.planningContext());

                return action.apply(qry, plan0);
            }, schema.getName(), removeSensitive(sqlNode), qrys, params);

            res.add(singleRes);
        }

        return res;
    }

    /** */
    private String removeSensitive(SqlNode qry) {
        if (QueryUtils.INCLUDE_SENSITIVE)
            return qry.toString();
        else {
            try {
                return qry.accept(
                    new SqlShuttle() {
                        @Override public SqlNode visit(SqlLiteral literal) {
                            return new SqlDynamicParam(-1, literal.getParserPosition());
                        }

                        @Override public SqlNode visit(SqlCall call) {
                            // Handle some special cases.
                            if (call instanceof IgniteSqlOption)
                                return call;
                            else if (call instanceof IgniteSqlCreateUser) {
                                return new IgniteSqlCreateUser(call.getParserPosition(), ((IgniteSqlCreateUser)call).user(),
                                    SqlLiteral.createCharString("hidden", SqlParserPos.ZERO));
                            }
                            else if (call instanceof IgniteSqlAlterUser) {
                                return new IgniteSqlAlterUser(call.getParserPosition(), ((IgniteSqlAlterUser)call).user(),
                                    SqlLiteral.createCharString("hidden", SqlParserPos.ZERO));
                            }
                            // Assume DDL statements except CREATE ... AS SELECT, and CREATE/ALTER USER can't contain
                            // sensitive data. Return these statements as is, since they can't be cloned by SqlShuttle
                            // correctly and can't be unparsed.
                            else if (call instanceof SqlDdl && !(call instanceof IgniteSqlCreateTable))
                                return call;

                            return super.visit(call);
                        }
                    }
                ).toString();
            }
            catch (Exception e) {
                String msg = "Unable to remove sensitive information from SQL node of class: " + qry.getClass().getName();

                log.warning(msg, e);

                return msg;
            }
        }
    }

    /** */
    private Object contextKey(QueryContext qryCtx) {
        if (qryCtx == null)
            return null;

        SqlFieldsQuery sqlFieldsQry = qryCtx.unwrap(SqlFieldsQuery.class);

        return sqlFieldsQry != null
            ? F.asList(sqlFieldsQry.isLocal(), sqlFieldsQry.isEnforceJoinOrder(), queryTransactionVersion(qryCtx) == null)
            : null;
    }

    /** */
    private static GridCacheVersion queryTransactionVersion(@Nullable QueryContext qryCtx) {
        return qryCtx == null ? null : qryCtx.unwrap(GridCacheVersion.class);
    }

    /** */
    private void ensureTransactionModeSupported(@Nullable QueryContext qryCtx) {
        if (!U.isTxAwareQueriesEnabled(ctx))
            return;

        GridCacheVersion ver = queryTransactionVersion(qryCtx);

        if (ver == null)
            return;

        IgniteTxManager.ensureTransactionModeSupported(ctx.cache().context().tm().tx(ver).isolation());
    }

    /** */
    private <T> T processQuery(
        @Nullable QueryContext qryCtx,
        Function<RootQuery<Object[]>, T> action,
        String schema,
        String sql,
        @Nullable List<RootQuery<Object[]>> qrys,
        Object... params
    ) {
        SqlFieldsQuery fldsQry = qryCtx != null ? qryCtx.unwrap(SqlFieldsQuery.class) : null;

        long timeout = fldsQry != null ? fldsQry.getTimeout() : 0;

        if (timeout <= 0)
            timeout = distrCfg.defaultQueryTimeout();

        RootQuery<Object[]> qry = new RootQuery<>(
            sql,
            schemaHolder.schema(schema),
            params,
            qryCtx,
            fldsQry != null && fldsQry.isLocal(),
            fldsQry != null && fldsQry.isEnforceJoinOrder(),
            fldsQry != null ? fldsQry.getPartitions() : null,
            exchangeSvc,
            (q, ex) -> qryReg.unregister(q.id(), ex),
            log,
            queryPlannerTimeout,
            timeout
        );

        if (qrys != null)
            qrys.add(qry);

        qryReg.register(qry);

        if (ctx.event().isRecordable(EVT_SQL_QUERY_EXECUTION)) {
            ctx.event().record(new SqlQueryExecutionEvent(
                ctx.discovery().localNode(),
                "SQL query execution.",
                sql,
                params,
                SecurityUtils.securitySubjectId(ctx))
            );
        }

        try {
            T res = action.apply(qry);

            // Queries without iterator (DDL, EXPLAIN, metadata requests) can be unregistered right after executing,
            // queries with iterator (SELECT, DML) must be unregistered only after fetching all data or on error/cancel.
            if (qry.iterator() == null)
                qryReg.unregister(qry.id(), null);

            return res;
        }
        catch (Throwable e) {
            boolean isCanceled = qry.isCancelled();

            if (qrys != null)
                qrys.forEach(RootQuery::cancel);

            if (isCanceled) {
                qryReg.unregister(qry.id(), new QueryCancelledException());

                throw new IgniteSQLException("The query was cancelled while planning",
                    IgniteQueryErrorCode.QUERY_CANCELED, e);
            }
            else {
                qryReg.unregister(qry.id(), e);

                throw e;
            }
        }
    }

    /**
     * @param plan Query plan.
     * @param isParamsMeta If {@code true}, return parameter metadata, otherwise result set metadata.
     * @return Return query fields metadata.
     */
    private List<GridQueryFieldMetadata> fieldsMeta(QueryPlan plan, boolean isParamsMeta) {
        IgniteTypeFactory typeFactory = Commons.typeFactory();

        switch (plan.type()) {
            case QUERY:
            case DML:
                MultiStepPlan msPlan = (MultiStepPlan)plan;

                FieldsMetadata meta = isParamsMeta ? msPlan.paramsMetadata() : msPlan.fieldsMetadata();

                return meta.queryFieldsMetadata(typeFactory);
            case EXPLAIN:
                ExplainPlan exPlan = (ExplainPlan)plan;

                return isParamsMeta ? Collections.emptyList() : exPlan.fieldsMeta().queryFieldsMetadata(typeFactory);
            default:
                return Collections.emptyList();
        }
    }

    /** */
    private void onStart(GridKernalContext ctx, Service... services) {
        for (Service srvc : services) {
            if (srvc instanceof LifecycleAware)
                ((LifecycleAware)srvc).onStart(ctx);
        }
    }

    /** */
    private void onStop(Service... services) {
        for (Service srvc : services) {
            if (srvc instanceof LifecycleAware)
                ((LifecycleAware)srvc).onStop();
        }
    }

    /** */
    public Query<?> runningQuery(UUID id) {
        return qryReg.query(id);
    }

    /** */
    public Collection<? extends Query<?>> runningQueries() {
        return qryReg.runningQueries();
    }

    /** */
    public QueryRegistry queryRegistry() {
        return qryReg;
    }

    /** */
    public CalciteQueryEngineConfiguration config() {
        return cfg;
    }

    /** */
    public DistributedCalciteConfiguration distributedConfiguration() {
        return distrCfg;
    }
}
