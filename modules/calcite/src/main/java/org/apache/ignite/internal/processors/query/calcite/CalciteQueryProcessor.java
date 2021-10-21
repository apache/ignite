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
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import org.apache.calcite.DataContexts;
import org.apache.calcite.config.Lex;
import org.apache.calcite.config.NullCollation;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.hint.HintStrategyTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.fun.SqlLibrary;
import org.apache.calcite.sql.fun.SqlLibraryOperatorTableFactory;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.failure.FailureProcessor;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.QueryContext;
import org.apache.ignite.internal.processors.query.QueryEngine;
import org.apache.ignite.internal.processors.query.RunningQuery;
import org.apache.ignite.internal.processors.query.calcite.exec.ArrayRowHandler;
import org.apache.ignite.internal.processors.query.calcite.exec.ExchangeService;
import org.apache.ignite.internal.processors.query.calcite.exec.ExchangeServiceImpl;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionService;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionServiceImpl;
import org.apache.ignite.internal.processors.query.calcite.exec.MailboxRegistry;
import org.apache.ignite.internal.processors.query.calcite.exec.MailboxRegistryImpl;
import org.apache.ignite.internal.processors.query.calcite.exec.QueryTaskExecutor;
import org.apache.ignite.internal.processors.query.calcite.exec.QueryTaskExecutorImpl;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.RexExecutorImpl;
import org.apache.ignite.internal.processors.query.calcite.message.MessageService;
import org.apache.ignite.internal.processors.query.calcite.message.MessageServiceImpl;
import org.apache.ignite.internal.processors.query.calcite.metadata.AffinityService;
import org.apache.ignite.internal.processors.query.calcite.metadata.AffinityServiceImpl;
import org.apache.ignite.internal.processors.query.calcite.metadata.MappingService;
import org.apache.ignite.internal.processors.query.calcite.metadata.MappingServiceImpl;
import org.apache.ignite.internal.processors.query.calcite.metadata.cost.IgniteCostFactory;
import org.apache.ignite.internal.processors.query.calcite.prepare.CacheKey;
import org.apache.ignite.internal.processors.query.calcite.prepare.IgniteConvertletTable;
import org.apache.ignite.internal.processors.query.calcite.prepare.PrepareServiceImpl;
import org.apache.ignite.internal.processors.query.calcite.prepare.QueryPlan;
import org.apache.ignite.internal.processors.query.calcite.prepare.QueryPlanCache;
import org.apache.ignite.internal.processors.query.calcite.prepare.QueryPlanCacheImpl;
import org.apache.ignite.internal.processors.query.calcite.schema.SchemaHolder;
import org.apache.ignite.internal.processors.query.calcite.schema.SchemaHolderImpl;
import org.apache.ignite.internal.processors.query.calcite.sql.IgniteSqlConformance;
import org.apache.ignite.internal.processors.query.calcite.sql.IgniteSqlParserImpl;
import org.apache.ignite.internal.processors.query.calcite.sql.fun.IgniteSqlOperatorTable;
import org.apache.ignite.internal.processors.query.calcite.trait.CorrelationTraitDef;
import org.apache.ignite.internal.processors.query.calcite.trait.DistributionTraitDef;
import org.apache.ignite.internal.processors.query.calcite.trait.RewindabilityTraitDef;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeSystem;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.processors.query.calcite.util.LifecycleAware;
import org.apache.ignite.internal.processors.query.calcite.util.Service;
import org.jetbrains.annotations.Nullable;

/** */
public class CalciteQueryProcessor extends GridProcessorAdapter implements QueryEngine {
    /** */
    public static final FrameworkConfig FRAMEWORK_CONFIG = Frameworks.newConfigBuilder()
        .executor(new RexExecutorImpl(DataContexts.EMPTY))
        .sqlToRelConverterConfig(SqlToRelConverter.config()
            .withTrimUnusedFields(true)
            // currently SqlToRelConverter creates not optimal plan for both optimization and execution
            // so it's better to disable such rewriting right now
            // TODO: remove this after IGNITE-14277
            .withInSubQueryThreshold(Integer.MAX_VALUE)
            .withDecorrelationEnabled(true)
            .withExpand(false)
            .withHintStrategyTable(
                HintStrategyTable.builder()
                    .hintStrategy("DISABLE_RULE", (hint, rel) -> true)
                    .hintStrategy("EXPAND_DISTINCT_AGG", (hint, rel) -> rel instanceof Aggregate)
                    .build()
            )
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
            .withSqlConformance(IgniteSqlConformance.INSTANCE))
        // Dialects support.
        .operatorTable(SqlOperatorTables.chain(
            SqlLibraryOperatorTableFactory.INSTANCE
                .getOperatorTable(
                    SqlLibrary.STANDARD,
                    SqlLibrary.POSTGRESQL,
                    SqlLibrary.ORACLE,
                    SqlLibrary.MYSQL),
            IgniteSqlOperatorTable.instance()))
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

    /** */
    private final QueryPlanCache qryPlanCache;

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
    private final ExecutionService executionSvc;

    /** */
    private final PrepareServiceImpl prepareSvc;

    /** */
    private final QueryRegistry<Object[]> qryReg;

    /**
     * @param ctx Kernal context.
     */
    public CalciteQueryProcessor(GridKernalContext ctx) {
        super(ctx);

        failureProcessor = ctx.failure();
        schemaHolder = new SchemaHolderImpl(ctx);
        qryPlanCache = new QueryPlanCacheImpl(ctx);
        mailboxRegistry = new MailboxRegistryImpl(ctx);
        taskExecutor = new QueryTaskExecutorImpl(ctx);
        executionSvc = new ExecutionServiceImpl<>(ctx, ArrayRowHandler.INSTANCE);
        partSvc = new AffinityServiceImpl(ctx);
        msgSvc = new MessageServiceImpl(ctx);
        mappingSvc = new MappingServiceImpl(ctx);
        exchangeSvc = new ExchangeServiceImpl(ctx);
        prepareSvc = new PrepareServiceImpl(ctx);
        qryReg = new QueryRegistryImpl<>(ctx.log(QueryRegistry.class));
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

    /** {@inheritDoc} */
    @Override public void start() {
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
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) {
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

    /** {@inheritDoc} */
    @Override public List<FieldsQueryCursor<List<?>>> query(@Nullable QueryContext qryCtx, @Nullable String schemaName,
        String sql, Object... params) throws IgniteSQLException {
        SchemaPlus schema = schemaHolder.schema(schemaName);

        QueryPlan plan = queryPlanCache().queryPlan(new CacheKey(schema.getName(), sql));

        if (plan != null) {
            RootQuery<Object[]> qry = new RootQuery<>(
                sql,
                schema,
                params,
                qryCtx,
                exchangeSvc,
                (q) -> qryReg.unregister(q.id()),
                log
            );

            qryReg.register(qry);

            try {
                return Collections.singletonList(executionSvc.executePlan(
                    qry,
                    plan
                ));
            }
                catch (Exception e) {
                qryReg.unregister(qry.id());

                if (qry.isCancelled())
                    throw new IgniteSQLException("The query was cancelled while planning", IgniteQueryErrorCode.QUERY_CANCELED, e);
                else
                    throw e;
            }
        }

        SqlNodeList qryList = Commons.parse(sql, FRAMEWORK_CONFIG.getParserConfig());
        List<FieldsQueryCursor<List<?>>> cursors = new ArrayList<>(qryList.size());

        for (final SqlNode sqlNode: qryList) {
            RootQuery<Object[]> qry = new RootQuery<>(
                sqlNode.toString(),
                schemaHolder.schema(schemaName), // Update schema for each query in multiple statements.
                params,
                qryCtx,
                exchangeSvc,
                (q) -> qryReg.unregister(q.id()),
                log
            );

            qryReg.register(qry);
            try {
                if (qryList.size() == 1) {
                    plan = queryPlanCache().queryPlan(
                        new CacheKey(schemaName, qry.sql()),
                        () -> prepareSvc.prepareSingle(sqlNode, qry.planningContext()));
                }
                else
                    plan = prepareSvc.prepareSingle(sqlNode, qry.planningContext());

                cursors.add(executionSvc.executePlan(qry, plan));
            }
            catch (Exception e) {
                qryReg.unregister(qry.id());

                if (qry.isCancelled())
                    throw new IgniteSQLException("The query was cancelled while planning", IgniteQueryErrorCode.QUERY_CANCELED, e);
                else
                    throw e;
            }
        }

        return cursors;
    }

    /** */
    private void onStart(GridKernalContext ctx, Service... services) {
        for (Service service : services) {
            if (service instanceof LifecycleAware)
                ((LifecycleAware) service).onStart(ctx);
        }
    }

    /** */
    private void onStop(Service... services) {
        for (Service service : services) {
            if (service instanceof LifecycleAware)
                ((LifecycleAware) service).onStop();
        }
    }

    /** {@inheritDoc} */
    @Override public RunningQuery runningQuery(UUID id) {
        return qryReg.query(id);
    }

    /** {@inheritDoc} */
    @Override public Collection<? extends RunningQuery> runningQueries() {
        return qryReg.runningQueries();
    }

    /** */
    public <Row> QueryRegistry<Row> queryRegistry() {
        return (QueryRegistry<Row>)qryReg;
    }
}
