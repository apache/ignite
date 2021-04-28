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

import java.util.List;

import java.util.UUID;
import org.apache.calcite.config.Lex;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.sql.fun.SqlLibrary;
import org.apache.calcite.sql.fun.SqlLibraryOperatorTableFactory;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.failure.FailureProcessor;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.QueryContext;
import org.apache.ignite.internal.processors.query.QueryEngine;
import org.apache.ignite.internal.processors.query.calcite.exec.ArrayRowHandler;
import org.apache.ignite.internal.processors.query.calcite.exec.ExchangeService;
import org.apache.ignite.internal.processors.query.calcite.exec.ExchangeServiceImpl;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionService;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionServiceImpl;
import org.apache.ignite.internal.processors.query.calcite.exec.MailboxRegistry;
import org.apache.ignite.internal.processors.query.calcite.exec.MailboxRegistryImpl;
import org.apache.ignite.internal.processors.query.calcite.exec.QueryTaskExecutor;
import org.apache.ignite.internal.processors.query.calcite.exec.QueryTaskExecutorImpl;
import org.apache.ignite.internal.processors.query.RunningQueryInfo;
import org.apache.ignite.internal.processors.query.calcite.exec.RunningQueryService;
import org.apache.ignite.internal.processors.query.calcite.message.MessageService;
import org.apache.ignite.internal.processors.query.calcite.message.MessageServiceImpl;
import org.apache.ignite.internal.processors.query.calcite.metadata.AffinityService;
import org.apache.ignite.internal.processors.query.calcite.metadata.AffinityServiceImpl;
import org.apache.ignite.internal.processors.query.calcite.metadata.MappingService;
import org.apache.ignite.internal.processors.query.calcite.metadata.MappingServiceImpl;
import org.apache.ignite.internal.processors.query.calcite.metadata.cost.IgniteCostFactory;
import org.apache.ignite.internal.processors.query.calcite.prepare.QueryPlanCache;
import org.apache.ignite.internal.processors.query.calcite.prepare.QueryPlanCacheImpl;
import org.apache.ignite.internal.processors.query.calcite.schema.SchemaHolder;
import org.apache.ignite.internal.processors.query.calcite.schema.SchemaHolderImpl;
import org.apache.ignite.internal.processors.query.calcite.sql.IgniteSqlParserImpl;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeSystem;
import org.apache.ignite.internal.processors.query.calcite.util.LifecycleAware;
import org.apache.ignite.internal.processors.query.calcite.util.Service;
import org.jetbrains.annotations.Nullable;

import static org.apache.calcite.rex.RexUtil.EXECUTOR;

/** */
public class CalciteQueryProcessor extends GridProcessorAdapter implements QueryEngine {
    /** */
    public static final FrameworkConfig FRAMEWORK_CONFIG = Frameworks.newConfigBuilder()
        .executor(EXECUTOR)
        .sqlToRelConverterConfig(SqlToRelConverter.config()
            .withTrimUnusedFields(true)
            // currently SqlToRelConverter creates not optimal plan for both optimization and execution
            // so it's better to disable such rewriting right now
            // TODO: remove this after IGNITE-14277
            .withInSubQueryThreshold(Integer.MAX_VALUE)
            .withDecorrelationEnabled(true))
        .parserConfig(
            SqlParser.config()
                .withParserFactory(IgniteSqlParserImpl.FACTORY)
                .withLex(Lex.ORACLE)
                .withConformance(SqlConformanceEnum.DEFAULT))
        .sqlValidatorConfig(SqlValidator.Config.DEFAULT
            .withIdentifierExpansion(true)
            .withSqlConformance(SqlConformanceEnum.DEFAULT))
        // Dialects support.
        .operatorTable(SqlLibraryOperatorTableFactory.INSTANCE
            .getOperatorTable(
                SqlLibrary.STANDARD,
                SqlLibrary.POSTGRESQL,
                SqlLibrary.ORACLE,
                SqlLibrary.MYSQL))
        // Context provides a way to store data within the planner session that can be accessed in planner rules.
        .context(Contexts.empty())
        // Custom cost factory to use during optimization
        .costFactory(new IgniteCostFactory())
        .typeSystem(IgniteTypeSystem.INSTANCE)
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

    private final RunningQueryService runningQryScv;

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
        runningQryScv = new RunningQueryService();
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

    /**
     * @return Running query manager.
     */
    public RunningQueryService runningQueryService() {
        return runningQryScv;
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
            runningQryScv
        );
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) {
        onStop(
            executionSvc,
            mailboxRegistry,
            partSvc,
            schemaHolder,
            msgSvc,
            taskExecutor,
            mappingSvc,
            qryPlanCache,
            exchangeSvc,
            runningQryScv
        );
    }

    /** {@inheritDoc} */
    @Override public List<FieldsQueryCursor<List<?>>> query(@Nullable QueryContext qryCtx, @Nullable String schemaName,
        String qry, Object... params) throws IgniteSQLException {

        return executionSvc.executeQuery(qryCtx, schemaName, qry, params);
    }

    /** {@inheritDoc} */
    @Override public List<RunningQueryInfo> runningSqlQueries() {
        return runningQryScv.runningSqlQueries();
    }

    /** {@inheritDoc} */
    @Override public void cancelQuery(UUID qryId) {
        runningQryScv.cancelQuery(qryId);
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
}
