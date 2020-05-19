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
import org.apache.calcite.config.Lex;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelOptCostImpl;
import org.apache.calcite.sql.fun.SqlLibrary;
import org.apache.calcite.sql.fun.SqlLibraryOperatorTableFactory;
import org.apache.calcite.sql.parser.SqlParser;
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
import org.apache.ignite.internal.processors.query.calcite.exec.ExchangeService;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionService;
import org.apache.ignite.internal.processors.query.calcite.exec.MailboxRegistry;
import org.apache.ignite.internal.processors.query.calcite.exec.QueryTaskExecutor;
import org.apache.ignite.internal.processors.query.calcite.message.MessageService;
import org.apache.ignite.internal.processors.query.calcite.metadata.MappingService;
import org.apache.ignite.internal.processors.query.calcite.metadata.PartitionService;
import org.apache.ignite.internal.processors.query.calcite.prepare.QueryPlanCache;
import org.apache.ignite.internal.processors.query.calcite.schema.SchemaHolder;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeSystem;
import org.apache.ignite.internal.processors.query.calcite.util.LifecycleAware;
import org.apache.ignite.internal.processors.query.calcite.util.Service;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public abstract class CalciteQueryProcessor<Row> extends GridProcessorAdapter implements QueryEngine {
    /** */
    public static final FrameworkConfig FRAMEWORK_CONFIG = Frameworks.newConfigBuilder()
            .sqlToRelConverterConfig(SqlToRelConverter.configBuilder()
                .withConvertTableAccess(true)
                .withTrimUnusedFields(false)
                .build())
            .parserConfig(SqlParser.configBuilder()
                // Lexical configuration defines how identifiers are quoted, whether they are converted to upper or lower
                // case when they are read, and whether identifiers are matched case-sensitively.
                .setLex(Lex.ORACLE)
//                .setParserFactory(SqlDdlParserImpl.FACTORY) // Enables DDL support
                .build())
            // Dialects support.
            .operatorTable(SqlLibraryOperatorTableFactory.INSTANCE
                .getOperatorTable(
                    SqlLibrary.STANDARD,
                    SqlLibrary.MYSQL))
            // Context provides a way to store data within the planner session that can be accessed in planner rules.
            .context(Contexts.empty())
            // Custom cost factory to use during optimization
            .costFactory(RelOptCostImpl.FACTORY)
            .typeSystem(IgniteTypeSystem.INSTANCE)
            .build();

    /** */
    private final QueryPlanCache qryPlanCache;

    /** */
    private final QueryTaskExecutor taskExecutor;

    /** */
    private final FailureProcessor failureProcessor;

    /** */
    private final PartitionService partSvc;

    /** */
    private final SchemaHolder schemaHolder;

    /** */
    private final MessageService msgSvc;

    /** */
    private final ExchangeService<Row> exchangeSvc;

    /** */
    private final MappingService mappingSvc;
    
    /** */
    private final MailboxRegistry<Row> mailboxRegistry;
    
    /** */
    private final ExecutionService executionSvc;

    /** */
    private final RowEngineFactory<Row> rowEngineFactory;

    /**
     * For tests purpose.
     * @param ctx Kernal context.
     * @param failureProcessor Failure processor.
     * @param schemaHolder Schema holder.
     * @param qryPlanCache Query cache;
     * @param mailboxRegistry Mailbox registry.
     * @param taskExecutor Task executor.
     * @param executionSvc Execution service.
     * @param partSvc Affinity service.
     * @param msgSvc Message service.
     * @param mappingSvc Mapping service.
     * @param exchangeSvc Exchange service.
     */
    CalciteQueryProcessor(
        GridKernalContext ctx,
        FailureProcessor failureProcessor,
        SchemaHolder schemaHolder,
        QueryPlanCache qryPlanCache,
        MailboxRegistry<Row> mailboxRegistry,
        QueryTaskExecutor taskExecutor,
        ExecutionService executionSvc,
        PartitionService partSvc,
        MessageService msgSvc,
        MappingService mappingSvc,
        ExchangeService<Row> exchangeSvc,
        RowEngineFactory<Row> rowEngineFactory
    ) {
        super(ctx);

        this.failureProcessor = failureProcessor;
        this.schemaHolder = schemaHolder;
        this.qryPlanCache = qryPlanCache;
        this.mailboxRegistry = mailboxRegistry;
        this.taskExecutor = taskExecutor;
        this.executionSvc = executionSvc;
        this.partSvc = partSvc;
        this.msgSvc = msgSvc;
        this.mappingSvc = mappingSvc;
        this.exchangeSvc = exchangeSvc;
        this.rowEngineFactory = rowEngineFactory;
    }

    /**
     * @return Affinity service.
     */
    public PartitionService affinityService() {
        return partSvc;
    }

    /**
     * @return Row engine factory.
     */
    public RowEngineFactory<Row> rowEngineFactory() {
        return rowEngineFactory;
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
    public ExchangeService<Row> exchangeService() {
        return exchangeSvc;
    }

    /**
     * @return Mailbox registry.
     */
    public MailboxRegistry<Row> mailboxRegistry() {
        return mailboxRegistry;
    }

    /**
     * @return Failure processor.
     */
    public FailureProcessor failureProcessor() {
        return failureProcessor;
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
            exchangeSvc
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
            exchangeSvc
        );
    }

    /** {@inheritDoc} */
    @Override public List<FieldsQueryCursor<List<?>>> query(@Nullable QueryContext qryCtx, @Nullable String schemaName,
        String qry, Object... params) throws IgniteSQLException {
        
        return executionSvc.executeQuery(qryCtx, schemaName, qry, params);
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
