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

import java.util.Collections;
import java.util.List;
import java.util.concurrent.Future;
import org.apache.calcite.config.Lex;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.sql.fun.SqlLibrary;
import org.apache.calcite.sql.fun.SqlLibraryOperatorTableFactory;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.QueryContext;
import org.apache.ignite.internal.processors.query.QueryEngine;
import org.apache.ignite.internal.processors.query.calcite.cluster.MappingServiceImpl;
import org.apache.ignite.internal.processors.query.calcite.exec.DelegatingStripedExecutor;
import org.apache.ignite.internal.processors.query.calcite.prepare.ContextFactory;
import org.apache.ignite.internal.processors.query.calcite.prepare.DistributedExecution;
import org.apache.ignite.internal.processors.query.calcite.prepare.IgnitePlanner;
import org.apache.ignite.internal.processors.query.calcite.prepare.PlannerContext;
import org.apache.ignite.internal.processors.query.calcite.prepare.Query;
import org.apache.ignite.internal.processors.query.calcite.prepare.QueryExecution;
import org.apache.ignite.internal.processors.query.calcite.schema.CalciteSchemaHolder;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeSystem;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.processors.subscription.GridInternalSubscriptionProcessor;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.thread.IgniteStripedThreadPoolExecutor;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class CalciteQueryProcessor implements QueryEngine {
    /** */
    private final CalciteSchemaHolder schemaHolder = new CalciteSchemaHolder();

    /** */
    private final FrameworkConfig config;

    /** */
    private IgniteLogger log;

    /** */
    private GridKernalContext kernalContext;

    /** */
    private IgniteStripedThreadPoolExecutor executorService;

    /** */
    public CalciteQueryProcessor() {
        config = Frameworks.newConfigBuilder()
            .parserConfig(SqlParser.configBuilder()
                // Lexical configuration defines how identifiers are quoted, whether they are converted to upper or lower
                // case when they are read, and whether identifiers are matched case-sensitively.
                .setLex(Lex.MYSQL)
                .build())
            // Dialects support.
            .operatorTable(SqlLibraryOperatorTableFactory.INSTANCE
                .getOperatorTable(
                    SqlLibrary.STANDARD,
                    SqlLibrary.MYSQL))
            // Context provides a way to store data within the planner session that can be accessed in planner rules.
            .context(Contexts.of(this))
            // Custom cost factory to use during optimization
            .costFactory(null)
            .typeSystem(IgniteTypeSystem.DEFAULT)
            .build();
    }

    /**
     * @param log Logger.
     */
    @LoggerResource
    public void setLogger(IgniteLogger log) {
        this.log = log;
    }

    /** {@inheritDoc} */
    @Override public void start(@NotNull GridKernalContext ctx) {
        kernalContext = ctx;

        GridInternalSubscriptionProcessor prc = ctx.internalSubscriptionProcessor();

        if (prc != null) // Stubbed context doesn't have such processor
            prc.registerSchemaChangeListener(schemaHolder);

        // TODO move to executors
        executorService = new IgniteStripedThreadPoolExecutor(
            8,
            kernalContext.igniteInstanceName(),
            "calciteQry",
            null,
            true,
            10_000
        );
    }

    /** {@inheritDoc} */
    @Override public void stop() {
        List<Runnable> scheduled = executorService.shutdownNow();

        for (Runnable task : scheduled) {
            if (task instanceof Future)
                ((Future<?>) task).cancel(true);
        }
    }

    /** {@inheritDoc} */
    @Override public List<FieldsQueryCursor<List<?>>> query(@Nullable QueryContext ctx, String query, Object... params) throws IgniteSQLException {
        QueryExecution execution = prepare(ctx, query, params);

        return Collections.singletonList(execution.execute());
    }

    /**
     * Creates a planner.
     *
     * @param ctx Planner context.
     * @return Ignite planner.
     */
    public IgnitePlanner planner(PlannerContext ctx) {
        return new IgnitePlanner(ctx);
    }

    /**
     * @param ctx External context.
     * @param query Query string.
     * @param params Query parameters.
     * @return Query execution context.
     */
    public PlannerContext buildContext(@Nullable QueryContext ctx, RelTraitDef<?>[] traitDefs, String query, Object[] params) {
        return buildContext(ctx, traitDefs, query, params, this::buildContext);
    }

    /**
     * For tests
     *
     * @param ctx External context.
     * @param query Query string.
     * @param params Query parameters.
     * @return Query execution context.
     */
    PlannerContext buildContext(@Nullable QueryContext ctx, RelTraitDef<?>[] traitDefs, String query, Object[] params, ContextFactory factory) {
        Context parent = Contexts.chain(Commons.convert(ctx), config().getContext());
        return factory.create(parent, new Query(query, params), traitDefs);
    }

    /** */
    private PlannerContext buildContext(@NotNull Context parent, @NotNull Query query, @NotNull RelTraitDef<?>[] traitDefs) {
        return PlannerContext.builder()
            .kernalContext(kernalContext)
            .parentContext(parent)
            .frameworkConfig(Frameworks.newConfigBuilder(config())
                .defaultSchema(schemaHolder.schema())
                .traitDefs(traitDefs)
                .build())
            .queryProcessor(this)
            .query(query)
            .topologyVersion(readyAffinityVersion())
            .mappingService(new MappingServiceImpl(kernalContext))
            .executor(new DelegatingStripedExecutor(executorService))
            .logger(log)
            .build();
    }


    /**
     *
     * @return Calcite configuration.
     */
    FrameworkConfig config() {
        return config;
    }

    /** */
    private QueryExecution prepare(@Nullable QueryContext ctx, String query, Object... params) {
        return new DistributedExecution(ctx, query, params);
    }

    /** */
    private AffinityTopologyVersion readyAffinityVersion() {
        return kernalContext.cache().context().exchange().readyAffinityVersion();
    }
}
