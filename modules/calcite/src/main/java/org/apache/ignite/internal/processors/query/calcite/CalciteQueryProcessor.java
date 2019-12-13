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
import java.util.function.BiFunction;
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
    }

    /** {@inheritDoc} */
    @Override public void stop() {
    }

    @Override public List<FieldsQueryCursor<List<?>>> query(@Nullable QueryContext ctx, String query, Object... params) throws IgniteSQLException {
        PlannerContext context = context(Commons.convert(ctx), query, params, this::buildContext);
        QueryExecution execution = prepare(context);
        FieldsQueryCursor<List<?>> cur = execution.execute();
        return Collections.singletonList(cur);
    }

    public FrameworkConfig config() {
        return config;
    }

    public IgniteLogger log() {
        return log;
    }

    /** */
    public IgnitePlanner planner(RelTraitDef[] traitDefs, PlannerContext ctx0) {
        FrameworkConfig cfg = Frameworks.newConfigBuilder(config())
                .defaultSchema(ctx0.schema())
                .traitDefs(traitDefs)
                .context(ctx0)
                .build();

        return new IgnitePlanner(cfg);
    }

    /**
     * @param ctx External context.
     * @param query Query string.
     * @param params Query parameters.
     * @return Query execution context.
     */
    PlannerContext context(@NotNull Context ctx, String query, Object[] params, BiFunction<Context, Query, PlannerContext> clo) { // Package private visibility for tests.
        return clo.apply(Contexts.chain(ctx, config.getContext()), new Query(query, params));
    }

    private PlannerContext buildContext(@NotNull Context parent, @NotNull Query query) {
        return PlannerContext.builder()
            .logger(log)
            .kernalContext(kernalContext)
            .queryProcessor(this)
            .parentContext(parent)
            .query(query)
            .schema(schemaHolder.schema())
            .topologyVersion(readyAffinityVersion())
            .mappingService(new MappingServiceImpl(kernalContext))
            .build();
    }

    private QueryExecution prepare(PlannerContext ctx) {
        return new DistributedExecution(ctx);
    }

    private AffinityTopologyVersion readyAffinityVersion() {
        return kernalContext.cache().context().exchange().readyAffinityVersion();
    }
}
