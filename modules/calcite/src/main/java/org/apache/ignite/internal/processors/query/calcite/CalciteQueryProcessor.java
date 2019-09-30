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
import org.apache.calcite.config.Lex;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.fun.SqlLibrary;
import org.apache.calcite.sql.fun.SqlLibraryOperatorTableFactory;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.QueryContext;
import org.apache.ignite.internal.processors.query.QueryEngine;
import org.apache.ignite.internal.processors.query.calcite.prepare.DistributedExecution;
import org.apache.ignite.internal.processors.query.calcite.prepare.IgnitePlanner;
import org.apache.ignite.internal.processors.query.calcite.prepare.Query;
import org.apache.ignite.internal.processors.query.calcite.prepare.QueryExecution;
import org.apache.ignite.internal.processors.query.calcite.schema.CalciteSchemaChangeListener;
import org.apache.ignite.internal.processors.query.calcite.schema.CalciteSchemaHolder;
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
    private GridKernalContext ctx;

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
            .typeSystem(RelDataTypeSystem.DEFAULT)
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
        this.ctx = ctx;

        GridInternalSubscriptionProcessor prc = ctx.internalSubscriptionProcessor();

        if (prc != null) // Stubbed context doesn't have such processor
            prc.registerSchemaChangeListener(new CalciteSchemaChangeListener(schemaHolder));
    }

    /** {@inheritDoc} */
    @Override public void stop() {
    }

    @Override public List<FieldsQueryCursor<List<?>>> query(@Nullable QueryContext ctx, String query, Object... params) throws IgniteSQLException {
        Context context = context(Commons.convert(ctx), query, params);
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

    public GridKernalContext context() {
        return ctx;
    }

    /** */
    public IgnitePlanner planner(RelTraitDef[] traitDefs, Context ctx) {
        FrameworkConfig cfg = Frameworks.newConfigBuilder(config())
                .defaultSchema(ctx.unwrap(SchemaPlus.class))
                .traitDefs(traitDefs)
                .context(ctx)
                .build();

        return new IgnitePlanner(cfg);
    }

    private QueryExecution prepare(Context ctx) {
        return new DistributedExecution(ctx);
    }

    /**
     * @param ctx External context.
     * @param query Query string.
     * @param params Query parameters.
     * @return Query execution context.
     */
    Context context(@NotNull Context ctx, String query, Object[] params) { // Package private visibility for tests.
        return Contexts.chain(
            config.getContext(),
            Contexts.of(schemaHolder.schema(), new Query(query, params)),
            ctx);
    }

    /**
     * @return Schema provider.
     */
    CalciteSchemaHolder schemaHolder() { // Package private visibility for tests.
        return schemaHolder;
    }
}
