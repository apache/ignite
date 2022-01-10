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

package org.apache.ignite.internal.sql.engine.util;

import static org.apache.calcite.tools.Frameworks.createRootSchema;
import static org.apache.ignite.internal.sql.engine.util.Commons.FRAMEWORK_CONFIG;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.metadata.CachingRelMetadataProvider;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.ignite.internal.sql.engine.QueryCancel;
import org.apache.ignite.internal.sql.engine.extension.SqlExtension;
import org.apache.ignite.internal.sql.engine.metadata.IgniteMetadata;
import org.apache.ignite.internal.sql.engine.metadata.RelMetadataQueryEx;
import org.apache.ignite.internal.sql.engine.metadata.cost.IgniteCostFactory;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.logger.NullLogger;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Base query context.
 */
public final class BaseQueryContext extends AbstractQueryContext {
    public static final CalciteConnectionConfig CALCITE_CONNECTION_CONFIG;

    public static final RelOptCluster CLUSTER;

    public static final IgniteTypeFactory TYPE_FACTORY;

    private static final IgniteCostFactory COST_FACTORY = new IgniteCostFactory();

    private static final BaseQueryContext EMPTY_CONTEXT;

    private static final VolcanoPlanner EMPTY_PLANNER;

    private static final RexBuilder DFLT_REX_BUILDER;

    static {
        Properties props = new Properties();

        props.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(),
                String.valueOf(FRAMEWORK_CONFIG.getParserConfig().caseSensitive()));
        props.setProperty(CalciteConnectionProperty.CONFORMANCE.camelName(),
                String.valueOf(FRAMEWORK_CONFIG.getParserConfig().conformance()));
        props.setProperty(CalciteConnectionProperty.MATERIALIZATIONS_ENABLED.camelName(),
                String.valueOf(true));

        CALCITE_CONNECTION_CONFIG = new CalciteConnectionConfigImpl(props);

        EMPTY_CONTEXT = builder().build();

        EMPTY_PLANNER = new VolcanoPlanner(COST_FACTORY, EMPTY_CONTEXT);

        RelDataTypeSystem typeSys = CALCITE_CONNECTION_CONFIG.typeSystem(RelDataTypeSystem.class, FRAMEWORK_CONFIG.getTypeSystem());
        TYPE_FACTORY = new IgniteTypeFactory(typeSys);

        DFLT_REX_BUILDER = new RexBuilder(TYPE_FACTORY);

        CLUSTER = RelOptCluster.create(EMPTY_PLANNER, DFLT_REX_BUILDER);

        CLUSTER.setMetadataProvider(new CachingRelMetadataProvider(IgniteMetadata.METADATA_PROVIDER, EMPTY_PLANNER));
        CLUSTER.setMetadataQuerySupplier(RelMetadataQueryEx::create);
    }

    private final FrameworkConfig cfg;

    private final IgniteLogger log;

    private final IgniteTypeFactory typeFactory;

    private final RexBuilder rexBuilder;

    private final QueryCancel qryCancel;

    private final Map<String, SqlExtension> extensions;

    private CalciteCatalogReader catalogReader;

    /**
     * Private constructor, used by a builder.
     */
    private BaseQueryContext(
            FrameworkConfig cfg,
            Context parentCtx,
            IgniteLogger log,
            Map<String, SqlExtension> extensions
    ) {
        super(Contexts.chain(parentCtx, cfg.getContext()));

        // link frameworkConfig#context() to this.
        this.cfg = Frameworks.newConfigBuilder(cfg).context(this).build();

        this.log = log;
        this.extensions = extensions;

        RelDataTypeSystem typeSys = CALCITE_CONNECTION_CONFIG.typeSystem(RelDataTypeSystem.class, cfg.getTypeSystem());

        typeFactory = new IgniteTypeFactory(typeSys);

        qryCancel = unwrap(QueryCancel.class);

        rexBuilder = new RexBuilder(typeFactory);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static BaseQueryContext empty() {
        return EMPTY_CONTEXT;
    }

    public List<SqlExtension> extensions() {
        return new ArrayList<>(extensions.values());
    }

    public @Nullable SqlExtension extension(String name) {
        return extensions.get(name);
    }

    public FrameworkConfig config() {
        return cfg;
    }

    public IgniteLogger logger() {
        return log;
    }

    public String schemaName() {
        return schema().getName();
    }

    public SchemaPlus schema() {
        return cfg.getDefaultSchema();
    }

    public IgniteTypeFactory typeFactory() {
        return typeFactory;
    }

    public RexBuilder rexBuilder() {
        return rexBuilder;
    }

    /**
     * Returns calcite catalog reader.
     */
    public CalciteCatalogReader catalogReader() {
        if (catalogReader != null) {
            return catalogReader;
        }

        SchemaPlus dfltSchema = schema();
        SchemaPlus rootSchema = dfltSchema;

        while (rootSchema.getParentSchema() != null) {
            rootSchema = rootSchema.getParentSchema();
        }

        return catalogReader = new CalciteCatalogReader(
                CalciteSchema.from(rootSchema),
                CalciteSchema.from(dfltSchema).path(null),
                typeFactory(), CALCITE_CONNECTION_CONFIG);
    }

    public QueryCancel queryCancel() {
        return qryCancel;
    }

    /**
     * Query context builder.
     */
    @SuppressWarnings("PublicInnerClass")
    public static class Builder {
        private static final FrameworkConfig EMPTY_CONFIG =
                Frameworks.newConfigBuilder(FRAMEWORK_CONFIG)
                        .defaultSchema(createRootSchema(false))
                        .build();

        private FrameworkConfig frameworkCfg = EMPTY_CONFIG;

        private Context parentCtx = Contexts.empty();

        private IgniteLogger log = new NullLogger();

        private Map<String, SqlExtension> extensions = Collections.emptyMap();

        public Builder frameworkConfig(@NotNull FrameworkConfig frameworkCfg) {
            this.frameworkCfg = frameworkCfg;
            return this;
        }

        public Builder parentContext(@NotNull Context parentCtx) {
            this.parentCtx = parentCtx;
            return this;
        }

        public Builder logger(@NotNull IgniteLogger log) {
            this.log = log;
            return this;
        }

        public Builder extensions(Map<String, SqlExtension> extensions) {
            this.extensions = extensions;
            return this;
        }

        public BaseQueryContext build() {
            return new BaseQueryContext(frameworkCfg, parentCtx, log, extensions);
        }
    }
}
