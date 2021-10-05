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

package org.apache.ignite.internal.processors.query.calcite.prepare;

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
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.query.GridQueryCancel;
import org.apache.ignite.internal.processors.query.calcite.metadata.IgniteMetadata;
import org.apache.ignite.internal.processors.query.calcite.metadata.RelMetadataQueryEx;
import org.apache.ignite.internal.processors.query.calcite.metadata.cost.IgniteCostFactory;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.logger.NullLogger;
import org.jetbrains.annotations.NotNull;

import static org.apache.calcite.tools.Frameworks.createRootSchema;
import static org.apache.ignite.internal.processors.query.calcite.CalciteQueryProcessor.FRAMEWORK_CONFIG;

/**
 * Base query context.
 */
public final class BaseQueryContext extends AbstractQueryContext {
    /** */
    private static final IgniteCostFactory COST_FACTORY = new IgniteCostFactory();

    /** */
    public static final CalciteConnectionConfig CALCITE_CONNECTION_CONFIG;

    /** */
    private static final BaseQueryContext EMPTY_CONTEXT;

    /** */
    private static final VolcanoPlanner EMPTY_PLANNER;

    /** */
    private static final RexBuilder DFLT_REX_BUILDER;

    /** */
    public static final RelOptCluster CLUSTER;

    /** */
    public static final IgniteTypeFactory TYPE_FACTORY;

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

    /** */
    private final FrameworkConfig cfg;

    /** */
    private final IgniteLogger log;

    /** */
    private final IgniteTypeFactory typeFactory;

    /** */
    private final RexBuilder rexBuilder;

    /** */
    private CalciteCatalogReader catalogReader;

    /** */
    private final GridQueryCancel qryCancel;

    /**
     * Private constructor, used by a builder.
     */
    private BaseQueryContext(
        FrameworkConfig cfg,
        Context parentCtx,
        IgniteLogger log
    ) {
        super(Contexts.chain(parentCtx, cfg.getContext()));

        // link frameworkConfig#context() to this.
        this.cfg = Frameworks.newConfigBuilder(cfg).context(this).build();

        this.log = log;

        RelDataTypeSystem typeSys = CALCITE_CONNECTION_CONFIG.typeSystem(RelDataTypeSystem.class, cfg.getTypeSystem());

        typeFactory = new IgniteTypeFactory(typeSys);

        qryCancel = unwrap(GridQueryCancel.class);

        rexBuilder = new RexBuilder(typeFactory);
    }

    /**
     * @return Framework config.
     */
    public FrameworkConfig config() {
        return cfg;
    }

    /**
     * @return Logger.
     */
    public IgniteLogger logger() {
        return log;
    }

    /**
     * @return Schema name.
     */
    public String schemaName() {
        return schema().getName();
    }

    /**
     * @return Schema.
     */
    public SchemaPlus schema() {
        return cfg.getDefaultSchema();
    }

    /**
     * @return Type factory.
     */
    public IgniteTypeFactory typeFactory() {
        return typeFactory;
    }

    /** */
    public RexBuilder rexBuilder() {
        return rexBuilder;
    }

    /**
     * @return New catalog reader.
     */
    public CalciteCatalogReader catalogReader() {
        if (catalogReader != null)
            return catalogReader;

        resetCatalogReader();

        return catalogReader;
    }

    /** */
    public void resetCatalogReader() {
        SchemaPlus dfltSchema = schema(), rootSchema = dfltSchema;

        while (rootSchema.getParentSchema() != null)
            rootSchema = rootSchema.getParentSchema();

        catalogReader = new CalciteCatalogReader(
            CalciteSchema.from(rootSchema),
            CalciteSchema.from(dfltSchema).path(null),
            typeFactory(), CALCITE_CONNECTION_CONFIG);
    }

    /**
     * @return Query cancel.
     */
    public GridQueryCancel queryCancel() {
        return qryCancel;
    }

    /**
     * @return Context builder.
     */
    public static Builder builder() {
        return new Builder();
    }

    /** */
    public static BaseQueryContext empty() {
        return EMPTY_CONTEXT;
    }

    /**
     * Query context builder.
     */
    @SuppressWarnings("PublicInnerClass") 
    public static class Builder {
        /** */
        private static final FrameworkConfig EMPTY_CONFIG =
            Frameworks.newConfigBuilder(FRAMEWORK_CONFIG)
                .defaultSchema(createRootSchema(false))
                .build();

        /** */
        private FrameworkConfig frameworkCfg = EMPTY_CONFIG;

        /** */
        private Context parentCtx = Contexts.empty();

        /** */
        private IgniteLogger log = new NullLogger();

        /**
         * @param frameworkCfg Framework config.
         * @return Builder for chaining.
         */
        public Builder frameworkConfig(@NotNull FrameworkConfig frameworkCfg) {
            this.frameworkCfg = frameworkCfg;
            return this;
        }

        /**
         * @param parentCtx Parent context.
         * @return Builder for chaining.
         */
        public Builder parentContext(@NotNull Context parentCtx) {
            this.parentCtx = parentCtx;
            return this;
        }

        /**
         * @param log Logger.
         * @return Builder for chaining.
         */
        public Builder logger(@NotNull IgniteLogger log) {
            this.log = log;
            return this;
        }

        /**
         * Builds planner context.
         *
         * @return Planner context.
         */
        public BaseQueryContext build() {
            return new BaseQueryContext(frameworkCfg, parentCtx, log);
        }
    }
}
