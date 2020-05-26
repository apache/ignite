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
import java.util.UUID;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.query.GridQueryCancel;
import org.apache.ignite.internal.processors.query.calcite.CalciteQueryProcessor;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.jetbrains.annotations.NotNull;

/**
 * Planning context.
 */
public final class PlanningContext implements Context {
    /** */
    private static final Context EMPTY_CONTEXT = Contexts.empty();

    /** */
    private static final FrameworkConfig EMPTY_CONFIG =
        Frameworks.newConfigBuilder(CalciteQueryProcessor.FRAMEWORK_CONFIG)
        .defaultSchema(Frameworks.createRootSchema(false))
        .traitDefs()
        .build();

    /** */
    public static final PlanningContext EMPTY = new PlanningContext();

    /** */
    private final FrameworkConfig cfg;

    /** */
    private final Context parentCtx;

    /** */
    private final UUID locNodeId;

    /** */
    private final UUID originatingNodeId;

    /** */
    private final String qry;

    /** */
    private final Object[] parameters;

    /** */
    private final AffinityTopologyVersion topVer;

    /** */
    private final GridQueryCancel qryCancel;

    /** */
    private final IgniteLogger log;

    /** */
    private final IgniteTypeFactory typeFactory;

    /** */
    private IgnitePlanner planner;

    /** */
    private CalciteConnectionConfig connCfg;

    /** */
    private CalciteCatalogReader catalogReader;

    /**
     * Private constructor, used by a builder.
     */
    private PlanningContext(
        FrameworkConfig cfg,
        Context parentCtx,
        UUID locNodeId,
        UUID originatingNodeId,
        String qry,
        Object[] parameters,
        AffinityTopologyVersion topVer,
        IgniteLogger log) {
        this.locNodeId = locNodeId;
        this.originatingNodeId = originatingNodeId;
        this.qry = qry;
        this.parameters = parameters;
        this.topVer = topVer;
        this.log = log;

        this.parentCtx = Contexts.chain(parentCtx, cfg.getContext());
        // link frameworkConfig#context() to this.
        this.cfg = Frameworks.newConfigBuilder(cfg).context(this).build();

        qryCancel = unwrap(GridQueryCancel.class);

        RelDataTypeSystem typeSys = connectionConfig().typeSystem(RelDataTypeSystem.class, cfg.getTypeSystem());
        typeFactory = new IgniteTypeFactory(typeSys);
    }

    /**
     * Constructor for empty context.
     */
    private PlanningContext() {
        cfg = EMPTY_CONFIG;
        parentCtx = EMPTY_CONTEXT;
        RelDataTypeSystem typeSys = connectionConfig().typeSystem(RelDataTypeSystem.class, cfg.getTypeSystem());
        typeFactory = new IgniteTypeFactory(typeSys);
        locNodeId = null;
        originatingNodeId = null;
        qry = null;
        parameters = null;
        topVer = null;
        qryCancel = null;
        log = null;
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
        return originatingNodeId == null ? locNodeId : originatingNodeId;
    }

    /**
     * @return Framework config.
     */
    public FrameworkConfig config() {
        return cfg;
    }

    /**
     * @return Query.
     */
    public String query() {
        return qry;
    }

    /**
     * @return Query parameters.
     */
    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
    public Object[] parameters() {
        return parameters;
    }

    /**
     * @return Topology version.
     */
    public AffinityTopologyVersion topologyVersion() {
        return topVer;
    }

    /**
     * @return Query cancel.
     */
    public GridQueryCancel queryCancel() {
        return qryCancel;
    }

    /**
     * @return Logger.
     */
    public IgniteLogger logger() {
        return log;
    }

    // Helper methods

    /**
     * @return Sql operators table.
     */
    public SqlOperatorTable opTable() {
        return config().getOperatorTable();
    }

    /**
     * @return Sql conformance.
     */
    public SqlConformance conformance() {
        return cfg.getParserConfig().conformance();
    }

    /**
     * @return Planner.
     */
    public IgnitePlanner planner() {
        if (planner == null)
            planner = new IgnitePlanner(this);

        return planner;
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

    /**
     * @return Connection config. Defines connected user parameters like TimeZone or Locale.
     */
    public CalciteConnectionConfig connectionConfig() {
        if (connCfg != null)
            return connCfg;

        CalciteConnectionConfig connCfg = unwrap(CalciteConnectionConfig.class);

        if (connCfg != null)
            return this.connCfg = connCfg;

        Properties props = new Properties();

        props.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(),
            String.valueOf(cfg.getParserConfig().caseSensitive()));
        props.setProperty(CalciteConnectionProperty.CONFORMANCE.camelName(),
            String.valueOf(cfg.getParserConfig().conformance()));
        props.setProperty(CalciteConnectionProperty.MATERIALIZATIONS_ENABLED.camelName(),
            String.valueOf(true));

        return this.connCfg = new CalciteConnectionConfigImpl(props);
    }

    /**
     * @return New catalog reader.
     */
    public CalciteCatalogReader catalogReader() {
        if (catalogReader != null)
            return catalogReader;

        SchemaPlus dfltSchema = schema(), rootSchema = dfltSchema;

        while (rootSchema.getParentSchema() != null)
            rootSchema = rootSchema.getParentSchema();

        return catalogReader = new CalciteCatalogReader(
            CalciteSchema.from(rootSchema),
            CalciteSchema.from(dfltSchema).path(null),
            typeFactory(), connectionConfig());
    }

    /**
     * @return New cluster based on a planner and its configuration.
     */
    public RelOptCluster createCluster() {
        return planner().createCluster();
    }

    /** {@inheritDoc} */
    @Override public <C> C unwrap(Class<C> aCls) {
        if (aCls == getClass())
            return aCls.cast(this);

        if (aCls.isInstance(connCfg))
            return aCls.cast(connCfg);

        return parentCtx.unwrap(aCls);
    }

    /**
     * @return Context builder.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * @return Empty context.
     */
    public static PlanningContext empty() {
        return EMPTY;
    }

    /**
     * Planner context builder.
     */
    @SuppressWarnings("PublicInnerClass") 
    public static class Builder {
        /** */
        private FrameworkConfig frameworkCfg = EMPTY_CONFIG;

        /** */
        private Context parentCtx = EMPTY_CONTEXT;

        /** */
        private UUID locNodeId;

        /** */
        private UUID originatingNodeId;

        /** */
        private String qry;

        /** */
        private Object[] parameters;

        /** */
        private AffinityTopologyVersion topVer;

        /** */
        private IgniteLogger log;

        /**
         * @param locNodeId Local node ID.
         * @return Builder for chaining.
         */
        public Builder localNodeId(@NotNull UUID locNodeId) {
            this.locNodeId = locNodeId;
            return this;
        }

        /**
         * @param originatingNodeId Originating node ID (the node, who started the execution).
         * @return Builder for chaining.
         */
        public Builder originatingNodeId(@NotNull UUID originatingNodeId) {
            this.originatingNodeId = originatingNodeId;
            return this;
        }

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
         * @param qry Query.
         * @return Builder for chaining.
         */
        public Builder query(@NotNull String qry) {
            this.qry = qry;
            return this;
        }

        /**
         * @param parameters Query parameters.
         * @return Builder for chaining.
         */
        @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
        public Builder parameters(@NotNull Object... parameters) {
            this.parameters = parameters;
            return this;
        }

        /**
         * @param topVer Topology version.
         * @return Builder for chaining.
         */
        public Builder topologyVersion(@NotNull AffinityTopologyVersion topVer) {
            this.topVer = topVer;
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
        public PlanningContext build() {
            return new PlanningContext(frameworkCfg, parentCtx, locNodeId, originatingNodeId, qry,
                parameters, topVer, log);
        }
    }
}
