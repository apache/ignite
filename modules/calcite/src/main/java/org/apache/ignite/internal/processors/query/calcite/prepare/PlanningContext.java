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
    private static final FrameworkConfig EMPTY_CONFIG = Frameworks.newConfigBuilder(CalciteQueryProcessor.FRAMEWORK_CONFIG)
        .defaultSchema(Frameworks.createRootSchema(false))
        .traitDefs()
        .build();

    /** */
    public static final PlanningContext EMPTY = builder().build();

    /** */
    private final FrameworkConfig config;

    /** */
    private final Context parentContext;

    /** */
    private final UUID localNodeId;

    /** */
    private final UUID originatingNodeId;

    /** */
    private final String query;

    /** */
    private final Object[] parameters;

    /** */
    private final AffinityTopologyVersion topologyVersion;

    /** */
    private final GridQueryCancel queryCancel;

    /** */
    private final IgniteLogger logger;

    /** */
    private IgnitePlanner planner;

    /** */
    private IgniteTypeFactory typeFactory;

    /** */
    private CalciteConnectionConfig connectionConfig;

    /** */
    private CalciteCatalogReader catalogReader;

    /**
     * Private constructor, used by a builder.
     */
    private PlanningContext(FrameworkConfig config, Context parentContext, UUID localNodeId, UUID originatingNodeId,
        String query, Object[] parameters, AffinityTopologyVersion topologyVersion, IgniteLogger logger) {
        this.localNodeId = localNodeId;
        this.originatingNodeId = originatingNodeId;
        this.query = query;
        this.parameters = parameters;
        this.topologyVersion = topologyVersion;
        this.logger = logger;

        this.parentContext = Contexts.chain(parentContext, config.getContext());
        // link frameworkConfig#context() to this.
        this.config = Frameworks.newConfigBuilder(config).context(this).build();

        queryCancel = unwrap(GridQueryCancel.class);
    }

    /**
     * @return Local node ID.
     */
    public UUID localNodeId() {
        return localNodeId;
    }

    /**
     * @return Originating node ID (the node, who started the execution).
     */
    public UUID originatingNodeId() {
        return originatingNodeId == null ? localNodeId : originatingNodeId;
    }

    /**
     * @return Framework config.
     */
    public FrameworkConfig config() {
        return config;
    }

    /**
     * @return Query.
     */
    public String query() {
        return query;
    }

    /**
     * @return Query parameters.
     */
    public Object[] parameters() {
        return parameters;
    }

    /**
     * @return Topology version.
     */
    public AffinityTopologyVersion topologyVersion() {
        return topologyVersion;
    }

    /**
     * @return Query cancel.
     */
    public GridQueryCancel queryCancel() {
        return queryCancel;
    }

    /**
     * @return Logger.
     */
    public IgniteLogger logger() {
        return logger;
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
        return config.getParserConfig().conformance();
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
        return config.getDefaultSchema();
    }

    /**
     * @return Type factory.
     */
    public IgniteTypeFactory typeFactory() {
        if (typeFactory != null)
            return typeFactory;

        RelDataTypeSystem typeSystem = connectionConfig().typeSystem(RelDataTypeSystem.class, config.getTypeSystem());

        return typeFactory = new IgniteTypeFactory(typeSystem);
    }

    /**
     * @return Connection config. Defines connected user parameters like TimeZone or Locale.
     */
    public CalciteConnectionConfig connectionConfig() {
        if (connectionConfig != null)
            return connectionConfig;

        CalciteConnectionConfig connConfig = unwrap(CalciteConnectionConfig.class);

        if (connConfig != null)
            return connectionConfig = connConfig;

        Properties properties = new Properties();

        properties.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(),
            String.valueOf(config.getParserConfig().caseSensitive()));
        properties.setProperty(CalciteConnectionProperty.CONFORMANCE.camelName(),
            String.valueOf(config.getParserConfig().conformance()));

        return connectionConfig = new CalciteConnectionConfigImpl(properties);
    }

    /**
     * @return New catalog reader.
     */
    public CalciteCatalogReader catalogReader() {
        if (catalogReader != null)
            return catalogReader;

        SchemaPlus defaultSchema = schema(), rootSchema = defaultSchema;

        while (rootSchema.getParentSchema() != null)
            rootSchema = rootSchema.getParentSchema();

        return catalogReader = new CalciteCatalogReader(
            CalciteSchema.from(rootSchema),
            CalciteSchema.from(defaultSchema).path(null),
            typeFactory(), connectionConfig());
    }

    /**
     * @return New cluster based on a planner and its configuration.
     */
    public RelOptCluster createCluster() {
        return planner().createCluster();
    }

    /** {@inheritDoc} */
    @Override public <C> C unwrap(Class<C> aClass) {
        if (aClass == getClass())
            return aClass.cast(this);

        return parentContext.unwrap(aClass);
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
    public static class Builder {
        /** */
        private FrameworkConfig frameworkConfig = EMPTY_CONFIG;

        /** */
        private Context parentContext = EMPTY_CONTEXT;

        /** */
        private UUID localNodeId;

        /** */
        private UUID originatingNodeId;

        /** */
        private String query;

        /** */
        private Object[] parameters;

        /** */
        private AffinityTopologyVersion topologyVersion;

        /** */
        private IgniteLogger logger;

        /**
         * @param localNodeId Local node ID.
         * @return Builder for chaining.
         */
        public Builder localNodeId(@NotNull UUID localNodeId) {
            this.localNodeId = localNodeId;
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
         * @param frameworkConfig Framework config.
         * @return Builder for chaining.
         */
        public Builder frameworkConfig(@NotNull FrameworkConfig frameworkConfig) {
            this.frameworkConfig = frameworkConfig;
            return this;
        }

        /**
         * @param parentContext Parent context.
         * @return Builder for chaining.
         */
        public Builder parentContext(@NotNull Context parentContext) {
            this.parentContext = parentContext;
            return this;
        }

        /**
         * @param query Query.
         * @return Builder for chaining.
         */
        public Builder query(@NotNull String query) {
            this.query = query;
            return this;
        }

        /**
         * @param parameters Query parameters.
         * @return Builder for chaining.
         */
        public Builder parameters(@NotNull Object[] parameters) {
            this.parameters = parameters;
            return this;
        }

        /**
         * @param topologyVersion Topology version.
         * @return Builder for chaining.
         */
        public Builder topologyVersion(@NotNull AffinityTopologyVersion topologyVersion) {
            this.topologyVersion = topologyVersion;
            return this;
        }

        /**
         * @param logger Logger.
         * @return Builder for chaining.
         */
        public Builder logger(@NotNull IgniteLogger logger) {
            this.logger = logger;
            return this;
        }

        /**
         * Builds planner context.
         *
         * @return Planner context.
         */
        public PlanningContext build() {
            return new PlanningContext(frameworkConfig, parentContext, localNodeId, originatingNodeId, query,
                parameters, topologyVersion, logger);
        }
    }
}
