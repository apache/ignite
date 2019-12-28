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
import java.util.concurrent.Future;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.plan.Context;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.query.calcite.CalciteQueryProcessor;
import org.apache.ignite.internal.processors.query.calcite.exchange.ExchangeProcessor;
import org.apache.ignite.internal.processors.query.calcite.exec.QueryExecutionService;
import org.apache.ignite.internal.processors.query.calcite.metadata.MappingService;
import org.apache.ignite.internal.processors.query.calcite.metadata.NodesMapping;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;

/**
 * Planner context, encapsulates services, kernal context, query string and its flags and parameters and helper methods
 * to work with them.
 */
public final class PlannerContext implements Context {
    private final UUID localNodeId;

    /** */
    private final FrameworkConfig frameworkConfig;

    /** */
    private final Context parentContext;

    /** */
    private final Query query;

    /** */
    private final AffinityTopologyVersion topologyVersion;

    /** */
    private final IgniteLogger logger;

    /** */
    private final GridKernalContext kernalContext;

    /** */
    private final CalciteQueryProcessor queryProcessor;

    /** */
    private final MappingService mappingService;

    /** */
    private final ExchangeProcessor exchangeProcessor;

    /** */
    private final QueryExecutionService executionService;

    /** */
    private IgnitePlanner planner;

    /** */
    private CalciteConnectionConfig connectionConfig;

    /** */
    private JavaTypeFactory typeFactory;

    /**
     * Private constructor, used by a builder.
     */
    private PlannerContext(UUID localNodeId, Query query, Context parentContext, GridKernalContext kernalContext,
        FrameworkConfig config, AffinityTopologyVersion topologyVersion, CalciteQueryProcessor queryProcessor,
        MappingService mappingService, ExchangeProcessor exchangeProcessor, QueryExecutionService executionService, IgniteLogger logger) {
        this.parentContext = parentContext;
        this.query = query;
        this.topologyVersion = topologyVersion;
        this.logger = logger;
        this.kernalContext = kernalContext;
        this.queryProcessor = queryProcessor;
        this.mappingService = mappingService;
        this.exchangeProcessor = exchangeProcessor;
        this.executionService = executionService;
        this.localNodeId = localNodeId;

        // link frameworkConfig#context() to this.
        Frameworks.ConfigBuilder b = config == null ? Frameworks.newConfigBuilder() :
            Frameworks.newConfigBuilder(config);

        frameworkConfig = b.context(this).build();
    }

    /**
     * @return Local node ID.
     */
    public UUID localNodeId() {
        return localNodeId;
    }

    /**
     * @return Framework config.
     */
    public FrameworkConfig frameworkConfig() {
        return frameworkConfig;
    }

    /**
     * @return Query and its parameters.
     */
    public Query query() {
        return query;
    }

    /**
     * @return Topology version.
     */
    public AffinityTopologyVersion topologyVersion() {
        return topologyVersion;
    }

    /**
     * @return Logger.
     */
    public IgniteLogger logger() {
        return logger;
    }

    /**
     * @return Kernal context.
     */
    public GridKernalContext kernalContext() {
        return kernalContext;
    }

    /**
     * @return Query processor.
     */
    public CalciteQueryProcessor queryProcessor() {
        return queryProcessor;
    }

    /**
     * Package private method to set a planner after it creates using provided PlannerContext.
     *
     * @param planner Planner.
     */
    void planner(IgnitePlanner planner) {
        this.planner = planner;
    }

    /**
     * @return Planner.
     */
    public IgnitePlanner planner() {
        return planner;
    }

    /**
     * @return Mapping service.
     */
    public MappingService mappingService() {
        return mappingService;
    }

    /**
     * @return Exchange processor.
     */
    public ExchangeProcessor exchangeProcessor() {
        return exchangeProcessor;
    }

    /**
     * @return Query execution service.
     */
    public QueryExecutionService executionService() {
        return executionService;
    }

    // Helper methods

    /**
     * Returns an affinityFunction for a given cache ID.
     *
     * @param cacheId Cache ID.
     * @return Affinity function.
     */
    public AffinityFunction affinityFunction(int cacheId) {
        return kernalContext.cache().context().cacheContext(cacheId).group().affinityFunction();
    }

    /**
     * @return Schema.
     */
    public SchemaPlus schema() {
        return frameworkConfig.getDefaultSchema();
    }

    /**
     * @return Type factory.
     */
    public JavaTypeFactory typeFactory() {
        if (typeFactory != null)
            return typeFactory;

        RelDataTypeSystem typeSystem = connectionConfig().typeSystem(RelDataTypeSystem.class, frameworkConfig.getTypeSystem());

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
            String.valueOf(frameworkConfig.getParserConfig().caseSensitive()));
        properties.setProperty(CalciteConnectionProperty.CONFORMANCE.camelName(),
            String.valueOf(frameworkConfig.getParserConfig().conformance()));

        return connectionConfig = new CalciteConnectionConfigImpl(properties);
    }

    /**
     * @return Local node mapping that consists of local node only, uses for root query fragment.
     */
    public NodesMapping mapForLocal() {
        return mappingService.local();
    }

    /**
     * Returns Nodes mapping for intermediate fragments, without Scan nodes leafs. Such fragments may be executed
     * on any cluster node, actual list of nodes is chosen on the basis of adopted selection strategy.
     *
     * @return Nodes mapping for intermediate fragments.
     */
    public NodesMapping mapForRandom() {
        return mappingService.random(topologyVersion);
    }

    /**
     * @param cacheId Cache ID.
     * @return Nodes mapping for particular table, depends on underlying cache distribution.
     */
    public NodesMapping mapForCache(int cacheId) {
        return mappingService.distributed(cacheId, topologyVersion);
    }

    /**
     * @return Query provider. Used to execute a query to remote database (federated database case)
     * or to execute a correlated query.
     */
    public QueryProvider queryProvider() {
        return null; // TODO
    }

    /**
     * Executes query task.
     * @param queryId Query ID.
     * @param fragmentId Fragment ID.
     * @param queryTask Query task.
     */
    public Future<Void> execute(UUID queryId, long fragmentId, Runnable queryTask) {
        return executionService.execute(queryId, fragmentId, queryTask);
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
     * @return Context builder.
     */
    public static Builder builder(PlannerContext template) {
        return new Builder()
            .executionService(template.executionService)
            .exchangeProcessor(template.exchangeProcessor)
            .mappingService(template.mappingService)
            .queryProcessor(template.queryProcessor)
            .kernalContext(template.kernalContext)
            .logger(template.logger)
            .topologyVersion(template.topologyVersion)
            .query(template.query)
            .parentContext(template.parentContext)
            .frameworkConfig(template.frameworkConfig)
            .localNodeId(template.localNodeId);
    }

    /**
     * Planner context builder.
     */
    public static class Builder {
        /** */
        private UUID localNodeId;

        /** */
        private FrameworkConfig frameworkConfig;

        /** */
        private Context parentContext;

        /** */
        private Query query;

        /** */
        private AffinityTopologyVersion topologyVersion;

        /** */
        private IgniteLogger logger;

        /** */
        private GridKernalContext kernalContext;

        /** */
        private CalciteQueryProcessor queryProcessor;

        /** */
        private MappingService mappingService;

        /** */
        private ExchangeProcessor exchangeProcessor;

        /** */
        private QueryExecutionService executionService;

        public Builder localNodeId(UUID localNodeId) {
            this.localNodeId = localNodeId;
            return this;
        }

        /**
         * @param frameworkConfig Framework config.
         * @return Builder for chaining.
         */
        public Builder frameworkConfig(FrameworkConfig frameworkConfig) {
            this.frameworkConfig = frameworkConfig;
            return this;
        }

        /**
         * @param parentContext Parent context.
         * @return Builder for chaining.
         */
        public Builder parentContext(Context parentContext) {
            this.parentContext = parentContext;
            return this;
        }

        /**
         * @param query Query.
         * @return Builder for chaining.
         */
        public Builder query(Query query) {
            this.query = query;
            return this;
        }

        /**
         * @param topologyVersion Topology version.
         * @return Builder for chaining.
         */
        public Builder topologyVersion(AffinityTopologyVersion topologyVersion) {
            this.topologyVersion = topologyVersion;
            return this;
        }

        /**
         * @param logger Logger.
         * @return Builder for chaining.
         */
        public Builder logger(IgniteLogger logger) {
            this.logger = logger;
            return this;
        }

        /**
         * @param kernalContext Kernal context.
         * @return Builder for chaining.
         */
        public Builder kernalContext(GridKernalContext kernalContext) {
            this.kernalContext = kernalContext;
            return this;
        }

        /**
         * @param queryProcessor Query processor.
         * @return Builder for chaining.
         */
        public Builder queryProcessor(CalciteQueryProcessor queryProcessor) {
            this.queryProcessor = queryProcessor;
            return this;
        }

        /**
         * @param mappingService Mapping service.
         * @return Builder for chaining.
         */
        public Builder mappingService(MappingService mappingService) {
            this.mappingService = mappingService;
            return this;
        }

        /**
         * @param exchangeProcessor Exchange processor.
         * @return Builder for chaining.
         */
        public Builder exchangeProcessor(ExchangeProcessor exchangeProcessor) {
            this.exchangeProcessor = exchangeProcessor;
            return this;
        }

        /**
         * @param executionService Query execution service.
         * @return Builder for chaining.
         */
        public Builder executionService(QueryExecutionService executionService) {
            this.executionService = executionService;
            return this;
        }

        /**
         * Builds planner context.
         *
         * @return Planner context.
         */
        public PlannerContext build() {
            return new PlannerContext(localNodeId, query, parentContext, kernalContext, frameworkConfig,
                topologyVersion, queryProcessor, mappingService, exchangeProcessor, executionService, logger);
        }
    }
}
