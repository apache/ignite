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

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.plan.Context;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.query.calcite.CalciteQueryProcessor;
import org.apache.ignite.internal.processors.query.calcite.exchange.ExchangeProcessor;
import org.apache.ignite.internal.processors.query.calcite.metadata.MappingService;
import org.apache.ignite.internal.processors.query.calcite.metadata.NodesMapping;

/**
 * Planner context, encapsulates services, kernal context, query string and its flags and parameters and helper methods
 * to work with them.
 */
public final class PlannerContext implements Context {
    /** */
    private final Context parentContext;

    /** */
    private final Query query;

    /** */
    private final AffinityTopologyVersion topologyVersion;

    /** */
    private final SchemaPlus schema;

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
    private IgnitePlanner planner;

    /**
     * Private constructor, used by a builder.
     */
    private PlannerContext(Context parentContext, Query query, AffinityTopologyVersion topologyVersion,
        SchemaPlus schema, IgniteLogger logger, GridKernalContext kernalContext, CalciteQueryProcessor queryProcessor, MappingService mappingService,
        ExchangeProcessor exchangeProcessor) {
        this.parentContext = parentContext;
        this.query = query;
        this.topologyVersion = topologyVersion;
        this.schema = schema;
        this.logger = logger;
        this.kernalContext = kernalContext;
        this.queryProcessor = queryProcessor;
        this.mappingService = mappingService;
        this.exchangeProcessor = exchangeProcessor;
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
     * @return Schema.
     */
    public SchemaPlus schema() {
        return schema;
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

    // Helper methods

    /**
     * @return Type factory.
     */
    public JavaTypeFactory typeFactory() {
        return planner.getTypeFactory();
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
     * Planner context builder.
     */
    public static class Builder {
        /** */
        private Context parentContext;

        /** */
        private Query query;

        /** */
        private AffinityTopologyVersion topologyVersion;

        /** */
        private SchemaPlus schema;

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
         * @param schema Schema.
         * @return Builder for chaining.
         */
        public Builder schema(SchemaPlus schema) {
            this.schema = schema;
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
         * Builds planner context.
         *
         * @return Planner context.
         */
        public PlannerContext build() {
            return new PlannerContext(parentContext, query, topologyVersion, schema, logger, kernalContext, queryProcessor, mappingService, exchangeProcessor);
        }
    }
}
