/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
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
 *
 */
public final class PlannerContext implements Context {
    private final Context parentContext;
    private final Query query;
    private final AffinityTopologyVersion topologyVersion;
    private final SchemaPlus schema;
    private final IgniteLogger logger;
    private final GridKernalContext kernalContext;
    private final CalciteQueryProcessor queryProcessor;
    private final MappingService mappingService;
    private final ExchangeProcessor exchangeProcessor;

    private IgnitePlanner planner;

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

    public Query query() {
        return query;
    }

    public AffinityTopologyVersion topologyVersion() {
        return topologyVersion;
    }

    public SchemaPlus schema() {
        return schema;
    }

    public IgniteLogger logger() {
        return logger;
    }

    public GridKernalContext kernalContext() {
        return kernalContext;
    }

    public CalciteQueryProcessor queryProcessor() {
        return queryProcessor;
    }

    void planner(IgnitePlanner planner) {
        this.planner = planner;
    }

    public IgnitePlanner planner() {
        return planner;
    }

    public MappingService mappingService() {
        return mappingService;
    }

    public ExchangeProcessor exchangeProcessor() {
        return exchangeProcessor;
    }

    // Helper methods

    public JavaTypeFactory typeFactory() {
        return planner.getTypeFactory();
    }

    public NodesMapping mapForLocal() {
        return mappingService.local();
    }

    public NodesMapping mapForRandom(AffinityTopologyVersion topVer) {
        return mappingService.random(topVer);
    }

    public NodesMapping mapForCache(int cacheId, AffinityTopologyVersion topVer) {
        return mappingService.distributed(cacheId, topVer);
    }

    public QueryProvider queryProvider() {
        return null; // TODO
    }

    @Override public <C> C unwrap(Class<C> aClass) {
        if (aClass == getClass())
            return aClass.cast(this);

        return parentContext.unwrap(aClass);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private Context parentContext;
        private Query query;
        private AffinityTopologyVersion topologyVersion;
        private SchemaPlus schema;
        private IgniteLogger logger;
        private GridKernalContext kernalContext;
        private CalciteQueryProcessor queryProcessor;
        private MappingService mappingService;
        private ExchangeProcessor exchangeProcessor;

        public Builder parentContext(Context parentContext) {
            this.parentContext = parentContext;
            return this;
        }

        public Builder query(Query query) {
            this.query = query;
            return this;
        }

        public Builder topologyVersion(AffinityTopologyVersion topologyVersion) {
            this.topologyVersion = topologyVersion;
            return this;
        }

        public Builder schema(SchemaPlus schema) {
            this.schema = schema;
            return this;
        }

        public Builder logger(IgniteLogger logger) {
            this.logger = logger;
            return this;
        }

        public Builder kernalContext(GridKernalContext kernalContext) {
            this.kernalContext = kernalContext;
            return this;
        }

        public Builder queryProcessor(CalciteQueryProcessor queryProcessor) {
            this.queryProcessor = queryProcessor;
            return this;
        }

        public Builder mappingService(MappingService mappingService) {
            this.mappingService = mappingService;
            return this;
        }

        public Builder exchangeProcessor(ExchangeProcessor exchangeProcessor) {
            this.exchangeProcessor = exchangeProcessor;
            return this;
        }

        public PlannerContext build() {
            return new PlannerContext(parentContext, query, topologyVersion, schema, logger, kernalContext, queryProcessor, mappingService, exchangeProcessor);
        }
    }
}
