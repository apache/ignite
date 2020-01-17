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

import java.util.Collections;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.function.Predicate;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.query.calcite.CalciteQueryProcessor;
import org.apache.ignite.internal.processors.query.calcite.exec.ExchangeService;
import org.apache.ignite.internal.processors.query.calcite.exec.MailboxRegistry;
import org.apache.ignite.internal.processors.query.calcite.exec.QueryTaskExecutor;
import org.apache.ignite.internal.processors.query.calcite.message.MessageService;
import org.apache.ignite.internal.processors.query.calcite.metadata.MappingService;
import org.apache.ignite.internal.processors.query.calcite.metadata.NodesMapping;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;

/**
 * Planner context, encapsulates services, kernal context, query string and its flags and parameters and helper methods
 * to work with them.
 */
public final class IgniteCalciteContext implements Context {
    /** */
    private final UUID localNodeId;

    /** */
    private final UUID originatingNodeId;

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
    private final ExchangeService exchangeService;

    /** */
    private final MailboxRegistry mailboxRegistry;

    /** */
    private final QueryTaskExecutor taskExecutor;

    /** */
    private final MessageService messageService;

    /** */
    private IgnitePlanner planner;

    /** */
    private CalciteConnectionConfig connectionConfig;

    /** */
    private JavaTypeFactory typeFactory;

    /**
     * Private constructor, used by a builder.
     */
    private IgniteCalciteContext(UUID localNodeId, UUID originatingNodeId, Query query, Context parentContext,
        GridKernalContext kernalContext, FrameworkConfig config, AffinityTopologyVersion topologyVersion,
        CalciteQueryProcessor queryProcessor, MappingService mappingService, ExchangeService exchangeService,
        MailboxRegistry mailboxRegistry, QueryTaskExecutor taskExecutor, MessageService messageService, IgniteLogger logger) {
        this.parentContext = parentContext;
        this.query = query;
        this.topologyVersion = topologyVersion;
        this.mailboxRegistry = mailboxRegistry;
        this.messageService = messageService;
        this.logger = logger;
        this.kernalContext = kernalContext;
        this.queryProcessor = queryProcessor;
        this.mappingService = mappingService;
        this.exchangeService = exchangeService;
        this.taskExecutor = taskExecutor;
        this.originatingNodeId = originatingNodeId == null ? localNodeId : originatingNodeId;
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
     * @return Originating node ID (the node, who started the execution).
     */
    public UUID originatingNodeId() {
        return originatingNodeId;
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
    public GridKernalContext kernal() {
        return kernalContext;
    }

    /**
     * @return Query processor.
     */
    public CalciteQueryProcessor queryProcessor() {
        return queryProcessor;
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
     * @return Mapping service.
     */
    public MappingService mappingService() {
        return mappingService;
    }

    /**
     * @return Exchange processor.
     */
    public ExchangeService exchangeService() {
        return exchangeService;
    }

    /**
     * @return Mailbox registry.
     */
    public MailboxRegistry mailboxRegistry() {
        return mailboxRegistry;
    }

    /**
     * @return Query task executor.
     */
    public QueryTaskExecutor taskExecutor() {
        return taskExecutor;
    }

    /**
     * @return Message service.
     */
    public MessageService messageService() {
        return messageService;
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
        return new NodesMapping(Collections.singletonList(localNodeId), null, (byte) (NodesMapping.CLIENT | NodesMapping.DEDUPLICATED));
    }

    /**
     * Returns Nodes mapping for intermediate fragments, without Scan nodes leafs. Such fragments may be executed
     * on any cluster node, actual list of nodes is chosen on the basis of adopted selection strategy (using nodes filter).
     *
     * @return Nodes mapping for intermediate fragments.
     * @param desiredCnt desired nodes count, {@code 0} means all possible nodes.
     * @param nodeFilter Node filter.
     */
    public NodesMapping mapForIntermediate(int desiredCnt, Predicate<ClusterNode> nodeFilter) {
        assert desiredCnt >= 0;

        return mappingService.intermediateMapping(topologyVersion, desiredCnt, nodeFilter);
    }

    /**
     * @param cacheId Cache ID.
     * @return Nodes mapping for particular table, depends on underlying cache distribution.
     */
    public NodesMapping mapForCache(int cacheId) {
        return mappingService.cacheMapping(cacheId, topologyVersion);
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
        return taskExecutor.execute(queryId, fragmentId, queryTask);
    }

    /**
     * @return New cluster based on a planner and its configuration.
     */
    public RelOptCluster createCluster() {
        return planner().createCluster();
    }

    /**
     * @return Keep binary flag.
     */
    public boolean keepBinary() {
        return false; // TODO extract from QueryContext
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
    public static Builder builder(IgniteCalciteContext template) {
        return new Builder()
            .messageService(template.messageService)
            .taskExecutor(template.taskExecutor)
            .exchangeService(template.exchangeService)
            .mailboxRegistry(template.mailboxRegistry)
            .mappingService(template.mappingService)
            .queryProcessor(template.queryProcessor)
            .kernalContext(template.kernalContext)
            .logger(template.logger)
            .topologyVersion(template.topologyVersion)
            .query(template.query)
            .parentContext(template.parentContext)
            .frameworkConfig(template.frameworkConfig)
            .originatingNodeId(template.originatingNodeId)
            .localNodeId(template.localNodeId);
    }

    /**
     * Planner context builder.
     */
    public static class Builder {
        /** */
        private UUID localNodeId;

        /** */
        private UUID originatingNodeId;

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
        private ExchangeService exchangeService;

        /** */
        private MailboxRegistry mailboxRegistry;

        /** */
        private QueryTaskExecutor taskExecutor;

        /** */
        private MessageService messageService;

        /**
         * @param localNodeId Local node ID.
         * @return Builder for chaining.
         */
        public Builder localNodeId(UUID localNodeId) {
            this.localNodeId = localNodeId;
            return this;
        }

        /**
         * @param originatingNodeId Originating node ID (the node, who started the execution).
         * @return Builder for chaining.
         */
        public Builder originatingNodeId(UUID originatingNodeId) {
            this.originatingNodeId = originatingNodeId;
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
         * @param exchangeService Exchange processor.
         * @return Builder for chaining.
         */
        public Builder exchangeService(ExchangeService exchangeService) {
            this.exchangeService = exchangeService;
            return this;
        }

        /**
         * @param mailboxRegistry Mailbox registry.
         * @return Builder for chaining.
         */
        public Builder mailboxRegistry(MailboxRegistry mailboxRegistry) {
            this.mailboxRegistry = mailboxRegistry;
            return this;
        }

        /**
         * @param taskExecutor Query task executor.
         * @return Builder for chaining.
         */
        public Builder taskExecutor(QueryTaskExecutor taskExecutor) {
            this.taskExecutor = taskExecutor;
            return this;
        }

        /**
         * @param messageService Message service.
         * @return Builder for chaining.
         */
        public Builder messageService(MessageService messageService) {
            this.messageService = messageService;
            return this;
        }

        /**
         * Builds planner context.
         *
         * @return Planner context.
         */
        public IgniteCalciteContext build() {
            return new IgniteCalciteContext(localNodeId, originatingNodeId, query, parentContext, kernalContext, frameworkConfig,
                topologyVersion, queryProcessor, mappingService, exchangeService, mailboxRegistry, taskExecutor, messageService, logger);
        }
    }
}
