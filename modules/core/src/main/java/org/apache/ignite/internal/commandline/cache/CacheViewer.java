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

package org.apache.ignite.internal.commandline.cache;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.client.GridClientException;
import org.apache.ignite.internal.commandline.Command;
import org.apache.ignite.internal.commandline.CommandArgIterator;
import org.apache.ignite.internal.commandline.OutputFormat;
import org.apache.ignite.internal.commandline.TaskExecutor;
import org.apache.ignite.internal.commandline.argument.CommandArgUtils;
import org.apache.ignite.internal.commandline.cache.argument.ListCommandArg;
import org.apache.ignite.internal.processors.cache.verify.CacheInfo;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.cache.VisorCacheAffinityConfiguration;
import org.apache.ignite.internal.visor.cache.VisorCacheConfiguration;
import org.apache.ignite.internal.visor.cache.VisorCacheConfigurationCollectorTask;
import org.apache.ignite.internal.visor.cache.VisorCacheConfigurationCollectorTaskArg;
import org.apache.ignite.internal.visor.cache.VisorCacheEvictionConfiguration;
import org.apache.ignite.internal.visor.cache.VisorCacheNearConfiguration;
import org.apache.ignite.internal.visor.cache.VisorCacheRebalanceConfiguration;
import org.apache.ignite.internal.visor.cache.VisorCacheStoreConfiguration;
import org.apache.ignite.internal.visor.query.VisorQueryConfiguration;
import org.apache.ignite.internal.visor.verify.VisorViewCacheCmd;
import org.apache.ignite.internal.visor.verify.VisorViewCacheTask;
import org.apache.ignite.internal.visor.verify.VisorViewCacheTaskArg;
import org.apache.ignite.internal.visor.verify.VisorViewCacheTaskResult;

import static org.apache.ignite.internal.commandline.CommandLogger.optional;
import static org.apache.ignite.internal.commandline.CommandLogger.or;
import static org.apache.ignite.internal.commandline.OutputFormat.MULTI_LINE;
import static org.apache.ignite.internal.commandline.OutputFormat.SINGLE_LINE;
import static org.apache.ignite.internal.commandline.TaskExecutor.executeTaskByNameOnNode;
import static org.apache.ignite.internal.commandline.cache.CacheCommands.OP_NODE_ID;
import static org.apache.ignite.internal.commandline.cache.CacheCommands.usageCache;
import static org.apache.ignite.internal.commandline.cache.CacheSubcommands.LIST;
import static org.apache.ignite.internal.commandline.cache.argument.ListCommandArg.CONFIG;
import static org.apache.ignite.internal.commandline.cache.argument.ListCommandArg.GROUP;
import static org.apache.ignite.internal.commandline.cache.argument.ListCommandArg.OUTPUT_FORMAT;
import static org.apache.ignite.internal.commandline.cache.argument.ListCommandArg.SEQUENCE;
import static org.apache.ignite.internal.visor.verify.VisorViewCacheCmd.CACHES;
import static org.apache.ignite.internal.visor.verify.VisorViewCacheCmd.GROUPS;
import static org.apache.ignite.internal.visor.verify.VisorViewCacheCmd.SEQ;

/**
 * Command to show caches on cluster.
 */
public class CacheViewer implements Command<CacheViewer.Arguments> {
    /** {@inheritDoc} */
    @Override public void printUsage(Logger logger) {
        String description = "Show information about caches, groups or sequences that match a regular expression. " +
            "When executed without parameters, this subcommand prints the list of caches.";

        Map<String, String> map = U.newLinkedHashMap(16);

        map.put(CONFIG.toString(), "print all configuration parameters for each cache.");
        map.put(OUTPUT_FORMAT + " " + MULTI_LINE, "print configuration parameters per line. This option has effect only " +
            "when used with " + CONFIG + " and without " + optional(or(GROUP, SEQUENCE)) + ".");
        map.put(GROUP.toString(), "print information about groups.");
        map.put(SEQUENCE.toString(), "print information about sequences.");

        usageCache(logger, LIST, description, map, "regexPattern",
            optional(or(GROUP, SEQUENCE)), OP_NODE_ID, optional(CONFIG), optional(OUTPUT_FORMAT, MULTI_LINE));
    }

    /**
     * Container for command arguments.
     */
    public static class Arguments {
        /** Regex. */
        private String regex;

        /** Full config flag. */
        private boolean fullConfig;

        /** Node id. */
        private UUID nodeId;

        /** Cache view command. */
        private VisorViewCacheCmd cacheCmd;

        /** Output format. */
        private OutputFormat outputFormat;

        /**
         *
         */
        public Arguments(String regex, boolean fullConfig, UUID nodeId, VisorViewCacheCmd cacheCmd, OutputFormat outputFormat) {
            this.regex = regex;
            this.fullConfig = fullConfig;
            this.nodeId = nodeId;
            this.cacheCmd = cacheCmd;
            this.outputFormat = outputFormat;
        }

        /**
         * @return Regex.
         */
        public String regex() {
            return regex;
        }

        /**
         * @return Node id.
         */
        public UUID nodeId() {
            return nodeId;
        }

        /**
         * @return Output format.
         */
        public OutputFormat outputFormat() { return outputFormat; }

        /**
         * @return Cache view command.
         */
        public VisorViewCacheCmd cacheCommand() {
            return cacheCmd;
        }

        /**
         * @return Full config flag.
         */
        public boolean fullConfig() { return fullConfig; }
    }

    /** Command parsed arguments */
    private Arguments args;

    /** {@inheritDoc} */
    @Override public Arguments arg() {
        return args;
    }

    /** {@inheritDoc} */
    @Override public Object execute(GridClientConfiguration clientCfg, Logger logger) throws Exception {
        VisorViewCacheTaskArg taskArg = new VisorViewCacheTaskArg(args.regex(), args.cacheCommand());

        VisorViewCacheTaskResult res;

        try (GridClient client = Command.startClient(clientCfg)) {
            res = TaskExecutor.executeTaskByNameOnNode(
                client,
                VisorViewCacheTask.class.getName(),
                taskArg,
                args.nodeId(),
                clientCfg
            );

            if (args.fullConfig() && args.cacheCommand() == CACHES)
                cachesConfig(client, args, res, clientCfg, logger);
            else
                printCacheInfos(res.cacheInfos(), args.cacheCommand(), logger);
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override public void parseArguments(CommandArgIterator argIter) {
        String regex = argIter.nextArg("Regex is expected");
        boolean fullConfig = false;
        VisorViewCacheCmd cacheCmd = CACHES;
        OutputFormat outputFormat = SINGLE_LINE;
        UUID nodeId = null;

        while (argIter.hasNextSubArg()) {
            String nextArg = argIter.nextArg("").toLowerCase();

            ListCommandArg arg = CommandArgUtils.of(nextArg, ListCommandArg.class);

            if (arg != null) {
                switch (arg) {
                    case GROUP:
                        cacheCmd = GROUPS;

                        break;

                    case SEQUENCE:
                        cacheCmd = SEQ;

                        break;

                    case OUTPUT_FORMAT:
                        String tmp2 = argIter.nextArg("output format must be defined!").toLowerCase();

                        outputFormat = OutputFormat.fromConsoleName(tmp2);

                        break;

                    case CONFIG:
                        fullConfig = true;

                        break;
                }
            }
            else
                nodeId = UUID.fromString(nextArg);
        }

        args = new Arguments(regex, fullConfig, nodeId, cacheCmd, outputFormat);
    }

    /**
     * Maps VisorCacheConfiguration to key-value pairs.
     *
     * @param cfg Visor cache configuration.
     * @return map of key-value pairs.
     */
    private static Map<String, Object> mapToPairs(VisorCacheConfiguration cfg) {
        Map<String, Object> params = new LinkedHashMap<>();

        VisorCacheAffinityConfiguration affinityCfg = cfg.getAffinityConfiguration();
        VisorCacheNearConfiguration nearCfg = cfg.getNearConfiguration();
        VisorCacheRebalanceConfiguration rebalanceCfg = cfg.getRebalanceConfiguration();
        VisorCacheEvictionConfiguration evictCfg = cfg.getEvictionConfiguration();
        VisorCacheStoreConfiguration storeCfg = cfg.getStoreConfiguration();
        VisorQueryConfiguration qryCfg = cfg.getQueryConfiguration();

        params.put("Name", cfg.getName());
        params.put("Group", cfg.getGroupName());
        params.put("Dynamic Deployment ID", cfg.getDynamicDeploymentId());
        params.put("System", cfg.isSystem());

        params.put("Mode", cfg.getMode());
        params.put("Atomicity Mode", cfg.getAtomicityMode());
        params.put("Statistic Enabled", cfg.isStatisticsEnabled());
        params.put("Management Enabled", cfg.isManagementEnabled());

        params.put("On-heap cache enabled", cfg.isOnheapCacheEnabled());
        params.put("Partition Loss Policy", cfg.getPartitionLossPolicy());
        params.put("Query Parallelism", cfg.getQueryParallelism());
        params.put("Copy On Read", cfg.isCopyOnRead());
        params.put("Listener Configurations", cfg.getListenerConfigurations());
        params.put("Load Previous Value", cfg.isLoadPreviousValue());
        params.put("Memory Policy Name", cfg.getMemoryPolicyName());
        params.put("Node Filter", cfg.getNodeFilter());
        params.put("Read From Backup", cfg.isReadFromBackup());
        params.put("Topology Validator", cfg.getTopologyValidator());

        params.put("Time To Live Eager Flag", cfg.isEagerTtl());

        params.put("Write Synchronization Mode", cfg.getWriteSynchronizationMode());
        params.put("Invalidate", cfg.isInvalidate());

        params.put("Affinity Function", affinityCfg.getFunction());
        params.put("Affinity Backups", affinityCfg.getPartitionedBackups());
        params.put("Affinity Partitions", affinityCfg.getPartitions());
        params.put("Affinity Exclude Neighbors", affinityCfg.isExcludeNeighbors());
        params.put("Affinity Mapper", affinityCfg.getMapper());

        params.put("Rebalance Mode", rebalanceCfg.getMode());
        params.put("Rebalance Batch Size", rebalanceCfg.getBatchSize());
        params.put("Rebalance Timeout", rebalanceCfg.getTimeout());
        params.put("Rebalance Delay", rebalanceCfg.getPartitionedDelay());
        params.put("Time Between Rebalance Messages", rebalanceCfg.getThrottle());
        params.put("Rebalance Batches Count", rebalanceCfg.getBatchesPrefetchCnt());
        params.put("Rebalance Cache Order", rebalanceCfg.getRebalanceOrder());

        params.put("Eviction Policy Enabled", (evictCfg.getPolicy() != null));
        params.put("Eviction Policy Factory", evictCfg.getPolicy());
        params.put("Eviction Policy Max Size", evictCfg.getPolicyMaxSize());
        params.put("Eviction Filter", evictCfg.getFilter());

        params.put("Near Cache Enabled", nearCfg.isNearEnabled());
        params.put("Near Start Size", nearCfg.getNearStartSize());
        params.put("Near Eviction Policy Factory", nearCfg.getNearEvictPolicy());
        params.put("Near Eviction Policy Max Size", nearCfg.getNearEvictMaxSize());

        params.put("Default Lock Timeout", cfg.getDefaultLockTimeout());
        params.put("Query Entities", cfg.getQueryEntities());
        params.put("Cache Interceptor", cfg.getInterceptor());

        params.put("Store Enabled", storeCfg.isEnabled());
        params.put("Store Class", storeCfg.getStore());
        params.put("Store Factory Class", storeCfg.getStoreFactory());
        params.put("Store Keep Binary", storeCfg.isStoreKeepBinary());
        params.put("Store Read Through", storeCfg.isReadThrough());
        params.put("Store Write Through", storeCfg.isWriteThrough());
        params.put("Store Write Coalescing", storeCfg.getWriteBehindCoalescing());

        params.put("Write-Behind Enabled", storeCfg.isWriteBehindEnabled());
        params.put("Write-Behind Flush Size", storeCfg.getFlushSize());
        params.put("Write-Behind Frequency", storeCfg.getFlushFrequency());
        params.put("Write-Behind Flush Threads Count", storeCfg.getFlushThreadCount());
        params.put("Write-Behind Batch Size", storeCfg.getBatchSize());

        params.put("Concurrent Asynchronous Operations Number", cfg.getMaxConcurrentAsyncOperations());

        params.put("Loader Factory Class Name", cfg.getLoaderFactory());
        params.put("Writer Factory Class Name", cfg.getWriterFactory());
        params.put("Expiry Policy Factory Class Name", cfg.getExpiryPolicyFactory());

        params.put("Query Execution Time Threshold", qryCfg.getLongQueryWarningTimeout());
        params.put("Query Escaped Names", qryCfg.isSqlEscapeAll());
        params.put("Query SQL Schema", qryCfg.getSqlSchema());
        params.put("Query SQL functions", qryCfg.getSqlFunctionClasses());
        params.put("Query Indexed Types", qryCfg.getIndexedTypes());
        params.put("Maximum payload size for offheap indexes", cfg.getSqlIndexMaxInlineSize());
        params.put("Query Metrics History Size", cfg.getQueryDetailMetricsSize());

        return params;
    }

    /**
     * Prints caches config.
     *
     * @param caches Caches config.
     * @param outputFormat Output format.
     * @param cacheToMapped Map cache name to mapped.
     */
    private void printCachesConfig(
        Map<String, VisorCacheConfiguration> caches,
        OutputFormat outputFormat,
        Map<String, Integer> cacheToMapped,
        Logger logger
    ) {

        for (Map.Entry<String, VisorCacheConfiguration> entry : caches.entrySet()) {
            String cacheName = entry.getKey();

            switch (outputFormat) {
                case MULTI_LINE:
                    Map<String, Object> params = mapToPairs(entry.getValue());

                    params.put("Mapped", cacheToMapped.get(cacheName));

                    logger.info(String.format("[cache = '%s']%n", cacheName));

                    for (Map.Entry<String, Object> innerEntry : params.entrySet())
                        logger.info(String.format("%s: %s%n", innerEntry.getKey(), innerEntry.getValue()));

                    logger.info("");

                    break;

                default:
                    int mapped = cacheToMapped.get(cacheName);

                    logger.info(String.format("%s: %s %s=%s%n", entry.getKey(), toString(entry.getValue()), "mapped", mapped));

                    break;
            }
        }
    }

    /**
     * Invokes toString() method and cuts class name from result string.
     *
     * @param cfg Visor cache configuration for invocation.
     * @return String representation without class name in begin of string.
     */
    private String toString(VisorCacheConfiguration cfg) {
        return cfg.toString().substring(cfg.getClass().getSimpleName().length() + 1);
    }

    /**
     * @param client Client.
     * @param cacheArgs Cache args.
     * @param viewRes Cache view task result.
     * @param clientCfg Client configuration.
     */
    private void cachesConfig(
        GridClient client,
        Arguments cacheArgs,
        VisorViewCacheTaskResult viewRes,
        GridClientConfiguration clientCfg,
        Logger logger
    ) throws GridClientException {
        VisorCacheConfigurationCollectorTaskArg taskArg = new VisorCacheConfigurationCollectorTaskArg(cacheArgs.regex());

        UUID nodeId = cacheArgs.nodeId() == null ? TaskExecutor.BROADCAST_UUID : cacheArgs.nodeId();

        Map<String, VisorCacheConfiguration> res =
            executeTaskByNameOnNode(client, VisorCacheConfigurationCollectorTask.class.getName(), taskArg, nodeId, clientCfg);

        Map<String, Integer> cacheToMapped =
            viewRes.cacheInfos().stream().collect(Collectors.toMap(CacheInfo::getCacheName, CacheInfo::getMapped));

        printCachesConfig(res, cacheArgs.outputFormat(), cacheToMapped, logger);
    }

    /**
     * Prints caches info.
     *
     * @param infos Caches info.
     * @param cmd Command.
     */
    private void printCacheInfos(Collection<CacheInfo> infos, VisorViewCacheCmd cmd, Logger logger) {
        for (CacheInfo info : infos) {
            Map<String, Object> map = info.toMap(cmd);

            SB sb = new SB("[");

            for (Map.Entry<String, Object> e : map.entrySet())
                sb.a(e.getKey()).a("=").a(e.getValue()).a(", ");

            sb.setLength(sb.length() - 2);

            sb.a("]");

            logger.info(sb.toString());
        }
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return LIST.text().toUpperCase();
    }
}
