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

package org.apache.ignite.internal.management.cache;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientException;
import org.apache.ignite.internal.client.GridClientNode;
import org.apache.ignite.internal.management.api.CommandUtils;
import org.apache.ignite.internal.management.api.LocalCommand;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.jetbrains.annotations.Nullable;

import static java.util.Collections.singletonList;
import static org.apache.ignite.internal.management.api.CommandUtils.nodes;
import static org.apache.ignite.internal.management.cache.ViewCacheCmd.CACHES;
import static org.apache.ignite.internal.management.cache.ViewCacheCmd.GROUPS;
import static org.apache.ignite.internal.management.cache.ViewCacheCmd.SEQ;

/** Prints info regarding caches, groups or sequences. */
public class CacheListCommand implements LocalCommand<CacheListCommandArg, ViewCacheTaskResult> {
    /** */
    Function<CacheListCommandArg, Predicate<GridClientNode>> FILTER = arg -> node ->
        node.connectable() && (arg.nodeId() == null || Objects.equals(node.nodeId(), arg.nodeId()));

    /** {@inheritDoc} */
    @Override public String description() {
        return "Show information about caches, groups or sequences that match a regular expression. " +
            "When executed without parameters, this subcommand prints the list of caches";
    }

    /** {@inheritDoc} */
    @Override public Class<CacheListCommandArg> argClass() {
        return CacheListCommandArg.class;
    }

    /** {@inheritDoc} */
    @Override public ViewCacheTaskResult execute(
        @Nullable GridClient cli,
        @Nullable Ignite ignite,
        CacheListCommandArg arg,
        Consumer<String> printer
    ) throws GridClientException {
        ViewCacheCmd cmd = arg.groups()
            ? GROUPS
            : (arg.seq() ? SEQ : CACHES);

        Optional<GridClientNode> node = nodes(cli, ignite)
            .stream()
            .filter(FILTER.apply(arg))
            .findFirst();

        if (!node.isPresent())
            throw new IllegalArgumentException("Node not found: id=" + arg.nodeId());

        ViewCacheTaskResult res = CommandUtils.execute(cli, ignite, ViewCacheTask.class, arg, singletonList(node.get()));

        if (arg.config() && cmd == CACHES)
            cachesConfig(cli, ignite, arg, res, printer);
        else
            printCacheInfos(res.cacheInfos(), cmd, printer);

        return res;
    }

    /**
     * @param cli Client.
     * @param arg Cache argument.
     * @param viewRes Cache view task result.
     */
    private void cachesConfig(
        GridClient cli,
        Ignite ignite,
        CacheListCommandArg arg,
        ViewCacheTaskResult viewRes,
        Consumer<String> printer
    ) throws GridClientException {
        Collection<GridClientNode> nodes = nodes(cli, ignite)
            .stream()
            .filter(FILTER.apply(arg))
            .collect(Collectors.toSet());

        Map<String, CacheConfiguration> res = CommandUtils.execute(
            cli,
            ignite,
            CacheConfigurationCollectorTask.class,
            new CacheConfigurationCollectorTaskArg(arg.regex()),
            nodes
        );

        Map<String, Integer> cacheToMapped =
            viewRes.cacheInfos().stream().collect(Collectors.toMap(CacheInfo::getCacheName, CacheInfo::getMapped));

        printCachesConfig(res, OutputFormat.fromConsoleName(arg.outputFormat()), cacheToMapped, printer);
    }

    /**
     * Prints caches info.
     *
     * @param infos Caches info.
     * @param cmd Command.
     */
    private void printCacheInfos(Collection<CacheInfo> infos, ViewCacheCmd cmd, Consumer<String> printer) {
        for (CacheInfo info : infos) {
            Map<String, Object> map = info.toMap(cmd);

            SB sb = new SB("[");

            for (Map.Entry<String, Object> e : map.entrySet())
                sb.a(e.getKey()).a("=").a(e.getValue()).a(", ");

            sb.setLength(sb.length() - 2);

            sb.a("]");

            printer.accept(sb.toString());
        }
    }

    /**
     * Prints caches config.
     *
     * @param caches Caches config.
     * @param outputFormat Output format.
     * @param cacheToMapped Map cache name to mapped.
     */
    private void printCachesConfig(
        Map<String, CacheConfiguration> caches,
        OutputFormat outputFormat,
        Map<String, Integer> cacheToMapped,
        Consumer<String> printer
    ) {

        for (Map.Entry<String, CacheConfiguration> entry : caches.entrySet()) {
            String cacheName = entry.getKey();

            switch (outputFormat) {
                case MULTI_LINE:
                    Map<String, Object> params = mapToPairs(entry.getValue());

                    params.put("Mapped", cacheToMapped.get(cacheName));

                    printer.accept(String.format("[cache = '%s']", cacheName));

                    for (Map.Entry<String, Object> innerEntry : params.entrySet())
                        printer.accept(String.format("%s: %s", innerEntry.getKey(), innerEntry.getValue()));

                    printer.accept("");

                    break;

                default:
                    int mapped = cacheToMapped.get(cacheName);

                    printer.accept(String.format("%s: %s %s=%s", entry.getKey(), toString(entry.getValue()), "mapped", mapped));

                    break;
            }
        }
    }

    /**
     * Maps CacheConfiguration to key-value pairs.
     *
     * @param cfg Visor cache configuration.
     * @return map of key-value pairs.
     */
    private static Map<String, Object> mapToPairs(CacheConfiguration cfg) {
        Map<String, Object> params = new LinkedHashMap<>();

        CacheAffinityConfiguration affinityCfg = cfg.getAffinityConfiguration();
        CacheNearConfiguration nearCfg = cfg.getNearConfiguration();
        CacheRebalanceConfiguration rebalanceCfg = cfg.getRebalanceConfiguration();
        CacheEvictionConfiguration evictCfg = cfg.getEvictionConfiguration();
        CacheStoreConfiguration storeCfg = cfg.getStoreConfiguration();
        QueryConfiguration qryCfg = cfg.getQueryConfiguration();

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
     * Invokes toString() method and cuts class name from result string.
     *
     * @param cfg Visor cache configuration for invocation.
     * @return String representation without class name in begin of string.
     */
    private String toString(CacheConfiguration cfg) {
        return cfg.toString().substring(cfg.getClass().getSimpleName().length() + 1);
    }

    /** */
    public static enum OutputFormat {
        /** Single line. */
        SINGLE_LINE("single-line"),

        /** Multi line. */
        MULTI_LINE("multi-line");

        /** */
        private final String text;

        /** */
        OutputFormat(String text) {
            this.text = text;
        }

        /**
         * @return Text.
         */
        public String text() {
            return text;
        }

        /**
         * Converts format name in console to enumerated value.
         *
         * @param text Format name in console.
         * @return Enumerated value.
         * @throws IllegalArgumentException If enumerated value not found.
         */
        public static OutputFormat fromConsoleName(String text) {
            if (text == null)
                return SINGLE_LINE;

            for (OutputFormat format : values()) {
                if (format.text.equals(text))
                    return format;
            }

            throw new IllegalArgumentException("Unknown output format " + text);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return text;
        }
    }
}
