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
package org.apache.ignite.internal.visor.cache;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.regex.Pattern;
import org.apache.ignite.IgniteException;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.cache.IgniteCacheProxy;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorMultiNodeTask;
import org.apache.ignite.internal.visor.query.VisorQueryConfiguration;
import org.jetbrains.annotations.Nullable;

/**
 * Task that will collect caches configuration matching with regex.
 */
@GridInternal
public class VisorCachesConfigurationListTask
    extends VisorMultiNodeTask<String, SortedMap<String, Map<String, Object>>, SortedMap<String, Map<String, Object>>> {
    /** */
    private static final long serialVersionUID = 0L;

    @Override protected VisorJob<String, SortedMap<String, Map<String, Object>>> job(String regex) {
        return new VisorCachesConfigurationListJob(regex, debug);
    }

    @Nullable @Override
    protected SortedMap<String, Map<String, Object>> reduce0(List<ComputeJobResult> results) throws IgniteException {
        SortedMap<String, Map<String, Object>> resultMap = new TreeMap<>();

        for (ComputeJobResult res : results)
            resultMap.putAll(res.getData());

        return resultMap;
    }

    /** Job that will find affinity node for key. */
    private static class VisorCachesConfigurationListJob
        extends VisorJob<String, SortedMap<String, Map<String, Object>>> {

        /** Regex. */
        private String regex;

        private VisorCachesConfigurationListJob(String regex, boolean debug) {
            super(regex, debug);

            this.regex = regex;
        }

        @Override protected SortedMap<String, Map<String, Object>> run(@Nullable String arg) throws IgniteException {
            Pattern compiled = Pattern.compile(regex);

            SortedMap<String, Map<String, Object>> res = new TreeMap<>();

            Collection<IgniteCacheProxy<?, ?>> caches = ignite.context().cache().jcaches();

            for (IgniteCacheProxy<?, ?> cache : caches) {
                String cacheName = cache.getName();

                if (compiled.matcher(cacheName).find()) {
                    VisorCacheConfiguration visorCfg =
                        new VisorCacheConfiguration(
                            ignite,
                            cache.getConfiguration(CacheConfiguration.class),
                            cache.context().dynamicDeploymentId()
                        );

                    res.put(cacheName, cfgParams(visorCfg));
                }
            }

            return res;
        }

        private Map<String, Object> cfgParams(VisorCacheConfiguration cfg) {
            Map<String, Object> params = new LinkedHashMap<>();

            VisorCacheAffinityConfiguration affinityCfg = cfg.getAffinityConfiguration();
            VisorCacheNearConfiguration nearCfg = cfg.getNearConfiguration();
            VisorCacheRebalanceConfiguration rebalanceCfg = cfg.getRebalanceConfiguration();
            VisorCacheEvictionConfiguration evictCfg = cfg.getEvictionConfiguration();
            VisorCacheStoreConfiguration storeCfg = cfg.getStoreConfiguration();
            VisorQueryConfiguration queryCfg = cfg.getQueryConfiguration();

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

            params.put("Query Execution Time Threshold", queryCfg.getLongQueryWarningTimeout());
            params.put("Query Escaped Names", queryCfg.isSqlEscapeAll());
            params.put("Query SQL Schema", queryCfg.getSqlSchema());
            params.put("Query SQL functions", queryCfg.getSqlFunctionClasses());
            params.put("Query Indexed Types", queryCfg.getIndexedTypes());
            params.put("Maximum payload size for offheap indexes", cfg.getSqlIndexMaxInlineSize());
            params.put("Query Metrics History Size", cfg.getQueryDetailMetricsSize());

            return params;
        }
    }
}