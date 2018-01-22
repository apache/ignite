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

package org.apache.ignite.ml.dlearn.context.cache;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.ml.dlearn.DLearnContextFactory;
import org.apache.ignite.ml.dlearn.DLearnPartitionStorage;
import org.apache.ignite.ml.dlearn.context.cache.utils.DLearnPartitionAffinityFunction;
import org.apache.ignite.ml.dlearn.utils.DLearnContextPartitionKey;

/**
 * Factory produces cache learning context by extracting data from other cache.
 *
 * @param <K> type of keys in upstream cache
 * @param <V> type of values in upstream values
 */
public class CacheDLearnContextFactory<K, V> implements DLearnContextFactory<CacheDLearnPartition<K, V>> {
    /** */
    private static final long serialVersionUID = 2903867793242785702L;

    /** Template for learning context cache name. */
    private static final String CONTEXT_CACHE_NAME = "%s_LEARNING_CONTEXT_%s";

    /** Ignite instance. */
    private final  Ignite ignite;

    /** Upstream cache with data. */
    private final IgniteCache<K, V> upstreamCache;

    /**
     * Constructs a new instance of cache learning context factory.
     *
     * @param ignite Ignite instance
     * @param upstreamCache upstream cache
     */
    public CacheDLearnContextFactory(Ignite ignite, IgniteCache<K, V> upstreamCache) {
        this.ignite = ignite;
        this.upstreamCache = upstreamCache;
    }

    /** {@inheritDoc} */
    @Override public CacheDLearnContext<CacheDLearnPartition<K, V>> createContext() {
        CacheConfiguration<DLearnContextPartitionKey, byte[]> learningCtxCacheCfg = new CacheConfiguration<>();
        learningCtxCacheCfg.setName(String.format(CONTEXT_CACHE_NAME, upstreamCache.getName(), UUID.randomUUID()));
        learningCtxCacheCfg.setAffinity(createLearningContextCacheAffinityFunction());

        IgniteCache<DLearnContextPartitionKey, byte[]> learningCtxCache = ignite.createCache(learningCtxCacheCfg);

        Affinity<?> affinity = ignite.affinity(upstreamCache.getName());
        UUID learningCtxId = UUID.randomUUID();

        for (int partIdx = 0; partIdx < affinity.partitions(); partIdx++) {
            DLearnPartitionStorage storage = new CacheDLearnPartitionStorage(learningCtxCache, learningCtxId, partIdx);
            CacheDLearnPartition<K, V> part = new CacheDLearnPartition<>(storage);

            part.setUpstreamCacheName(upstreamCache.getName());
            part.setPart(partIdx);
        }

        return new CacheDLearnContext<>(ignite, learningCtxCache.getName(), CacheDLearnPartition::new, learningCtxId,
            Arrays.asList(upstreamCache.getName(), learningCtxCache.getName()));
    }

    /**
     * Creates learning context cache affinity function based in upstream cache. This function retrieves current
     * topology version and layout (partition-to-node map) of upstream cache corresponding to it, then applies this
     * retrieved layout to create an affinity function for learning context cache.
     *
     * @return affinity function
     */
    private DLearnPartitionAffinityFunction createLearningContextCacheAffinityFunction() {
        Affinity<?> affinity = ignite.affinity(upstreamCache.getName());

        // tries to collect partition-to-node map and checks that topology version hasn't been changed during this
        // process
        List<List<UUID>> initAssignment;
        long topVer;
        while (true) {
            topVer = ignite.cluster().topologyVersion();

            initAssignment = new ArrayList<>(affinity.partitions());

            for (int part = 0; part < affinity.partitions(); part++) {
                Collection<ClusterNode> nodes = affinity.mapPartitionToPrimaryAndBackups(part);
                List<UUID> nodeIds = new ArrayList<>(nodes.size());
                for (ClusterNode node : nodes)
                    nodeIds.add(node.id());
                initAssignment.add(nodeIds);
            }

            // if topology version changed we need to try again
            if (topVer == ignite.cluster().topologyVersion())
                break;
        }

        return new DLearnPartitionAffinityFunction(initAssignment);
    }
}