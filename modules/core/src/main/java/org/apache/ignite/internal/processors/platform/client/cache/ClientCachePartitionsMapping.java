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

package org.apache.ignite.internal.processors.platform.client.cache;

import java.util.ArrayList;
import java.util.Collection;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.binary.BinaryRawWriterEx;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.processors.cache.DynamicCacheDescriptor;
import org.apache.ignite.internal.processors.odbc.ClientListenerProcessor;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;
import org.apache.ignite.internal.sql.optimizer.affinity.PartitionAffinityFunctionType;
import org.apache.ignite.internal.sql.optimizer.affinity.PartitionTableAffinityDescriptor;

/**
 * Partition mapping associated with the group of caches.
 */
class ClientCachePartitionsMapping
{
    /** IDs of the assotiated caches. */
    private ArrayList<Integer> cacheIds;

    /** Affinity descriptor. */
    private PartitionTableAffinityDescriptor affinityDescriptor;

    /** Partitions map for caches. */
    private ArrayList<ClientCacheNodePartitions> partitionsMap;

    /**
     * Get partition map for a cache.
     * @param ctx Context.
     * @param cacheDesc Cache descriptor.
     * @return Partitions mapping for cache.
     */
    private ArrayList<ClientCacheNodePartitions> getPartitionsMap(
        ClientConnectionContext ctx, DynamicCacheDescriptor cacheDesc) {
        String cacheName = cacheDesc.cacheName();

        GridDiscoveryManager discovery = ctx.kernalContext().discovery();
        Collection<ClusterNode> nodes = discovery.discoCache().cacheNodes(cacheName);

        Affinity aff = ctx.kernalContext().affinity().affinityProxy(cacheName);

        ArrayList<ClientCacheNodePartitions> res = new ArrayList<>();

        for (ClusterNode node : nodes) {
            int[] parts = aff.primaryPartitions(node);

            res.add(new ClientCacheNodePartitions(node.id(), parts));
        }

        return res;
    }

    /**
     * @param cacheDesc Descriptor of the initial cache.
     */
    public ClientCachePartitionsMapping(ClientConnectionContext ctx, DynamicCacheDescriptor cacheDesc) {
        cacheIds = new ArrayList<>();

        cacheIds.add(cacheDesc.cacheId());

        affinityDescriptor = affinityForCache(cacheDesc);
        partitionsMap = getPartitionsMap(ctx, cacheDesc);
    }

    /**
     * Adds cache to the mapping if the mapping is applicable for the cache.
     * @param cacheDesc Descriptor of cache.
     */
    public void addIfCompatible(DynamicCacheDescriptor cacheDesc) {
        if (isCompatible(cacheDesc))
            cacheIds.add(cacheDesc.cacheId());
    }

    /**
     * Check if the mapping is applicable for the cache.
     * @param cacheDesc Descriptor of cache to be checked.
     */
    private boolean isCompatible(DynamicCacheDescriptor cacheDesc) {
        PartitionTableAffinityDescriptor another = affinityForCache(cacheDesc);

        if (affinityDescriptor == null)
            return another == null;

        return affinityDescriptor.isCompatible(another);
    }

    /**
     * Prepare affinity identifier for cache.
     *
     * @param cacheDesc Cache descriptor.
     * @return Affinity identifier.
     */
    private static PartitionTableAffinityDescriptor affinityForCache(DynamicCacheDescriptor cacheDesc) {
        CacheConfiguration ccfg = cacheDesc.cacheConfiguration();

        // Partition could be extracted only from PARTITIONED caches.
        if (ccfg.getCacheMode() != CacheMode.PARTITIONED)
            return null;

        PartitionAffinityFunctionType aff = ccfg.getAffinity().getClass().equals(RendezvousAffinityFunction.class) ?
            PartitionAffinityFunctionType.RENDEZVOUS : PartitionAffinityFunctionType.CUSTOM;

        boolean hasNodeFilter = ccfg.getNodeFilter() != null &&
            !(ccfg.getNodeFilter() instanceof CacheConfiguration.IgniteAllNodesPredicate);

        return new PartitionTableAffinityDescriptor(
            aff,
            ccfg.getAffinity().partitions(),
            hasNodeFilter,
            ccfg.getDataRegionName()
        );
    }

    public void write(BinaryRawWriterEx writer) {

    }
}
