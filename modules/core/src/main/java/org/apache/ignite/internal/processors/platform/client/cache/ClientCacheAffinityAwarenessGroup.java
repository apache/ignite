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
import java.util.Set;
import org.apache.ignite.binary.BinaryRawWriter;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.AffinityKeyMapper;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.affinity.AffinityAssignment;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.DynamicCacheDescriptor;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheDefaultAffinityKeyMapper;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;
import org.apache.ignite.internal.processors.platform.client.ClientPartitionAffinityDescriptor;

/**
 * Partition mapping associated with the group of caches.
 */
class ClientCachePartitionsMapping
{
    /** IDs of the associated caches. */
    private ArrayList<Integer> cacheIds;

    /** Affinity descriptor. */
    private ClientPartitionAffinityDescriptor affinityDescriptor;

    /** Partitions map for caches. */
    private ArrayList<ClientCacheNodePartitions> partitionsMap;

    /**
     * Get partition map for a cache.
     * @param ctx Context.
     * @param cacheDesc Cache descriptor.
     * @return Partitions mapping for cache.
     */
    private ArrayList<ClientCacheNodePartitions> getPartitionsMap(ClientConnectionContext ctx,
        DynamicCacheDescriptor cacheDesc, AffinityTopologyVersion affVer) {

        GridCacheContext cacheContext = ctx.kernalContext().cache().context().cacheContext(cacheDesc.cacheId());
        AffinityAssignment assignment = cacheContext.affinity().assignment(affVer);
        Collection<ClusterNode> nodes = assignment.primaryPartitionNodes();

        ArrayList<ClientCacheNodePartitions> res = new ArrayList<>();

        for (ClusterNode node : nodes) {
            Set<Integer> parts = assignment.primaryPartitions(node.id());

            res.add(new ClientCacheNodePartitions(node.id(), parts));
        }

        return res;
    }

    /**
     * @param cacheDesc Descriptor of the initial cache.
     */
    public ClientCachePartitionsMapping(ClientConnectionContext ctx, DynamicCacheDescriptor cacheDesc,
        AffinityTopologyVersion affVer) {
        cacheIds = new ArrayList<>();

        cacheIds.add(cacheDesc.cacheId());

        affinityDescriptor = affinityForCache(cacheDesc);
        partitionsMap = getPartitionsMap(ctx, cacheDesc, affVer);
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
        ClientPartitionAffinityDescriptor another = affinityForCache(cacheDesc);

        if (affinityDescriptor == null)
            return another == null;

        return affinityDescriptor.isCompatible(another);
    }

    /**
     * Prepare affinity identifier for cache.
     *
     * @param cacheDesc Cache descriptor.
     * @return Affinity identifier, if applicable, and null if not.
     */
    private static ClientPartitionAffinityDescriptor affinityForCache(DynamicCacheDescriptor cacheDesc) {
        CacheConfiguration ccfg = cacheDesc.cacheConfiguration();

        // Only caches with no custom affinity key mapper is supported.
        // TODO: Investigate
        AffinityKeyMapper keyMapper = ccfg.getAffinityMapper();
        if (!(keyMapper instanceof GridCacheDefaultAffinityKeyMapper))
            return null;

        // Partition could be extracted only from PARTITIONED caches.
        if (ccfg.getCacheMode() != CacheMode.PARTITIONED)
            return null;

        // Only RendezvousAffinityFunction is supported for now.
        if (!ccfg.getAffinity().getClass().equals(RendezvousAffinityFunction.class))
            return null;

        return new ClientPartitionAffinityDescriptor(
            ccfg.getAffinity().partitions()
        );
    }

    /**
     * Write mapping using binary writer.
     * @param writer Writer.
     */
    public void write(BinaryRawWriter writer) {
        writer.writeInt(cacheIds.size());

        for (int id: cacheIds)
            writer.writeInt(id);

        writer.writeBoolean(affinityDescriptor != null);

        if (affinityDescriptor == null)
            return;

        writer.writeInt(partitionsMap.size());

        for (ClientCacheNodePartitions partitions: partitionsMap)
            partitions.write(writer);
    }
}
