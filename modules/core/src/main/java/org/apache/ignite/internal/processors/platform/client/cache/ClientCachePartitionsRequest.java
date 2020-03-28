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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.AffinityKeyMapper;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.affinity.AffinityAssignment;
import org.apache.ignite.internal.processors.cache.CacheDefaultBinaryAffinityKeyMapper;
import org.apache.ignite.internal.processors.cache.DynamicCacheDescriptor;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.binary.CacheObjectBinaryProcessorImpl;
import org.apache.ignite.internal.processors.platform.client.ClientAffinityTopologyVersion;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;
import org.apache.ignite.internal.processors.platform.client.ClientRequest;
import org.apache.ignite.internal.processors.platform.client.ClientResponse;
import org.apache.ignite.lang.IgnitePredicate;
import org.jetbrains.annotations.Nullable;

/**
 * Cluster node list request.
 * Currently used to request list of nodes, to calculate affinity on the client side.
 */
public class ClientCachePartitionsRequest extends ClientRequest {
    /** IDs of caches. */
    private final int[] cacheIds;

    /**
     * Initializes a new instance of ClientRawRequest class.
     * @param reader Reader.
     */
    public ClientCachePartitionsRequest(BinaryRawReader reader) {
        super(reader);

        int len = reader.readInt();

        cacheIds = new int[len];

        for (int i = 0; i < len; ++i)
            cacheIds[i] = reader.readInt();
    }

    /** {@inheritDoc} */
    @Override public ClientResponse process(ClientConnectionContext ctx) {
        ArrayList<ClientCachePartitionAwarenessGroup> groups = new ArrayList<>(cacheIds.length);
        HashMap<Integer, ClientCachePartitionAwarenessGroup> cacheGroupIds = new HashMap<>(cacheIds.length);

        ClientAffinityTopologyVersion affinityVer = ctx.checkAffinityTopologyVersion();

        // As a fisrt step, get a set of mappings that we need to return.
        // To do that, check if any of the caches listed in request can be grouped.
        for (int cacheId : cacheIds) {
            DynamicCacheDescriptor cacheDesc = ctx.kernalContext().cache().cacheDescriptor(cacheId);

            // Just ignoring, if the cache is absent - i.e. was deleted concurrently.
            if (cacheDesc == null)
                continue;

            ClientCachePartitionAwarenessGroup grp = processCache(ctx, groups, cacheGroupIds, affinityVer, cacheDesc);

            // Cache already processed.
            if (grp == null)
                continue;

            groups.add(grp);
            cacheGroupIds.put(cacheDesc.groupId(), grp);
        }

        Map<String, DynamicCacheDescriptor> allCaches = ctx.kernalContext().cache().cacheDescriptors();

        // As a second step, check all other caches and add them to groups they are compatible with.
        for (DynamicCacheDescriptor cacheDesc: allCaches.values()) {
            // Ignoring system caches
            if (!cacheDesc.cacheType().userCache())
                continue;

            processCache(ctx, groups, cacheGroupIds, affinityVer, cacheDesc);
        }

        return new ClientCachePartitionsResponse(requestId(), groups, affinityVer);
    }

    /**
     * Process cache and create new partition mapping, if it does not belong to any existent.
     * @param ctx Connection context.
     * @param groups Cache affinity groups.
     * @param cacheGroupIds Map of known group IDs.
     * @param affinityVer Affinity topology version.
     * @param cacheDesc Cache descriptor.
     * @return Null if cache was processed and new client cache partition awareness group if it does not belong to any
     * existent.
     */
    private static ClientCachePartitionAwarenessGroup processCache(
        ClientConnectionContext ctx,
        List<ClientCachePartitionAwarenessGroup> groups,
        Map<Integer, ClientCachePartitionAwarenessGroup> cacheGroupIds,
        ClientAffinityTopologyVersion affinityVer,
        DynamicCacheDescriptor cacheDesc)
    {
        int cacheGroupId = cacheDesc.groupId();
        int cacheId = cacheDesc.cacheId();

        ClientCachePartitionAwarenessGroup group = cacheGroupIds.get(cacheGroupId);
        if (group != null) {
            // Cache group is found. It means that cache belongs to one of cache groups with known mapping.
            // Just adding our cache to this group here.
            group.addCache(cacheDesc);

            return null;
        }

        AffinityAssignment assignment = getCacheAssignment(ctx, affinityVer, cacheId);

        // If assignment is not available for the cache for required affinity version, ignore the cache.
        if (assignment == null)
            return null;

        ClientCachePartitionMapping mapping = null;
        if (isApplicable(cacheDesc.cacheConfiguration()))
            mapping = new ClientCachePartitionMapping(cacheId, assignment);

        group = getCompatibleGroup(groups, mapping);
        if (group != null) {
            group.addCache(cacheDesc);
            cacheGroupIds.put(cacheGroupId, group);

            return null;
        }

        CacheObjectBinaryProcessorImpl proc = (CacheObjectBinaryProcessorImpl)ctx.kernalContext().cacheObjects();

        return new ClientCachePartitionAwarenessGroup(proc, mapping, cacheDesc);
    }

    /**
     * Get cache partition awareness group which is compatible with the mapping.
     * @param groups Group list.
     * @param mapping Partition mapping.
     * @return Compatible cache partition awareness group if present, or null.
     */
    @Nullable private static ClientCachePartitionAwarenessGroup getCompatibleGroup(
        List<ClientCachePartitionAwarenessGroup> groups,
        ClientCachePartitionMapping mapping) {
        for (ClientCachePartitionAwarenessGroup group : groups) {
            if (group.isCompatible(mapping))
                return group;
        }

        return null;
    }

    /**
     * Get assignment for a cache in a safe way.
     * @param ctx Client connection context.
     * @param affinityVer Affinity version.
     * @param cacheId Cache ID.
     * @return Affinity assignment for a cache, or null if is not possible to get.
     */
    @Nullable private static AffinityAssignment getCacheAssignment(ClientConnectionContext ctx,
        ClientAffinityTopologyVersion affinityVer, int cacheId) {
        try {
            GridCacheContext cacheContext = ctx.kernalContext().cache().context().cacheContext(cacheId);
            return cacheContext.affinity().assignment(affinityVer.getVersion());
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * @param ccfg Cache configuration.
     * @return True if cache is applicable for partition awareness optimisation.
     */
    private static boolean isApplicable(CacheConfiguration ccfg) {
        // Partition could be extracted only from PARTITIONED caches.
        if (ccfg.getCacheMode() != CacheMode.PARTITIONED)
            return false;

        // Only caches with no custom affinity key mapper is supported.
        AffinityKeyMapper keyMapper = ccfg.getAffinityMapper();
        if (!(keyMapper instanceof CacheDefaultBinaryAffinityKeyMapper))
            return false;

        // Only RendezvousAffinityFunction is supported for now.
        if (!ccfg.getAffinity().getClass().equals(RendezvousAffinityFunction.class))
            return false;

        IgnitePredicate filter = ccfg.getNodeFilter();
        boolean hasNodeFilter = filter != null && !(filter instanceof CacheConfiguration.IgniteAllNodesPredicate);

        // We cannot be sure that two caches are co-located if custom node filter is present.
        // Note that technically we may try to compare two filters. However, this adds unnecessary complexity
        // and potential deserialization issues.
        return !hasNodeFilter;
    }
}
