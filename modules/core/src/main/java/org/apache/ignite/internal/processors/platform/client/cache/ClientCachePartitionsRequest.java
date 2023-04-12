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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.affinity.AffinityAssignment;
import org.apache.ignite.internal.processors.cache.CacheType;
import org.apache.ignite.internal.processors.cache.DynamicCacheDescriptor;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.platform.client.ClientAffinityTopologyVersion;
import org.apache.ignite.internal.processors.platform.client.ClientBitmaskFeature;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;
import org.apache.ignite.internal.processors.platform.client.ClientProtocolContext;
import org.apache.ignite.internal.processors.platform.client.ClientRequest;
import org.apache.ignite.internal.processors.platform.client.ClientResponse;
import org.apache.ignite.internal.util.lang.gridfunc.NotContainsPredicate;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgnitePredicate;
import org.jetbrains.annotations.Nullable;
import static java.util.Optional.ofNullable;
import static org.apache.ignite.internal.processors.query.QueryUtils.isCustomAffinityMapper;

/**
 * Cluster node list request.
 * Currently, used to request list of nodes, to calculate affinity on the client side.
 */
public class ClientCachePartitionsRequest extends ClientRequest {
    /** IDs of caches. */
    private final int[] cacheIds;

    /**
     * {@code true} if a custom mapping factory is set on the client side and mappings for all requested caches
     * need to be sent back to the client even if they are related to a custom affinity function.
     */
    private final boolean withCustomMappings;

    /**
     * Initializes a new instance of ClientRawRequest class.
     * @param reader Reader.
     */
    public ClientCachePartitionsRequest(BinaryRawReader reader, ClientProtocolContext protocolCtx) {
        super(reader);

        if (protocolCtx.isFeatureSupported(ClientBitmaskFeature.ALL_AFFINITY_MAPPINGS))
            withCustomMappings = reader.readBoolean();
        else
            withCustomMappings = false;

        int len = reader.readInt();

        cacheIds = new int[len];

        for (int i = 0; i < len; ++i)
            cacheIds[i] = reader.readInt();
    }

    /** {@inheritDoc} */
    @Override public ClientResponse process(ClientConnectionContext ctx) {
        if (!ctx.kernalContext().state().publicApiActiveState(true))
            return new ClientResponse(requestId(), "Can not perform cache operation because the cluster is inactive.");

        Map<ClientCachePartitionAwarenessGroup, ClientCachePartitionAwarenessGroup> grps = new HashMap<>(cacheIds.length);
        ClientAffinityTopologyVersion affinityVer = ctx.checkAffinityTopologyVersion();

        Set<Integer> affectedGroupIds = Arrays.stream(cacheIds)
            .mapToObj(id -> ctx.kernalContext().cache().cacheDescriptor(id))
            .filter(Objects::nonNull)
            .map(DynamicCacheDescriptor::groupId)
            .collect(Collectors.toSet());

        Map<Integer, List<DynamicCacheDescriptor>> allCaches = ctx.kernalContext().cache().cacheDescriptors().values()
            .stream()
            .filter(Objects::nonNull)
            .filter(c -> c.cacheType() == CacheType.USER || c.cacheType() == CacheType.DATA_STRUCTURES)
            .collect(Collectors.groupingBy(DynamicCacheDescriptor::groupId));

        // As a first step, get a set of mappings that we need to return.
        // To do that, check if any of the caches listed in request can be grouped.
        for (List<DynamicCacheDescriptor> affected : F.view(allCaches, affectedGroupIds::contains).values()) {
            ClientCachePartitionAwarenessGroup grp = processCache(ctx, affinityVer, F.first(affected), withCustomMappings);

            if (grp == null)
                continue;

            ofNullable(grps.putIfAbsent(grp, grp))
                .orElse(grp)
                .addAll(affected);
        }

        // As a second step, check all other caches and add them to groups they are compatible with.
        for (List<DynamicCacheDescriptor> descs : F.view(allCaches, new NotContainsPredicate<>(affectedGroupIds)).values()) {
            ClientCachePartitionAwarenessGroup grp = processCache(ctx, affinityVer, F.first(descs), withCustomMappings);

            if (grp == null)
                continue;

            ClientCachePartitionAwarenessGroup grp0 = grps.get(grp);

            if (grp0 != null)
                grp0.addAll(descs);
        }

        return new ClientCachePartitionsResponse(requestId(), new ArrayList<>(grps.keySet()), affinityVer);
    }

    /**
     * Process cache and create new partition mapping, if it does not belong to any existent.
     * @param ctx Connection context.
     * @param affinityVer Affinity topology version.
     * @param cacheDesc Cache descriptor.
     * @param withCustomMappings {@code true} to verify a non-default affinity function also.
     * @return Null if cache was processed and new client cache partition awareness group if it does not belong to any
     * existent.
     */
    private static ClientCachePartitionAwarenessGroup processCache(
        ClientConnectionContext ctx,
        ClientAffinityTopologyVersion affinityVer,
        DynamicCacheDescriptor cacheDesc,
        boolean withCustomMappings
    ) {
        AffinityAssignment assignment = getCacheAssignment(ctx, affinityVer, cacheDesc.cacheId());

        // If assignment is not available for the cache for required affinity version, ignore the cache.
        if (assignment == null)
            return null;

        ClientCachePartitionMapping mapping = null;

        if (isApplicable(cacheDesc.cacheConfiguration(), withCustomMappings))
            mapping = new ClientCachePartitionMapping(assignment);

        return new ClientCachePartitionAwarenessGroup(mapping,
            !withCustomMappings || isDefaultMapping(cacheDesc.cacheConfiguration()));
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
        }
        catch (Exception e) {
            return null;
        }
    }

    /**
     * @param ccfg Cache configuration.
     * @param withCustomMappings {@code true} to verify a non-default affinity function also.
     * @return True if cache is applicable for partition awareness optimisation.
     */
    private static boolean isApplicable(CacheConfiguration<?, ?> ccfg, boolean withCustomMappings) {
        // Partition could be extracted only from PARTITIONED caches.
        if (ccfg.getCacheMode() != CacheMode.PARTITIONED)
            return false;

        IgnitePredicate<?> filter = ccfg.getNodeFilter();
        boolean hasNodeFilter = filter != null && !(filter instanceof CacheConfiguration.IgniteAllNodesPredicate);

        // We cannot be sure that two caches are co-located if custom node filter is present.
        // Note that technically we may try to compare two filters. However, this adds unnecessary complexity
        // and potential deserialization issues.
        if (hasNodeFilter)
            return false;

        return withCustomMappings || isDefaultMapping(ccfg);
    }

    /**
     * @param ccfg Cache configuration.
     * @return {@code true} if the default affinity was used for cache.
     */
    public static boolean isDefaultMapping(CacheConfiguration<?, ?> ccfg) {
        // Only caches with no custom affinity key mapper is supported.
        if (isCustomAffinityMapper(ccfg.getAffinityMapper()))
            return false;

        // Only RendezvousAffinityFunction is supported for now.
        return ccfg.getAffinity().getClass().equals(RendezvousAffinityFunction.class);
    }
}
