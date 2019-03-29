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
import java.util.Map;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.DynamicCacheDescriptor;
import org.apache.ignite.internal.processors.platform.client.ClientAffinityTopologyVersion;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;
import org.apache.ignite.internal.processors.platform.client.ClientRequest;
import org.apache.ignite.internal.processors.platform.client.ClientResponse;
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
        ArrayList<ClientCacheAffinityAwarenessGroup> mappings = new ArrayList<>(cacheIds.length);

        ClientAffinityTopologyVersion affinityVer = ctx.checkAffinityTopologyVersion();

        // As a fisrt step, get a set of mappings that we need to return.
        // To do that, check if any of the caches listed in request can be grouped.
        for (int cacheId : cacheIds) {
            DynamicCacheDescriptor cacheDesc = ctx.kernalContext().cache().cacheDescriptor(cacheId);

            if (cacheDesc == null)
                continue;

            ClientCacheAffinityAwarenessGroup currentMapping =
                tryMakeAffinityAwarenessGroup(ctx, affinityVer.getVersion(), cacheDesc);

            if (currentMapping == null)
                continue;

            boolean merged = tryMerge(mappings, currentMapping);

            if (!merged)
                mappings.add(currentMapping);
        }

        Map<String, DynamicCacheDescriptor> allCaches = ctx.kernalContext().cache().cacheDescriptors();

        // As a second step, check all other caches and add them to groups they are compatible with.
        for (DynamicCacheDescriptor cacheDesc: allCaches.values()) {
            // Ignoring system caches
            if (!cacheDesc.cacheType().userCache())
                continue;

            ClientCacheAffinityAwarenessGroup currentMapping =
                tryMakeAffinityAwarenessGroup(ctx, affinityVer.getVersion(), cacheDesc);

            if (currentMapping == null)
                continue;

            tryMerge(mappings, currentMapping);
        }

        return new ClientCachePartitionsResponse(requestId(), mappings, affinityVer);
    }

    /**
     * Make affinity awareness group.
     * @param ctx Connection context.
     * @param affinityVer Affinity topology version.
     * @param cacheDesc Cache descriptor.
     * @return ClientCacheAffinityAwarenessGroup or null, if is not available for the cache.
     */
    @Nullable private ClientCacheAffinityAwarenessGroup tryMakeAffinityAwarenessGroup(ClientConnectionContext ctx,
        AffinityTopologyVersion affinityVer, DynamicCacheDescriptor cacheDesc) {

        try {
            return new ClientCacheAffinityAwarenessGroup(ctx, cacheDesc, affinityVer);
        }
        catch (IgniteException e)
        {
            return null;
        }
    }

    /**
     * Try add mapping to other mappings if compatible to one of them.
     * @param mappings Mappings.
     * @param mappingToMerge Mapping to merge.
     * @return True if merged.
     */
    private static boolean tryMerge(ArrayList<ClientCacheAffinityAwarenessGroup> mappings,
        ClientCacheAffinityAwarenessGroup mappingToMerge) {
        for (ClientCacheAffinityAwarenessGroup mapping : mappings) {
            boolean merged = mapping.tryMerge(mappingToMerge);

            if (merged)
                return true;
        }

        return false;
    }

    /**
     * Gets the cache for cache id.
     *
     * @param ctx Kernal context.
     * @param cacheId Cache id.
     * @return Cache.
     */
    static IgniteCache cache(ClientConnectionContext ctx, int cacheId) {
        DynamicCacheDescriptor cacheDesc = ClientCacheRequest.cacheDescriptor(ctx, cacheId);

        return cache(ctx, cacheDesc);
    }

    /**
     * Gets the cache from cache descriptor.
     *
     * @param ctx Kernal context.
     * @param cacheDesc Cache descriptor.
     * @return Cache.
     */
    static IgniteCache cache(ClientConnectionContext ctx, DynamicCacheDescriptor cacheDesc) {
        String cacheName = cacheDesc.cacheName();

        return ctx.kernalContext().grid().cache(cacheName);
    }

}
