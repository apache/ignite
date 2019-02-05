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
import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.internal.processors.cache.DynamicCacheDescriptor;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;
import org.apache.ignite.internal.processors.platform.client.ClientRequest;
import org.apache.ignite.internal.processors.platform.client.ClientResponse;

/**
 * Cluster node list request.
 * Currently used to request list of nodes, to calculate affinity on the client side.
 */
public class ClientCachePartitionsRequest extends ClientRequest {
    int[] cacheIds;

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
        Map<String, DynamicCacheDescriptor> allCaches = ctx.kernalContext().cache().cacheDescriptors();

        ArrayList<ClientCachePartitionsMapping> mappings = new ArrayList<>(cacheIds.length);

        for (int cacheId : cacheIds) {
            DynamicCacheDescriptor cacheDesc = ClientCacheRequest.cacheDescriptor(ctx, cacheId);
            ClientCachePartitionsMapping currentMapping = new ClientCachePartitionsMapping(ctx, cacheDesc);

            for (DynamicCacheDescriptor another: allCaches.values()) {
                // Ignoring system caches
                if (!another.cacheType().userCache() || another.cacheId() == cacheId)
                    continue;

                currentMapping.addIfCompatible(another);
            }

            mappings.add(currentMapping);
        }

        return new ClientCachePartitionsResponse(requestId(), mappings);
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
     * Gets the cache for cache id.
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
