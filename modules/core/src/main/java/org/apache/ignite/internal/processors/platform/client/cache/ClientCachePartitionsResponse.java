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
import org.apache.ignite.internal.binary.BinaryRawWriterEx;
import org.apache.ignite.internal.processors.cache.binary.CacheObjectBinaryProcessorImpl;
import org.apache.ignite.internal.processors.platform.client.ClientAffinityTopologyVersion;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;
import org.apache.ignite.internal.processors.platform.client.ClientResponse;

/**
 * Client cache nodes partitions response.
 */
class ClientCachePartitionsResponse extends ClientResponse {
    /** Node partitions. */
    private final ArrayList<ClientCachePartitionAwarenessGroup> mappings;

    /** Affinity version. */
    private final ClientAffinityTopologyVersion affinityVer;

    /**
     * @param requestId Request id.
     * @param mappings Mappings for caches.
     * @param affinityVer Affinity version.
     */
    ClientCachePartitionsResponse(long requestId, ArrayList<ClientCachePartitionAwarenessGroup> mappings,
        ClientAffinityTopologyVersion affinityVer) {
        super(requestId);

        assert mappings != null;

        this.mappings = mappings;
        this.affinityVer = affinityVer;
    }

    /** {@inheritDoc} */
    @Override public void encode(ClientConnectionContext ctx, BinaryRawWriterEx writer) {
        encode(ctx, writer, affinityVer);

        CacheObjectBinaryProcessorImpl proc = (CacheObjectBinaryProcessorImpl)ctx.kernalContext().cacheObjects();

        affinityVer.write(writer);

        writer.writeInt(mappings.size());

        for (ClientCachePartitionAwarenessGroup mapping : mappings)
            mapping.write(proc, writer, ctx.currentProtocolContext());
    }
}
