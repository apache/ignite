/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.platform.client.cache;

import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;
import org.apache.ignite.internal.processors.platform.client.ClientRequest;
import org.apache.ignite.internal.processors.platform.client.ClientResponse;

/**
 * Cache destroy request.
 */
public class ClientCacheDestroyRequest extends ClientRequest {
    /** Cache ID. */
    private final int cacheId;

    /**
     * Constructor.
     *
     * @param reader Reader.
     */
    public ClientCacheDestroyRequest(BinaryRawReader reader) {
        super(reader);

        cacheId = reader.readInt();
    }

    /** {@inheritDoc} */
    @Override public ClientResponse process(ClientConnectionContext ctx) {
        String cacheName = ClientCacheRequest.cacheDescriptor(ctx, cacheId).cacheName();

        ctx.kernalContext().grid().destroyCache(cacheName);

        return super.process(ctx);
    }
}
