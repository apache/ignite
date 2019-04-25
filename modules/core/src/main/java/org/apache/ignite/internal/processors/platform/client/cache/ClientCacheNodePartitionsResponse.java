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

import java.util.Collection;
import org.apache.ignite.internal.binary.BinaryRawWriterEx;
import org.apache.ignite.internal.processors.platform.client.ClientConnectableNodePartitions;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;
import org.apache.ignite.internal.processors.platform.client.ClientResponse;

/**
 * Client cache nodes partitions response.
 * Deprecated since 1.3.0. Replaced with {@link ClientCachePartitionsResponse}.
 */
class ClientCacheNodePartitionsResponse extends ClientResponse {
    /** Node partitions. */
    private final Collection<ClientConnectableNodePartitions> nodeParts;

    /**
     * @param requestId Request id.
     * @param nodeParts Node partitions info.
     */
    ClientCacheNodePartitionsResponse(long requestId, Collection<ClientConnectableNodePartitions> nodeParts) {
        super(requestId);

        assert nodeParts != null;

        this.nodeParts = nodeParts;
    }

    /** {@inheritDoc} */
    @Override public void encode(ClientConnectionContext ctx, BinaryRawWriterEx writer) {
        super.encode(ctx, writer);

        writer.writeInt(nodeParts.size());

        for (ClientConnectableNodePartitions nodePart : nodeParts) {
            nodePart.write(writer);
        }
    }
}
