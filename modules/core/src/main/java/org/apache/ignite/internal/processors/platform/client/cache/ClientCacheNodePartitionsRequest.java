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
import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.processors.odbc.ClientListenerProcessor;
import org.apache.ignite.internal.processors.platform.client.ClientConnectableNodePartitions;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;
import org.apache.ignite.internal.processors.platform.client.ClientResponse;

/**
 * Cluster node list request.
 * Used to request list of nodes, to calculate affinity on the client side.
 * Deprecated since 1.3.0. Replaced with {@link ClientCachePartitionsRequest}.
 */
public class ClientCacheNodePartitionsRequest extends ClientCacheRequest {
    /**
     * Initializes a new instance of ClientRawRequest class.
     * @param reader Reader.
     */
    public ClientCacheNodePartitionsRequest(BinaryRawReader reader) {
        super(reader);
    }

    /** {@inheritDoc} */
    @Override public ClientResponse process(ClientConnectionContext ctx) {
        IgniteCache cache = cache(ctx);

        GridDiscoveryManager discovery = ctx.kernalContext().discovery();
        Collection<ClusterNode> nodes = discovery.discoCache().cacheNodes(cache.getName());

        Affinity aff = ctx.kernalContext().affinity().affinityProxy(cache.getName());

        ArrayList<ClientConnectableNodePartitions> res = new ArrayList<>();

        for (ClusterNode node : nodes) {
            Integer port = node.attribute(ClientListenerProcessor.CLIENT_LISTENER_PORT);

            if (port == null)
                continue;

            Collection<String> addrs = node.addresses();

            int[] parts = aff.primaryPartitions(node);

            res.add(new ClientConnectableNodePartitions(port, addrs, parts));
        }

        return new ClientCacheNodePartitionsResponse(requestId(), res);
    }
}
