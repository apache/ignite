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
import org.apache.ignite.cache.CacheExistsException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.odbc.ClientListenerProtocolVersion;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;
import org.apache.ignite.internal.processors.platform.client.ClientRequest;
import org.apache.ignite.internal.processors.platform.client.ClientResponse;
import org.apache.ignite.internal.processors.platform.client.ClientStatus;
import org.apache.ignite.internal.processors.platform.client.IgniteClientException;

/**
 * Cache get or create with configuration request.
 */
@SuppressWarnings("unchecked")
public class ClientCacheGetOrCreateWithConfigurationRequest extends ClientRequest {
    /** Cache configuration. */
    private final CacheConfiguration cacheCfg;

    /**
     * Constructor.
     *
     * @param reader Reader.
     * @param ver Client version.
     */
    public ClientCacheGetOrCreateWithConfigurationRequest(BinaryRawReader reader, ClientListenerProtocolVersion ver) {
        super(reader);

        cacheCfg = ClientCacheConfigurationSerializer.read(reader, ver);
    }

    /** {@inheritDoc} */
    @Override public ClientResponse process(ClientConnectionContext ctx) {
        try {
            ctx.kernalContext().grid().getOrCreateCache(cacheCfg);
        }
        catch (CacheExistsException e) {
            throw new IgniteClientException(ClientStatus.CACHE_EXISTS, e.getMessage());
        }

        return super.process(ctx);
    }
}
