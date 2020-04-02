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

import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.cache.CacheExistsException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;
import org.apache.ignite.internal.processors.platform.client.ClientProtocolContext;
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
     * @param protocolContext Client protocol context.
     */
    public ClientCacheGetOrCreateWithConfigurationRequest(BinaryRawReader reader, ClientProtocolContext protocolContext) {
        super(reader);

        cacheCfg = ClientCacheConfigurationSerializer.read(reader, protocolContext);
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
