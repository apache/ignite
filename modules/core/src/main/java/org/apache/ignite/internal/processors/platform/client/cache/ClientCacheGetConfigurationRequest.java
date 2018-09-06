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

import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.odbc.ClientListenerProtocolVersion;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;
import org.apache.ignite.internal.processors.platform.client.ClientResponse;

/**
 * Cache configuration request.
 */
public class ClientCacheGetConfigurationRequest extends ClientCacheRequest {
    /** Client version. */
    private final ClientListenerProtocolVersion ver;
    
    /**
     * Constructor.
     *
     * @param reader Reader.
     * @param ver Client version.
     */
    public ClientCacheGetConfigurationRequest(BinaryRawReader reader, ClientListenerProtocolVersion ver) {
        super(reader);
        
        this.ver = ver;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public ClientResponse process(ClientConnectionContext ctx) {
        CacheConfiguration cfg = ((IgniteCache<Object, Object>) rawCache(ctx))
                .getConfiguration(CacheConfiguration.class);

        return new ClientCacheGetConfigurationResponse(requestId(), cfg, ver);
    }
}
