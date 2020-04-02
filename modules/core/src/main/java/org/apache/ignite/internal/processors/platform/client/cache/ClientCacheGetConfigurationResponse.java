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

import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.binary.BinaryRawWriterEx;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;
import org.apache.ignite.internal.processors.platform.client.ClientProtocolContext;
import org.apache.ignite.internal.processors.platform.client.ClientResponse;

/**
 * Cache configuration response.
 */
public class ClientCacheGetConfigurationResponse extends ClientResponse {
    /** Cache configuration. */
    private final CacheConfiguration cfg;

    /** Client protocol context. */
    private final ClientProtocolContext protocolContext;

    /**
     * Constructor.
     *
     * @param reqId Request id.
     * @param cfg Cache configuration.
     * @param protocolContext Client protocol context.
     */
    ClientCacheGetConfigurationResponse(long reqId, CacheConfiguration cfg, ClientProtocolContext protocolContext) {
        super(reqId);

        assert cfg != null;
        assert protocolContext != null;

        this.cfg = cfg;
        this.protocolContext = protocolContext;
    }

    /** {@inheritDoc} */
    @Override public void encode(ClientConnectionContext ctx, BinaryRawWriterEx writer) {
        super.encode(ctx, writer);

        ClientCacheConfigurationSerializer.write(writer, cfg, protocolContext);
    }
}
