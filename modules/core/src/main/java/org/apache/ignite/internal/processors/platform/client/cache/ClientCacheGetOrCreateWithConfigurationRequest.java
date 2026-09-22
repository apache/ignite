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
import org.apache.ignite.internal.processors.cache.CacheType;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;
import org.apache.ignite.internal.processors.platform.client.ClientProtocolContext;
import org.apache.ignite.internal.processors.platform.client.ClientRequest;
import org.apache.ignite.internal.processors.platform.client.ClientResponse;
import org.apache.ignite.internal.processors.platform.client.ClientStatus;
import org.apache.ignite.internal.processors.platform.client.IgniteClientException;
import org.apache.ignite.internal.util.typedef.T2;

import static org.apache.ignite.internal.processors.platform.client.cache.ClientCacheCreateWithConfigurationRequest.checkClientCacheConfiguration;

/**
 * Cache get or create with configuration request.
 */
@SuppressWarnings("unchecked")
public class ClientCacheGetOrCreateWithConfigurationRequest extends ClientRequest {
    /** Cache configuration. */
    private final T2<CacheConfiguration, Boolean> cacheCfg;

    /**
     * Constructor.
     *
     * @param reader Reader.
     * @param protocolCtx Client protocol context.
     */
    public ClientCacheGetOrCreateWithConfigurationRequest(BinaryRawReader reader, ClientProtocolContext protocolCtx) {
        super(reader);

        cacheCfg = ClientCacheConfigurationSerializer.read(reader, protocolCtx);
    }

    /** {@inheritDoc} */
    @Override public ClientResponse process(ClientConnectionContext ctx) {
        CacheConfiguration ccfg = cacheCfg.get1();
        boolean sql = cacheCfg.get2();

        checkClientCacheConfiguration(ccfg);

        try {
            if (sql)
                createSqlCache(ctx, ccfg, false);
            else
                ctx.kernalContext().grid().getOrCreateCache(ccfg);
        }
        catch (CacheExistsException e) {
            throw new IgniteClientException(ClientStatus.CACHE_EXISTS, e.getMessage());
        }

        return super.process(ctx);
    }

    /**
     * @param ctx Client connection context.
     * @param ccfg  Cache configuration.
     * @param failIfExists If {@code True} then don't fail if cache exists, already.
     */
    public static void createSqlCache(ClientConnectionContext ctx, CacheConfiguration<?, ?> ccfg, boolean failIfExists) {
        ctx.kernalContext().cache().dynamicStartCache(ccfg,
            ccfg.getName(),
            ccfg.getNearConfiguration(),
            CacheType.USER,
            true,         /* sql. */
            failIfExists,
            true,         /* fail if not started. */
            true);        /* check thread tx */
    }
}
