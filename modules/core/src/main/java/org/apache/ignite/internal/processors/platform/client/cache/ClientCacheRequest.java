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

import javax.cache.expiry.ExpiryPolicy;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.internal.processors.cache.DynamicCacheDescriptor;
import org.apache.ignite.internal.processors.platform.cache.expiry.PlatformExpiryPolicy;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;
import org.apache.ignite.internal.processors.platform.client.ClientRequest;
import org.apache.ignite.internal.processors.platform.client.ClientStatus;
import org.apache.ignite.internal.processors.platform.client.IgniteClientException;

/**
 * Cache request.
 */
class ClientCacheRequest extends ClientRequest {
    /** "Keep binary" flag mask. */
    private static final byte KEEP_BINARY_FLAG_MASK = 0x01;

    /** "Under transaction" flag mask. */
    private static final byte TRANSACTIONAL_FLAG_MASK = 0x02;

    /** "With expiry policy" flag mask. */
    private static final byte FLAG_WITH_EXPIRY_POLICY = 0x04;

    /** Cache ID. */
    private final int cacheId;

    /** Flags. */
    private final byte flags;

    /** Expiry policy. */
    private final ExpiryPolicy expiryPolicy;

    /**
     * Constructor.
     *
     * @param reader Reader.
     */
    ClientCacheRequest(BinaryRawReader reader) {
        super(reader);

        cacheId = reader.readInt();

        flags = reader.readByte();

        expiryPolicy = withExpiryPolicy()
                ? new PlatformExpiryPolicy(reader.readLong(), reader.readLong(), reader.readLong())
                : null;
    }

    /**
     * Gets the cache for current cache id, with binary mode enabled.
     *
     * @param ctx Kernal context.
     * @return Cache.
     */
    protected IgniteCache cache(ClientConnectionContext ctx) {
        return rawCache(ctx).withKeepBinary();
    }

    /**
     * Gets a value indicating whether keepBinary flag is set in this request.
     *
     * @return keepBinary flag value.
     */
    protected boolean isKeepBinary() {
        return (flags & KEEP_BINARY_FLAG_MASK) != 0;
    }

    /**
     * Gets a value indicating whether request was made under transaction.
     *
     * @return Flag value.
     */
    protected boolean isTransactional() {
        return (flags & TRANSACTIONAL_FLAG_MASK) != 0;
    }

    /**
     *  Gets a value indicating whether expiry policy is set in this request.
     *
     * @return expiry policy flag value.
     */
    private boolean withExpiryPolicy() {
        return (flags & FLAG_WITH_EXPIRY_POLICY) == FLAG_WITH_EXPIRY_POLICY;
    }

    /**
     * Gets the cache for current cache id, ignoring any flags.
     *
     * @param ctx Kernal context.
     * @return Cache.
     */
    protected IgniteCache rawCache(ClientConnectionContext ctx) {
        DynamicCacheDescriptor cacheDesc = cacheDescriptor(ctx);

        String cacheName = cacheDesc.cacheName();

        IgniteCache<Object, Object> cache = ctx.kernalContext().grid().cache(cacheName);
        if (withExpiryPolicy())
            cache = cache.withExpiryPolicy(expiryPolicy);
        
        return cache;
    }

    /**
     * Gets the cache descriptor.
     *
     * @param ctx Context.
     * @return Cache descriptor.
     */
    protected DynamicCacheDescriptor cacheDescriptor(ClientConnectionContext ctx) {
        return cacheDescriptor(ctx, cacheId);
    }

    /**
     * Gets the cache descriptor.
     *
     * @param ctx Context.
     * @param cacheId Cache id.
     * @return Cache descriptor.
     */
    public static DynamicCacheDescriptor cacheDescriptor(ClientConnectionContext ctx, int cacheId) {
        DynamicCacheDescriptor desc = ctx.kernalContext().cache().cacheDescriptor(cacheId);

        if (desc == null)
            throw new IgniteClientException(ClientStatus.CACHE_DOES_NOT_EXIST, "Cache does not exist [cacheId= " +
                    cacheId + "]", null);

        return desc;
    }

    /**
     * Gets the cache id.
     *
     * @return Cache id.
     */
    protected int cacheId() {
        return cacheId;
    }
}
