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

import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.binary.BinaryRawReaderEx;
import org.apache.ignite.internal.processors.cache.DynamicCacheDescriptor;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;
import org.apache.ignite.internal.processors.platform.client.ClientResponse;
import org.apache.ignite.internal.processors.platform.client.tx.ClientTxAwareRequest;
import org.apache.ignite.internal.util.typedef.F;

/**
 * Cache request involving key.
 */
public abstract class ClientCacheKeyRequest extends ClientCacheDataRequest implements ClientTxAwareRequest {
    /** Key. */
    private final Object key;

    /**
     * Ctor.
     *
     * @param reader Reader.
     */
    ClientCacheKeyRequest(BinaryRawReaderEx reader) {
        super(reader);

        key = reader.readObjectDetached();
    }

    /** {@inheritDoc} */
    @Override public final ClientResponse process(ClientConnectionContext ctx) {
        updateMetrics(ctx);

        // Process request in overriden method.
        return process0(ctx);
    }

    /** {@inheritDoc} */
    @Override public final IgniteInternalFuture<ClientResponse> processAsync(ClientConnectionContext ctx) {
        updateMetrics(ctx);

        // Process request in overriden method.
        return processAsync0(ctx);
    }

    /** {@inheritDoc} */
    @Override public boolean isAsync(ClientConnectionContext ctx) {
        // Every cache data request on the transactional cache can lock the thread, even with implicit transaction.
        return cacheDescriptor(ctx).cacheConfiguration().getAtomicityMode() == CacheAtomicityMode.TRANSACTIONAL;
    }

    /** */
    private void updateMetrics(ClientConnectionContext ctx) {
        if (!isTransactional()) {
            // Calculate affinity metrics.
            DynamicCacheDescriptor desc = cacheDescriptor(ctx);
            CacheConfiguration<?, ?> cfg = desc.cacheConfiguration();

            if (cfg.getCacheMode() == CacheMode.PARTITIONED && cfg.isStatisticsEnabled()) {
                String cacheName = desc.cacheName();

                try {
                    GridKernalContext kctx = ctx.kernalContext();

                    if (F.first(kctx.affinity().mapKeyToPrimaryAndBackups(cacheName, key, null)).isLocal())
                        kctx.clientListener().metrics().onAffinityKeyHit();
                    else
                        kctx.clientListener().metrics().onAffinityKeyMiss();
                }
                catch (Exception ignored) {
                    // No-op.
                }
            }
        }
    }

    /** */
    protected abstract ClientResponse process0(ClientConnectionContext ctx);

    /** */
    protected IgniteInternalFuture<ClientResponse> processAsync0(ClientConnectionContext ctx) {
        throw new IllegalStateException("Async operation is not implemented for request " + getClass().getName());
    }

    /**
     * Gets the key.
     *
     * @return Key.
     */
    public Object key() {
        return key;
    }
}
