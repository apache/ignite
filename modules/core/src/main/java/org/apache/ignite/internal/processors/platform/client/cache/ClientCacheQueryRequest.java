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
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.DynamicCacheDescriptor;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;

/**
 * Abstract query request.
 */
public abstract class ClientCacheQueryRequest extends ClientCacheDataRequest {
    /** */
    ClientCacheQueryRequest(BinaryRawReader reader) {
        super(reader);
    }

    /** */
    protected void updateAffinityMetrics(ClientConnectionContext ctx, int part) {
        DynamicCacheDescriptor desc = cacheDescriptor(ctx);
        CacheConfiguration<?, ?> cfg = desc.cacheConfiguration();

        if (cfg.getCacheMode() == CacheMode.PARTITIONED && cfg.isStatisticsEnabled()) {
            String cacheName = desc.cacheName();

            try {
                GridKernalContext kctx = ctx.kernalContext();

                if (kctx.affinity().mapPartitionToNode(cacheName, part, null).isLocal())
                    kctx.clientListener().metrics().onAffinityQryHit();
                else
                    kctx.clientListener().metrics().onAffinityQryMiss();
            }
            catch (Exception ignored) {
                // No-op.
            }
        }
    }
}
