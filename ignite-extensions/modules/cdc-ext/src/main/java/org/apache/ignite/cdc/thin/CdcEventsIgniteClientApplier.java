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

package org.apache.ignite.cdc.thin;

import java.util.Map;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cdc.AbstractCdcEventsApplier;
import org.apache.ignite.cdc.CdcEvent;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.internal.client.thin.TcpClientCache;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.collection.IntHashMap;
import org.apache.ignite.internal.util.collection.IntMap;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.CU;

/**
 * Contains logic to process {@link CdcEvent} and apply them to the destination cluster by thin client.
 *
 * @see TcpClientCache#putAllConflict(Map)
 * @see TcpClientCache#removeAllConflict(Map)
 */
public class CdcEventsIgniteClientApplier extends AbstractCdcEventsApplier<Object, T2<Object, GridCacheVersion>> {
    /** Client connected to the destination cluster. */
    private final IgniteClient client;

    /** Caches. */
    private final IntMap<TcpClientCache<Object, Object>> ignCaches = new IntHashMap<>();

    /**
     * @param client Client connected to the destination cluster.
     * @param maxBatchSize Maximum batch size.
     * @param log Logger.
     */
    public CdcEventsIgniteClientApplier(IgniteClient client, int maxBatchSize, IgniteLogger log) {
        super(maxBatchSize, log);

        this.client = client;
    }

    /** {@inheritDoc} */
    @Override protected Object toKey(CdcEvent evt) {
        return evt.key();
    }

    /** {@inheritDoc} */
    @Override protected T2<Object, GridCacheVersion> toValue(int cacheId, Object val, GridCacheVersion ver) {
        return new T2<>(val, ver);
    }

    /** {@inheritDoc} */
    @Override protected void putAllConflict(int cacheId, Map<Object, T2<Object, GridCacheVersion>> drMap) {
        cache(cacheId).putAllConflict(drMap);
    }

    /** {@inheritDoc} */
    @Override protected void removeAllConflict(int cacheId, Map<Object, GridCacheVersion> drMap) {
        cache(cacheId).removeAllConflict(drMap);
    }

    /** @return Cache. */
    private TcpClientCache<Object, Object> cache(int cacheId) {
        return ignCaches.computeIfAbsent(cacheId, id -> {
            for (String cacheName : client.cacheNames()) {
                if (CU.cacheId(cacheName) == id)
                    return (TcpClientCache<Object, Object>)client.cache(cacheName).withKeepBinary();
            }

            throw new IllegalStateException("Cache with id not found [cacheId=" + id + ']');
        });
    }
}
