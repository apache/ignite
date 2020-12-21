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

package org.apache.ignite.internal.processors.dr;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheProxyImpl;
import org.apache.ignite.internal.processors.cache.IgniteCacheProxy;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.dr.GridCacheDrExpirationInfo;
import org.apache.ignite.internal.processors.cache.dr.GridCacheDrInfo;
import org.apache.ignite.internal.processors.cache.version.GridCacheRawVersionedEntry;
import org.apache.ignite.internal.processors.datastreamer.DataStreamerCacheUpdaters;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.stream.StreamReceiver;

/**
 * Data center replication cache receiver for data streamer.
 */
public class IgniteDrDataStreamerCacheUpdater implements StreamReceiver<KeyCacheObject, CacheObject>,
    DataStreamerCacheUpdaters.InternalUpdater {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override public void receive(IgniteCache<KeyCacheObject, CacheObject> cache0,
        Collection<Map.Entry<KeyCacheObject, CacheObject>> col) {
        try {
            GridKernalContext ctx = ((IgniteKernal)cache0.unwrap(Ignite.class)).context();

            IgniteInternalCache<KeyCacheObject, CacheObject> cache;

            if (cache0 instanceof IgniteCacheProxy)
                cache = ((IgniteCacheProxy<KeyCacheObject, CacheObject>)cache0).internalProxy().proxyNoGate();
            else {
                final IgniteInternalCache<KeyCacheObject, CacheObject> internalCache = ctx.cache().cache(cache0.getName());
                final GridCacheContext<KeyCacheObject, CacheObject> cctx = internalCache.context();

                cache = new GridCacheProxyImpl<>(cctx, internalCache, cctx.operationContextPerCall(), false);
            }

            assert !F.isEmpty(col);

            CacheObjectContext cacheObjCtx = cache.context().cacheObjectContext();

            for (Map.Entry<KeyCacheObject, CacheObject> entry0 : col) {
                GridCacheRawVersionedEntry<KeyCacheObject, CacheObject> entry = (GridCacheRawVersionedEntry<KeyCacheObject, CacheObject>)entry0;

                entry.unmarshal(cacheObjCtx, ctx.config().getMarshaller());

                KeyCacheObject key = entry.getKey();

                // Ensure that receiver to not receive special-purpose values for TTL and expire time.
                assert entry.ttl() != CU.TTL_NOT_CHANGED && entry.ttl() != CU.TTL_ZERO && entry.ttl() >= 0;
                assert entry.expireTime() != CU.EXPIRE_TIME_CALCULATE && entry.expireTime() >= 0;

                CacheObject cacheVal = entry.getValue();

                GridCacheDrInfo val = cacheVal != null ? entry.ttl() != CU.TTL_ETERNAL ?
                    new GridCacheDrExpirationInfo(cacheVal, entry.version(), entry.ttl(), entry.expireTime()) :
                    new GridCacheDrInfo(cacheVal, entry.version()) : null;

                if (val == null)
                    cache.removeAllConflict(Collections.singletonMap(key, entry.version()));
                else
                    cache.putAllConflict(Collections.singletonMap(key, val));
            }
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }
}