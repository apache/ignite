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

package org.apache.ignite.cdc;

import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheObjectImpl;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.KeyCacheObjectImpl;
import org.apache.ignite.internal.processors.cache.dr.GridCacheDrExpirationInfo;
import org.apache.ignite.internal.processors.cache.dr.GridCacheDrInfo;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.collection.IntHashMap;
import org.apache.ignite.internal.util.collection.IntMap;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.processors.cache.GridCacheUtils.EXPIRE_TIME_CALCULATE;
import static org.apache.ignite.internal.processors.cache.GridCacheUtils.TTL_NOT_CHANGED;

/**
 * Contains logic to process {@link CdcEvent} and apply them to the destination cluster.
 *
 * @see IgniteInternalCache#putAllConflict(Map)
 * @see IgniteInternalCache#removeAllConflict(Map)
 */
public class CdcEventsIgniteApplier extends AbstractCdcEventsApplier<KeyCacheObject, GridCacheDrInfo> {
    /** Destination cluster. */
    private final IgniteEx ignite;

    /** Caches. */
    private final IntMap<IgniteInternalCache<BinaryObject, BinaryObject>> ignCaches = new IntHashMap<>();

    /**
     * @param ignite Destination cluster.
     * @param maxBatchSize Maximum batch size.
     * @param log Logger.
     */
    public CdcEventsIgniteApplier(IgniteEx ignite, int maxBatchSize, IgniteLogger log) {
        super(maxBatchSize, log);

        this.ignite = ignite;
    }

    /** {@inheritDoc} */
    @Override protected void putAllConflict(int cacheId, Map<KeyCacheObject, GridCacheDrInfo> drMap) {
        try {
            cache(cacheId).putAllConflict(drMap);
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override protected void removeAllConflict(int cacheId, Map<KeyCacheObject, GridCacheVersion> drMap) {
        try {
            cache(cacheId).removeAllConflict(drMap);
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override protected KeyCacheObject toKey(CdcEvent evt) {
        Object key = evt.key();

        if (key instanceof KeyCacheObject)
            return (KeyCacheObject)key;
        else
            return new KeyCacheObjectImpl(key, null, evt.partition());
    }

    /** {@inheritDoc} */
    @Override protected GridCacheDrInfo toValue(int cacheId, Object val, GridCacheVersion ver) {
        CacheObject cacheObj;

        if (val instanceof CacheObject)
            cacheObj = (CacheObject)val;
        else
            cacheObj = new CacheObjectImpl(val, null);

        return cache(cacheId).configuration().getExpiryPolicyFactory() != null ?
            new GridCacheDrExpirationInfo(cacheObj, ver, TTL_NOT_CHANGED, EXPIRE_TIME_CALCULATE) :
            new GridCacheDrInfo(cacheObj, ver);
    }

    /** @return Cache. */
    private IgniteInternalCache<BinaryObject, BinaryObject> cache(int cacheId) {
        return ignCaches.computeIfAbsent(cacheId, id -> {
            for (String cacheName : ignite.cacheNames()) {
                if (CU.cacheId(cacheName) == id) {
                    // IgniteEx#cachex(String) will return null if cache not initialized with regular Ignite#cache(String) call.
                    ignite.cache(cacheName);

                    return ignite.cachex(cacheName).keepBinary();
                }
            }

            throw new IllegalStateException("Cache with id not found [cacheId=" + id + ']');
        });
    }
}
