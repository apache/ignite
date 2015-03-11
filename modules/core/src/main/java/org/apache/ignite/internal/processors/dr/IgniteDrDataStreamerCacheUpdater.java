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

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.cache.dr.*;
import org.apache.ignite.internal.processors.cache.version.*;
import org.apache.ignite.internal.processors.datastreamer.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;

import java.util.*;

/**
 * Data center replication cache updater for data streamer.
 */
public class IgniteDrDataStreamerCacheUpdater implements IgniteDataStreamer.Updater<KeyCacheObject, CacheObject>,
    DataStreamerCacheUpdaters.InternalUpdater {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override public void update(IgniteCache<KeyCacheObject, CacheObject> cache0,
        Collection<Map.Entry<KeyCacheObject, CacheObject>> col) {
        try {
            String cacheName = cache0.getConfiguration(CacheConfiguration.class).getName();

            GridKernalContext ctx = ((IgniteKernal)cache0.unwrap(Ignite.class)).context();
            IgniteLogger log = ctx.log(IgniteDrDataStreamerCacheUpdater.class);
            GridCacheAdapter cache = ctx.cache().internalCache(cacheName);

            assert !F.isEmpty(col);

            if (log.isDebugEnabled())
                log.debug("Running DR put job [nodeId=" + ctx.localNodeId() + ", cacheName=" + cacheName + ']');

            IgniteInternalFuture<?> f = cache.context().preloader().startFuture();

            if (!f.isDone())
                f.get();

            CacheObjectContext cacheObjCtx = cache.context().cacheObjectContext();

            for (Map.Entry<KeyCacheObject, CacheObject> entry0 : col) {
                GridCacheRawVersionedEntry entry = (GridCacheRawVersionedEntry)entry0;

                entry.unmarshal(cacheObjCtx, ctx.config().getMarshaller());

                KeyCacheObject key = entry.getKey();

                // Ensure that updater to not receive special-purpose values for TTL and expire time.
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

            if (log.isDebugEnabled())
                log.debug("DR put job finished [nodeId=" + ctx.localNodeId() + ", cacheName=" + cacheName + ']');
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }
}
