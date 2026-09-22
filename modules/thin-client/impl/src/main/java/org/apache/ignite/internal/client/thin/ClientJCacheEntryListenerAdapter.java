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

package org.apache.ignite.internal.client.thin;

import java.util.Collections;
import javax.cache.event.CacheEntryCreatedListener;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryExpiredListener;
import javax.cache.event.CacheEntryListener;
import javax.cache.event.CacheEntryRemovedListener;
import javax.cache.event.CacheEntryUpdatedListener;

/**
 * Adapter to convert CQ listener calls to JCache listener calls.
 */
class ClientJCacheEntryListenerAdapter<K, V> implements CacheEntryUpdatedListener<K, V> {
    /** Created listener. */
    private final CacheEntryCreatedListener<K, V> crtLsnr;

    /** Updated listener. */
    private final CacheEntryUpdatedListener<K, V> updLsnr;

    /** Removed listener. */
    private final CacheEntryRemovedListener<K, V> rmvLsnr;

    /** Expired listener. */
    private final CacheEntryExpiredListener<K, V> expLsnr;

    /** */
    ClientJCacheEntryListenerAdapter(CacheEntryListener<? super K, ? super V> impl) {
        crtLsnr = impl instanceof CacheEntryCreatedListener ? (CacheEntryCreatedListener<K, V>)impl : evts -> {};
        updLsnr = impl instanceof CacheEntryUpdatedListener ? (CacheEntryUpdatedListener<K, V>)impl : evts -> {};
        rmvLsnr = impl instanceof CacheEntryRemovedListener ? (CacheEntryRemovedListener<K, V>)impl : evts -> {};
        expLsnr = impl instanceof CacheEntryExpiredListener ? (CacheEntryExpiredListener<K, V>)impl : evts -> {};
    }

    /** {@inheritDoc} */
    @Override public void onUpdated(Iterable<CacheEntryEvent<? extends K, ? extends V>> evts) {
        for (CacheEntryEvent<? extends K, ? extends V> evt : evts) {
            try {
                Iterable<CacheEntryEvent<? extends K, ? extends V>> evtColl = Collections.singleton(evt);

                switch (evt.getEventType()) {
                    case CREATED: crtLsnr.onCreated(evtColl); break;
                    case UPDATED: updLsnr.onUpdated(evtColl); break;
                    case REMOVED: rmvLsnr.onRemoved(evtColl); break;
                    case EXPIRED: expLsnr.onExpired(evtColl); break;
                }
            }
            catch (Exception ignored) {
                // Ignore exceptions in user code.
            }
        }
    }
}
