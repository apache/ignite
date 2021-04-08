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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.cache.configuration.CacheEntryListenerConfiguration;

/**
 * Per-cache cache entry listeners registry. Listeners can't be stored inside ClientCache instance, since there can be
 * several such instances per one cache.
 */
public class ClientCacheEntryListenersRegistry {
    /** */
    private final Map<String, Map<CacheEntryListenerConfiguration<?, ?>,
        ClientCacheEntryListenerHandler<?, ?>>> lsnrs = new ConcurrentHashMap<>();

    /**
     * Register listener handler.
     *
     * @return {@code True} if listener was succesfuly registered,
     *         {@code false} if listener was already registered before.
     */
    public boolean registerCacheEntryListener(String cacheName, CacheEntryListenerConfiguration<?, ?> cfg,
        ClientCacheEntryListenerHandler<?, ?> hnd) {
        Map<CacheEntryListenerConfiguration<?, ?>, ClientCacheEntryListenerHandler<?, ?>> cacheLsnrs =
            lsnrs.computeIfAbsent(cacheName, k -> new ConcurrentHashMap<>());

        ClientCacheEntryListenerHandler<?, ?> old = cacheLsnrs.putIfAbsent(cfg, hnd);

        return old == null;
    }

    /**
     * Deregister listener handler.
     *
     * @return Listener handler.
     */
    public ClientCacheEntryListenerHandler<?, ?> deregisterCacheEntryListener(String cacheName,
        CacheEntryListenerConfiguration<?, ?> cfg) {
        Map<CacheEntryListenerConfiguration<?, ?>, ClientCacheEntryListenerHandler<?, ?>> cacheLsnrs =
            lsnrs.get(cacheName);

        return cacheLsnrs != null ? cacheLsnrs.remove(cfg) : null;
    }
}
