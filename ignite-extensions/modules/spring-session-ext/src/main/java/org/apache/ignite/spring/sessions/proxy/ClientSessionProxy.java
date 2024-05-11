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

package org.apache.ignite.spring.sessions.proxy;

import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.expiry.ExpiryPolicy;
import org.apache.ignite.cache.query.Query;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.spring.sessions.IgniteSession;

/**
 * Represents {@link SessionProxy} implementation that uses {@link ClientCache} to perform session operations.
 */
public class ClientSessionProxy implements SessionProxy {
    /** Client cache instance. */
    private final ClientCache<String, IgniteSession> cache;

    /**
     * @param cache Client cache instance.
     */
    public ClientSessionProxy(ClientCache<String, IgniteSession> cache) {
        this.cache = cache;
    }

    /** {@inheritDoc} */
    @Override public void registerCacheEntryListener(
        CacheEntryListenerConfiguration<String, IgniteSession> lsnrCfg
    ) {
        cache.registerCacheEntryListener(lsnrCfg);
    }

    /** {@inheritDoc} */
    @Override public void deregisterCacheEntryListener(
        CacheEntryListenerConfiguration<String, IgniteSession> lsnrCfg
    ) {
        cache.deregisterCacheEntryListener(lsnrCfg);
    }

    /** {@inheritDoc} */
    @Override public SessionProxy withExpiryPolicy(ExpiryPolicy expirePlc) {
        return new ClientSessionProxy(cache.withExpirePolicy(expirePlc));
    }

    /** {@inheritDoc} */
    @Override public IgniteSession get(String key) {
        return cache.get(key);
    }

    /** {@inheritDoc} */
    @Override public void put(String key, IgniteSession val) {
        cache.put(key, val);
    }

    /** {@inheritDoc} */
    @Override public boolean remove(String key) {
        return cache.remove(key);
    }

    /** {@inheritDoc} */
    @Override public boolean replace(String key, IgniteSession val) {
        return cache.replace(key, val);
    }

    /** {@inheritDoc} */
    @Override public boolean replace(String key, IgniteSession oldVal, IgniteSession newVal) {
        return cache.replace(key, oldVal, newVal);
    }

    /** {@inheritDoc} */
    @Override public <R> QueryCursor<R> query(Query<R> qry) {
        return cache.query(qry);
    }
}
