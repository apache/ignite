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

package org.apache.ignite.transactions.spring;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.client.ClientCache;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/**
 * Service.
 */
public class GridSpringTransactionService {
    /** */
    @Autowired
    private GridSpringTransactionService self;

    /**
     * @param cache Cache.
     * @param entryCnt Entries count.
     */
    @Transactional
    public void put(CacheProxy<Integer, String> cache, int entryCnt) {
        for (int i = 0; i < entryCnt; i++)
            cache.put(i, String.valueOf(i));
    }

    /**
     * @param cache Cache.
     * @param entryCnt Entries count.
     */
    @Transactional
    public void putWithError(CacheProxy<Integer, String> cache, int entryCnt) {
        for (int i = 0; i < entryCnt; i++)
            cache.put(i, String.valueOf(i));

        cache.put(Integer.valueOf("one"), "one");
    }

    /**
     * @param cache Cache.
     */
    @Transactional(propagation = Propagation.MANDATORY)
    public void putWithMandatoryPropagation(CacheProxy<Integer, String> cache) {
        cache.put(1, "1");
    }

    /**
     * @param cache Cache.
     */
    @Transactional(isolation = Isolation.READ_UNCOMMITTED)
    public void putWithUnsupportedIsolationLevel(CacheProxy<Integer, String> cache) {
        cache.put(1, "1");
    }

    /** */
    @Transactional
    public void putWithNestedError(CacheProxy<Integer, String> cache, int entryCnt) {
        self.put(cache, entryCnt);

        try {
            self.putWithError(cache, entryCnt);
        }
        catch (Exception ignored) {
            // No-op.
        }
    }

    /** */
    public static class ClientCacheProxy<K, V> implements CacheProxy<K, V> {
        /** */
        private final ClientCache<K, V> cliCache;

        /** */
        public ClientCacheProxy(ClientCache<K, V> cliCache) {
            this.cliCache = cliCache;
        }

        /** {@inheritDoc} */
        @Override public void put(K key, V val) {
            cliCache.put(key, val);
        }

        /** {@inheritDoc} */
        @Override public int size() {
            return cliCache.size();
        }

        /** {@inheritDoc} */
        @Override public void removeAll() {
            cliCache.removeAll();
        }
    }

    /** */
    public static class IgniteCacheProxy<K, V> implements CacheProxy<K, V> {
        /** */
        private final IgniteCache<K, V> cache;

        /** */
        public IgniteCacheProxy(IgniteCache<K, V> cache) {
            this.cache = cache;
        }

        /** {@inheritDoc} */
        @Override public void put(K key, V val) {
             cache.put(key, val);
        }

        /** {@inheritDoc} */
        @Override public int size() {
            return cache.size();
        }

        /** {@inheritDoc} */
        @Override public void removeAll() {
            cache.removeAll();
        }
    }

    /** */
    public static interface CacheProxy<K, V> {
        /** */
        public void put(K key, V val);

        /** */
        public int size();

        /** */
        public void removeAll();
    }
}
