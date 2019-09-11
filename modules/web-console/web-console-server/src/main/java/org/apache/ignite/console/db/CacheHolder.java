/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.console.db;

import java.util.function.Function;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.transactions.TransactionException;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.REPLICATED;

/**
 * Cache holder.
 *
 * @param <K>
 * @param <V>
 */
public class CacheHolder<K, V> {
    /** */
    protected final Ignite ignite;

    /** */
    protected final String cacheName;

    /** */
    protected IgniteCache cache;

    /**
     * @param ignite Ignite.
     * @param cacheName Cache name.
     */
    public CacheHolder(Ignite ignite, String cacheName) {
        this(ignite, cacheName, Function.identity());
    }

    /**
     * @param ignite Ignite.
     * @param cacheName Cache name.
     * @param f Cache configuration mapper.
     */
    public CacheHolder(Ignite ignite, String cacheName, Function<CacheConfiguration<K, V>, CacheConfiguration<K, V>> f) {
        this.ignite = ignite;
        this.cacheName = cacheName;

        CacheConfiguration<K, V> ccfg = new CacheConfiguration<K, V>(cacheName)
            .setAtomicityMode(TRANSACTIONAL)
            .setCacheMode(REPLICATED);

        cache = ignite.getOrCreateCache(f.apply(ccfg));
    }

    /**
     * @param key the key whose associated value is to be returned
     * @return {@code true} If table containsKey specified key
     */
    public boolean containsKey(K key) throws TransactionException {
        return cache().containsKey(key);
    }

    /**
     * @param key the key whose associated value is to be returned
     * @return DTO.
     */
    public V get(K key) throws TransactionException {
        return cache().get(key);
    }

    /**
     * @param key key with which the specified value is to be associated
     * @param val value to be associated with the specified key
     */
    public void put(K key, V val) throws TransactionException {
        cache().put(key, val);
    }

    /**
     * @param key key with which the specified value is to be associated
     * @param val value to be associated with the specified key
     * @return the value associated with the key at the start of the operation or null if none was associated
     */
    public V getAndPut(K key, V val) throws TransactionException {
        return cache().getAndPut(key, val);
    }

    /**
     * @return Underlying cache
     */
    public IgniteCache<K, V> cache() {
        return cache;
    }
}
