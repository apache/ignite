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

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.transactions.TransactionException;

import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
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
    private IgniteCache<K, V> cache;

    /** */
    protected ExpiryPolicy expiryPlc;

    /**
     * @param ignite Ignite.
     * @param cacheName Cache name.
     */
    public CacheHolder(Ignite ignite, String cacheName) {
        this(ignite, cacheName, -1);
    }

    /**
     * @param ignite Ignite.
     * @param cacheName Cache name.
     * @param expirationTimeout Cache expiration timeout.
     */
    public CacheHolder(Ignite ignite, String cacheName, long expirationTimeout) {
        this.ignite = ignite;
        this.cacheName = cacheName;

        if (expirationTimeout > 0)
            expiryPlc = CreatedExpiryPolicy.factoryOf(new Duration(MILLISECONDS, expirationTimeout)).create();

        CacheConfiguration<K, V> ccfg = new CacheConfiguration<K, V>(cacheName)
            .setAtomicityMode(TRANSACTIONAL)
            .setCacheMode(REPLICATED);

        cache = ignite.getOrCreateCache(ccfg);        
        
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
    
    public V getAndRemove(K key) throws TransactionException {
        return cache().getAndRemove(key);
    }

    /**
     * @return Underlying cache
     */
    public IgniteCache<K, V> cache() {
        return expiryPlc  == null ? cache : cache.withExpiryPolicy(expiryPlc);
    }
 
}
