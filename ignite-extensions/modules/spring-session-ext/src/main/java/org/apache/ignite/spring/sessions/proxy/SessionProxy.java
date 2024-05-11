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

import javax.cache.Cache;
import javax.cache.CacheException;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.event.CacheEntryListener;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.integration.CacheWriter;
import org.apache.ignite.cache.query.Query;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.spring.sessions.IgniteSession;

/** Represents Ignite client-independent session operations. */
public interface SessionProxy {
    /**
     * Registers a {@link CacheEntryListener}. The supplied {@link CacheEntryListenerConfiguration} is used to
     * instantiate a listener and apply it to those events specified in the configuration.
     *
     * @param cacheEntryListenerConfiguration a factory and related configuration for creating the listener.
     * @throws IllegalArgumentException is the same CacheEntryListenerConfiguration is used more than once or
     *          if some unsupported by thin client properties are set.
     * @see CacheEntryListener
     */
    public void registerCacheEntryListener(CacheEntryListenerConfiguration<String, IgniteSession> cacheEntryListenerConfiguration);

    /**
     * Deregisters a listener, using the {@link CacheEntryListenerConfiguration} that was used to register it.
     *
     * @param cacheEntryListenerConfiguration the factory and related configuration that was used to create the
     *         listener.
     */
    public void deregisterCacheEntryListener(CacheEntryListenerConfiguration<String, IgniteSession> cacheEntryListenerConfiguration);

    /**
     * Returns cache with the specified expired policy set. This policy will be used for each operation invoked on
     * the returned cache.
     *
     * @param expirePlc Expire policy.
     * @return Cache instance with the specified expiry policy set.
     */
    public SessionProxy withExpiryPolicy(ExpiryPolicy expirePlc);

    /**
     * Gets an entry from the cache.
     *
     * @param key the key whose associated value is to be returned
     * @return the element, or null, if it does not exist.
     */
    public IgniteSession get(String key);

    /**
     * Associates the specified value with the specified key in the cache.
     *
     * @param key key with which the specified value is to be associated
     * @param val value to be associated with the specified key.
     */
    public void put(String key, IgniteSession val);

    /**
     * Removes the mapping for a key from this cache if it is present.
     *
     * @param key Key whose mapping is to be removed from the cache.
     * @return <tt>false</tt> if there was no matching key.
     */
    public boolean remove(String key);

    /**
     * Atomically replaces the entry for a key only if currently mapped to some
     * value.
     * <p>
     * This is equivalent to
     * <pre><code>
     * if (cache.containsKey(key)) {
     *   cache.put(key, value);
     *   return true;
     * } else {
     *   return false;
     * }</code></pre>
     * except that the action is performed atomically.
     *
     * @param key  the key with which the specified value is associated
     * @param val the value to be associated with the specified key
     * @return <tt>true</tt> if the value was replaced
     * @throws NullPointerException  if key is null or if value is null
     * @throws CacheException        if there is a problem during the replace
     * @throws ClassCastException    if the implementation is configured to perform
     *                               runtime-type-checking, and the key or value
     *                               types are incompatible with those that have been
     *                               configured for the {@link Cache}
     * @see CacheWriter#write
     */
    public boolean replace(String key, IgniteSession val);

    /**
     * Atomically replaces the entry for a key only if currently mapped to a given value.
     * <p>
     * This is equivalent to performing the following operations as a single atomic action:
     * <pre><code>
     * if (cache.containsKey(key) &amp;&amp; equals(cache.get(key), oldValue)) {
     *  cache.put(key, newValue);
     * return true;
     * } else {
     *  return false;
     * }
     * </code></pre>
     *
     * @param key Key with which the specified value is associated.
     * @param oldVal Value expected to be associated with the specified key.
     * @param newVal Value to be associated with the specified key.
     * @return <tt>true</tt> if the value was replaced
     */
    public boolean replace(String key, IgniteSession oldVal, IgniteSession newVal);

    /**
     * Execute SQL query and get cursor to iterate over results.
     *
     * @param <R> Type of query data.
     * @param qry SQL query.
     * @return Query result cursor.
     */
    public <R> QueryCursor<R> query(Query<R> qry);
}
