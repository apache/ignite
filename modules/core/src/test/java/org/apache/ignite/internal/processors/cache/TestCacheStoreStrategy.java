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

package org.apache.ignite.internal.processors.cache;

import java.util.Map;
import javax.cache.configuration.Factory;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.configuration.CacheConfiguration;

/**
 * Interface for cache store backend manipulation and stats routines.
 */
public interface TestCacheStoreStrategy {
    /**
     * @return Number of reads to store.
     */
    public int getReads();

    /**
     * @return Number of writes to store.
     */
    public int getWrites();

    /**
     * @return Number of removals from store.
     */
    public int getRemoves();

    /**
     * @return Total number of items in the store.
     */
    public int getStoreSize();

    /**
     * Clear store contents.
     */
    public void resetStore();

    /**
     * Put entry to cache store.
     *
     * @param key Key.
     * @param val Value.
     */
    public void putToStore(Object key, Object val);

    /**
     * @param data Items to put to store.
     */
    public void putAllToStore(Map<?, ?> data);

    /**
     * @param key Key to look for.
     * @return {@link Object} pointed to by given key or {@code null} if no object is present.
     */
    public Object getFromStore(Object key);

    /**
     * @param key to look for
     */
    public void removeFromStore(Object key);

    /**
     * @param key to look for.
     * @return {@code True} if object pointed to by key is in store, false otherwise.
     */
    public boolean isInStore(Object key);

    /**
     * Called from {@link GridCacheAbstractSelfTest#cacheConfiguration(String)},
     * this method allows implementations to tune cache config.
     *
     * @param cfg {@link CacheConfiguration} to tune.
     */
    public void updateCacheConfiguration(CacheConfiguration<Object, Object> cfg);

    /**
     * @return {@link Factory} for write-through storage emulator.
     */
    public Factory<? extends CacheStore<Object, Object>> getStoreFactory();
}
