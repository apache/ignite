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

package org.apache.ignite.ml.util;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * LRU cache with fixed size and expiration listener.
 *
 * @param <K> Type of a key.
 * @param <V> Type of a value.
 */
public class LRUCache<K, V> extends LinkedHashMap<K, V> {
    /** */
    private static final long serialVersionUID = 4266640700294024306L;

    /** Cache size. */
    private final int cacheSize;

    /** Removal listeners. */
    private final LRUCacheExpirationListener<V> expirationLsnr;

    /**
     * Constructs a new instance of LRU cache.
     *
     * @param cacheSize Cache size.
     */
    public LRUCache(int cacheSize) {
        this(cacheSize, e -> {});
    }

    /**
     * Constructs a new instance of LRU cache.
     *
     * @param cacheSize Cache size.
     * @param expirationLsnr Expiration listener.
     */
    public LRUCache(int cacheSize, LRUCacheExpirationListener<V> expirationLsnr) {
        super(10, 0.75f, true);
        this.cacheSize = cacheSize;
        this.expirationLsnr = expirationLsnr;
    }

    /** {@inheritDoc} */
    @Override protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
        if (size() > cacheSize) {
            expirationLsnr.entryExpired(eldest.getValue());
            return true;
        }

        return false;
    }
}
