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

import javax.cache.*;

/**
 *
 */
public class CacheEntryImpl<K, V> implements Cache.Entry<K, V> {
    /** */
    private final K key;

    /** */
    private final V val;

    /**
     * @param key Key.
     * @param val Value.
     */
    public CacheEntryImpl(K key, V val) {
        this.key = key;
        this.val = val;
    }

    /** {@inheritDoc} */
    @Override public K getKey() {
        return key;
    }

    /** {@inheritDoc} */
    @Override public V getValue() {
        return val;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public <T> T unwrap(Class<T> cls) {
        if (!cls.equals(getClass()))
            throw new IllegalArgumentException("Unwrapping to class is not supported: " + cls);

        return (T)this;
    }

    /** {@inheritDoc} */
    public String toString() {
        return "CacheEntry [key=" + key + ", val=" + val + ']';
    }
}
