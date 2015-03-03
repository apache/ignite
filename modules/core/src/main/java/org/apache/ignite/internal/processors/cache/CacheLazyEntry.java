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

import org.apache.ignite.internal.util.typedef.internal.*;

import javax.cache.*;

/**
 * 
 */
public class CacheLazyEntry<K, V> implements Cache.Entry<K, V> {
    /** Cache context. */
    private GridCacheContext<K, V> cctx;

    /** Key cache object. */
    private KeyCacheObject keyObj;

    /** Cache object value. */
    private CacheObject valObj;

    /** Key. */
    private K key;

    /** Value. */
    private V val;

    /**
     * @param keyObj Key cache object.
     * @param valObj Cache object value.
     * @param cctx Cache context.
     */
    public CacheLazyEntry(KeyCacheObject keyObj, CacheObject valObj, GridCacheContext<K, V> cctx) {
        this.keyObj = keyObj;
        this.valObj = valObj;
        this.cctx = cctx;
    }

    /**
     * @param key Key.
     * @param val Value.
     */
    public CacheLazyEntry(K key, V val) {
        this.key = key;
        this.val = val;
    }

    /**
     * @param keyObj Key cache object.
     * @param valObj Cache object value.
     * @param key Key.
     * @param val Value.
     * @param cctx Cache context.
     */
    public CacheLazyEntry(KeyCacheObject keyObj,
        CacheObject valObj,
        K key,
        V val, 
        GridCacheContext<K, V> cctx) {
        this.keyObj = keyObj;
        this.valObj = valObj;
        this.val = val;
        this.key = key;
        this.cctx = cctx;
    }

    /** {@inheritDoc} */
    @Override public K getKey() {
        if (key == null)
            key = CU.value(keyObj, cctx, true);

        return key;
    }

    /** {@inheritDoc} */
    @Override public V getValue() {
        if (val == null)
            val = CU.value(valObj, cctx, true);

        return val;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public <T> T unwrap(Class<T> cls) {
        if(cls.isAssignableFrom(getClass()))
            return cls.cast(this);

        throw new IllegalArgumentException("Unwrapping to class is not supported: " + cls);
    }

    /** {@inheritDoc} */
    public String toString() {
        return "CacheEntry [key=" + getKey() + ", val=" + getValue() + ']';
    }
}
