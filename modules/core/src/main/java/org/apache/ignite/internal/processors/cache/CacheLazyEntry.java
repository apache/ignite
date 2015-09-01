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

import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 *
 */
public class CacheLazyEntry<K, V> implements Cache.Entry<K, V> {
    /** Cache context. */
    protected GridCacheContext cctx;

    /** Key cache object. */
    protected KeyCacheObject keyObj;

    /** Cache object value. */
    protected CacheObject valObj;

    /** Key. */
    @GridToStringInclude
    protected K key;

    /** Value. */
    @GridToStringInclude
    protected V val;

    /**
     * @param cctx Cache context.
     * @param keyObj Key cache object.
     * @param valObj Cache object value.
     */
    public CacheLazyEntry(GridCacheContext cctx, KeyCacheObject keyObj, CacheObject valObj) {
        this.keyObj = keyObj;
        this.valObj = valObj;
        this.cctx = cctx;
    }

    /**
     * @param keyObj Key cache object.
     * @param val Value.
     * @param cctx Cache context.
     */
    public CacheLazyEntry(GridCacheContext cctx, KeyCacheObject keyObj, V val) {
        this.keyObj = keyObj;
        this.val = val;
        this.cctx = cctx;
    }

    /**
     * @param ctx Cache context.
     * @param keyObj Key cache object.
     * @param key Key value.
     * @param valObj Cache object
     * @param val Cache value.
     */
    public CacheLazyEntry(GridCacheContext<K, V> ctx, 
        KeyCacheObject keyObj,
        K key,
        CacheObject valObj,
        V val) {
        this.cctx = ctx;
        this.keyObj = keyObj;
        this.key = key;
        this.valObj = valObj;
        this.val = val;
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

    /**
     * @return Return value. This methods doesn't initialize value.
     */
    public V value() {
        return val;
    }

    /**
     * @return Return key. This methods doesn't initialize key.
     */
    public K key() {
        return key;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public <T> T unwrap(Class<T> cls) {
        if (cls.isAssignableFrom(Ignite.class))
            return (T)cctx.kernalContext().grid();
        else if (cls.isAssignableFrom(getClass()))
            return cls.cast(this);

        throw new IllegalArgumentException("Unwrapping to class is not supported: " + cls);
    }

    /** {@inheritDoc} */
    public String toString() {
        return S.toString(CacheLazyEntry.class, this);
    }
}