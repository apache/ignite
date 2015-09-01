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
import javax.cache.Cache;
import org.apache.ignite.cache.CacheEntry;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersionAware;

/**
 *
 */
public class CacheEntryImpl0<K, V> implements Cache.Entry<K, V> {
    /** */
    private final Map.Entry<K, V> e;

    /**
     * @param e Entry.
     */
    public CacheEntryImpl0(Map.Entry<K, V> e) {
        this.e = e;
    }

    /** {@inheritDoc} */
    @Override public K getKey() {
        return e.getKey();
    }

    /** {@inheritDoc} */
    @Override public V getValue() {
        return e.getValue();
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public <T> T unwrap(Class<T> cls) {
        if(cls.isAssignableFrom(getClass()))
            return cls.cast(this);
        else if (cls.isAssignableFrom(CacheEntry.class) && e instanceof GridCacheVersionAware)
            return (T)new CacheEntryImplEx<>(e.getKey(), e.getValue(), ((GridCacheVersionAware)e).version());

        throw new IllegalArgumentException("Unwrapping to class is not supported: " + cls);
    }

    /** {@inheritDoc} */
    public String toString() {
        return "CacheEntry [key=" + getKey() + ", val=" + getValue() + ']';
    }
}