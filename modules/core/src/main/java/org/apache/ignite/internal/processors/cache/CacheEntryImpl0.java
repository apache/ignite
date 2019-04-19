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
    @Override public <T> T unwrap(Class<T> cls) {
        if(cls.isAssignableFrom(getClass()))
            return cls.cast(this);
        else if (cls.isAssignableFrom(CacheEntry.class) && e instanceof GridCacheVersionAware)
            return (T)new CacheEntryImplEx<>(e.getKey(), e.getValue(), ((GridCacheVersionAware)e).version());

        throw new IllegalArgumentException("Unwrapping to class is not supported: " + cls);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "CacheEntry [key=" + getKey() + ", val=" + getValue() + ']';
    }
}