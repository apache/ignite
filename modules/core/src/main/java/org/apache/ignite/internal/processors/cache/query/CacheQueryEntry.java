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

package org.apache.ignite.internal.processors.cache.query;

import javax.cache.Cache;
import org.apache.ignite.cache.CacheEntry;
import org.apache.ignite.internal.processors.cache.CacheEntryImpl;
import org.apache.ignite.internal.processors.cache.CacheEntryImplEx;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.Nullable;

/** */
final class CacheQueryEntry<K,V> extends IgniteBiTuple<K,V> implements Cache.Entry<K,V> {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    public CacheQueryEntry() {
        // No-op.
    }

    /**
     * @param key Key.
     * @param val Value.
     */
    CacheQueryEntry(@Nullable K key, @Nullable V val) {
        super(key, val);
    }

    /** {@inheritDoc} */
    @Override public <T> T unwrap(Class<T> cls) {
        if (cls != null && cls.isAssignableFrom(getClass()))
            return cls.cast(this);

        if (cls.isAssignableFrom(CacheEntryImpl.class))
            return (T)new CacheEntryImpl<>(getKey(), getValue());

        if (cls.isAssignableFrom(CacheEntry.class))
            return (T)new CacheEntryImplEx<>(getKey(), getValue(), null);

        throw new IllegalArgumentException("Unwrapping to class is not supported: " + cls);
    }
}
