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

package org.apache.ignite.cache.spring;

import java.io.Serializable;
import org.apache.ignite.IgniteCache;
import org.springframework.cache.Cache;
import org.springframework.cache.support.SimpleValueWrapper;

/**
 * Spring cache implementation.
 */
class SpringCache implements Cache {
    /** */
    private static final Object NULL = new NullValue();

    /** */
    private final IgniteCache<Object, Object> cache;

    /**
     * @param cache Cache.
     */
    SpringCache(IgniteCache<Object, Object> cache) {
        assert cache != null;

        this.cache = cache;
    }

    /** {@inheritDoc} */
    @Override public String getName() {
        return cache.getName();
    }

    /** {@inheritDoc} */
    @Override public Object getNativeCache() {
        return cache;
    }

    /** {@inheritDoc} */
    @Override public ValueWrapper get(Object key) {
        Object val = cache.get(key);

        return val != null ? fromValue(val) : null;
    }

    /** {@inheritDoc} */
    @Override public void put(Object key, Object val) {
        if (val == null)
            cache.withSkipStore().put(key, NULL);
        else
            cache.put(key, val);
    }

    /** {@inheritDoc} */
    @Override public void evict(Object key) {
        cache.remove(key);
    }

    /** {@inheritDoc} */
    @Override public void clear() {
        cache.removeAll();
    }

    /**
     * @param val Cache value.
     * @return Wrapped value.
     */
    private static ValueWrapper fromValue(Object val) {
        assert val != null;

        return new SimpleValueWrapper(NULL.equals(val) ? null : val);
    }

    /** */
    private static class NullValue implements Serializable {
        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            return this == o || (o != null && getClass() == o.getClass());
        }
    }
}
