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
import java.util.concurrent.Callable;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheEntry;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.springframework.cache.Cache;
import org.springframework.cache.support.SimpleValueWrapper;

/**
 * Spring cache implementation.
 */
class SpringCache implements Cache {
    /** */
    private static final Object NULL = new NullValue();

    /** */
    private static final Object LOCK = new LockValue();

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
    @SuppressWarnings("unchecked")
    @Override public <T> T get(Object key, Class<T> type) {
        Object val = cache.get(key);

        if (NULL.equals(val) || LOCK.equals(val))
            val = null;

        if (val != null && type != null && !type.isInstance(val))
            throw new IllegalStateException("Cached value is not of required type [cacheName=" + cache.getName() +
                ", key=" + key + ", val=" + val + ", requiredType=" + type + ']');

        return (T)val;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public <T> T get(Object key, Callable<T> valueLoader) {
        // This is a workaround solution
        // "cache.invoke(key, new ValueLoaderEntryProcessor<T>(), valueLoader)" method
        // doesn't work properly with Spring AOP - @Cacheable(sync = true)
        synchronized (this) {
            long startTime = U.currentTimeMillis();

            Object val = cache.getAndPutIfAbsent(key, LOCK);

            if (val == null)
                return loadAndPut(key, valueLoader);

            if (val.equals(LOCK)) {
                // it is the lock of another node
                CacheEntry entry = cache.getEntry(key);
                val = entry.getValue();

                if (val.equals(LOCK))
                    return waitAndLoad(entry, valueLoader, startTime);
            }

            return (T)fromStoreValue(val);
        }
    }

    /** {@inheritDoc} */
    @Override public void put(Object key, Object val) {
        if (val == null)
            cache.withSkipStore().put(key, NULL);
        else
            cache.put(key, toStoreValue(val));
    }

    /** {@inheritDoc} */
    @Override public ValueWrapper putIfAbsent(Object key, Object val) {
        Object old;

        if (val == null)
            old = cache.withSkipStore().getAndPutIfAbsent(key, NULL);
        else
            old = cache.getAndPutIfAbsent(key, val);

        return old != null ? fromValue(old) : null;
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
     * @param entry CacheEntry.
     * @param valueLoader ValueLoader.
     * @param startTime Invocation start time.
     * @param <T> Type of return type.
     * @return Value.
     */
    @SuppressWarnings({"unchecked", "ConstantConditions"})
    private synchronized <T> T waitAndLoad(CacheEntry entry, Callable<T> valueLoader, long startTime) {
        long updateTime = entry.updateTime();
        boolean isUpdated = false;

        while (!isUpdated) { // the cycle instead of recursion

            while (2000 < (startTime - U.currentTimeMillis()) && !isUpdated) // try to detect failover
                isUpdated = (updateTime != entry.updateTime());

            if (updateTime != entry.updateTime()) {
                cache.put(entry.getKey(), LOCK);

                return loadAndPut(entry.getKey(), valueLoader);
            }

            Object val = entry.getValue();

            if (!val.equals(LOCK))
                return (T)fromStoreValue(val);

            // the lock was updated by another node, try again
            startTime = U.currentTimeMillis();
            isUpdated = false;
        }

        throw new AssertionError();
    }

    /**
     * @param key Key.
     * @param valueLoader Value loader.
     * @param <T> Type of return type.
     * @return Loaded value.
     */
    @SuppressWarnings("unchecked")
    private synchronized <T> T loadAndPut(Object key, Callable<T> valueLoader) {
        Object val;

        try {
            val = valueLoader.call();
        }
        catch (Exception e) {
            throw new ValueRetrievalException(key, valueLoader, e);
        }

        cache.put(key, val);

        return (T)val;
    }

    /**
     * @param val Cache value.
     * @return Wrapped value.
     */
    private static ValueWrapper fromValue(Object val) {
        assert val != null;

        return new SimpleValueWrapper(fromStoreValue(val));
    }

    /** */
    private static class NullValue implements Serializable {
        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            return this == o || (o != null && getClass() == o.getClass());
        }
    }

    /** */
    private static class LockValue implements Serializable {
        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            return this == o || (o != null && getClass() == o.getClass());
        }
    }

    /**
     * @param val User value.
     * @return Value to store.
     */
    private static Object toStoreValue(@Nullable Object val) {
        return val == null ? NULL : val;
    }

    /**
     * @param val Stored value.
     * @return User value.
     */
    @Nullable private static Object fromStoreValue(@NotNull Object val) {
        return (NULL.equals(val) || LOCK.equals(val)) ? null : val;
    }
}
