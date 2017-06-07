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
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.IgniteCache;
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

        if (NULL.equals(val))
            val = null;

        if (val != null && type != null && !type.isInstance(val))
            throw new IllegalStateException("Cached value is not of required type [cacheName=" + cache.getName() +
                ", key=" + key + ", val=" + val + ", requiredType=" + type + ']');

        return (T)val;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public <T> T get(Object key, Callable<T> valueLoader) {
        Object val = cache.get(key);

        if (val != null)
            return (T)fromStoreValue(val);

        try {
            return cache.invoke(key, new ValueLoaderEntryProcessor<T>(), valueLoader);
        }
        catch (Exception e) {
            throw new ValueRetrievalException(key, valueLoader, e);
        }
    }

    /** {@inheritDoc} */
    @Override public void put(Object key, Object val) {
        if (val == null)
            cache.withSkipStore().put(key, NULL);
        else
            cache.put(key, val);
    }

    /** {@inheritDoc} */
    @Override public <T> T get(Object key, Callable<T> valLdr) {
        throw new UnsupportedOperationException();
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

    /**
     * An invocable function that allows applications to perform compound operations
     * on a {@link javax.cache.Cache.Entry} atomically, according the defined
     * consistency of a {@link Cache}.
     *
     * @param <T> The type of the return value
     */
    private class ValueLoaderEntryProcessor<T> implements EntryProcessor<Object, Object, T> {
        /** {@inheritDoc} */
        @SuppressWarnings("unchecked")
        @Override public T process(MutableEntry<Object, Object> entry, Object... args)
            throws EntryProcessorException {
            Callable<T> valueLoader = (Callable<T>)args[0];

            if (entry.exists())
                return (T)fromStoreValue(entry.getValue());
            else {
                T val;

                try {
                    val = valueLoader.call();
                }
                catch (Exception e) {
                    throw new EntryProcessorException("Value loader '" + valueLoader + "' failed " +
                        "to compute  value for key '" + entry.getKey() + "'", e);
                }

                entry.setValue(toStoreValue(val));

                return val;
            }
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
        return NULL.equals(val) ? null : val;
    }
}
