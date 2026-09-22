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

package org.apache.ignite.internal.client.thin;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.Configuration;
import javax.cache.integration.CompletionListener;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;
import org.apache.ignite.client.ClientCache;

/**
 * Thin client cache to JCache compatible cache adapter.
 */
class ClientJCacheAdapter<K, V> implements Cache<K, V> {
    /** Delegate. */
    private final ClientCache<K, V> delegate;

    /**
     * @param delegate Delegate.
     */
    ClientJCacheAdapter(ClientCache<K, V> delegate) {
        this.delegate = delegate;
    }

    /** {@inheritDoc} */
    @Override public V get(K key) {
        return delegate.get(key);
    }

    /** {@inheritDoc} */
    @Override public Map<K, V> getAll(Set<? extends K> keys) {
        return delegate.getAll(keys);
    }

    /** {@inheritDoc} */
    @Override public boolean containsKey(K key) {
        return delegate.containsKey(key);
    }

    /** {@inheritDoc} */
    @Override public void loadAll(Set<? extends K> keys, boolean replaceExistingValues,
        CompletionListener completionListener) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void put(K key, V val) {
        delegate.put(key, val);
    }

    /** {@inheritDoc} */
    @Override public V getAndPut(K key, V val) {
        return delegate.getAndPut(key, val);
    }

    /** {@inheritDoc} */
    @Override public void putAll(Map<? extends K, ? extends V> map) {
        delegate.putAll(map);
    }

    /** {@inheritDoc} */
    @Override public boolean putIfAbsent(K key, V val) {
        return delegate.putIfAbsent(key, val);
    }

    /** {@inheritDoc} */
    @Override public boolean remove(K key) {
        return delegate.remove(key);
    }

    /** {@inheritDoc} */
    @Override public boolean remove(K key, V oldVal) {
        return delegate.remove(key, oldVal);
    }

    /** {@inheritDoc} */
    @Override public V getAndRemove(K key) {
        return delegate.getAndRemove(key);
    }

    /** {@inheritDoc} */
    @Override public boolean replace(K key, V oldVal, V newVal) {
        return delegate.replace(key, oldVal, newVal);
    }

    /** {@inheritDoc} */
    @Override public boolean replace(K key, V val) {
        return delegate.replace(key, val);
    }

    /** {@inheritDoc} */
    @Override public V getAndReplace(K key, V val) {
        return delegate.getAndReplace(key, val);
    }

    /** {@inheritDoc} */
    @Override public void removeAll(Set<? extends K> keys) {
        delegate.removeAll(keys);
    }

    /** {@inheritDoc} */
    @Override public void removeAll() {
        delegate.removeAll();
    }

    /** {@inheritDoc} */
    @Override public void clear() {
        delegate.clear();
    }

    /** {@inheritDoc} */
    @Override public <C extends Configuration<K, V>> C getConfiguration(Class<C> clazz) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public <T> T invoke(
        K key,
        EntryProcessor<K, V, T> entryProc,
        Object... arguments
    ) throws EntryProcessorException {
        return delegate.invoke(key, entryProc, arguments);
    }

    /** {@inheritDoc} */
    @Override public <T> Map<K, EntryProcessorResult<T>> invokeAll(
        Set<? extends K> keys,
        EntryProcessor<K, V, T> entryProc,
        Object... arguments
    ) {
        return delegate.invokeAll(keys, entryProc, arguments);
    }

    /** {@inheritDoc} */
    @Override public String getName() {
        return delegate.getName();
    }

    /** {@inheritDoc} */
    @Override public CacheManager getCacheManager() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void close() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public boolean isClosed() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public <T> T unwrap(Class<T> clazz) {
        if (clazz.isAssignableFrom(delegate.getClass()))
            return (T)delegate;

        throw new IllegalArgumentException("Unwrapping to class is not supported: " + clazz);
    }

    /** {@inheritDoc} */
    @Override public void registerCacheEntryListener(
        CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration) {
        delegate.registerCacheEntryListener(cacheEntryListenerConfiguration);
    }

    /** {@inheritDoc} */
    @Override public void deregisterCacheEntryListener(
        CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration) {
        delegate.deregisterCacheEntryListener(cacheEntryListenerConfiguration);
    }

    /** {@inheritDoc} */
    @Override public Iterator<Entry<K, V>> iterator() {
        throw new UnsupportedOperationException();
    }
}
