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

package org.apache.ignite.internal.processors.platform.client.cache;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.NotNull;

/**
 * Map implementation without extra overhead related to {@link Map.Entry} and bucket initialization.
 * Relies on underling arrays for keys and values. Unique keys for input only to prevent deadlocks.
 * Note: For internal use only
 */
public class GridArrayMap<K, V> implements Map<K, V>, Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private static final int DEFAULT_CAPACITY = 10;

    /** */
    private static final float GROWTH_FACTOR = 1.5f;

    /** */
    private K[] keys;

    /** */
    private V[] values;

    /** */
    private int size;

    /** */
    public GridArrayMap() {
        this(DEFAULT_CAPACITY);
    }

    /** */
    @SuppressWarnings("unchecked")
    public GridArrayMap(int capacity) {
        keys = (K[])new Object[capacity];
        values = (V[])new Object[capacity];
        size = 0;
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return size;
    }

    /** {@inheritDoc} */
    @Override public boolean isEmpty() {
        return size == 0;
    }

    /** {@inheritDoc} */
    @Override public boolean containsKey(Object key) {
        throw new UnsupportedOperationException("'containsKey' operation is not supported by GridArrayMap.");
    }

    /** {@inheritDoc} */
    @Override public boolean containsValue(Object val) {
        throw new UnsupportedOperationException("'containsValue' operation is not supported by GridArrayMap.");
    }

    /** {@inheritDoc} */
    @Override public V get(Object key) {
        throw new UnsupportedOperationException("'get' operation is not supported by GridArrayMap.");
    }

    /** {@inheritDoc} */
    @Override public V put(K key, V val) {
        ensureCapacity();

        keys[size] = key;
        values[size] = val;

        size++;

        return null;
    }

    /** {@inheritDoc} */
    @Override public V remove(Object key) {
        throw new UnsupportedOperationException("'remove' operation is not supported by GridArrayMap.");
    }

    /** {@inheritDoc} */
    @Override public void putAll(@NotNull Map<? extends K, ? extends V> m) {
        throw new UnsupportedOperationException("'remove' operation is not supported by GridArrayMap.");
    }

    /** {@inheritDoc} */
    @Override public void clear() {
        Arrays.fill(keys, 0, size, null);
        Arrays.fill(values, 0, size, null);

        size = 0;
    }

    /** {@inheritDoc} */
    @Override public @NotNull Set<K> keySet() {
        return Set.of(keys);
    }

    /** {@inheritDoc} */
    @Override public @NotNull Collection<V> values() {
        return List.of(values);
    }

    /** {@inheritDoc} */
    @Override public @NotNull Set<Entry<K, V>> entrySet() {
        throw new UnsupportedOperationException("'remove' operation is not supported by GridArrayMap.");
    }

    /** */
    private void ensureCapacity() {
        if (size == keys.length) {
            int newCapacity = (int)(keys.length * GROWTH_FACTOR);

            keys = Arrays.copyOf(keys, newCapacity);
            values = Arrays.copyOf(values, newCapacity);
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridArrayMap.class, this);
    }
}
