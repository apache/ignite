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

package org.apache.ignite.internal.util;

import java.io.Serializable;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.NotNull;

/**
 * Map implementation without extra overhead related to {@link Map.Entry} and bucket initialization.
 * Relies on underling arrays for keys and values. Unique keys for input only to prevent deadlocks.
 * Note: For internal use only
 */
public class GridClientPutAllMap<K, V> implements Map<K, V>, Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private static final int DEFAULT_CAPACITY = 10;

    /** */
    private static final float GROWTH_FACTOR = 1.5f;

    /** HashSet to track keys */
    @GridToStringExclude
    private final Set<K> keySet = new HashSet<>();

    /** */
    private K[] keys;

    /** */
    private V[] values;

    /** */
    private int size;

    /** */
    public GridClientPutAllMap() {
        this(DEFAULT_CAPACITY);
    }

    /** */
    @SuppressWarnings("unchecked")
    public GridClientPutAllMap(int capacity) {
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
        return keySet.contains(key);
    }

    /** {@inheritDoc} */
    @Override public boolean containsValue(Object val) {
        for (int i = 0; i < size; ++i) {
            if (Objects.equals(values[i], val))
                return true;
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public V get(Object key) {
        for (int i = 0; i < size; ++i) {
            if (Objects.equals(keys[i], key))
                return values[i];
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override public V put(K key, V val) {
        if (keySet.contains(key))
            throw new IllegalArgumentException("Unique keys are allowed only!");

        ensureCapacity();

        keys[size] = key;
        values[size] = val;

        keySet.add(key);

        size++;

        return null;
    }

    /** {@inheritDoc} */
    @Override public V remove(Object key) {
        if (!keySet.contains(key))
            return null;

        for (int i = 0; i < size; ++i) {
            if (Objects.equals(keys[i], key)) {
                V oldVal = values[i];

                for (int j = i; j < size - 1; ++j) {
                    keys[j] = keys[j + 1];
                    values[j] = values[j + 1];
                }

                keys[size - 1] = null;
                values[size - 1] = null;

                keySet.remove(key);

                size--;

                return oldVal;
            }
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override public void putAll(Map<? extends K, ? extends V> m) {
        for (Entry<? extends K, ? extends V> entry : m.entrySet())
            put(entry.getKey(), entry.getValue());
    }

    /** {@inheritDoc} */
    @Override public void clear() {
        Arrays.fill(keys, 0, size, null);
        Arrays.fill(values, 0, size, null);

        size = 0;

        keySet.clear();
    }

    /** {@inheritDoc} */
    @Override public @NotNull Set<K> keySet() {
        return Collections.unmodifiableSet(keySet);
    }

    /** {@inheritDoc} */
    @Override public @NotNull Collection<V> values() {
        return List.of(values);
    }

    /** {@inheritDoc} */
    @Override public @NotNull Set<Entry<K, V>> entrySet() {
        return new AbstractSet<Entry<K, V>>() {
            @Override public @NotNull Iterator<Entry<K, V>> iterator() {
                return new Iterator<Entry<K, V>>() {
                    private int curIdx = 0;

                    @Override public boolean hasNext() {
                        return curIdx < size;
                    }

                    @Override public Entry<K, V> next() {
                        if (!hasNext())
                            throw new NoSuchElementException();

                        K key = keys[curIdx];
                        V val = values[curIdx];

                        curIdx++;

                        return new AbstractMap.SimpleImmutableEntry<>(key, val);
                    }

                    @Override public void remove() {
                        throw new UnsupportedOperationException("remove() is not supported in this EntrySet iterator.");
                    }
                };
            }

            @Override public int size() {
                return size;
            }
        };
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
        return S.toString(GridClientPutAllMap.class, this);
    }
}
