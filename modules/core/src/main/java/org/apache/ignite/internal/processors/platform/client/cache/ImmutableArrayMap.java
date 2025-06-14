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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.NotNull;

/**
 * Lightweight, array-backed read-only implementation of the {@link Map} interface.
 * <p>
 * This class provides a fixed mapping of keys to values, based on the arrays provided
 * during construction. It does not support most modification operations such as
 * {@code put}, {@code remove}, or {@code clear}. Only {@code size}, {@code isEmpty},
 * {@code keySet}, and {@code values} are supported.
 * <p>
 * Intended for high-performance, read-only use cases where data is managed externally.
 *
 * @param <K> Type of keys maintained by this map.
 * @param <V> Type of mapped values.
 */
public class ImmutableArrayMap<K, V> implements Map<K, V>, Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Backing array for keys. */
    private final K[] keys;

    /** Backing array for values. */
    private final V[] values;

    /**
     * Constructs a new {@code ImmutableArrayMap} with the given keys and values.
     * The arrays must be of the same length, and represent a one-to-one mapping.
     *
     * @param keys   Array of keys.
     * @param values Array of values.
     * @throws AssertionError if the array lengths differ.
     */
    public ImmutableArrayMap(K[] keys, V[] values) {
        assert keys.length == values.length : "Arrays should be equal in size!";

        this.keys = keys;
        this.values = values;
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return keys.length;
    }

    /** {@inheritDoc} */
    @Override public boolean isEmpty() {
        return keys.length == 0;
    }

    /**
     * Unsupported operation.
     *
     * @throws UnsupportedOperationException always.
     */
    @Override public boolean containsKey(Object key) {
        throw new UnsupportedOperationException("'containsKey' operation is not supported by ImmutableArrayMap.");
    }

    /**
     * Unsupported operation.
     *
     * @throws UnsupportedOperationException always.
     */
    @Override public boolean containsValue(Object val) {
        throw new UnsupportedOperationException("'containsValue' operation is not supported by ImmutableArrayMap.");
    }

    /**
     * Unsupported operation.
     *
     * @throws UnsupportedOperationException always.
     */
    @Override public V get(Object key) {
        throw new UnsupportedOperationException("'get' operation is not supported by ImmutableArrayMap.");
    }

    /**
     * Unsupported operation.
     *
     * @throws UnsupportedOperationException always.
     */
    @Override public V put(K key, V val) {
        throw new UnsupportedOperationException("'put' operation is not supported by ImmutableArrayMap.");
    }

    /**
     * Unsupported operation.
     *
     * @throws UnsupportedOperationException always.
     */
    @Override public V remove(Object key) {
        throw new UnsupportedOperationException("'remove' operation is not supported by ImmutableArrayMap.");
    }

    /**
     * Unsupported operation.
     *
     * @throws UnsupportedOperationException always.
     */
    @Override public void putAll(@NotNull Map<? extends K, ? extends V> m) {
        throw new UnsupportedOperationException("'remove' operation is not supported by ImmutableArrayMap.");
    }

    /**
     * Unsupported operation.
     *
     * @throws UnsupportedOperationException always.
     */
    @Override public void clear() {
        throw new UnsupportedOperationException("'clear' operation is not supported by ImmutableArrayMap.");
    }

    /**
     * Returns an immutable set view of the keys in this map.
     *
     * @return An immutable {@code Set} of keys.
     */
    @Override public @NotNull Set<K> keySet() {
        return Set.of(keys);
    }

    /**
     * Returns an immutable collection view of the values in this map.
     *
     * @return An immutable {@code Collection} of values.
     */
    @Override public @NotNull Collection<V> values() {
        return List.of(values);
    }

    /**
     * Unsupported operation.
     *
     * @throws UnsupportedOperationException always.
     */
    @Override public @NotNull Set<Entry<K, V>> entrySet() {
        throw new UnsupportedOperationException("'remove' operation is not supported by ImmutableArrayMap.");
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ImmutableArrayMap.class, this);
    }
}
