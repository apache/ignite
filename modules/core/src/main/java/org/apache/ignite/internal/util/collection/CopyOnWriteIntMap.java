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

package org.apache.ignite.internal.util.collection;

import java.util.AbstractCollection;
import java.util.Collection;
import java.util.Iterator;
import java.util.Objects;
import java.util.function.IntFunction;
import org.jetbrains.annotations.Nullable;

/**
 * Thread-safe {@link IntMap} for data that is looked up on hot paths but changes rarely: reads never block or box,
 * every write copies the map. With a noticeable share of writes it is slower than a concurrent map.
 */
public class CopyOnWriteIntMap<V> implements IntMap<V> {
    /** Current map, never modified after publication: writes replace it with a copy. */
    private volatile IntHashMap<V> map = new IntHashMap<>();

    /** Live view over the current map. */
    private final Collection<V> vals = new AbstractCollection<V>() {
        @Override public Iterator<V> iterator() {
            return map.valuesIterator();
        }

        @Override public int size() {
            return map.size();
        }
    };

    /** {@inheritDoc} */
    @Override public V get(int key) {
        return map.get(key);
    }

    /** {@inheritDoc} */
    @Override public boolean containsKey(int key) {
        return map.containsKey(key);
    }

    /** {@inheritDoc} */
    @Override public boolean containsValue(V val) {
        return map.containsValue(val);
    }

    /** {@inheritDoc} */
    @Override public <E extends Throwable> void forEach(EntryConsumer<V, E> act) throws E {
        map.forEach(act);
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return map.size();
    }

    /** {@inheritDoc} */
    @Override public boolean isEmpty() {
        return map.isEmpty();
    }

    /** {@inheritDoc} */
    @Override public int[] keys() {
        return map.keys();
    }

    /**
     * {@inheritDoc}
     * <p>
     * The collection is weakly consistent, like {@link java.util.concurrent.ConcurrentHashMap#values()}: it reflects
     * the current state, its iterator traverses the map as it was when the iterator was created and does not support
     * removal.
     */
    @Override public Collection<V> values() {
        return vals;
    }

    /** {@inheritDoc} */
    @Override public synchronized V put(int key, V val) {
        IntHashMap<V> copy = map.copy();

        V old = copy.put(key, val);

        map = copy;

        return old;
    }

    /** {@inheritDoc} */
    @Override public synchronized V putIfAbsent(int key, V val) {
        IntHashMap<V> map = this.map;

        return map.containsKey(key) ? map.get(key) : put(key, val);
    }

    /** {@inheritDoc} */
    @Override public synchronized @Nullable V computeIfAbsent(int key, IntFunction<? extends V> mappingFunction) {
        // The default implementation is a lookup followed by a put, the lock makes the pair atomic.
        return IntMap.super.computeIfAbsent(key, mappingFunction);
    }

    /** {@inheritDoc} */
    @Override public synchronized V remove(int key) {
        IntHashMap<V> map = this.map;

        return map.containsKey(key) ? removeFromCopy(map, key) : null;
    }

    /**
     * Removes the mapping if the key is mapped to the given value.
     *
     * @param key Key.
     * @param val Expected value.
     * @return {@code True} if the mapping was removed.
     */
    public synchronized boolean remove(int key, V val) {
        IntHashMap<V> map = this.map;

        if (!map.containsKey(key) || !Objects.equals(map.get(key), val))
            return false;

        removeFromCopy(map, key);

        return true;
    }

    /**
     * @param map Current map, must contain the key.
     * @param key Key.
     * @return Removed value.
     */
    private V removeFromCopy(IntHashMap<V> map, int key) {
        IntHashMap<V> copy = map.copy();

        V old = copy.remove(key);

        this.map = copy;

        return old;
    }

    /** {@inheritDoc} */
    @Override public synchronized void clear() {
        map = new IntHashMap<>();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return map.toString();
    }
}
