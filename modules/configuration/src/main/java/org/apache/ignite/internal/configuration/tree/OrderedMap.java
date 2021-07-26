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

package org.apache.ignite.internal.configuration.tree;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Simplified map class that preserves keys order.
 *
 * @param <V> Type of the value.
 */
class OrderedMap<V> {
    /** Underlying hash map. */
    private final Map<String, V> map;

    /** Ordered keys. */
    private final List<String> orderedKeys;

    /** Default constructor. */
    OrderedMap() {
        map = new HashMap<>();
        orderedKeys = new ArrayList<>();
    }

    /**
     * Copy constructor.
     *
     * @param other Source of keys/values to copy from.
     */
    OrderedMap(OrderedMap<V> other) {
        map = new HashMap<>(other.map);
        orderedKeys = new ArrayList<>(other.orderedKeys);
    }

    /**
     * Same as {@link Map#containsKey(Object)}.
     *
     * @param key Key to check.
     * @return {@code true} if map contains the key.
     */
    public boolean containsKey(String key) {
        return map.containsKey(key);
    }

    /**
     * Same as {@link Map#get(Object)}.
     *
     * @param key Key to search.
     * @return Value associated with the key or {@code null} is it's not found.
     */
    public V get(String key) {
        return map.get(key);
    }

    /**
     * Returns value located at the specified index.
     *
     * @param index Value index.
     * @return Requested value.
     * @throws IndexOutOfBoundsException If index is out of bounds.
     */
    public V get(int index) {
        return map.get(orderedKeys.get(index));
    }

    /**
     * Same as {@link Map#remove(Object)}.
     *
     * @param key Key to remove.
     * @return Previous value associated with the key or {@code null} if the map had no such key.
     */
    public V remove(String key) {
        V res = map.remove(key);

        if (res != null)
            orderedKeys.remove(key);

        return res;
    }

    /**
     * Inserts a value into the map under the specified key. If the key was not present in the map, it will be ordered last.
     * ordering index will be used.
     *
     * @param key Key to put.
     * @param value Value associated with the key.
     */
    public void put(String key, V value) {
        if (map.put(key, value) == null)
            orderedKeys.add(key);
    }

    /**
     * Inserts a value into the map under the specified key. The key will be positioned at the given index, shifting any
     * existing values at that position to the right. Key must not be present in the map when the method is called.
     *
     * @param idx Ordering index for the key. Can't be negative. Every value bigger or equal than {@code size()} is
     *            treated like {@code size()}.
     * @param key Key to put.
     * @param value Value associated with the key.
     */
    public void putByIndex(int idx, String key, V value) {
        assert !map.containsKey(key) : key + " " + map;

        if (idx >= orderedKeys.size())
            orderedKeys.add(key);
        else
            orderedKeys.add(idx, key);

        map.put(key, value);
    }

    /**
     * Put value to the map at the position after the {@code precedingKey}. Key must not be present in the map when the
     * method is called.
     *
     * @param precedingKey Previous key for the new key. Last key will be used if this one is missing from the map.
     * @param key Key to put.
     * @param value Value associated with the key.
     */
    public void putAfter(String precedingKey, String key, V value) {
        assert map.containsKey(precedingKey) : precedingKey + " " + map;
        assert !map.containsKey(key) : key + " " + map;

        int idx = orderedKeys.indexOf(precedingKey);

        putByIndex(idx + 1, key, value);
    }

    /**
     * Re-associates the value under the {@code oldKey} to the {@code newKey}. Does nothing if the {@code oldKey}
     * is not present in the map.
     * was missing from the map.
     *
     * @param oldKey Old key.
     * @param newKey New key.
     *
     * @throws IllegalArgumentException If both {@code oldKey} and {@code newKey} already exist in the map.
     */
    public void rename(String oldKey, String newKey) {
        if (!map.containsKey(oldKey))
            return;

        if (map.containsKey(newKey))
            throw new IllegalArgumentException();

        int idx = orderedKeys.indexOf(oldKey);

        orderedKeys.set(idx, newKey);

        V value = map.remove(oldKey);

        map.put(newKey, value);
    }

    /**
     * @return List of keys.
     */
    public List<String> keys() {
        return new ArrayList<>(orderedKeys);
    }

    /**
     * Reorders keys in the map.
     *
     * @param orderedKeys List of keys in new order. Must have the same set of keys in it.
     */
    public void reorderKeys(List<String> orderedKeys) {
        assert map.keySet().equals(Set.copyOf(orderedKeys)) : map.keySet() + " : " + orderedKeys;

        this.orderedKeys.clear();

        this.orderedKeys.addAll(orderedKeys);
    }

    /**
     * @return Size of the map.
     */
    public int size() {
        return map.size();
    }

    /**
     * Clears the map.
     */
    public void clear() {
        map.clear();

        orderedKeys.clear();
    }
}
