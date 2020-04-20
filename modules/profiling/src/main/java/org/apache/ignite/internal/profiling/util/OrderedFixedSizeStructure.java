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

package org.apache.ignite.internal.profiling.util;

import java.util.Collection;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * Data structure for keeping the top N elements in DESC sort order.
 *
 * @param <K> Key.
 * @param <V> Value.
 */
public class OrderedFixedSizeStructure<K extends Comparable<? super K>, V> {
    /** Default capacity. */
    private static final int DEFAULT_SIZE = 30;

    /** Capacity. */
    private final int capacity;

    /** Map to store elements. */
    private final NavigableMap<K, V> map = new TreeMap<>();

    /** */
    public OrderedFixedSizeStructure() {
        this(DEFAULT_SIZE);
    }

    /**
     * @param capacity The capacity.
     */
    public OrderedFixedSizeStructure(int capacity) {
        this.capacity = capacity;
    }

    /**
     * @param key Key.
     * @param value Value.
     */
    public void put(K key, V value) {
        if (map.size() < capacity) {
            map.put(key, value);

            return;
        }

        if (map.firstKey().compareTo(key) < 0) {
            map.pollFirstEntry();

            map.put(key, value);
        }
    }

    /** @return Values. */
    public Collection<V> values() {
        return map.values();
    }
}
