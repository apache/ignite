/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.util;

import org.apache.ignite.internal.util.typedef.internal.S;
import org.jsr166.ConcurrentLinkedHashMap;

import static org.jsr166.ConcurrentLinkedHashMap.QueuePolicy.SINGLE_Q;

/**
 * Concurrent map with an upper bound. Once map reaches its maximum capacity,
 * the eldest elements will be removed based on insertion order.
 *
 * @param <K> Map key.
 * @param <V> Map entry.
 *
 */
public class GridBoundedConcurrentLinkedHashMap<K, V> extends ConcurrentLinkedHashMap<K, V> {
    /**
     * Creates a new, empty map with a default initial capacity (16),
     * load factor (0.75) and concurrencyLevel (16).
     *
     * @param max Upper bound of this map.
     */
    public GridBoundedConcurrentLinkedHashMap(int max) {
        this(max, DFLT_INIT_CAP, DFLT_LOAD_FACTOR, DFLT_CONCUR_LVL);
    }

    /**
     * Creates a new, empty map with the specified initial capacity,
     * and with default load factor (0.75) and concurrencyLevel (16).
     *
     * @param max Upper bound of this map.
     * @param initCap the initial capacity. The implementation
     *      performs internal sizing to accommodate this many elements.
     * @throws IllegalArgumentException if the initial capacity of
     *      elements is negative.
     */
    public GridBoundedConcurrentLinkedHashMap(int max, int initCap) {
        this(max, initCap, DFLT_LOAD_FACTOR, DFLT_CONCUR_LVL);
    }

    /**
     * Creates a new, empty map with the specified initial capacity
     * and load factor and with the default concurrencyLevel (16).
     *
     * @param max Upper bound of this map.
     * @param initCap The implementation performs internal
     *      sizing to accommodate this many elements.
     * @param loadFactor  the load factor threshold, used to control resizing.
     *      Resizing may be performed when the average number of elements per
     *      bin exceeds this threshold.
     * @throws IllegalArgumentException if the initial capacity of
     *      elements is negative or the load factor is nonpositive.
     */
    public GridBoundedConcurrentLinkedHashMap(int max, int initCap, float loadFactor) {
        this(max, initCap, loadFactor, DFLT_CONCUR_LVL);
    }

    /**
     * Creates a new, empty map with the specified initial
     * capacity, load factor and concurrency level.
     *
     * @param max Upper bound of this map.
     * @param initCap the initial capacity. The implementation
     *      performs internal sizing to accommodate this many elements.
     * @param loadFactor  the load factor threshold, used to control resizing.
     *      Resizing may be performed when the average number of elements per
     *      bin exceeds this threshold.
     * @param concurLvl the estimated number of concurrently
     *      updating threads. The implementation performs internal sizing
     *      to try to accommodate this many threads.
     * @throws IllegalArgumentException if the initial capacity is
     *      negative or the load factor or concurLvl are
     *      nonpositive.
     */
    public GridBoundedConcurrentLinkedHashMap(int max, int initCap, float loadFactor, int concurLvl) {
        this(max, initCap, loadFactor, concurLvl, SINGLE_Q);
    }
    /**
     * Creates a new, empty map with the specified initial
     * capacity, load factor and concurrency level.
     *
     * @param max Upper bound of this map.
     * @param initCap the initial capacity. The implementation
     *      performs internal sizing to accommodate this many elements.
     * @param loadFactor  the load factor threshold, used to control resizing.
     *      Resizing may be performed when the average number of elements per
     *      bin exceeds this threshold.
     * @param concurLvl the estimated number of concurrently
     *      updating threads. The implementation performs internal sizing
     *      to try to accommodate this many threads.
     * @param qPlc Queue policy.
     * @throws IllegalArgumentException if the initial capacity is
     *      negative or the load factor or concurLvl are
     *      nonpositive.
     */
    public GridBoundedConcurrentLinkedHashMap(int max, int initCap, float loadFactor, int concurLvl, QueuePolicy qPlc) {
        super(initCap, loadFactor, concurLvl, max, qPlc);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridBoundedConcurrentLinkedHashMap.class, this, "entrySet", keySet());
    }
}
