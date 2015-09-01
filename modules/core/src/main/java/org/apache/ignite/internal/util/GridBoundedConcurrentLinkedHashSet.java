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

import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentLinkedHashMap;

import static org.jsr166.ConcurrentLinkedHashMap.DFLT_CONCUR_LVL;
import static org.jsr166.ConcurrentLinkedHashMap.DFLT_INIT_CAP;
import static org.jsr166.ConcurrentLinkedHashMap.DFLT_LOAD_FACTOR;
import static org.jsr166.ConcurrentLinkedHashMap.QueuePolicy;
import static org.jsr166.ConcurrentLinkedHashMap.QueuePolicy.SINGLE_Q;

/**
 * Concurrent set with an upper bound. Once set reaches its maximum capacity,
 * the eldest elements will be removed based on insertion or access order.
 */
public class GridBoundedConcurrentLinkedHashSet<E> extends GridSetWrapper<E> {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Creates a new, empty set with specified order and default initial capacity (16),
     * load factor (0.75) and concurrencyLevel (16).
     *
     * @param max Upper bound of this set.
     */
    public GridBoundedConcurrentLinkedHashSet(int max) {
        this(max, DFLT_INIT_CAP, DFLT_LOAD_FACTOR, DFLT_CONCUR_LVL);
    }

    /**
     * Creates a new, empty set with the specified initial capacity,
     * and with default load factor (0.75) and concurrencyLevel (16).
     *
     * @param max Upper bound of this set.
     * @param initCap the initial capacity. The implementation
     *      performs internal sizing to accommodate this many elements.
     * @throws IllegalArgumentException if the initial capacity of
     *      elements is negative.
     */
    public GridBoundedConcurrentLinkedHashSet(int max, int initCap) {
        this(max, initCap, DFLT_LOAD_FACTOR, DFLT_CONCUR_LVL);
    }

    /**
     * Creates a new, empty set with the specified initial capacity
     * and load factor and with the default concurrencyLevel (16).
     *
     * @param max Upper bound of this set.
     * @param initCap The implementation performs internal
     *      sizing to accommodate this many elements.
     * @param loadFactor  the load factor threshold, used to control resizing.
     *      Resizing may be performed when the average number of elements per
     *      bin exceeds this threshold.
     * @throws IllegalArgumentException if the initial capacity of
     *      elements is negative or the load factor is nonpositive.
     */
    public GridBoundedConcurrentLinkedHashSet(int max, int initCap, float loadFactor) {
        this(max, initCap, loadFactor, DFLT_CONCUR_LVL);
    }

    /**
     * Creates a new, empty set with the specified initial
     * capacity, load factor and concurrency level.
     *
     * @param max Upper bound of this set.
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
    public GridBoundedConcurrentLinkedHashSet(int max, int initCap, float loadFactor, int concurLvl) {
        this(max, initCap, loadFactor, concurLvl, SINGLE_Q);
    }

    /**
     * Creates a new, empty set with the specified initial
     * capacity, load factor and concurrency level.
     *
     *
     * @param max Upper bound of this set.
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
    public GridBoundedConcurrentLinkedHashSet(int max, int initCap, float loadFactor, int concurLvl, QueuePolicy qPlc) {
        super(new GridBoundedConcurrentLinkedHashMap<E, Object>(max, initCap, loadFactor, concurLvl, qPlc));
    }

    /**
     * Note that unlike regular add operation on a set, this method will only
     * add the passed in element if it's not already present in set.
     *
     * @param e Element to add.
     * @return {@code True} if element was added.
     */
    @Override public boolean add(E e) {
        ConcurrentMap<E, Object> m = (ConcurrentMap<E, Object>)map;

        return m.putIfAbsent(e, e) == null;
    }

    /**
     * Note that unlike regular add operation on a set, this method will only
     * add the passed in element if it's not already present in set.
     *
     * @param e Element to add.
     * @return Value previously present in set or {@code null} if set didn't have this value.
     */
    @Nullable public E addx(E e) {
        ConcurrentMap<E, Object> map0 = map();

        return (E)map0.putIfAbsent(e, e);
    }

    /**
     * @return Size of the set
     */
    public int sizex() {
        return ((ConcurrentLinkedHashMap<E, Object>)map()).sizex();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        // TODO GG-4788
        return ((ConcurrentLinkedHashMap<E, Object>)map()).policy() != SINGLE_Q ?
            S.toString(GridBoundedConcurrentLinkedHashSet.class, this) :
            S.toString(GridBoundedConcurrentLinkedHashSet.class, this, "elements", map().keySet());
    }
}