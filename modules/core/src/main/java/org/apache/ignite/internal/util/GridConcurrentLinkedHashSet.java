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

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentLinkedHashMap;

import static org.jsr166.ConcurrentLinkedHashMap.QueuePolicy.SINGLE_Q;

/**
 * Concurrent linked set implementation.
 */
public class GridConcurrentLinkedHashSet<E> extends GridSetWrapper<E> {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Creates a new, empty set with a default initial capacity,
     * load factor, and concurrencyLevel.
     */
    public GridConcurrentLinkedHashSet() {
        super(new ConcurrentLinkedHashMap<E, E>());
    }

    /**
     * Creates a new, empty set with the specified initial
     * capacity, and with default load factor and concurrencyLevel.
     *
     * @param initCap The initial capacity. The implementation
     *      performs internal sizing to accommodate this many elements.
     * @throws IllegalArgumentException if the initial capacity of
     *      elements is negative.
     */
    public GridConcurrentLinkedHashSet(int initCap) {
        super(new ConcurrentLinkedHashMap<E, E>(initCap));
    }

    /**
     * Creates a new, empty set with the specified initial
     * capacity, load factor, and concurrency level.
     *
     * @param initCap The initial capacity. The implementation
     *      performs internal sizing to accommodate this many elements.
     * @param loadFactor The load factor threshold, used to control resizing.
     *      Resizing may be performed when the average number of elements per
     *      bin exceeds this threshold.
     * @param conLevel The estimated number of concurrently
     *      updating threads. The implementation performs internal sizing
     *      to try to accommodate this many threads.
     * @throws IllegalArgumentException if the initial capacity is
     *      negative or the load factor or concurrency level are
     *      non-positive.
     */
    public GridConcurrentLinkedHashSet(int initCap, float loadFactor, int conLevel) {
        super(new ConcurrentLinkedHashMap<E, E>(initCap, loadFactor, conLevel));
    }

    /**
     * Creates a new set with the same elements as the given collection. The
     * collection is created with a capacity of twice the number of mappings in
     * the given map or 11 (whichever is greater), and a default load factor
     * and concurrencyLevel.
     *
     * @param c Collection to add.
     */
    public GridConcurrentLinkedHashSet(Collection<E> c) {
        super(new ConcurrentLinkedHashMap<E, E>(c.size()));

        addAll(c);
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
        ConcurrentMap<E, E> m = (ConcurrentMap<E, E>)map;

        return m.putIfAbsent(e, e);
    }

    /**
     * @return Descending iterator.
     */
    public Iterator<E> descendingIterator() {
        return ((ConcurrentLinkedHashMap<E, E>)map).descendingKeySet().iterator();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        // TODO GG-4788
        return ((ConcurrentLinkedHashMap)map()).policy() != SINGLE_Q ?
            S.toString(GridConcurrentLinkedHashSet.class, this) :
            S.toString(GridConcurrentLinkedHashSet.class, this, "elements", map().keySet());
    }
}