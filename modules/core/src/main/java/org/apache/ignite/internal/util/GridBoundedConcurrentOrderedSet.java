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
import java.util.Comparator;
import java.util.NoSuchElementException;
import java.util.SortedSet;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Concurrent ordered set that automatically manages its maximum size.
 * Once it exceeds its maximum, it will start removing smallest elements
 * until the maximum is reached again.
 * <p>
 * Note that due to concurrent nature of this set, it may grow slightly
 * larger than its maximum allowed size, but in this case it will quickly
 * readjust back to allowed size.
 * <p>
 * Note that {@link #remove(Object)} method is not supported for this kind of set.
 */
public class GridBoundedConcurrentOrderedSet<E> extends GridConcurrentSkipListSet<E> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Element count. */
    private final AtomicInteger cnt = new AtomicInteger(0);

    /** Maximum size. */
    private int max;

    /**
     * Constructs a new, empty set that orders its elements according to
     * their {@linkplain Comparable natural ordering}.
     *
     * @param max Upper bound of this set.
     */
    public GridBoundedConcurrentOrderedSet(int max) {
        assert max > 0;

        this.max = max;
    }

    /**
     * Constructs a new, empty set that orders its elements according to
     * the specified comparator.
     *
     * @param max Upper bound of this set.
     * @param comp the comparator that will be used to order this set.
     *      If <tt>null</tt>, the {@linkplain Comparable natural
     *      ordering} of the elements will be used.
     */
    public GridBoundedConcurrentOrderedSet(int max, Comparator<? super E> comp) {
        super(comp);

        assert max > 0;

        this.max = max;
    }

    /**
     * Constructs a new set containing the elements in the specified
     * collection, that orders its elements according to their
     * {@linkplain Comparable natural ordering}.
     *
     * @param max Upper bound of this set.
     * @param c The elements that will comprise the new set
     * @throws ClassCastException if the elements in <tt>c</tt> are
     *      not {@link Comparable}, or are not mutually comparable
     * @throws NullPointerException if the specified collection or any
     *      of its elements are {@code null}.
     */
    public GridBoundedConcurrentOrderedSet(int max, Collection<? extends E> c) {
        super(c);

        assert max > 0;

        this.max = max;
    }

    /**
     * Constructs a new set containing the same elements and using the
     * same ordering as the specified sorted set.
     *
     * @param max Upper bound of this set.
     * @param s sorted set whose elements will comprise the new set
     * @throws NullPointerException if the specified sorted set or any
     *      of its elements are {@code null}.
     */
    public GridBoundedConcurrentOrderedSet(int max, SortedSet<E> s) {
        super(s);

        assert max > 0;

        this.max = max;
    }

    /** {@inheritDoc} */
    @Override public boolean add(E e) {
        GridArgumentCheck.notNull(e, "e");

        if (super.add(e)) {
            cnt.incrementAndGet();

            int c;

            while ((c = cnt.get()) > max) {
                // Decrement count.
                if (cnt.compareAndSet(c, c - 1)) {
                    try {
                        while (!super.remove(first())) {
                            // No-op.
                        }
                    }
                    catch (NoSuchElementException e1) {
                        e1.printStackTrace(); // Should never happen.

                        assert false : "Internal error in grid bounded ordered set.";
                    }
                }
            }

            return true;
        }

        return false;
    }

    /**
     * Approximate size at this point of time. Note, that unlike {@code size}
     * methods on other {@code concurrent} collections, this method executes
     * in constant time without traversal of the elements.
     *
     * @return Approximate set size at this point of time.
     */
    @Override public int size() {
        int size = cnt.get();

        return size < 0 ? 0 : size;
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"unchecked"})
    @Override public GridBoundedConcurrentOrderedSet<E> clone() {
        GridBoundedConcurrentOrderedSet<E> s = (GridBoundedConcurrentOrderedSet<E>)super.clone();

        s.max = max;

        return s;
    }

    /**
     * This method is not supported and always throws {@link UnsupportedOperationException}.
     *
     * @param o {@inheritDoc}
     * @return {@inheritDoc}
     */
    @Override public boolean remove(Object o) {
        if (super.remove(o)) {
            cnt.decrementAndGet();

            return true;
        }

        return false;
    }
}