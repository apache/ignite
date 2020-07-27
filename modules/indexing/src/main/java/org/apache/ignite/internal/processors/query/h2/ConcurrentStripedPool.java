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

package org.apache.ignite.internal.processors.query.h2;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.jetbrains.annotations.NotNull;

/**
 * Concurrent pool of object based on ConcurrentLinkedDeque.
 */
public class ConcurrentStripedPool<E> implements Iterable<E> {
    /** Stripe pools. */
    private final ConcurrentLinkedQueue<E>[] stripePools;

    /** Stripes count. */
    private final int stripes;

    /** Stripe pools size (calculates fast, optimistic and approximate). */
    private AtomicInteger[] stripeSize;

    /** Max pool size. */
    private int maxPoolSize;

    /**
     * Constructor.
     *
     * @param stripes Count of stripes.
     * @param maxPoolSize Max pool size.
     */
    @SuppressWarnings("unchecked")
    public ConcurrentStripedPool(int stripes, int maxPoolSize) {
        this.stripes = stripes;
        this.maxPoolSize = maxPoolSize;

        stripePools = new ConcurrentLinkedQueue[stripes];
        stripeSize = new AtomicInteger[stripes];

        for (int i = 0; i < stripes; ++i) {
            stripePools[i] = new ConcurrentLinkedQueue<>();
            stripeSize[i] = new AtomicInteger();
        }
    }

    /**
     * Pushes an element onto the pool.
     *
     * @param e the element to push
     * @throws NullPointerException if the specified element is null and this deque does not permit null elements
     * @return {@code true} if the element is returned to the pool, {@code false} if the is no space at the pool.
     */
    public boolean recycle(E e) {
        int idx = (int)(Thread.currentThread().getId() % stripes);

        if (stripeSize[idx].get() > maxPoolSize)
            return false;

        stripePools[idx].add(e);

        stripeSize[idx].incrementAndGet();

        return true;
    }

    /**
     * Retrieves element from pool, or returns {@code null} if the pool is empty.
     *
     * @return the  element of the pool, or {@code null} if the pool is empty.
     */
    public E borrow() {
        int idx = (int)(Thread.currentThread().getId() % stripes);

        E r = stripePools[idx].poll();

        if (r != null)
            stripeSize[idx].decrementAndGet();

        return r;
    }

    /**
     * Performs the given action for each element of the pool until all elements have been processed or the action
     * throws an exception. Exceptions thrown by the action are relayed to the caller.
     *
     * @param action The action to be performed for each element
     * @throws NullPointerException if the specified action is null
     */
    @Override public void forEach(Consumer<? super E> action) {
        Objects.requireNonNull(action);

        for (int i = 0; i < stripes; ++i)
            stripePools[i].forEach(action);
    }

    /**
     * Removes all of the elements from the pool..
     */
    public void clear() {
        for (int i = 0; i < stripes; ++i)
            stripePools[i].clear();
    }

    /** {@inheritDoc} */
    @Override public @NotNull Iterator<E> iterator() {
        return new Iterator<E>() {
            private int idx;

            private Iterator<E> it = stripePools[idx].iterator();

            @Override public boolean hasNext() {
                while (true) {
                    if (it.hasNext())
                        return true;

                    if (++idx >= stripes)
                        break;

                    it = stripePools[idx].iterator();
                }

                return false;
            }

            @Override public E next() {
                if (hasNext())
                    return it.next();

                throw new NoSuchElementException();
            }
        };
    }

    /**
     * Returns a sequential {@code Stream} of the pool.
     *
     * @return a sequential {@code Stream} over the elements iof the pool.
     */
    public Stream<E> stream() {
        return StreamSupport.stream(spliterator(), false);
    }

    /**
     * @param size New max pool size.
     */
    public void resize(int size) {
        maxPoolSize = size;
    }
}
