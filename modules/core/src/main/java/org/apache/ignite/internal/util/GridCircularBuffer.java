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

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.util.lang.IgniteInClosureX;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteInClosure;
import org.jetbrains.annotations.Nullable;

/**
 * This class implements a circular buffer for efficient data exchange.
 */
public class GridCircularBuffer<T> {
    /** */
    private final long sizeMask;

    /** */
    private final Item<T>[] arr;

    /** */
    private final AtomicLong idxGen = new AtomicLong();

    /**
     * @param size Size.
     */
    public GridCircularBuffer(int size) {
        A.ensure(size > 0, "Size should be greater than 0: " + size);
        A.ensure((size & (size - 1)) == 0, "Size should be power of two: " + size);

        sizeMask = size - 1;

        arr = (Item<T>[])new Item[size];

        // Fill the array.
        for (int i = 0; i < arr.length; i++)
            arr[i] = new Item<>();
    }

    /**
     * @return Items currently in buffer.
     */
    public Collection<T> items() {
        Collection<T> res = new ArrayList<>(arr.length);

        for (Item<T> t : arr) {
            T item = t.item();

            if (item == null)
                break;

            res.add(item);
        }

        return res;
    }

    /**
     * Executes given closure for every item in circular buffer.
     *
     * @param c Closure to execute.
     */
    public void forEach(IgniteInClosure<T> c) {
        for (Item<T> t : arr) {
            T item = t.item();

            if (item == null)
                break;

            c.apply(item);
        }
    }

    /**
     * @param idx Index.
     * @return Item data and index.
     */
    public T2<T, Long> get(long idx) {
        int idx0 = (int)(idx & sizeMask);

        return arr[idx0].get();
    }

    /**
     * @param t Item to add.
     * @return Evicted object or {@code null} if nothing evicted.
     * @throws InterruptedException If interrupted.
     */
    @Nullable public T add(T t) throws InterruptedException {
        long idx = idxGen.getAndIncrement();

        int idx0 = (int)(idx & sizeMask);

        return arr[idx0].update(idx, t, arr.length);
    }

    /**
     * @param t Item to add.
     * @param c Closure to by applied on evicted object before eviction.
     * @return Evicted object or {@code null} if nothing evicted.
     * @throws InterruptedException If interrupted.
     * @throws IgniteCheckedException If closure throws exception.
     */
    @Nullable public T add(T t, @Nullable IgniteInClosureX<T> c) throws InterruptedException, IgniteCheckedException {
        long idx = idxGen.getAndIncrement();

        int idx0 = (int)(idx & sizeMask);

        return arr[idx0].update(idx, t, arr.length, c);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCircularBuffer.class, this);
    }

    /**
     *
     */
    private static class Item<V> {
        /** */
        private long idx;

        /** */
        @GridToStringInclude
        private V item;

        /**
         *
         */
        Item() {
            // No-op.
        }

        /**
         * @return Item.
         */
        @SuppressWarnings("MethodNamesDifferingOnlyByCase")
        synchronized V item() {
            return item;
        }

        /**
         * @param newIdx Index.
         * @param newItem Item.
         * @param maxIdxDiff Max difference in indexes.
         * @return Evicted value on success or {@code null} if update failed.
         * @throws InterruptedException If interrupted.
         */
        @Nullable synchronized V update(long newIdx, V newItem, long maxIdxDiff) throws InterruptedException {
            assert newIdx >= 0;

            // Thread should wait and allow previous update to finish.
            while (newIdx - idx > maxIdxDiff)
                wait();

            V old = item;

            idx = newIdx;
            item = newItem;

            notifyAll();

            return old;
        }

        /**
         * @param newIdx Index.
         * @param newItem Item.
         * @param maxIdxDiff Max difference in indexes.
         * @param c Closure applied on evicted object before eviction.
         * @return Evicted value on success or {@code null} if update failed.
         * @throws InterruptedException If interrupted.
         * @throws IgniteCheckedException If closure throws exception.
         */
        @Nullable synchronized V update(long newIdx, V newItem, long maxIdxDiff, @Nullable IgniteInClosureX<V> c)
            throws InterruptedException, IgniteCheckedException {
            assert newIdx >= 0;

            // Thread should wait and allow previous update to finish.
            while (newIdx - idx > maxIdxDiff)
                wait();

            idx = newIdx; // Index should be updated even if closure fails.

            if (c != null && item != null)
                c.applyx(item);

            V old = item;

            item = newItem;

            notifyAll();

            return old;
        }

        /**
         * @return Item data and index.
         */
        synchronized T2<V, Long> get() {
            return new T2<>(item, idx);
        }

        /** {@inheritDoc} */
        @Override public synchronized String toString() {
            return S.toString(Item.class, this, "hash=" + System.identityHashCode(this));
        }
    }
}